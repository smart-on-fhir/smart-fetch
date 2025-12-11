"""Do a managed export workflow"""

import argparse
import datetime
import enum
import os
import sys
from functools import partial

import cumulus_fhir_support as cfs
import rich
import rich.progress

from smart_fetch import (
    bulk_utils,
    cli_utils,
    crawl_utils,
    filtering,
    hydrate_utils,
    lifecycle,
    merges,
    symlinks,
    timing,
)


class ExportMode(enum.StrEnum):
    AUTO = enum.auto()
    BULK = enum.auto()
    CRAWL = enum.auto()


def make_subparser(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("folder", metavar="OUTPUT_DIR")
    cli_utils.add_general(parser)
    parser.add_argument(
        "--nickname", metavar="NAME", help="nickname for this export, defaults to a hash of args"
    )
    parser.add_argument(
        "--export-mode",
        metavar="MODE",
        choices=list(ExportMode),
        default=ExportMode.AUTO,
        help="how to export data (default is bulk if server supports it well)",
    )
    parser.add_argument(
        "--since",
        metavar="TIMESTAMP",
        help="only get data since this date "
        "(provide 'auto' to detect the date from previous exports)",
    )
    parser.add_argument(
        "--since-mode",
        choices=list(filtering.SinceMode),
        default=filtering.SinceMode.AUTO,
        help="how to interpret --since (defaults to 'updated' if server supports it)",
    )
    parser.add_argument(
        "--hydration-tasks",
        metavar="NAMES",
        help="which hydration tasks to run "
        "(comma separated, defaults to 'all', use 'help' to see list)",
    )
    cli_utils.add_compression(parser)

    cli_utils.add_auth(parser)
    cli_utils.add_cohort_selection(parser)
    cli_utils.add_type_selection(parser)
    parser.set_defaults(func=export_main)


async def export_main(args: argparse.Namespace) -> None:
    """Exports data from an EHR to a folder."""
    rich.get_console().rule()

    cli_utils.validate_output_folder(args.folder)

    rest_client, bulk_client = cli_utils.prepare(args)
    res_types = cli_utils.parse_resource_selection(args.type)
    hydration_tasks = cli_utils.parse_hydration_tasks(args.hydration_tasks)

    async with bulk_client:
        res_types = cli_utils.limit_to_server_resources(bulk_client, res_types)
        export_mode = calculate_export_mode(args.export_mode, bulk_client.server_type)
        client = bulk_client if export_mode == ExportMode.BULK else rest_client

    async with client:
        source_dir = args.folder
        filters = filtering.Filters(
            res_types,
            client=client,
            type_filters=args.type_filter,
            since=args.since,
            since_mode=args.since_mode,
            use_default_filters=args.default_filters,
        )
        filters.detailed_since = calculate_detailed_since(source_dir, filters=filters)

        subdir = find_workdir(source_dir, filters=filters, nickname=args.nickname)
        workdir = os.path.join(source_dir, subdir)

        metadata = lifecycle.ManagedMetadata(source_dir)
        metadata.note_context(fhir_url=args.fhir_url, group=args.group)

        if export_mode == ExportMode.BULK:
            await bulk_utils.perform_bulk(
                fhir_url=args.fhir_url,
                bulk_client=bulk_client,
                filters=filters,
                group=args.group,
                workdir=workdir,
                managed_dir=source_dir,
                compress=args.compress,
            )
            await finish_resource(
                rest_client,
                workdir,
                source_dir,
                filters,
                filters.resources(),
                open_client=True,
                hydration_tasks=hydration_tasks,
                compress=args.compress,
            )
        else:
            await crawl_utils.perform_crawl(
                fhir_url=args.fhir_url,
                filters=filters,
                managed_dir=source_dir,
                source_dir=source_dir,
                workdir=workdir,
                rest_client=rest_client,
                bulk_client=bulk_client,
                group_nickname=args.group_nickname,
                group=args.group,
                id_file=args.id_file,
                id_list=args.id_list,
                id_system=args.id_system,
                compress=args.compress,
                finish_callback=partial(
                    finish_resource,
                    rest_client,
                    workdir,
                    source_dir,
                    filters,
                    hydration_tasks=hydration_tasks,
                    compress=args.compress,
                ),
            )

    lifecycle.OutputMetadata(workdir).mark_complete()
    cli_utils.print_done()


def calculate_export_mode(export_mode: ExportMode, server_type: cfs.ServerType) -> ExportMode:
    if not export_mode or export_mode == ExportMode.AUTO:
        # Epic's bulk export implementation is slow (it hits a live server, rather than a
        # shadow server). We prefer crawling it for now.
        if server_type == cfs.ServerType.EPIC:
            export_mode = ExportMode.CRAWL
            rich.get_console().print(
                "Epic server detected. Defaulting to a 'crawl' instead of a 'bulk' export for "
                "non-Patient resources."
            )
        else:
            export_mode = ExportMode.BULK
    return export_mode


def calculate_detailed_since(
    source_dir: str,
    *,
    filters: filtering.Filters,
) -> dict[str, datetime.datetime | None] | None:
    # Early exit if we don't need to calculate anything
    if filters.since != filtering.SinceMode.AUTO:
        return None

    max_dones = {}  # the newest "done" value for each resource we're interested in
    for folder in lifecycle.list_workdirs(source_dir):
        metadata = lifecycle.OutputMetadata(os.path.join(source_dir, folder))
        matches = metadata.get_matching_timestamps(filters)
        for res_type, timestamp in matches.items():
            if res_type not in max_dones or max_dones[res_type] < timestamp:
                max_dones[res_type] = timestamp

    if not max_dones:
        sys.exit(
            "Could not detect a since value to use from previous exports.\n"
            "Try without a --since parameter, or provide a specific timestamp."
        )

    # Fill in some None for any resources we didn't find a previous date for
    for res_type in filters.resources():
        max_dones.setdefault(res_type, None)

    return max_dones


def find_workdir(
    source_dir: str,
    *,
    filters: filtering.Filters,
    nickname: str | None,
) -> str:
    """Finds a matching workdir, if it exists. Only looks one workdir back."""
    workdirs = lifecycle.list_workdirs(source_dir)

    # Grab the number of the newest workdir
    if workdirs:
        # workdirs are returned in order of most recent to oldest. So first one is the one we want.
        _folder, (prev_num, _name) = next(iter(workdirs.items()))
    else:
        prev_num = 0

    # Check if we have a matching nickname
    if nickname:
        for folder, (num, name) in workdirs.items():
            if name == nickname:
                if num == prev_num:
                    rich.print(f"Re-using existing subfolder '{folder}' with the same nickname.")
                    return folder
                else:
                    sys.exit(
                        f"Existing subfolder '{folder}' with the same nickname is too "
                        f"old to resume. Choose a new nickname."
                    )
    elif workdirs:
        folder = next(iter(workdirs))
        metadata = lifecycle.OutputMetadata(os.path.join(source_dir, folder))
        if metadata.has_same_context(filters=filters):
            rich.print(f"Re-using existing subfolder '{folder}' with similar arguments.")
            return folder

    # Workdir not found. Let's just make a new one!
    next_num = prev_num + 1
    nickname = nickname or timing.now().strftime("%Y-%m-%d")
    folder = f"{next_num:03}.{nickname}"
    rich.print(f"Creating new subfolder '{folder}'.")
    return folder


async def finish_resource(
    client: cfs.FhirClient,
    workdir: str,
    managed_dir: str,
    filters: filtering.Filters,
    res_types: str | set[str],
    *,
    open_client: bool = False,
    hydration_tasks: list[type[hydrate_utils.Task]],
    compress: bool = False,
):
    if isinstance(res_types, str):
        res_types = {res_types}

    run_tasks = run_hydration_tasks(client, workdir, res_types, hydration_tasks, compress=compress)
    if open_client:
        async with client:
            await run_tasks
    else:
        await run_tasks

    # Check for deleted resources if we are doing a full update (non-incremental / non-since).
    # (Regardless of bulk or crawl mode, even when we're in bulk which gives us deleted info for
    # since runs, because we still want the deleted info from the last full export.)
    # Always re-run this, because hydration tasks always re-run, and they could have pulled
    # something new.
    for res_type in res_types:
        if res_type not in filters.since_resources():
            merges.note_deleted_resource(res_type, workdir, managed_dir, filters, compress=compress)

    symlinks.reset_all_links(managed_dir)


async def run_hydration_tasks(
    client: cfs.FhirClient,
    workdir: str,
    res_types: set[str],
    hydration_tasks: list[type[hydrate_utils.Task]],
    compress: bool = False,
) -> None:
    first = True
    done_types = set()
    loop_types = res_types
    while loop_types:
        done_types |= loop_types
        next_loop_types = set()
        for task in hydration_tasks:
            if task.INPUT_RES_TYPE not in loop_types:
                continue

            if first:
                rich.get_console().rule()
                first = False

            # We don't provide a source_dir, because we want the hydration to only affect
            # this most recent export subfolder, not other exports.
            await task(client, compress=compress).run(workdir)

            rich.get_console().rule()

            if task.OUTPUT_RES_TYPE not in done_types:
                # We wrote a new type out - we should iterate again to hydrate the new
                # resources, as needed
                next_loop_types.add(task.OUTPUT_RES_TYPE)

        loop_types = next_loop_types
