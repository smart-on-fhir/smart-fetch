"""Do a managed export workflow"""

import argparse
import enum
import glob
import logging
import os
import re
import sys
from functools import partial

import cumulus_fhir_support as cfs
import rich
import rich.progress

from smart_fetch import bulk_utils, cli_utils, crawl_utils, lifecycle, tasks, timing


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
        choices=list(cli_utils.SinceMode),
        default=cli_utils.SinceMode.AUTO,
        help="how to interpret --since (defaults to 'updated' if server supports it)",
    )

    cli_utils.add_auth(parser)
    cli_utils.add_cohort_selection(parser)
    cli_utils.add_type_selection(parser)
    parser.set_defaults(func=export_main)


async def export_main(args: argparse.Namespace) -> None:
    """Exports data from an EHR to a folder."""
    rich.get_console().rule()

    rest_client, bulk_client = cli_utils.prepare(args)
    res_types = cli_utils.parse_resource_selection(args.type)

    async with bulk_client:
        res_types = cli_utils.limit_to_server_resources(bulk_client, res_types)
        export_mode = calculate_export_mode(args.export_mode, bulk_client.server_type)
        client = bulk_client if export_mode == ExportMode.BULK else rest_client

    async with client:
        source_dir = args.folder
        filters = cli_utils.parse_type_filters(client.server_type, res_types, args.type_filter)
        since_mode = cli_utils.calculate_since_mode(args.since, args.since_mode, client.server_type)
        since = calculate_since(
            source_dir, filters=filters, since=args.since, since_mode=since_mode
        )

        subdir = find_workdir(
            source_dir, filters=filters, since=since, since_mode=since_mode, nickname=args.nickname
        )
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
                since=since,
                since_mode=since_mode,
            )
            await finish_resource(rest_client, workdir, set(filters), open_client=True)
        else:
            await crawl_utils.perform_crawl(
                fhir_url=args.fhir_url,
                filters=filters,
                since=since,
                since_mode=since_mode,
                source_dir=source_dir,
                workdir=workdir,
                rest_client=rest_client,
                bulk_client=bulk_client,
                group_nickname=args.group_nickname,
                group=args.group,
                mrn_file=args.mrn_file,
                mrn_system=args.mrn_system,
                finish_callback=partial(finish_resource, rest_client, workdir),
            )

    cli_utils.print_done()


def calculate_export_mode(export_mode: ExportMode, server_type: cfs.ServerType) -> ExportMode:
    if not export_mode or export_mode == ExportMode.AUTO:
        # Epic's bulk export implementation is slow (it hits a live server, rather than a
        # shadow server). We prefer crawling it for now.
        if server_type == cfs.ServerType.EPIC:
            export_mode = ExportMode.CRAWL
            rich.get_console().print(
                "Epic server detected. Defaulting to a 'crawl' instead of a 'bulk' export."
            )
        else:
            export_mode = ExportMode.BULK
    return export_mode


def calculate_since(
    source_dir: str,
    *,
    filters: cli_utils.Filters,
    since: str | None,
    since_mode: cli_utils.SinceMode,
) -> str | None:
    # Early exit if we don't need to calculate anything
    if since != "auto":
        return since

    max_dones = {}  # the newest "done" value for each resource we're interested in
    for folder in list_workdirs(source_dir):
        metadata = lifecycle.OutputMetadata(os.path.join(source_dir, folder))
        matches = metadata.get_matching_timestamps(filters, since_mode)
        for res_type, timestamp in matches.items():
            if res_type not in max_dones or max_dones[res_type] < timestamp:
                max_dones[res_type] = timestamp

    if not max_dones:
        sys.exit(
            "Could not detect a since value to use from previous exports.\n"
            "Try without a --since parameter, or provide a specific timestamp."
        )

    # Now grab the oldest "done" value of all our target resources and use that.
    timestamp = min(max_dones.values()).isoformat()
    logging.warning(f"Using since value of {timestamp}.")
    return timestamp


def find_workdir(
    source_dir: str,
    *,
    filters: cli_utils.Filters,
    since: str | None,
    since_mode: cli_utils.SinceMode,
    nickname: str | None,
) -> str:
    # First, scan the workdirs to find the highest num and see if there's an exact nickname match.
    highest_num = 0
    for folder, (num, name) in list_workdirs(source_dir).items():
        if not highest_num:
            highest_num = num

        if name == nickname:
            logging.warning(f"Re-using existing subfolder '{folder}' with the same nickname.")
            return folder

    # Didn't find exact nickname match. Can we find the same context?
    for folder in list_workdirs(source_dir):
        metadata = lifecycle.OutputMetadata(os.path.join(source_dir, folder))
        if metadata.has_same_context(filters=filters, since=since, since_mode=since_mode):
            logging.warning(f"Re-using existing subfolder '{folder}' with similar arguments.")
            return folder

    # Workdir not found. Let's just make a new one!
    next_num = highest_num + 1
    nickname = nickname or timing.now().strftime("%Y-%m-%d")
    folder = f"{next_num:03}.{nickname}"
    logging.warning(f"Creating new subfolder '{folder}'.")
    return folder


def list_workdirs(source_dir: str) -> dict[str, tuple[int, str]]:
    """
    Returns workdirs in reverse order (i.e. latest first)

    Return format is filename -> (num, nickname) for filenames like {num}.{nickname}
    """
    try:
        with os.scandir(source_dir) as scanner:
            folders = [entry.name for entry in scanner if entry.is_dir()]
    except FileNotFoundError:
        return {}

    matches = {re.fullmatch(r"(\d+)\.(.*)", folder): folder for folder in folders}
    nums = {int(m.group(1)): (m.group(2), val) for m, val in matches.items() if m}

    return {nums[num][1]: (num, nums[num][0]) for num in sorted(nums, reverse=True)}


async def finish_resource(
    client: cfs.FhirClient,
    workdir: str,
    res_types: str | set[str],
    *,
    open_client: bool = False,
    progress: rich.progress.Progress | None = None,
):
    if isinstance(res_types, str):
        res_types = {res_types}

    async def run_hydration_tasks():
        done_types = set()
        loop_types = res_types
        while loop_types:
            done_types |= loop_types
            next_loop_types = set()
            for task_name, task_info in tasks.all_tasks.items():
                input_type, output_type, task_func = task_info
                if input_type not in loop_types:
                    continue

                # We don't provide a source_dir, because we want the hydration to only affect
                # this most recent export subfolder, not other exports.
                await task_func(client, workdir, progress=progress)

                if output_type not in done_types:
                    # We wrote a new type out - we should iterate again to hydrate the new
                    # resources, as needed
                    next_loop_types.add(output_type)

            loop_types = next_loop_types

    if open_client:
        async with client:
            await run_hydration_tasks()
    else:
        await run_hydration_tasks()

    for res_type in res_types:
        make_links(workdir, res_type)


def make_links(workdir: str, res_type: str) -> None:
    work_name = os.path.basename(workdir)
    source_dir = os.path.dirname(workdir)

    current_links = glob.glob(f"{source_dir}/{res_type}.*.ndjson.gz")
    current_targets = {os.readlink(link) for link in current_links}
    current_matches = [re.fullmatch(r".*\.(\d+)\.ndjson\.gz", path) for path in current_links]
    current_nums = [int(m.group(1)) for m in current_matches]
    index = max(current_nums) if current_nums else 0

    for filename in cfs.list_multiline_json_in_dir(workdir, res_type):
        ndjson_name = os.path.basename(filename)
        if not ndjson_name.endswith(".ndjson.gz"):
            logging.warning(f"Found unexpected filename {ndjson_name}, not linking.")
            continue

        target = os.path.join(work_name, ndjson_name)
        if target in current_targets:
            continue

        index += 1
        link_name = f"{res_type}.{index:03}.ndjson.gz"

        os.symlink(target, os.path.join(source_dir, link_name))

    # Some resources have linked resources created by the hydration tasks
    for input_type, output_type, task_func in tasks.all_tasks.values():
        if res_type == input_type and res_type != output_type:
            make_links(workdir, output_type)
