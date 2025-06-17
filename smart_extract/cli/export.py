"""Do a managed export workflow"""

import argparse
import enum
import glob
import hashlib
import os
import re
from functools import partial

import cumulus_fhir_support as cfs

from smart_extract import bulk_utils, cli_utils, crawl_utils, fixes, lifecycle


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
        choices=ExportMode,
        default=ExportMode.AUTO,
        help="how to export data (default is bulk if server supports it well)",
    )
    parser.add_argument("--since", metavar="TIMESTAMP", help="only get data since this date")
    parser.add_argument(
        "--since-mode",
        choices=cli_utils.SinceMode,
        help="how to interpret --since",
        default=cli_utils.SinceMode.AUTO,
    )

    cli_utils.add_auth(parser)
    cli_utils.add_cohort_selection(parser)
    cli_utils.add_type_selection(parser)
    parser.set_defaults(func=export_main)


async def export_main(args: argparse.Namespace) -> None:
    """Exports data from an EHR to a folder."""
    rest_client, bulk_client = cli_utils.prepare(args)
    res_types = cli_utils.parse_resource_selection(args.type)

    async with bulk_client:
        export_mode = calculate_export_mode(args.export_mode, bulk_client.server_type)
        client = bulk_client if export_mode == ExportMode.BULK else rest_client

    async with client:
        filters = cli_utils.parse_type_filters(client.server_type, res_types, args.type_filter)
        since_mode = cli_utils.calculate_since_mode(args.since_mode, client.server_type)
        if since_mode == cli_utils.SinceMode.CREATED or export_mode == ExportMode.CRAWL:
            cli_utils.add_since_filter(filters, args.since, since_mode)
            args.since = None
        # else if in bulk/UPDATED mode, we use Bulk Export's _since param, which is better than
        # faking it with _lastUpdated, because _since has extra logic around older resources of
        # patients added to the group after _since.

        source_dir = args.folder
        subdir = str(args.nickname or calculate_workdir(filters, args.since))
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
                since=args.since,
                resume=None,  # FIXME
                finish_callback=partial(finish_resource, rest_client, workdir),
            )
        else:
            await crawl_utils.perform_crawl(
                fhir_url=args.fhir_url,
                filters=filters,
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


def calculate_export_mode(export_mode: ExportMode, server_type: cfs.ServerType) -> ExportMode:
    if export_mode == ExportMode.AUTO:
        # Epic's bulk export implementation is slow (it hits a live server, rather than a
        # shadow server). We prefer crawling it for now.
        return ExportMode.CRAWL if server_type == cfs.ServerType.EPIC else ExportMode.BULK
    return export_mode


def calculate_workdir(filters: cli_utils.Filters, since: str | None) -> str:
    # FHIR URL and group identifiers should be consistent for a given managed dir, so we don't
    # include them here.

    raw = "Filters:\n"
    for key in sorted(filters.keys()):
        sorted_filters = sorted(filters[key])
        if sorted_filters:
            raw += "".join(f"  {key}={res_filter}\n" for res_filter in sorted_filters)
        else:
            raw += f"  {key}=\n"

    raw += "Since:\n"
    if since:
        raw += f"  {since}\n"

    return hashlib.md5(raw.encode("utf8"), usedforsecurity=False).hexdigest()


async def finish_resource(client: cfs.FhirClient, workdir: str, res_type: str):
    source_dir = os.path.dirname(workdir)

    # Now run fixes on the folder
    for fix_type, fix_func in fixes.all_fixes.values():
        if fix_type == res_type:
            await fix_func(client, workdir, source_dir=source_dir)

    make_links(workdir, res_type)


def make_links(workdir: str, res_type: str) -> None:
    work_name = os.path.basename(workdir)
    source_dir = os.path.dirname(workdir)

    current_links = glob.glob(f"{source_dir}/{res_type}.*.ndjson.gz")
    current_matches = [re.fullmatch(r".*\.(\d+)\.ndjson\.gz", path) for path in current_links]
    current_nums = [int(m.group(1)) for m in current_matches] + [-1]
    index = max(current_nums)

    for filename in cfs.list_multiline_json_in_dir(workdir, res_type):
        ndjson_name = os.path.basename(filename)
        if not ndjson_name.endswith(".ndjson.gz"):
            print(f"Found unexpected filename {ndjson_name}, not linking.")
            continue

        index += 1
        link_name = f"{res_type}.{index:03}.ndjson.gz"

        os.symlink(os.path.join(work_name, ndjson_name), os.path.join(source_dir, link_name))
