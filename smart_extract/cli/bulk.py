"""Do a standalone bulk export from an EHR"""

import argparse
import os
import sys

import cumulus_fhir_support as cfs

from smart_extract import bulk_utils, cli_utils, lifecycle


def make_subparser(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("folder", metavar="OUTPUT_DIR")
    cli_utils.add_general(parser)
    parser.add_argument(
        "--group", metavar="GROUP", help="which group to export (default is whole system)"
    )
    parser.add_argument("--since", metavar="TIMESTAMP", help="only get data since this date")
    parser.add_argument(
        "--since-mode",
        choices=cli_utils.SinceMode,
        help="how to interpret --since",
        default=cli_utils.SinceMode.AUTO,
    )
    parser.add_argument("--resume", metavar="URL", help="polling status URL from a previous export")
    parser.add_argument(
        "--cancel", action="store_true", help="cancel an interrupted export, use with --resume"
    )

    cli_utils.add_auth(parser)
    cli_utils.add_type_selection(parser)
    parser.set_defaults(func=export_main)


async def export_main(args: argparse.Namespace) -> None:
    """Exports data from an EHR to a folder."""
    _rest_client, bulk_client = cli_utils.prepare(args)

    if args.cancel:
        await cancel_bulk(bulk_client, args.resume)
        return

    res_types = cli_utils.parse_resource_selection(args.type)
    workdir = args.folder
    metadata = lifecycle.Metadata(workdir)

    # See which resources we can skip
    already_done = set()
    for res_type in res_types:
        if metadata.is_done(res_type):
            print(f"Skipping {res_type}, already done.")
            already_done.add(res_type)
    res_types = set(res_types) - already_done
    if not res_types:
        return

    async with bulk_client:
        filters = cli_utils.parse_type_filters(bulk_client.server_type, res_types, args.type_filter)
        since_mode = cli_utils.calculate_since_mode(args.since_mode, bulk_client.server_type)
        if since_mode == cli_utils.SinceMode.CREATED:
            cli_utils.add_since_filter(filters, args.since, since_mode)
            args.since = None
        # else if SinceMode.UPDATED, we use Bulk Export's _since param, which is better than faking
        # it with _lastUpdated, because _since has extra logic around older resources of patients
        # added after _since.

        exporter = bulk_utils.BulkExporter(
            bulk_client,
            res_types,
            bulk_utils.export_url(args.fhir_url, args.group),
            workdir,
            since=args.since,
            type_filter=filters,
            resume=args.resume,
        )
        os.makedirs(workdir, exist_ok=True)
        await exporter.export()

    for res_type in res_types:
        metadata.mark_done(res_type)


async def cancel_bulk(bulk_client: cfs.FhirClient, resume_url: str | None) -> None:
    if not resume_url:
        sys.exit("You provided --cancel without a --resume URL, but you must provide both.")

    async with bulk_client:
        exporter = bulk_utils.BulkExporter(bulk_client, set(), "", "", resume=resume_url)
        if not await exporter.cancel():
            sys.exit(1)
        print("Export cancelled.")
