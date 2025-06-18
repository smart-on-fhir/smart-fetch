"""Do a standalone bulk export from an EHR"""

import argparse
import sys

import cumulus_fhir_support as cfs
import rich

from smart_extract import bulk_utils, cli_utils


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
    rich.get_console().rule()

    _rest_client, bulk_client = cli_utils.prepare(args)

    if args.cancel:
        await cancel_bulk(bulk_client, args.resume)
        return

    res_types = cli_utils.parse_resource_selection(args.type)
    workdir = args.folder

    async with bulk_client:
        res_types = cli_utils.limit_to_server_resources(bulk_client, res_types)
        filters = cli_utils.parse_type_filters(bulk_client.server_type, res_types, args.type_filter)
        since_mode = cli_utils.calculate_since_mode(args.since_mode, bulk_client.server_type)
        if since_mode == cli_utils.SinceMode.CREATED:
            cli_utils.add_since_filter(filters, args.since, since_mode)
            args.since = None
        # else if SinceMode.UPDATED, we use Bulk Export's _since param, which is better than faking
        # it with _lastUpdated, because _since has extra logic around older resources of patients
        # added to the group after _since.

        await bulk_utils.perform_bulk(
            bulk_client=bulk_client,
            fhir_url=args.fhir_url,
            filters=filters,
            group=args.group,
            workdir=workdir,
            since=args.since,
            resume=args.resume,
        )


async def cancel_bulk(bulk_client: cfs.FhirClient, resume_url: str | None) -> None:
    if not resume_url:
        sys.exit("You provided --cancel without a --resume URL, but you must provide both.")

    async with bulk_client:
        exporter = bulk_utils.BulkExporter(bulk_client, set(), "", "", resume=resume_url)
        await exporter.cancel()
        print("Export cancelled.")
