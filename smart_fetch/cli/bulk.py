"""Do a standalone bulk export from an EHR"""

import argparse
import sys

import cumulus_fhir_support as cfs
import rich

from smart_fetch import bulk_utils, cli_utils, filtering, lifecycle, ndjson


def make_subparser(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("folder", metavar="OUTPUT_DIR")
    cli_utils.add_general(parser)
    parser.add_argument(
        "--group", metavar="GROUP", help="which group to export (default is whole system)"
    )
    parser.add_argument("--since", metavar="TIMESTAMP", help="only get data since this date")
    parser.add_argument(
        "--since-mode",
        choices=list(filtering.SinceMode),
        default=filtering.SinceMode.AUTO,
        help="how to interpret --since (defaults to 'updated' if server supports it)",
    )
    parser.add_argument(
        "--bundle",
        action="store_true",
        help="put all resources into one Bundle file",
    )
    cli_utils.add_compression(parser)
    parser.add_argument("--cancel", action="store_true", help="cancel an interrupted export")

    cli_utils.add_auth(parser)
    cli_utils.add_type_selection(parser)
    parser.set_defaults(func=export_main)


async def export_main(args: argparse.Namespace) -> None:
    """Exports data from an EHR to a folder."""
    rich.get_console().rule()

    cli_utils.validate_output_folder(args.folder)

    _rest_client, bulk_client = cli_utils.prepare(args)
    workdir = args.folder

    if args.cancel:
        await cancel_bulk(bulk_client, workdir)
        return

    res_types = cli_utils.parse_resource_selection(args.type)

    async with bulk_client:
        res_types = cli_utils.limit_to_server_resources(bulk_client, res_types)
        filters = filtering.Filters(
            res_types,
            client=bulk_client,
            type_filters=args.type_filter,
            since=args.since,
            since_mode=args.since_mode,
            use_default_filters=args.default_filters,
        )

        await bulk_utils.perform_bulk(
            bulk_client=bulk_client,
            fhir_url=args.fhir_url,
            filters=filters,
            group=args.group,
            workdir=workdir,
            compress=args.compress,
        )

    if args.bundle:
        ndjson.bundle_folder(workdir, compress=args.compress, exist_ok=True)

    lifecycle.OutputMetadata(workdir).mark_complete()
    cli_utils.print_done()


async def cancel_bulk(bulk_client: cfs.FhirClient, workdir: str) -> None:
    metadata = lifecycle.OutputMetadata(workdir)

    if not metadata.get_bulk_status_url():
        sys.exit(f"You provided --cancel but no in-progress bulk export was found in {workdir}.")

    async with bulk_client:
        exporter = bulk_utils.BulkExporter(bulk_client, set(), "", "", metadata=metadata)
        await exporter.cancel()
