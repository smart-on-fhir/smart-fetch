"""Do a standalone bulk export from an EHR"""

import argparse
import sys

from cumulus_etl.loaders.fhir.bulk_export import BulkExporter

from smart_extract import bulk_utils, cli_utils


def make_subparser(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("export_to", metavar="OUTPUT_DIR")
    cli_utils.add_general(parser)
    parser.add_argument(
        "--group", metavar="GROUP", help="which group to export (default is whole system)"
    )
    parser.add_argument(
        "--since", metavar="TIMESTAMP", help="start date for export from the FHIR server"
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
    client = cli_utils.prepare(args)
    resources = cli_utils.parse_resource_selection(args.type)

    async with client:
        exporter = BulkExporter(
            client,
            set(resources),
            bulk_utils.export_url(args.fhir_url, args.group),
            args.export_to,
            since=args.since,
            type_filter=args.type_filter,
            resume=args.resume,
        )

        if args.cancel:
            if not args.resume:
                sys.exit("You provided --cancel without a --resume URL, but you must provide both.")
            if not await exporter.cancel():
                sys.exit(1)
            print("Export cancelled.")
        else:
            cli_utils.confirm_dir_is_empty(args.export_to)
            await exporter.export()
