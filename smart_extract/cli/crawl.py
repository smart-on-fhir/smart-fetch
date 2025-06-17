"""Do a REST crawl from an EHR"""

import argparse

from smart_extract import cli_utils, crawl_utils


def make_subparser(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("folder", metavar="OUTPUT_DIR")
    cli_utils.add_general(parser)

    parser.add_argument("--since", metavar="TIMESTAMP", help="only get data since this date")
    parser.add_argument(
        "--since-mode",
        choices=cli_utils.SinceMode,
        help="how to interpret --since",
        default=cli_utils.SinceMode.AUTO,
    )

    group = cli_utils.add_cohort_selection(parser)
    group.add_argument(
        "--source-dir",
        metavar="DIR",
        help="folder with existing Patient resources, to use as a cohort "
        "(defaults to output folder)",
    )

    cli_utils.add_auth(parser)
    cli_utils.add_type_selection(parser)
    parser.set_defaults(func=crawl_main)


async def crawl_main(args: argparse.Namespace) -> None:
    """Exports data from an EHR to a folder."""
    rest_client, bulk_client = cli_utils.prepare(args)
    res_types = cli_utils.parse_resource_selection(args.type)

    async with rest_client:
        filters = cli_utils.parse_type_filters(rest_client.server_type, res_types, args.type_filter)
        since_mode = cli_utils.calculate_since_mode(args.since_mode, rest_client.server_type)
        cli_utils.add_since_filter(filters, args.since, since_mode)
        workdir = args.folder
        source_dir = args.source_dir or workdir

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
        )
