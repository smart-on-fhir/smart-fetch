"""Do a REST crawl from an EHR"""

import argparse

import rich

from smart_fetch import cli_utils, crawl_utils


def make_subparser(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("folder", metavar="OUTPUT_DIR")
    cli_utils.add_general(parser)

    parser.add_argument("--since", metavar="TIMESTAMP", help="only get data since this date")
    parser.add_argument(
        "--since-mode",
        choices=list(cli_utils.SinceMode),
        default=cli_utils.SinceMode.AUTO,
        help="how to interpret --since (defaults to 'updated' if server supports it)",
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
    rich.get_console().rule()

    rest_client, bulk_client = cli_utils.prepare(args)
    res_types = cli_utils.parse_resource_selection(args.type)

    async with rest_client:
        res_types = cli_utils.limit_to_server_resources(rest_client, res_types)
        filters = cli_utils.parse_type_filters(rest_client.server_type, res_types, args.type_filter)
        since_mode = cli_utils.calculate_since_mode(
            args.since, args.since_mode, rest_client.server_type
        )
        workdir = args.folder
        source_dir = args.source_dir or workdir

        await crawl_utils.perform_crawl(
            fhir_url=args.fhir_url,
            filters=filters,
            since=args.since,
            since_mode=since_mode,
            source_dir=source_dir,
            workdir=workdir,
            rest_client=rest_client,
            bulk_client=bulk_client,
            group_nickname=args.group_nickname,
            group=args.group,
            mrn_file=args.mrn_file,
            mrn_system=args.mrn_system,
        )

    cli_utils.print_done()
