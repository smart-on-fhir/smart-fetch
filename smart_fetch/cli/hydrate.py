"""Modify extracted data in various ways"""

import argparse

import rich

from smart_fetch import cli_utils


def make_subparser(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("folder", metavar="OUTPUT_DIR")
    parser.add_argument(
        "--tasks",
        metavar="NAMES",
        help="which hydration tasks to run "
        "(comma separated, defaults to 'all', use 'help' to see list)",
    )
    cli_utils.add_general(parser)
    parser.add_argument(
        "--source-dir",
        metavar="DIR",
        help="folder with your existing source resources (defaults to output folder)",
    )
    parser.add_argument(
        "--mimetypes",
        metavar="MIMES",
        help="mimetypes to inline, comma separated (default is text, HTML, and XHTML)",
    )
    cli_utils.add_compression(parser)

    cli_utils.add_auth(parser)
    parser.set_defaults(func=hydrate_main)


async def hydrate_main(args: argparse.Namespace) -> None:
    """Hydrate some data."""
    cli_utils.validate_output_folder(args.folder)
    cli_utils.validate_input_folder(args.source_dir)

    client, _bulk_client = cli_utils.prepare(args)
    task_classes = cli_utils.parse_hydration_tasks(args.tasks)

    async with client:
        for task_class in task_classes:
            rich.get_console().rule()
            task = task_class(client, compress=args.compress)
            await task.run(args.folder, source_dir=args.source_dir, mimetypes=args.mimetypes)

    cli_utils.print_done()
