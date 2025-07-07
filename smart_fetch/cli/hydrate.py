"""Modify extracted data in various ways"""

import argparse
import sys

import rich

from smart_fetch import cli_utils, tasks


def make_subparser(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("folder", metavar="OUTPUT_DIR")
    parser.add_argument(
        "--hydration-tasks",
        metavar="TASK",
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

    cli_utils.add_auth(parser)
    parser.set_defaults(func=hydrate_main)


def print_help():
    rich.get_console().print("These hydration tasks are supported:")
    rich.get_console().print("  all")
    for task_name in sorted(tasks.all_tasks.keys()):
        rich.get_console().print(f"  {task_name}")


async def hydrate_main(args: argparse.Namespace) -> None:
    """Hydrate some data."""
    client, _bulk_client = cli_utils.prepare(args)
    cli_tasks = set(args.hydration_tasks.split(",")) if args.hydration_tasks else {"all"}

    if "help" in cli_tasks:
        print_help()
        sys.exit(0)

    for task_name in cli_tasks:
        if task_name != "all" and task_name not in tasks.all_tasks:
            rich.get_console().print(f"Unknown hydration task provided: {task_name}")
            rich.get_console().print()
            print_help()
            sys.exit(2)

    async with client:
        for task_name in tasks.all_tasks:
            if task_name in cli_tasks or "all" in cli_tasks:
                await tasks.all_tasks[task_name][2](
                    client, args.folder, source_dir=args.source_dir, mimetypes=args.mimetypes
                )

    cli_utils.print_done()
