"""The main command line interface entry point"""

import argparse
import asyncio
import logging
import sys

import rich.logging

from smart_extract.cli import export, bulk, crawl, fix


def define_parser() -> argparse.ArgumentParser:
    """Fills out an argument parser with all the CLI options."""
    parser = argparse.ArgumentParser()

    subparsers = parser.add_subparsers()
    export.make_subparser(subparsers.add_parser("export", help="run a managed export"))
    bulk.make_subparser(subparsers.add_parser("bulk", help="run a bulk export"))
    crawl.make_subparser(
        subparsers.add_parser("crawl", help="run a crawl (REST alternative to bulk export)")
    )
    fix.make_subparser(subparsers.add_parser("fix", help="fix up exported data"))

    return parser


async def main(argv: list[str]) -> None:
    # Use RichHandler for logging because it works better when interacting with other rich
    # components (e.g. I've seen the default logger lose the last warning emitted when progress
    # bars are also active). But also turn off all the complex bits - we just want the message.
    logging.basicConfig(
        format="%(message)s",
        handlers=[rich.logging.RichHandler(show_time=False, show_level=False, show_path=False)],
    )

    parser = define_parser()
    args = parser.parse_args(argv)
    await args.func(args)


def main_cli():
    asyncio.run(main(sys.argv[1:]))  # pragma: no cover


if __name__ == "__main__":
    main_cli()  # pragma: no cover
