"""Resets generated symlinks for a managed dir"""

import argparse
import sys

from smart_fetch import cli_utils, lifecycle, symlinks


def make_subparser(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("folder", metavar="OUTPUT_DIR")
    cli_utils.add_general(parser)
    parser.set_defaults(func=reset_symlinks_main)


async def reset_symlinks_main(args: argparse.Namespace) -> None:
    """Exports data from an EHR to a folder."""
    if lifecycle.ManagedMetadata(args.folder).is_empty():
        sys.exit(f"Folder '{args.folder}' does not look like a SMART Fetch managed export folder.")

    symlinks.reset_all_links(args.folder)
