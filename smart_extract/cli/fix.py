"""Modify extracted data in various ways"""

import argparse
import sys

from smart_extract import cli_utils, fixes


def make_subparser(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("folder", metavar="OUTPUT_DIR")
    parser.add_argument("fixes", metavar="FIX", nargs="*", default=["all"])
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
    parser.set_defaults(func=fix_main)


def print_help():
    print("These fixes are supported:")
    print("  all")
    for fix_name in sorted(fixes.all_fixes.keys()):
        print(f"  {fix_name}")


async def fix_main(args: argparse.Namespace) -> None:
    """Fixes up data."""
    client, _bulk_client = cli_utils.prepare(args)
    cli_fixes = set(args.fixes)

    if "help" in cli_fixes:
        print_help()
        sys.exit(0)

    for fix_name in cli_fixes:
        if fix_name != "all" and fix_name not in fixes.all_fixes:
            print(f"Unknown fix provided: {fix_name}")
            print()
            print_help()
            sys.exit(2)

    async with client:
        for fix_name in fixes.all_fixes:
            if fix_name in cli_fixes or "all" in cli_fixes:
                await fixes.all_fixes[fix_name][1](
                    client, args.folder, source_dir=args.source_dir, mimetypes=args.mimetypes
                )
