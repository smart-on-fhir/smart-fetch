"""Convert a folder of NDJSON into a single Bundle file"""

import argparse
import os
import sys

import rich

from smart_fetch import cli_utils, ndjson


def make_subparser(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("input_folder", metavar="DIR", help="input folder of FHIR data")
    cli_utils.add_general(parser)
    parser.set_defaults(func=bundle_main)


async def bundle_main(args: argparse.Namespace) -> None:
    rich.get_console().rule()

    cli_utils.validate_input_folder(args.input_folder)

    output_path = os.path.join(args.input_folder, "Bundle.json.gz")
    if os.path.exists(output_path):
        sys.exit(f"Bundle file '{output_path}' already exists.")

    if not ndjson.bundle_folder(args.input_folder, output_name="Bundle.json.gz"):
        sys.exit(f"No FHIR files found in '{args.input_folder}'.")

    rich.print(f"Bundle file '{output_path}' created.")
