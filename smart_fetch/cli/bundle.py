"""Convert a folder of NDJSON into a single Bundle file"""

import argparse
import sys

import cumulus_fhir_support as cfs
import rich

from smart_fetch import cli_utils, ndjson


def make_subparser(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("folder", metavar="DIR", help="input folder of FHIR data")
    cli_utils.add_general(parser)
    cli_utils.add_compression(parser)
    cli_utils.add_s3_auth(parser)
    parser.set_defaults(func=bundle_main)


async def bundle_main(args: argparse.Namespace) -> None:
    rich.get_console().rule()

    cli_utils.load_config(args)
    cli_utils.validate_output_folder(args.folder)
    cfs.FsPath.register_options(kms_key=args.kms_key, region=args.s3_region)

    folder, workdir, workdir_cfs = cli_utils.download_from_s3(args)
    if not workdir_cfs.exists():
        # remove the temp directory
        if not folder.is_local:
            workdir_cfs.rm()
        sys.exit(f"Local folder '{workdir}' does not exist.")

    output_path = ndjson.bundle_folder(workdir, compress=args.compress)
    if not output_path:
        if not folder.is_local:
            workdir_cfs.rm()
        sys.exit(f"No FHIR files found in '{args.folder}'.")

    cli_utils.upload_to_s3(folder, workdir_cfs)
    rich.print(f"Bundle file '{output_path}' created.")
