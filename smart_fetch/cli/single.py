"""Request a single query"""

import argparse
import base64
import sys

import cumulus_fhir_support as cfs
import rich
import rich.highlighter
import rich.progress

from smart_fetch import cli_utils, ndjson, resources


def make_subparser(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("resource", metavar="ResourceType/ID")
    cli_utils.add_general(parser)
    parser.add_argument(
        "--binary",
        action="store_true",
        help="if resource is a Binary object, prints the underlying binary data rather than JSON",
    )
    parser.add_argument(
        "--compact",
        action="store_true",
        help="print JSON in compact mode (all on one line)",
    )

    cli_utils.add_auth(parser)
    parser.set_defaults(func=single_main)


async def single_main(args: argparse.Namespace) -> None:
    """Exports data from an EHR to a folder."""
    rest_client, _bulk_client = cli_utils.prepare(args)

    async with rest_client:
        try:
            response = await rest_client.request("GET", args.resource)
        except cfs.NetworkError as exc:
            sys.exit(str(exc))

        fhir_json = response.json()

        if args.binary and fhir_json.get("resourceType") == resources.BINARY:
            data = fhir_json.get("data", "")
            binary = base64.standard_b64decode(data)
            sys.stdout.buffer.write(binary)
        elif args.compact:
            # Use our extremely tight formatting (no spaces at all),
            # but with rich's nice highlighting
            compact_str = ndjson.compact_json(fhir_json)
            compact_str = rich.highlighter.JSONHighlighter()(compact_str)
            rich.get_console().print(compact_str, soft_wrap=True)
        else:
            rich.print_json(data=fhir_json)
