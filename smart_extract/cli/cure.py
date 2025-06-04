"""Modify extracted data in various ways"""

import argparse

from cumulus_etl import inliner, store

from smart_extract.cli import cli_utils
from smart_extract import resources


def make_subparser(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("folder", metavar="INPUT_DIR")
    parser.add_argument("fixes", metavar="FIX", nargs="*", default=["all"])
    parser.add_argument("--config", metavar="PATH", help="config file")
    parser.add_argument(
        "--mimetypes", metavar="MIMES",
        help="mimetypes to inline, comma separated (default is text, HTML, and XHTML)"
    )

    cli_utils.add_auth(parser)
    parser.set_defaults(func=cure_main)


async def cure_main(args: argparse.Namespace) -> None:
    """Fixes up data."""
    client = cli_utils.prepare(args)
    fixes = set(args.fixes)

    async with client:
        if "doc-inline" in fixes or "all" in fixes:
            await doc_inline(client, args)
        if "dxr-inline" in fixes or "all" in fixes:
            await dxr_inline(client, args)


def parse_mimetypes(mimetypes: str | None) -> set[str]:
    if mimetypes is None:
        return {"text/plain", "text/html", "application/xhtml+xml"}

    return set(mimetypes.casefold().split(","))


async def doc_inline(client, args):
    mimetypes = parse_mimetypes(args.mimetypes)
    await inliner.inliner(client, store.Root(args.folder), {resources.DOCUMENT_REFERENCE},
                          mimetypes)


async def dxr_inline(client, args):
    mimetypes = parse_mimetypes(args.mimetypes)
    await inliner.inliner(client, store.Root(args.folder), {resources.DIAGNOSTIC_REPORT},
                          mimetypes)
