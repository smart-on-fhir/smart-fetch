"""Modify extracted data in various ways"""

import argparse
import os

import cumulus_fhir_support as cfs
import rich
from cumulus_etl import common, errors, fhir, inliner, store

from smart_extract import cli_utils, fix_stats, lifecycle, resources


def make_subparser(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("folder", metavar="INPUT_DIR")
    parser.add_argument("fixes", metavar="FIX", nargs="*", default=["all"])
    cli_utils.add_general(parser)
    parser.add_argument(
        "--mimetypes",
        metavar="MIMES",
        help="mimetypes to inline, comma separated (default is text, HTML, and XHTML)",
    )

    cli_utils.add_auth(parser)
    parser.set_defaults(func=fix_main)


async def fix_main(args: argparse.Namespace) -> None:
    """Fixes up data."""
    client = cli_utils.prepare(args)
    fixes = set(args.fixes)

    async with client:
        if "doc-inline" in fixes or "all" in fixes:
            with lifecycle.skip_or_mark_done_for_fix(
                os.path.join(args.folder, resources.DOCUMENT_REFERENCE), "doc-inline"
            ):
                await doc_inline(client, args)
        if "dxr-inline" in fixes or "all" in fixes:
            with lifecycle.skip_or_mark_done_for_fix(
                os.path.join(args.folder, resources.DIAGNOSTIC_REPORT), "dxr-inline"
            ):
                await dxr_inline(client, args)
        if "meds" in fixes or "all" in fixes:
            with lifecycle.skip_or_mark_done_for_fix(
                os.path.join(args.folder, resources.MEDICATION_REQUEST), "meds"
            ):
                await meds(client, args)


def parse_mimetypes(mimetypes: str | None) -> set[str]:
    if mimetypes is None:
        return {"text/plain", "text/html", "application/xhtml+xml"}

    return set(mimetypes.casefold().split(","))


async def doc_inline(client, args):
    mimetypes = parse_mimetypes(args.mimetypes)
    await inliner.inliner(
        client, store.Root(args.folder), {resources.DOCUMENT_REFERENCE}, mimetypes
    )


async def dxr_inline(client, args):
    mimetypes = parse_mimetypes(args.mimetypes)
    await inliner.inliner(client, store.Root(args.folder), {resources.DIAGNOSTIC_REPORT}, mimetypes)


async def meds(client, args):
    with cli_utils.make_progress_bar() as progress:
        med_req_folder = os.path.join(args.folder, resources.MEDICATION_REQUEST)

        # Calculate total progress needed
        found_files = cfs.list_multiline_json_in_dir(med_req_folder, resources.MEDICATION_REQUEST)
        total_lines = sum(common.read_local_line_count(path) for path in found_files)
        progress_task = progress.add_task("Downloading Meds", total=total_lines)

        # See what is already downloaded
        downloaded_ids = set()
        for med in cfs.read_multiline_json_from_dir(med_req_folder, resources.MEDICATION):
            downloaded_ids.add(f"{resources.MEDICATION}/{med['id']}")

        # Get new meds
        stats = fix_stats.FixStats()
        output = os.path.join(med_req_folder, f"{resources.MEDICATION}.ndjson.gz")
        with common.NdjsonWriter(output, append=True, compressed=True) as writer:
            reader = cfs.read_multiline_json_from_dir(med_req_folder, resources.MEDICATION_REQUEST)
            for med_req in reader:
                stats.total += 1

                med_id = med_req.get("medicationReference", {}).get("reference")
                if med_id in downloaded_ids:
                    stats.already_done += 1
                elif med_id.startswith(f"{resources.MEDICATION}/"):
                    med = None
                    try:
                        med = await fhir.download_reference(client, med_id)
                    except errors.FatalNetworkError:
                        stats.fatal_errors += 1
                    except errors.TemporaryNetworkError:
                        stats.retry_errors += 1

                    if med and med["resourceType"] != resources.MEDICATION:
                        # Hmm, wrong type. Could be OperationOutcome? Mark as fatal.
                        stats.fatal_errors += 1
                    elif med:
                        stats.newly_done += 1
                        writer.write(med)

                progress.update(progress_task, advance=1)

    stats.print("MedicationRequests", "downloaded")
