"""Do a REST crawl from an EHR"""

import argparse
import csv
import os
import sys
from collections.abc import AsyncIterable
from functools import partial

import cumulus_fhir_support as cfs
from cumulus_etl import common
from cumulus_etl.loaders.fhir.bulk_export import BulkExporter

from smart_extract import bulk_utils, cli_utils, iter_utils, lifecycle, resources


def make_subparser(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("folder", metavar="OUTPUT_DIR")
    cli_utils.add_general(parser)

    group = parser.add_argument_group("cohort selection")
    group.add_argument(
        "--group",
        metavar="GROUP",
        help="which group to export (bulk export for patients, crawl for the rest)",
    )
    group.add_argument("--mrn-system", metavar="SYSTEM", help="system identifier for MRNs")
    group.add_argument(
        "--mrn-file",
        metavar="PATH",
        help="file with MRNs, one per line (or a .csv with an 'mrn' column)",
    )

    cli_utils.add_auth(parser)
    cli_utils.add_type_selection(parser)
    parser.set_defaults(func=crawl_main)


async def crawl_main(args: argparse.Namespace) -> None:
    """Exports data from an EHR to a folder."""
    client = cli_utils.prepare(args)
    res_types = cli_utils.parse_resource_selection(args.type)

    async with client:
        filters = cli_utils.parse_type_filters(client, args.type_filter)

        # The ID pool is meant to keep track of IDs that we've seen per resource, so that we can
        # avoid writing out duplicates, in the situations where we have multiple search streams
        # per resource (which can happen when we have multiple type filters OR'd together).
        # A pool is only defined if multiple filters exist for it, to save memory.
        id_pool = {}
        for res_type in res_types:
            if res_type in filters and len(filters[res_type]) > 1:
                id_pool[res_type] = set()

        processor = iter_utils.ResourceProcessor(
            args.folder, "crawl", "Crawling", partial(process, client, id_pool), append=False
        )

        # Before crawling, we have to decide if we need to do anything special with patients,
        # like a bulk export or even a normal crawl using MRN, in order to get the patient IDs.
        if resources.PATIENT in res_types:
            await gather_patients(client, processor, args, filters)
            res_types.remove(resources.PATIENT)
        patient_ids = read_patient_ids(args.folder)

        for res_type in res_types:
            processor.add(res_type, resource_urls(res_type, patient_ids, filters), len(patient_ids))
        await processor.run()


async def gather_patients(client, processor, args, filters) -> None:
    if args.mrn_file and args.mrn_system:
        with open(args.mrn_file, encoding="utf8", newline="") as f:
            if args.mrn_file.casefold().endswith(".csv"):
                reader = csv.DictReader(f)
                fieldnames = {name.casefold(): name for name in reader.fieldnames}
                if "mrn" not in fieldnames:
                    sys.exit(f"MRN file {args.mrn_file} has no 'mrn' header")
                mrns = {row[fieldnames["mrn"]] for row in reader}
            else:
                mrns = {row.strip() for row in f}
        mrns = set(filter(None, mrns))  # ignore empty lines

        processor.add(resources.PATIENT, patient_urls(args.mrn_system, mrns, filters), len(mrns))
        await processor.run()

    elif args.group is not None:
        # OK we're doing a bulk export
        patient_folder = os.path.join(args.folder, resources.PATIENT)
        os.makedirs(patient_folder, exist_ok=True)

        with lifecycle.skip_or_mark_done(patient_folder, "crawl", resources.PATIENT):
            # TODO: Confirm it's empty? - and run in silent mode, using pulsing bar?
            exporter = BulkExporter(
                client,
                {resources.PATIENT},
                bulk_utils.export_url(args.fhir_url, args.group),
                patient_folder,
                type_filter=filters.get(resources.PATIENT),
            )
            await exporter.export()

    else:
        sys.exit("Provide either --group or --mrn-system and --mrn-file, to define the cohort")


def read_patient_ids(folder: str) -> set[str]:
    patient_folder = os.path.join(folder, resources.PATIENT)
    return {
        patient["id"]
        for patient in cfs.read_multiline_json_from_dir(patient_folder, resources.PATIENT)
    }


async def patient_urls(mrn_system, mrns, filters) -> AsyncIterable[str]:
    for mrn in mrns:
        url = f"{resources.PATIENT}?identifier={mrn_system}|{mrn}"

        if res_filters := filters.get(resources.PATIENT):
            for res_filter in res_filters:
                yield f"{url}&{res_filter}"
        else:
            yield url


async def resource_urls(res_type, patients, filters) -> AsyncIterable[str]:
    for patient in patients:
        url = f"{res_type}?patient={patient}"

        if res_filters := filters.get(res_type):
            for res_filter in res_filters:
                yield f"{url}&{res_filter}"
        else:
            yield url


async def crawl_bundle_chain(client, url: str) -> AsyncIterable[dict]:
    response = await client.request("GET", url)
    bundle = response.json()
    for entry in bundle.get("entry", []):
        if resource := entry.get("resource"):
            yield resource

    for link in bundle.get("link", []):
        if link.get("relation") == "next" and link.get("url"):
            async for res in crawl_bundle_chain(client, link.get("url")):
                yield res
            break


async def process(
    client, id_pool: dict[str, set[str]], writer: common.NdjsonWriter, url: str
) -> None:
    async for resource in crawl_bundle_chain(client, url):
        if resource["resourceType"] == "OperationOutcome":
            continue
        if res_pool := id_pool.get(resource["resourceType"]):
            if resource["id"] in res_pool:
                return
            res_pool.add(resource["id"])
        writer.write(resource)
