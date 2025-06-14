"""Do a REST crawl from an EHR"""

import argparse
import csv
import datetime
import os
import sys
from collections.abc import AsyncIterable
from functools import partial

import cumulus_fhir_support as cfs

from smart_extract import (
    bulk_utils,
    cli_utils,
    crawl_utils,
    iter_utils,
    lifecycle,
    ndjson,
    resources,
    timing,
)


def make_subparser(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("folder", metavar="OUTPUT_DIR")
    cli_utils.add_general(parser)

    parser.add_argument("--since", metavar="TIMESTAMP", help="only get data since this date")
    parser.add_argument(
        "--since-mode",
        choices=cli_utils.SinceMode,
        help="how to interpret --since",
        default=cli_utils.SinceMode.AUTO,
    )

    group = parser.add_argument_group("cohort selection")
    group.add_argument(
        "--group",
        metavar="GROUP",
        help="which group to export (bulk export for patients, crawl for the rest)",
    )
    group.add_argument(
        "--group-nickname",
        metavar="GROUP",
        help="a human-friendly name for the cohort, used in log files and such",
    )
    group.add_argument("--mrn-system", metavar="SYSTEM", help="system identifier for MRNs")
    group.add_argument(
        "--mrn-file",
        metavar="PATH",
        help="file with MRNs, one per line (or a .csv with an 'mrn' column)",
    )
    group.add_argument(
        "--source-dir",
        metavar="DIR",
        help="folder with existing Patient resources, to use as a cohort "
        "(defaults to output folder)",
    )

    cli_utils.add_auth(parser)
    cli_utils.add_type_selection(parser)
    parser.set_defaults(func=crawl_main)


async def crawl_main(args: argparse.Namespace) -> None:
    """Exports data from an EHR to a folder."""
    rest_client, bulk_client = cli_utils.prepare(args)
    res_types = cli_utils.parse_resource_selection(args.type)

    async with rest_client:
        filters = cli_utils.parse_type_filters(rest_client.server_type, res_types, args.type_filter)
        cli_utils.add_since_filter(
            filters,
            args.since,
            server_type=rest_client.server_type,
            since_mode=args.since_mode,
        )
        workdir = args.folder
        source_dir = args.source_dir or workdir
        os.makedirs(workdir, exist_ok=True)

        # The ID pool is meant to keep track of IDs that we've seen per resource, so that we can
        # avoid writing out duplicates, in the situations where we have multiple search streams
        # per resource (which can happen when we have multiple type filters OR'd together).
        # A pool is only defined if multiple filters exist for it, to save memory.
        id_pool = {}
        for res_type in filters:
            if len(filters[res_type]) > 1:
                id_pool[res_type] = set()

        if args.group_nickname:
            group_name = args.group_nickname
        elif args.group is not None:
            group_name = args.group
        elif args.mrn_file:
            group_name = os.path.splitext(os.path.basename(args.mrn_file))[0]
        else:
            group_name = os.path.basename(source_dir)

        metadata = lifecycle.Metadata(workdir)

        processor = iter_utils.ResourceProcessor(
            workdir,
            "Crawling",
            callback=partial(process, rest_client, id_pool, workdir),
            finish_callback=partial(mark_done, metadata),
            append=False,
        )

        transaction_time = timing.now()

        # Before crawling, we have to decide if we need to do anything special with patients,
        # like a bulk export or even a normal crawl using MRN, in order to get the patient IDs.
        if resources.PATIENT in res_types:
            if metadata.is_done(resources.PATIENT):
                print(f"Skipping {resources.PATIENT}, already done.")
            else:
                await gather_patients(bulk_client, processor, args, filters, workdir)
                metadata.mark_done(resources.PATIENT)
            res_types.remove(resources.PATIENT)
            patient_ids = read_patient_ids(workdir)
        else:
            patient_ids = read_patient_ids(source_dir)
        if not patient_ids:
            sys.exit(
                f"No cohort patients found in {source_dir}.\n"
                f"You can provide a cohort from a previous export with --source-dir, "
                "or export patients in this crawl too."
            )

        for res_type in res_types:
            if metadata.is_done(res_type):
                print(f"Skipping {res_type}, already done.")
                continue
            processor.add_source(
                res_type, resource_urls(res_type, patient_ids, filters), len(patient_ids)
            )

        if processor.sources:
            await processor.run()
            fake_bulk_export(group_name, args.fhir_url, workdir, transaction_time)


async def gather_patients(bulk_client, processor, args, filters, workdir: str) -> None:
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

        processor.add_source(
            resources.PATIENT, patient_urls(args.mrn_system, mrns, filters), len(mrns)
        )
        await processor.run()

    elif args.group is not None:
        # OK we're doing a bulk export
        async with bulk_client:
            # TODO: Confirm it's empty? - and run in silent mode, using pulsing bar?
            exporter = bulk_utils.BulkExporter(
                bulk_client,
                {resources.PATIENT},
                bulk_utils.export_url(args.fhir_url, args.group),
                workdir,
                type_filter=filters.get(resources.PATIENT),
            )
            await exporter.export()

    else:
        sys.exit("Provide either --group or --mrn-system and --mrn-file, to define the cohort")


async def mark_done(metadata: lifecycle.Metadata, res_type: str) -> None:
    metadata.mark_done(res_type)


def read_patient_ids(folder: str) -> set[str]:
    return {
        patient["id"]
        for patient in cfs.read_multiline_json_from_dir(folder, resources.PATIENT)
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
    if bundle.get("resourceType") != resources.BUNDLE:
        return

    for entry in bundle.get("entry", []):
        if resource := entry.get("resource"):
            yield resource

    for link in bundle.get("link", []):
        if link.get("relation") == "next" and link.get("url"):
            async for res in crawl_bundle_chain(client, link.get("url")):
                yield res
            break


async def process(
    client,
    id_pool: dict[str, set[str]],
    folder: str,
    res_type: str,
    writer: ndjson.NdjsonWriter,
    url: str,
) -> None:
    async for resource in crawl_bundle_chain(client, url):
        if resource["resourceType"] == "OperationOutcome":
            # Make a fake "error" folder, just like we'd see in a bulk export
            error_subfolder = os.path.join(folder, "error")
            os.makedirs(error_subfolder, exist_ok=True)
            error_file = os.path.join(error_subfolder, f"{resources.OPERATION_OUTCOME}.ndjson.gz")
            with ndjson.NdjsonWriter(error_file, append=True) as error_writer:
                error_writer.write(resource)
            continue

        res_pool = id_pool.get(resource["resourceType"])
        if res_pool is not None:
            if resource["id"] in res_pool:
                continue
            res_pool.add(resource["id"])

        writer.write(resource)


def fake_bulk_export(group: str, fhir_url: str, folder: str, start_time: datetime.datetime) -> None:
    crawl_utils.create_fake_log(folder, fhir_url, group, start_time)
