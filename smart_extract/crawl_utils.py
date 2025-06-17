import csv
import datetime
import json
import os
import sys
from collections.abc import AsyncIterable, Awaitable, Callable
from functools import partial

import cumulus_fhir_support as cfs

from smart_extract import bulk_utils, cli_utils, iter_utils, lifecycle, ndjson, resources, timing


def create_fake_log(folder: str, fhir_url: str, group: str, transaction_time: datetime.datetime):
    timestamp = timing.now().isoformat()
    url = (
        os.path.join(fhir_url, "Group", group, "$export")
        if group
        else os.path.join(fhir_url, "$export")
    )
    with open(f"{folder}/log.ndjson", "w", encoding="utf8") as f:
        json.dump(
            {
                "exportId": "fake-log",
                "timestamp": timestamp,
                "eventId": "kickoff",
                "eventDetail": {
                    "exportUrl": url,
                },
            },
            f,
        )
        f.write("\n")
        json.dump(
            {
                "exportId": "fake-log",
                "timestamp": timestamp,
                "eventId": "status_complete",
                "eventDetail": {
                    "transactionTime": transaction_time.astimezone().isoformat(),
                },
            },
            f,
        )
        f.write("\n")


async def perform_crawl(
    *,
    fhir_url: str,
    filters: cli_utils.Filters,
    source_dir: str,
    workdir: str,
    rest_client: cfs.FhirClient,
    bulk_client: cfs.FhirClient,
    group_nickname: str | None,
    group: str | None,
    mrn_file: str | None,
    mrn_system: str | None,
    finish_callback: Callable[[str], None] | None = None,
) -> None:
    # The ID pool is meant to keep track of IDs that we've seen per resource, so that we can
    # avoid writing out duplicates, in the situations where we have multiple search streams
    # per resource (which can happen when we have multiple type filters OR'd together).
    # A pool is only defined if multiple filters exist for it, to save memory.
    id_pool = {}
    for res_type in filters:
        if len(filters[res_type]) > 1:
            id_pool[res_type] = set()

    if group_nickname:
        group_name = group_nickname
    elif group is not None:
        group_name = group
    elif mrn_file:
        group_name = os.path.splitext(os.path.basename(mrn_file))[0]
    else:
        group_name = os.path.basename(source_dir)

    os.makedirs(workdir, exist_ok=True)
    metadata = lifecycle.OutputMetadata(workdir)

    processor = iter_utils.ResourceProcessor(
        workdir,
        "Crawling",
        callback=partial(process, rest_client, id_pool, workdir),
        finish_callback=partial(finish_wrapper, metadata, finish_callback),
        append=False,
    )

    transaction_time = timing.now()

    # Before crawling, we have to decide if we need to do anything special with patients,
    # like a bulk export or even a normal crawl using MRN, in order to get the patient IDs.
    if resources.PATIENT in filters:
        if metadata.is_done(resources.PATIENT):
            print(f"Skipping {resources.PATIENT}, already done.")
        else:
            await gather_patients(
                bulk_client=bulk_client,
                processor=processor,
                filters=filters,
                workdir=workdir,
                mrn_file=mrn_file,
                mrn_system=mrn_system,
                fhir_url=fhir_url,
                group=group,
                metadata=metadata,
                finish_callback=finish_callback,
            )
        del filters[resources.PATIENT]
        patient_ids = read_patient_ids(workdir)
    else:
        patient_ids = read_patient_ids(source_dir)
    if not patient_ids:
        sys.exit(
            f"No cohort patients found in {source_dir}.\n"
            f"You can provide a cohort from a previous export with --source-dir, "
            "or export patients in this crawl too."
        )

    for res_type in filters:
        if metadata.is_done(res_type):
            print(f"Skipping {res_type}, already done.")
            continue
        processor.add_source(
            res_type, resource_urls(res_type, patient_ids, filters), len(patient_ids)
        )

    if processor.sources:
        await processor.run()
        fake_bulk_export(group_name, fhir_url, workdir, transaction_time)


async def gather_patients(
    *,
    bulk_client,
    processor,
    filters,
    workdir: str,
    mrn_file: str | None,
    mrn_system: str | None,
    fhir_url: str,
    group: str,
    metadata: lifecycle.OutputMetadata,
    finish_callback: Callable[[str], Awaitable[None]] | None = None,
) -> None:
    if mrn_file and mrn_system:
        with open(mrn_file, encoding="utf8", newline="") as f:
            if mrn_file.casefold().endswith(".csv"):
                reader = csv.DictReader(f)
                fieldnames = {name.casefold(): name for name in reader.fieldnames}
                if "mrn" not in fieldnames:
                    sys.exit(f"MRN file {mrn_file} has no 'mrn' header")
                mrns = {row[fieldnames["mrn"]] for row in reader}
            else:
                mrns = {row.strip() for row in f}
        mrns = set(filter(None, mrns))  # ignore empty lines

        processor.add_source(resources.PATIENT, patient_urls(mrn_system, mrns, filters), len(mrns))
        await processor.run()

    else:
        # OK we're doing a bulk export
        async with bulk_client:
            # TODO: Confirm it's empty? - and run in silent mode, using pulsing bar?
            exporter = bulk_utils.BulkExporter(
                bulk_client,
                {resources.PATIENT},
                bulk_utils.export_url(fhir_url, group),
                workdir,
                type_filter=filters,
            )
            await exporter.export()
            await finish_wrapper(metadata, finish_callback, resources.PATIENT)


async def finish_wrapper(
    metadata: lifecycle.OutputMetadata,
    custom_finish: Callable[[str], Awaitable[None]] | None,
    res_type: str,
) -> None:
    metadata.mark_done(res_type)
    if custom_finish:
        await custom_finish(res_type)


def read_patient_ids(folder: str) -> set[str]:
    return {
        patient["id"] for patient in cfs.read_multiline_json_from_dir(folder, resources.PATIENT)
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
    create_fake_log(folder, fhir_url, group, start_time)
