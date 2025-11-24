import csv
import datetime
import json
import os
import sys
from collections.abc import AsyncIterable, Awaitable, Callable
from functools import partial

import cumulus_fhir_support as cfs
import httpx
import rich

from smart_fetch import (
    bulk_utils,
    filtering,
    iter_utils,
    lifecycle,
    merges,
    ndjson,
    resources,
    timing,
)


def create_fake_log(folder: str, fhir_url: str, group: str, transaction_time: datetime.datetime):
    """
    Creates a "fake" bulk export log, like a bulk export would have made.

    This is useful to let other tools that process bulk logs to treat this folder as one, and
    extract information like transactionTime and the group name.
    """
    url = (
        os.path.join(fhir_url, "Group", group, "$export")
        if group
        else os.path.join(fhir_url, "$export")
    )
    log = bulk_utils.BulkExportLogWriter(folder)
    log.export_id = "fake-log"
    log.kickoff(url, {}, httpx.Response(202))
    log.status_complete(httpx.Response(200, json={"transactionTime": transaction_time.isoformat()}))
    log.export_complete()


async def perform_crawl(
    *,
    fhir_url: str,
    filters: filtering.Filters,
    source_dir: str,
    workdir: str,
    rest_client: cfs.FhirClient,
    bulk_client: cfs.FhirClient,
    group_nickname: str | None,
    group: str | None,
    id_file: str | None,
    id_list: str | None,
    id_system: str | None,
    finish_callback: Callable[[str], Awaitable[None]] | None = None,
    managed_dir: str | None = None,
    compress: bool = False,
) -> None:
    if group_nickname:
        group_name = group_nickname
    elif group is not None:
        group_name = group
    elif id_file:
        group_name = os.path.splitext(os.path.basename(id_file))[0]
    else:
        group_name = os.path.basename(source_dir)

    os.makedirs(workdir, exist_ok=True)
    metadata = lifecycle.OutputMetadata(workdir)
    metadata.note_context(filters=filters)

    filter_params = filters.params()
    filters.print_since()

    # The ID pool is meant to keep track of IDs that we've seen per resource, so that we can
    # avoid writing out duplicates, in the situations where we have multiple search streams
    # per resource (which can happen when we have multiple type filters OR'd together).
    # A pool is only defined if multiple filters exist for it, to save memory.
    id_pool = {}
    for res_type in filter_params:
        if len(filter_params[res_type]) > 1:
            id_pool[res_type] = set()

    # `transaction_times` holds the date we will send to mark_done() when done with each resource,
    # to mark the equivalent of Bulk Export's transactionTime - the last moment that the export
    # data covers. We will record the latest update/creation date we find for all resources, and
    # take the lower of that time and when we started the export. This way, if the server has stale
    # data (like it's a infrequently updated replica server), we will appropriately record an older
    # transaction time for the exported data set.
    transaction_times: dict[str, datetime.datetime] = {}

    processor_finish = partial(
        finish_wrapper,
        metadata,
        finish_callback,
        transaction_times,
        workdir,
        managed_dir,
        filters,
        compress=compress,
    )

    processor = iter_utils.ResourceProcessor(
        workdir,
        "Crawling",
        callback=partial(
            process_resource, rest_client, id_pool, workdir, transaction_times, compress=compress
        ),
        finish_callback=processor_finish,
        append=False,
        compress=compress,
    )

    # Before crawling, we have to decide if we need to do anything special with patients,
    # like a bulk export or even a normal crawl using MRN, in order to get the patient IDs.
    download_patients = False
    patient_ids = None
    if resources.PATIENT in filter_params:
        if metadata.is_done(resources.PATIENT):
            rich.print(f"Skipping {resources.PATIENT}, already done.")
            if finish_callback:
                await finish_callback(resources.PATIENT)
        else:
            download_patients = True
        del filter_params[resources.PATIENT]
    elif id_file or id_list:
        # TODO: once support becomes more widespread, we could do the chained query
        #  transparently for the user instead of a separate download step. But today,
        #  support isn't there (for Epic and Oracle at least):
        #  - Condition?patient.identifier= (Epic supports, Oracle does not)
        #  - Condition?patient:identifier= (Neither Epic nor Oracle supports)
        rich.print("Fetching Patients for the provided IDs.")
        download_patients = True
    else:
        # Read from source dir, not workdir
        patient_ids = merges.read_resource_ids(resources.PATIENT, source_dir)

    if download_patients:
        await gather_patients(
            bulk_client=bulk_client,
            processor=processor,
            filters=filters,
            workdir=workdir,
            id_file=id_file,
            id_list=id_list,
            id_system=id_system,
            fhir_url=fhir_url,
            group=group,
            metadata=metadata,
            compress=compress,
            finish_callback=processor_finish,
        )
    if patient_ids is None:
        patient_ids = merges.read_resource_ids(resources.PATIENT, workdir)
    if not patient_ids:
        sys.exit(
            f"No cohort patients found in {source_dir}.\n"
            f"You must either provide a cohort from a previous export with --source-dir, "
            "--id-list, or --id-file, or export patients in this crawl too."
        )

    for res_type in filter_params:
        if metadata.is_done(res_type):
            rich.print(f"Skipping {res_type}, already done.")
            if finish_callback:
                await finish_callback(res_type)
            continue

        processor.add_source(
            res_type,
            resource_urls_with_new_patients(res_type, metadata, managed_dir, patient_ids, filters),
            total=len(patient_ids),
            output_file=ndjson.filename(f"{res_type}.ndjson", compress=compress),
        )

    if processor.sources:
        await processor.run()

    if log_time := metadata.get_earliest_done_date():
        create_fake_log(workdir, fhir_url, group_name, log_time)


def load_specified_ids(*, id_list: str | None, id_file: str | None) -> set[str]:
    ids = set(id_list.split(",")) if id_list else set()

    if id_file:
        with open(id_file, encoding="utf8", newline="") as f:
            if id_file.casefold().endswith(".csv"):
                reader = csv.DictReader(f)
                fieldnames = {name.casefold(): name for name in reader.fieldnames}
                if "id" in fieldnames:
                    ids |= {row[fieldnames["id"]] for row in reader}
                elif "mrn" in fieldnames:
                    ids |= {row[fieldnames["mrn"]] for row in reader}
                else:
                    sys.exit(f"ID file {id_file} has no 'id' or 'mrn' header")
            else:
                ids |= {row.strip() for row in f}

    return set(filter(None, ids))  # ignore empty lines


async def gather_patients(
    *,
    bulk_client,
    processor,
    filters: filtering.Filters,
    workdir: str,
    id_file: str | None,
    id_list: str | None,
    id_system: str | None,
    fhir_url: str,
    group: str,
    metadata: lifecycle.OutputMetadata,
    compress: bool = False,
    finish_callback: Callable[[str], Awaitable[None]],
) -> None:
    if id_file or id_list:
        ids = load_specified_ids(id_file=id_file, id_list=id_list)

        if id_system:
            urls = resource_urls(
                resources.PATIENT, f"identifier={id_system}|", ids, filters.params()
            )
        else:
            urls = resource_urls(resources.PATIENT, "_id=", ids, filters.params())

        processor.add_source(
            resources.PATIENT,
            urls,
            total=len(ids),
            output_file=ndjson.filename(f"{resources.PATIENT}.ndjson", compress=compress),
        )
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
                since=filters.get_bulk_since(),
                type_filter=filters.params(bulk=True),
                metadata=metadata,
                compress=compress,
            )
            await exporter.export()
            await finish_callback(resources.PATIENT, timestamp=exporter.transaction_time)


async def finish_wrapper(
    metadata: lifecycle.OutputMetadata,
    custom_finish: Callable[[str], Awaitable[None]] | None,
    transaction_times: dict[str, datetime.datetime],
    workdir: str,
    managed_dir: str,
    filters: filtering.Filters,
    res_type: str,
    *,
    timestamp: datetime.datetime,
    compress: bool = False,
) -> None:
    # Calculate new/deleted resources, if possible
    if res_type == resources.PATIENT:
        new, deleted = merges.find_new_patients(workdir, managed_dir, filters)
        if new:
            metadata.note_new_patients(new)
            rich.print(f"Count of new patients: {len(new):,}.")
        if deleted:
            merges.write_deleted_file(workdir, res_type, deleted, compress=compress)
            rich.print(f"Count of deleted patients: {len(deleted):,}.")

    # If `timestamp` (which is when we started crawling) is earlier than our latest found date,
    # use it as our transaction time instead (this way we ensure we don't miss any resources that
    # got created during our crawl, at the cost of getting some duplicate resources next time we
    # crawl using this transaction time as a --since value).
    if res_type not in transaction_times or transaction_times[res_type] > timestamp:
        transaction_times[res_type] = timestamp

    metadata.mark_done(res_type, transaction_times[res_type])
    if custom_finish:
        await custom_finish(res_type)


async def resource_urls_with_new_patients(
    res_type: str,
    metadata: lifecycle.OutputMetadata,
    managed_dir: str | None,
    patient_ids: set[str],
    filters: filtering.Filters,
) -> AsyncIterable[str]:
    if filters.since:
        new_ids = merges.find_new_patients_for_resource(res_type, metadata, managed_dir, filters)
    else:
        # Not necessary to care about new IDs - we are getting the historical data anyway
        new_ids = set()

    existing_ids = patient_ids - new_ids
    new_ids &= patient_ids

    async for url in resource_urls(res_type, "patient=", existing_ids, filters.params()):
        yield url
    async for url in resource_urls(res_type, "patient=", new_ids, filters.params(with_since=False)):
        yield url


async def resource_urls(res_type, query_prefix, ids, params) -> AsyncIterable[str]:
    for one_id in ids:
        url = f"{res_type}?{query_prefix}{one_id}"

        if res_filters := params.get(res_type):
            for res_filter in res_filters:
                yield f"{url}&{res_filter}"
        else:
            yield url


async def crawl_bundle_chain(client: cfs.FhirClient, url: str) -> AsyncIterable[dict]:
    try:
        # Specially handle plus signs because the HTTP spec is whack and they'll be converted to
        # spaces unless we encode them manually here.
        response = await client.request("GET", url.replace("+", "%2B"))
    except cfs.NetworkError as exc:
        try:
            resource = exc.response and exc.response.json()
        except json.JSONDecodeError:
            resource = None
        if resource and resource.get("resourceType") == resources.OPERATION_OUTCOME:
            yield resource
        else:
            # Make up our own OperationOutcome to hold the error
            yield {
                "resourceType": resources.OPERATION_OUTCOME,
                "issue": [{"severity": "error", "code": "exception", "diagnostics": str(exc)}],
            }
        return

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


def _log_error(folder: str, resource: dict, *, compress: bool = False) -> None:
    # Make a fake "error" folder, just like we'd see in a bulk export
    error_subfolder = os.path.join(folder, "error")
    os.makedirs(error_subfolder, exist_ok=True)
    error_file = ndjson.filename(
        error_subfolder, f"{resources.OPERATION_OUTCOME}.ndjson", compress=compress
    )
    with ndjson.NdjsonWriter(error_file, append=True) as error_writer:
        error_writer.write(resource)


def update_transaction_time(
    transaction_times: dict[str, datetime.datetime], res_type: str, val: str | None
) -> None:
    if parsed := timing.parse_datetime(val):
        if parsed > datetime.datetime.now(datetime.UTC):
            return  # exclude mistaken / typo dates that are in the future
        if res_type not in transaction_times or transaction_times[res_type] < parsed:
            transaction_times[res_type] = parsed


async def process_resource(
    client,
    id_pool: dict[str, set[str]],
    folder: str,
    transaction_times: dict[str, datetime.datetime],
    res_type: str,
    writer: ndjson.NdjsonWriter,
    url: str,
    compress: bool = False,
    **kwargs,
) -> None:
    async for resource in crawl_bundle_chain(client, url):
        if resource["resourceType"] == resources.OPERATION_OUTCOME:
            _log_error(folder, resource, compress=compress)
            continue

        res_pool = id_pool.get(resource["resourceType"])
        if res_pool is not None:
            if resource["id"] in res_pool:
                continue
            res_pool.add(resource["id"])

        # See if we have a later updated/created date than we've seen so far.
        update_transaction_time(transaction_times, res_type, resources.get_updated_date(resource))
        update_transaction_time(transaction_times, res_type, resources.get_created_date(resource))

        writer.write(resource)
