import cumulus_fhir_support as cfs
import rich.progress

from smart_fetch import hydrate_utils, resources


async def _download_med(client, resource: dict, id_pool: set[str]) -> hydrate_utils.Result:
    med_ref = resource.get("medicationReference", {}).get("reference")
    return [await hydrate_utils.download_reference(client, id_pool, med_ref, resources.MEDICATION)]


async def task_meds(
    client: cfs.FhirClient,
    workdir: str,
    source_dir: str | None = None,
    progress: rich.progress.Progress | None = None,
    **kwargs,
):
    stats = await hydrate_utils.process(
        client=client,
        task_name="meds",
        desc="Downloading",
        workdir=workdir,
        source_dir=source_dir or workdir,
        input_type=resources.MEDICATION_REQUEST,
        output_type=resources.MEDICATION,
        callback=_download_med,
        progress=progress,
    )
    if stats:
        stats.print("downloaded", f"{resources.MEDICATION}s")
