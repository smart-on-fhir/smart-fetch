import cumulus_fhir_support as cfs
import rich.progress

from smart_fetch import hydrate_utils, resources


async def _download_members(client, resource: dict, id_pool: set[str]) -> hydrate_utils.Result:
    results = [
        await hydrate_utils.download_reference(
            client, id_pool, member.get("reference"), resources.OBSERVATION
        )
        for member in resource.get("hasMember", [])
    ]
    for result in results:
        if result[0]:
            results.extend(await _download_members(client, result[0], id_pool))
    return results


async def task_obs_members(
    client: cfs.FhirClient,
    workdir: str,
    source_dir: str | None = None,
    progress: rich.progress.Progress | None = None,
    **kwargs,
):
    stats = await hydrate_utils.process(
        client=client,
        task_name="obs-members",
        desc="Downloading member",
        workdir=workdir,
        source_dir=source_dir or workdir,
        input_type=resources.OBSERVATION,
        callback=_download_members,
        progress=progress,
        file_slug="members",
    )
    if stats:
        stats.print("downloaded", f"{resources.OBSERVATION}s", "Members")


async def _download_dxr_result(client, resource: dict, id_pool: set[str]) -> hydrate_utils.Result:
    return [
        await hydrate_utils.download_reference(
            client, id_pool, result.get("reference"), resources.OBSERVATION
        )
        for result in resource.get("result", [])
    ]


async def task_obs_dxr(
    client: cfs.FhirClient,
    workdir: str,
    source_dir: str | None = None,
    progress: rich.progress.Progress | None = None,
    **kwargs,
):
    stats = await hydrate_utils.process(
        client=client,
        task_name="dxr-results",
        desc="Downloading result",
        workdir=workdir,
        source_dir=source_dir or workdir,
        input_type=resources.DIAGNOSTIC_REPORT,
        output_type=resources.OBSERVATION,
        callback=_download_dxr_result,
        progress=progress,
        file_slug="results",
    )
    if stats:
        stats.print(
            "downloaded", f"{resources.DIAGNOSTIC_REPORT}s", f"Result {resources.OBSERVATION}s"
        )
