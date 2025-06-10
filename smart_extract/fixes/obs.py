from smart_extract import fix_utils, resources


async def _download_members(client, resource: dict, id_pool: set[str]) -> fix_utils.Result:
    results = [
        await fix_utils.download_reference(
            client, id_pool, member.get("reference"), resources.OBSERVATION
        )
        for member in resource.get("hasMember", [])
    ]
    for result in results:
        if result[0]:
            results.extend(await _download_members(client, result[0], id_pool))
    return results


async def fix_obs_members(client, args):
    stats = await fix_utils.process(
        client=client,
        folder=args.folder,
        fix_name="obs-members",
        desc="Downloading",
        input_type=resources.OBSERVATION,
        callback=_download_members,
    )
    stats.print("downloaded", f"{resources.OBSERVATION}s", "Members")


async def _download_dxr_result(client, resource: dict, id_pool: set[str]) -> fix_utils.Result:
    return [
        await fix_utils.download_reference(
            client, id_pool, result.get("reference"), resources.OBSERVATION
        )
        for result in resource.get("result", [])
    ]


async def fix_obs_dxr(client, args):
    stats = await fix_utils.process(
        client=client,
        folder=args.folder,
        fix_name="obs-dxr",
        desc="Downloading",
        input_type=resources.DIAGNOSTIC_REPORT,
        output_type=resources.OBSERVATION,
        callback=_download_dxr_result,
    )
    stats.print("downloaded", f"{resources.DIAGNOSTIC_REPORT}s", f"Result {resources.OBSERVATION}s")
