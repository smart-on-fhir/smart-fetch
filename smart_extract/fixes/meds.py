from smart_extract import fix_utils, resources


async def _download_med(client, resource: dict, id_pool: set[str]) -> fix_utils.Result:
    med_ref = resource.get("medicationReference", {}).get("reference")
    return [await fix_utils.download_reference(client, id_pool, med_ref, resources.MEDICATION)]


async def fix_meds(client, args):
    stats = await fix_utils.process(
        client=client,
        fix_name="meds",
        desc="Downloading Meds",
        workdir=args.folder,
        source_dir=args.source_dir or args.folder,
        input_type=resources.MEDICATION_REQUEST,
        output_type=resources.MEDICATION,
        callback=_download_med,
    )
    if stats:
        stats.print("downloaded", f"{resources.MEDICATION}s")
