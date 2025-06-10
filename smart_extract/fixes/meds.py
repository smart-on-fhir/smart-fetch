from smart_extract import fix_utils, resources


async def _download_med(client, resource: dict, id_pool: set[str]) -> fix_utils.Result:
    med_ref = resource.get("medicationReference", {}).get("reference")
    return [await fix_utils.download_reference(client, id_pool, med_ref, resources.MEDICATION)]


async def fix_meds(client, args):
    stats = await fix_utils.process(
        client=client,
        folder=args.folder,
        input_type=resources.MEDICATION_REQUEST,
        fix_name="meds",
        desc="Downloading Meds",
        callback=_download_med,
        output_folder=resources.MEDICATION_REQUEST,
        output_type=resources.MEDICATION,
    )
    stats.print("downloaded", f"{resources.MEDICATION}s")
