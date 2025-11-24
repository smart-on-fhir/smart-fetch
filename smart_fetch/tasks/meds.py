from smart_fetch import hydrate_utils, resources


class MedsTask(hydrate_utils.Task):
    NAME = "meds"
    INPUT_RES_TYPE = resources.MEDICATION_REQUEST
    OUTPUT_RES_TYPE = resources.MEDICATION

    async def run(self, workdir: str, source_dir: str | None = None, **kwargs) -> None:
        stats = await hydrate_utils.process(
            task_name=self.NAME,
            desc="Downloading",
            workdir=workdir,
            source_dir=source_dir or workdir,
            input_type=self.INPUT_RES_TYPE,
            output_type=self.OUTPUT_RES_TYPE,
            callback=self.process_one,
            compress=self.compress,
        )
        if stats:
            stats.print("downloaded", f"{self.OUTPUT_RES_TYPE}s")

    async def process_one(
        self, resource: dict, id_pool: set[str], **kwargs
    ) -> hydrate_utils.Result:
        med_ref = resource.get("medicationReference", {}).get("reference")
        return [
            await hydrate_utils.download_reference(
                self.client, id_pool, med_ref, self.OUTPUT_RES_TYPE
            )
        ]


MEDICATION_TASKS = [MedsTask]
