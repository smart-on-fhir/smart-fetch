import abc

from smart_fetch import hydrate_utils, resources


class ObsTask(hydrate_utils.Task, abc.ABC):
    OUTPUT_RES_TYPE = resources.OBSERVATION


class DxrResultsTask(ObsTask):
    NAME = "dxr-results"
    INPUT_RES_TYPE = resources.DIAGNOSTIC_REPORT

    async def run(self, workdir: str, source_dir: str | None = None, **kwargs) -> None:
        stats = await hydrate_utils.process(
            task_name=self.NAME,
            desc="Downloading result",
            workdir=workdir,
            source_dir=source_dir or workdir,
            input_type=self.INPUT_RES_TYPE,
            output_type=self.OUTPUT_RES_TYPE,
            callback=self.process_one,
            file_slug="results",
            compress=self.compress,
        )
        if stats:
            stats.print("downloaded", f"{self.INPUT_RES_TYPE}s", f"Result {self.OUTPUT_RES_TYPE}s")

    async def process_one(
        self, resource: dict, id_pool: set[str], **kwargs
    ) -> hydrate_utils.Result:
        return [
            await hydrate_utils.download_reference(
                self.client, id_pool, result.get("reference"), self.OUTPUT_RES_TYPE
            )
            for result in resource.get("result", [])
        ]


class ObsMembersTask(ObsTask):
    NAME = "obs-members"
    INPUT_RES_TYPE = resources.OBSERVATION

    async def run(self, workdir: str, source_dir: str | None = None, **kwargs) -> None:
        stats = await hydrate_utils.process(
            task_name=self.NAME,
            desc="Downloading member",
            workdir=workdir,
            source_dir=source_dir or workdir,
            input_type=self.INPUT_RES_TYPE,
            callback=self.process_one,
            file_slug="members",
            compress=self.compress,
        )
        if stats:
            stats.print("downloaded", f"{resources.OBSERVATION}s", "Members")

    async def process_one(
        self, resource: dict, id_pool: set[str], **kwargs
    ) -> hydrate_utils.Result:
        results = [
            await hydrate_utils.download_reference(
                self.client, id_pool, member.get("reference"), self.OUTPUT_RES_TYPE
            )
            for member in resource.get("hasMember", [])
        ]
        for result in results:
            if result[0]:
                results.extend(await self.process_one(result[0], id_pool))
        return results


OBSERVATION_TASKS = [DxrResultsTask, ObsMembersTask]
