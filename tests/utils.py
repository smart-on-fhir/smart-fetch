import datetime
import gzip
import json
import os
import pathlib
import tempfile
import unittest

import httpx
import respx
import time_machine

import smart_extract
from smart_extract.cli import main

FROZEN_DATETIME = datetime.datetime(
    2021, 9, 15, 1, 23, 45, tzinfo=datetime.timezone(datetime.timedelta(hours=4))
)
FROZEN_TIMESTAMP = FROZEN_DATETIME.astimezone().isoformat()

version = smart_extract.__version__


class TestCase(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(tempdir.cleanup)
        self.folder = pathlib.Path(tempdir.name)

        traveller = time_machine.travel(FROZEN_DATETIME, tick=False)
        self.addCleanup(traveller.stop)
        self.time_machine = traveller.start()

        self.server = respx.MockRouter(
            assert_all_called=False, base_url="http://example.invalid/R4"
        )
        self.server.get("metadata").respond(200, json={})

    @staticmethod
    def basic_resource(request: httpx.Request, res_type: str, res_id: str) -> httpx.Response:
        return httpx.Response(200, request=request, json={"resourceType": res_type, "id": res_id})

    def set_basic_resource_route(self):
        self.set_resource_route(self.basic_resource)

    def set_resource_route(self, callback):
        route = self.server.get(
            url__regex=r"http://example.invalid/R4/(?P<res_type>[^/]+)/(?P<res_id>[^/]+)"
        )
        route.side_effect = callback

    async def cli(self, *args) -> None:
        default_args = ["--fhir-url=http://example.invalid/R4"]
        with self.server:
            await main.main([str(arg) for arg in args] + default_args)

    def write_res(self, res_type: str, resources: list[dict]) -> None:
        subfolder = self.folder / res_type
        subfolder.mkdir(exist_ok=True)
        output_path = subfolder / f"{res_type}.ndjson.gz"
        with gzip.open(output_path, "wt", encoding="utf8") as f:
            for index, resource in enumerate(resources):
                resource["resourceType"] = res_type
                resource.setdefault("id", str(index))
                json.dump(resource, f)
                f.write("\n")

    def assert_folder(self, expected: dict[str, dict]) -> None:
        found_res_types = set(os.listdir(self.folder))
        assert found_res_types == set(expected.keys()), found_res_types

        for res_type, expected_files in expected.items():
            res_folder = self.folder / res_type
            found_files = set(os.listdir(res_folder))
            assert found_files == set(expected_files.keys()), found_files

            for name, val in expected_files.items():
                if val is None:
                    continue

                if name.endswith(".gz"):
                    open_func = gzip.open
                else:
                    open_func = open

                with open_func(self.folder / res_type / name, "rt", encoding="utf8") as f:
                    if isinstance(val, list):
                        for index, row in enumerate(f):
                            assert val[index] == json.loads(row)
                        assert len(val) == index + 1
                    else:
                        loaded = json.load(f)
                        assert loaded == val, (loaded, val)
