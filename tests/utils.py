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
from smart_extract import resources, timing
from smart_extract.cli import main

FROZEN_DATETIME = datetime.datetime(
    2021, 9, 15, 1, 23, 45, tzinfo=datetime.timezone(datetime.timedelta(hours=4))
)
FROZEN_TIMESTAMP = FROZEN_DATETIME.astimezone().isoformat()

DEFAULT_OBS_CATEGORIES = (
    "social-history,vital-signs,imaging,laboratory,survey,exam,procedure,therapy,activity"
)
DEFAULT_OBS_FILTER = (
    f"{resources.OBSERVATION}?category={DEFAULT_OBS_CATEGORIES.replace(',', '%2C')}"
)

version = smart_extract.__version__


class TestCase(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.maxDiff = None

        tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(tempdir.cleanup)
        self.folder = pathlib.Path(tempdir.name)

        traveller = time_machine.travel(FROZEN_DATETIME, tick=False)
        self.addCleanup(traveller.stop)
        self.time_machine = traveller.start()

        self.url = "http://example.invalid/R4"

        # Use a separate download root, to avoid colliding with any search URL regexes above.
        self.dlserver = "http://example.invalid/dl"

        self.server = respx.MockRouter(assert_all_called=False, base_url=self.url)
        self.server.get("metadata").respond(200, json={})

    def tmp_file(self, **kwargs):
        tmp = tempfile.NamedTemporaryFile("wt", delete=False, **kwargs)
        self.addCleanup(os.unlink, tmp.name)
        return tmp

    @staticmethod
    def basic_resource(
        request: httpx.Request, res_type: str, res_id: str, **kwargs
    ) -> httpx.Response:
        return httpx.Response(
            200, request=request, json={"resourceType": res_type, "id": res_id, **kwargs}
        )

    def set_basic_resource_route(self):
        self.set_resource_route(self.basic_resource)

    def set_resource_route(self, callback):
        route = self.server.get(
            url__regex=rf"{self.url}/(?P<res_type>[^/]+)/(?P<res_id>[^/?]+)[^/\$]*$"
        )
        route.side_effect = callback

    def set_resource_search_queries(
        self, all_results: dict[str, list[httpx.QueryParams] | dict[httpx.QueryParams, list[dict]]]
    ):
        all_params = []
        for params in all_results.values():
            if isinstance(params, list):
                all_params.extend(params)
            else:
                all_params.extend(params.keys())

        def respond(request: httpx.Request, res_type: str) -> httpx.Response:
            res_results = all_results.get(res_type, [])
            if request.url.params in res_results:
                all_params.remove(request.url.params)
                entries = [] if isinstance(res_results, list) else res_results[request.url.params]
                return httpx.Response(
                    200,
                    json={
                        "resourceType": resources.BUNDLE,
                        "entry": [{"resource": resource} for resource in entries],
                    },
                )
            assert False, f"Invalid request: {request}"

        self.set_resource_search_route(respond)

        return all_params

    def set_resource_search_route(self, callback):
        route = self.server.get(url__regex=rf"{self.url}/(?P<res_type>[^/?]+)[^/\$]*$")
        route.side_effect = callback

    async def cli(self, *args) -> None:
        default_args = ["--fhir-url", self.url]
        with self.server:
            await main.main([str(arg) for arg in args] + default_args)

    def write_res(self, res_type: str, resources: list[dict], subfolder: str | None = None) -> None:
        if subfolder:
            subfolder = self.folder / subfolder
            subfolder.mkdir(exist_ok=True)
            output_path = subfolder / f"{res_type}.ndjson.gz"
        else:
            output_path = self.folder / f"{res_type}.ndjson.gz"
        with gzip.open(output_path, "wt", encoding="utf8") as f:
            for index, resource in enumerate(resources):
                resource.setdefault("resourceType", res_type)
                resource.setdefault("id", str(index))
                json.dump(resource, f)
                f.write("\n")

    def _assert_folder(self, root: pathlib.Path, expected: dict) -> None:
        found_files = set(os.listdir(root))
        self.assertEqual(found_files, set(expected.keys()))

        for name, val in expected.items():
            if val is None:
                continue

            if isinstance(val, dict) and os.path.isdir(root / name):
                self._assert_folder(root / name, val)
                continue
            elif isinstance(val, str):
                self.assertEqual(os.readlink(root / name), val, name)
                continue

            if name.endswith(".gz"):
                open_func = gzip.open
            else:
                open_func = open

            with open_func(root / name, "rt", encoding="utf8") as f:
                if isinstance(val, list):
                    rows = [json.loads(row) for row in f]
                    # Allow any order, since we deal with so much async code
                    self.assertEqual(len(rows), len(val), rows)
                    missing = [row for row in rows if row not in val]
                    self.assertEqual(missing, [])
                else:
                    loaded = json.load(f)
                    self.assertEqual(loaded, val)

    def assert_folder(self, expected: dict) -> None:
        self._assert_folder(self.folder, expected)

    def mock_bulk(
        self,
        group: str,
        *,
        params: dict[str, str] | None = None,
        output: list[dict | httpx.Response] | None = None,
        error: list[dict | httpx.Response] | None = None,
        deleted: list[dict | httpx.Response] | None = None,
        kickoff_response: httpx.Response | None = None,
        skip_kickoff: bool = False,
        status_response: httpx.Response | list[httpx.Response] | None = None,
        skip_status: bool = False,
        delete_response: httpx.Response | None = None,
        skip_delete: bool = False,
    ) -> None:
        if not skip_kickoff:
            if kickoff_response is None:
                kickoff_response = httpx.Response(
                    202, headers={"Content-Location": f"{self.dlserver}/exports/1"}
                )
            param_args = {"params__eq": params} if params else {}
            self.server.get(f"{self.url}/Group/{group}/$export", **param_args).mock(
                kickoff_response
            )

        if not skip_status:
            output = output or []
            error = error or []
            deleted = deleted or []

            def make_download_refs(mode: str, resources: list[dict | httpx.Response]) -> list[dict]:
                # Download each resource separately, to test how we handle multiples
                refs = []
                for index, resource in enumerate(resources):
                    url = f"{self.dlserver}/{mode}/{index}"
                    if isinstance(resource, dict):
                        self.server.get(url).respond(200, json=resource)
                        res_type = resource["resourceType"]
                    else:
                        self.server.get(url).mock(resource)
                        res_type = "Patient"  # does not really matter in this flow
                    refs.append({"type": res_type, "url": url})
                return refs

            output_refs = make_download_refs("output", output)
            error_refs = make_download_refs("error", error)
            deleted_refs = make_download_refs("deleted", deleted)

            if status_response is None:
                status_response = [
                    httpx.Response(
                        200,
                        json={
                            "transactionTime": timing.now().isoformat(),
                            "output": output_refs,
                            "error": error_refs,
                            "deleted": deleted_refs,
                        },
                    )
                ]
            elif isinstance(status_response, httpx.Response):
                status_response = [status_response]
            self.server.get(f"{self.dlserver}/exports/1").mock(side_effect=status_response)

        if not skip_delete:
            if delete_response is None:
                delete_response = httpx.Response(202)
            self.server.delete(f"{self.dlserver}/exports/1").mock(delete_response)
