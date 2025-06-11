import os

import ddt
import httpx

import smart_extract
from smart_extract import resources, timing
from tests import utils


@ddt.ddt
class CrawlTests(utils.TestCase):
    @staticmethod
    def make_bundle(entries: list[dict], link: str | None = None) -> dict:
        return {
            "resourceType": resources.BUNDLE,
            "entry": [{"resource": entry} for entry in entries],
            "link": [{"relation": "next", "url": link}],
        }

    @ddt.data(
        ["--group-nickname", "blarg"],
        ["--group", "blarg"],
        ["--mrn-file", "blarg.txt"],
        [],
    )
    async def test_metadata(self, args):
        """Confirm we write a done file and a log.ndjson"""
        pat1 = {"resourceType": resources.PATIENT, "id": "pat1"}
        self.write_res(resources.PATIENT, [pat1])

        def respond(request: httpx.Request, res_type: str) -> httpx.Response:
            if res_type == resources.DEVICE:
                if request.url.params == httpx.QueryParams(patient="pat1"):
                    return httpx.Response(200, json=self.make_bundle([]))
            assert False, f"Invalid request: {request.url.params}"

        self.set_resource_search_route(respond)

        await self.cli("crawl", self.folder, "--type", resources.DEVICE, *args)

        expected_group = "blarg" if args else os.path.basename(self.folder)

        self.assert_folder(
            {
                resources.DEVICE: {
                    ".export.done": {
                        "duration": 0,
                        "timestamp": timing.now().isoformat(),
                        "version": smart_extract.__version__,
                    },
                    "log.ndjson": [
                        {
                            "exportId": "fake-log",
                            "timestamp": timing.now().isoformat(),
                            "eventId": "kickoff",
                            "eventDetail": {
                                "exportUrl": f"{self.url}/Group/{expected_group}/$export",
                            },
                        },
                        {
                            "exportId": "fake-log",
                            "timestamp": timing.now().isoformat(),
                            "eventId": "status_complete",
                            "eventDetail": {
                                "transactionTime": timing.now().astimezone().isoformat(),
                            },
                        },
                    ],
                },
                resources.PATIENT: {
                    f"{resources.PATIENT}.ndjson.gz": [pat1],
                },
            }
        )

    @ddt.data(True, False)
    async def test_from_mrn_file(self, is_csv):
        """Simple patient crawl from MRNs"""
        mrn_file = self.tmp_file(suffix=(".csv" if is_csv else ""))
        with mrn_file:
            if is_csv:
                mrn_file.write("MRN\n")
            mrn_file.write("abc\n\ndef\n")  # the blank line will be ignored

        pat1 = [{"resourceType": resources.PATIENT, "id": "pat1"}]
        pat2 = [{"resourceType": resources.PATIENT, "id": "pat2"}]
        con1 = [
            {"resourceType": resources.CONDITION, "id": "con1.1"},
            {"resourceType": resources.CONDITION, "id": "con1.2"},
        ]
        con2 = [{"resourceType": resources.CONDITION, "id": "con2.1"}]

        def respond(request: httpx.Request, res_type: str) -> httpx.Response:
            if res_type == resources.PATIENT:
                if request.url.params == httpx.QueryParams(identifier="uri:mrn|abc"):
                    return httpx.Response(200, json=self.make_bundle(pat1))
                elif request.url.params == httpx.QueryParams(identifier="uri:mrn|def"):
                    return httpx.Response(200, json=self.make_bundle(pat2))
            elif res_type == resources.CONDITION:
                if request.url.params == httpx.QueryParams(patient="pat1"):
                    return httpx.Response(200, json=self.make_bundle(con1))
                elif request.url.params == httpx.QueryParams(patient="pat2"):
                    return httpx.Response(200, json=self.make_bundle(con2))
            assert False, f"Invalid request: {request.url.params}"

        self.set_resource_search_route(respond)

        await self.cli(
            "crawl",
            self.folder,
            "--mrn-system=uri:mrn",
            "--mrn-file",
            mrn_file.name,
            f"--type={resources.CONDITION},{resources.PATIENT}",
        )

        self.assert_folder(
            {
                resources.CONDITION: {
                    ".export.done": None,
                    "log.ndjson": None,
                    f"{resources.CONDITION}.ndjson.gz": con1 + con2,
                },
                resources.PATIENT: {
                    ".export.done": None,
                    "log.ndjson": None,
                    f"{resources.PATIENT}.ndjson.gz": pat1 + pat2,
                },
            }
        )

    async def test_from_bulk(self):
        """Simple patient crawl from bulk export"""
        pat1 = {"resourceType": resources.PATIENT, "id": "pat1"}
        pat2 = {"resourceType": resources.PATIENT, "id": "pat2"}

        self.mock_bulk("best-group", output=[pat1, pat2])

        await self.cli("crawl", self.folder, "--group=best-group", f"--type={resources.PATIENT}")

        self.assert_folder(
            {
                resources.PATIENT: {
                    ".export.done": None,
                    "log.ndjson": None,
                    f"{resources.PATIENT}.000.ndjson": [pat1],
                    f"{resources.PATIENT}.001.ndjson": [pat2],
                },
            }
        )

    async def test_follow_bundles(self):
        """Confirm we follow bundle links"""
        self.write_res(resources.PATIENT, [{"resourceType": resources.PATIENT, "id": "pat1"}])

        proc1 = [{"resourceType": resources.PROCEDURE, "id": "proc1"}]
        proc2 = [{"resourceType": resources.PROCEDURE, "id": "proc2"}]

        def respond(request: httpx.Request, res_type: str) -> httpx.Response:
            if res_type == resources.PROCEDURE:
                if request.url.params == httpx.QueryParams(patient="pat1"):
                    return httpx.Response(
                        200, json=self.make_bundle(proc1, link=f"{resources.PROCEDURE}?resume=2")
                    )
                elif request.url.params == httpx.QueryParams(resume="2"):
                    return httpx.Response(200, json=self.make_bundle(proc2))
            assert False, f"Invalid request: {request.url.params}"

        self.set_resource_search_route(respond)

        await self.cli("crawl", self.folder, "--type", resources.PROCEDURE)

        self.assert_folder(
            {
                resources.PROCEDURE: {
                    ".export.done": None,
                    "log.ndjson": None,
                    f"{resources.PROCEDURE}.ndjson.gz": proc1 + proc2,
                },
                resources.PATIENT: {
                    f"{resources.PATIENT}.ndjson.gz": None,
                },
            }
        )

    @ddt.data(
        # server metadata, CLI args, expected query params
        # Default obs categories
        (
            {},
            [],
            [
                {
                    "category": "social-history,vital-signs,imaging,laboratory,survey,exam,"
                    "procedure,therapy,activity"
                }
            ],
        ),
        # Epic cuts off a few unsupported categories
        (
            {"software": {"name": "Epic"}},
            [],
            [{"category": "social-history,vital-signs,imaging,laboratory,survey,exam"}],
        ),
        # Oracle uses a second query for smoking status codes
        (
            {"publisher": "Oracle Health"},
            [],
            [
                {
                    "category": "social-history,vital-signs,imaging,laboratory,survey,exam,"
                    "procedure,therapy,activity"
                },
                {"code": "http://loinc.org|72166-2"},
            ],
        ),
        # Custom filters
        (
            {"publisher": "Oracle Health"},  # should be ignored with manual filters
            [
                "--type-filter",
                f"{resources.OBSERVATION}?date=lt2020-01-01&status=final",
                "--type-filter",
                f"{resources.OBSERVATION}?identifier=blarg",
            ],
            [{"date": "lt2020-01-01", "status": "final"}, {"identifier": "blarg"}],
        ),
    )
    @ddt.unpack
    async def test_type_filter(self, metadata, cli_args, query_params):
        """Confirm we manage --type-filter correctly"""
        self.write_res(resources.PATIENT, [{"resourceType": resources.PATIENT, "id": "pat1"}])

        obs1 = [{"resourceType": resources.OBSERVATION, "id": "obs1"}]

        def respond(request: httpx.Request, res_type: str) -> httpx.Response:
            if res_type == resources.OBSERVATION:
                for queries in query_params:
                    if request.url.params == httpx.QueryParams(patient="pat1", **queries):
                        query_params.remove(queries)
                        return httpx.Response(200, json=self.make_bundle(obs1))
            assert False, f"Invalid request: {request.url.params}"

        self.set_resource_search_route(respond)
        self.server.get("metadata").respond(200, json=metadata)

        await self.cli("crawl", self.folder, "--type", resources.OBSERVATION, *cli_args)

        self.assertEqual(query_params, [], "Not all queries were made")

        self.assert_folder(
            {
                resources.OBSERVATION: {
                    ".export.done": None,
                    "log.ndjson": None,
                    f"{resources.OBSERVATION}.ndjson.gz": obs1,
                },
                resources.PATIENT: {
                    f"{resources.PATIENT}.ndjson.gz": None,
                },
            }
        )

    async def test_errors(self):
        """Confirm we write out errors like a bulk export does"""
        self.write_res(resources.PATIENT, [{"resourceType": resources.PATIENT, "id": "pat1"}])

        outcome1 = {"resourceType": resources.OPERATION_OUTCOME, "id": "outcome1"}
        enc1 = {"resourceType": resources.ENCOUNTER, "id": "enc1"}

        def respond(request: httpx.Request, res_type: str) -> httpx.Response:
            if res_type == resources.ENCOUNTER:
                if request.url.params == httpx.QueryParams(patient="pat1"):
                    return httpx.Response(200, json=self.make_bundle([enc1, outcome1]))
            assert False, f"Invalid request: {request.url.params}"

        self.set_resource_search_route(respond)

        await self.cli("crawl", self.folder, "--type", resources.ENCOUNTER)

        self.assert_folder(
            {
                resources.ENCOUNTER: {
                    ".export.done": None,
                    "log.ndjson": None,
                    f"{resources.ENCOUNTER}.ndjson.gz": [enc1],
                    "error": {
                        f"{resources.OPERATION_OUTCOME}.ndjson.gz": [outcome1],
                    },
                },
                resources.PATIENT: {
                    f"{resources.PATIENT}.ndjson.gz": None,
                },
            }
        )

    async def test_skip_done(self):
        """Confirm we skip already done folders"""
        pat1 = {"resourceType": resources.PATIENT, "id": "pat1"}
        self.write_res(resources.PATIENT, [pat1])
        with open(f"{self.folder}/{resources.PATIENT}/.export.done", "w", encoding="utf8") as f:
            f.write("{}")  # just an empty marker

        os.makedirs(self.folder / resources.DEVICE)
        with open(f"{self.folder}/{resources.DEVICE}/.export.done", "w", encoding="utf8") as f:
            f.write("{}")  # just an empty marker

        await self.cli(
            "crawl",
            self.folder,
            f"--type={resources.DEVICE},{resources.PATIENT}",
            "--group=my-group",
        )

        # We would have errored out if any network attempts were made.
        # Just confirm we also didn't write anything new out.

        self.assert_folder(
            {
                resources.DEVICE: {
                    ".export.done": {},
                },
                resources.PATIENT: {
                    ".export.done": {},
                    f"{resources.PATIENT}.ndjson.gz": [pat1],
                },
            }
        )
