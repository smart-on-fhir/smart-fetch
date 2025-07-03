import json
import os

import ddt
import httpx

from smart_fetch import resources
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
                ".metadata": {
                    "kind": "output",
                    "timestamp": utils.FROZEN_TIMESTAMP,
                    "version": utils.version,
                    "done": {resources.DEVICE: utils.FROZEN_TIMESTAMP},
                    "filters": {resources.DEVICE: []},
                    "since": None,
                    "sinceMode": None,
                },
                "log.ndjson": [
                    {
                        "exportId": "fake-log",
                        "timestamp": utils.FROZEN_TIMESTAMP,
                        "eventId": "kickoff",
                        "eventDetail": {
                            "exportUrl": f"{self.url}/Group/{expected_group}/$export",
                            "softwareName": None,
                            "softwareVersion": None,
                            "softwareReleaseDate": None,
                            "fhirVersion": None,
                            "requestParameters": {},
                            "errorCode": None,
                            "errorBody": None,
                            "responseHeaders": {},
                        },
                        "_client": "smart-fetch",
                        "_clientVersion": utils.version,
                    },
                    {
                        "exportId": "fake-log",
                        "timestamp": utils.FROZEN_TIMESTAMP,
                        "eventId": "status_complete",
                        "eventDetail": {"transactionTime": utils.FROZEN_TIMESTAMP},
                    },
                    {
                        "exportId": "fake-log",
                        "timestamp": utils.FROZEN_TIMESTAMP,
                        "eventId": "status_page_complete",
                        "eventDetail": {
                            "transactionTime": utils.FROZEN_TIMESTAMP,
                            "outputFileCount": 0,
                            "deletedFileCount": 0,
                            "errorFileCount": 0,
                        },
                    },
                    {
                        "exportId": "fake-log",
                        "timestamp": utils.FROZEN_TIMESTAMP,
                        "eventId": "manifest_complete",
                        "eventDetail": {
                            "transactionTime": utils.FROZEN_TIMESTAMP,
                            "totalOutputFileCount": 0,
                            "totalDeletedFileCount": 0,
                            "totalErrorFileCount": 0,
                            "totalManifests": 1,
                        },
                    },
                    {
                        "exportId": "fake-log",
                        "timestamp": utils.FROZEN_TIMESTAMP,
                        "eventId": "export_complete",
                        "eventDetail": {
                            "files": 0,
                            "resources": 0,
                            "bytes": 0,
                            "attachments": None,
                            "duration": 0,
                        },
                    },
                ],
                f"{resources.PATIENT}.ndjson.gz": [pat1],
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
                ".metadata": None,
                "log.ndjson": None,
                f"{resources.CONDITION}.ndjson.gz": con1 + con2,
                f"{resources.PATIENT}.ndjson.gz": pat1 + pat2,
            }
        )

    async def test_from_bulk(self):
        """Simple patient crawl from bulk export"""
        pat1 = {"resourceType": resources.PATIENT, "id": "pat1"}
        pat2 = {"resourceType": resources.PATIENT, "id": "pat2"}

        self.mock_bulk("best-group", output=[pat1, pat2], params={"_type": resources.PATIENT})

        await self.cli("crawl", self.folder, "--group=best-group", f"--type={resources.PATIENT}")

        self.assert_folder(
            {
                ".metadata": None,
                "log.ndjson": None,
                f"{resources.PATIENT}.001.ndjson.gz": [pat1],
                f"{resources.PATIENT}.002.ndjson.gz": [pat2],
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
                    return httpx.Response(
                        200, json=self.make_bundle(proc2, link=f"{resources.PROCEDURE}?resume=3")
                    )
                elif request.url.params == httpx.QueryParams(resume="3"):
                    # Fake an issue of some sort, where we don't get a bundle.
                    # We should quietly ignore this, as a weird edge case.
                    return httpx.Response(200, json={"resourceType": resources.PATIENT})
            assert False, f"Invalid request: {request.url.params}"

        self.set_resource_search_route(respond)

        await self.cli("crawl", self.folder, "--type", resources.PROCEDURE)

        self.assert_folder(
            {
                ".metadata": None,
                "log.ndjson": None,
                f"{resources.PATIENT}.ndjson.gz": None,
                f"{resources.PROCEDURE}.ndjson.gz": proc1 + proc2,
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

        expected_filter_metadata = [
            "&".join(f"{key}={val}" for key, val in params.items()) for params in query_params
        ]

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
                ".metadata": {
                    "done": {resources.OBSERVATION: utils.FROZEN_TIMESTAMP},
                    "filters": {resources.OBSERVATION: expected_filter_metadata},
                    "kind": "output",
                    "since": None,
                    "sinceMode": None,
                    "timestamp": utils.FROZEN_TIMESTAMP,
                    "version": utils.version,
                },
                "log.ndjson": None,
                f"{resources.PATIENT}.ndjson.gz": None,
                f"{resources.OBSERVATION}.ndjson.gz": obs1,
            }
        )

    async def test_errors(self):
        """Confirm we write out errors like a bulk export does"""
        self.write_res(
            resources.PATIENT,
            [
                {"resourceType": resources.PATIENT, "id": "pat1"},
                {"resourceType": resources.PATIENT, "id": "pat2"},
                {"resourceType": resources.PATIENT, "id": "pat3"},
            ],
        )

        outcome1 = {"resourceType": resources.OPERATION_OUTCOME, "id": "outcome1"}
        outcome2 = {
            "resourceType": resources.OPERATION_OUTCOME,
            "id": "outcome2",
            "issue": [{"diagnostics": "outcome2"}],
        }
        outcome3 = {  # outcome created by us as a client
            "resourceType": resources.OPERATION_OUTCOME,
            "issue": [
                {
                    "severity": "error",
                    "code": "exception",
                    "diagnostics": "An error occurred when connecting to "
                    '"http://example.invalid/R4/Encounter?patient=pat3": boo',
                }
            ],
        }
        enc1 = {"resourceType": resources.ENCOUNTER, "id": "enc1"}

        def respond(request: httpx.Request, res_type: str) -> httpx.Response:
            if res_type == resources.ENCOUNTER:
                if request.url.params == httpx.QueryParams(patient="pat1"):
                    # Internal "error" (as part of a successful request)
                    return httpx.Response(200, json=self.make_bundle([enc1, outcome1]))
                elif request.url.params == httpx.QueryParams(patient="pat2"):
                    # More in-your-face "error" outcome (a failed request)
                    return httpx.Response(404, json=outcome2)
                elif request.url.params == httpx.QueryParams(patient="pat3"):
                    # In-your-face random text
                    return httpx.Response(404, text="boo")
            assert False, f"Invalid request: {request.url.params}"

        self.set_resource_search_route(respond)

        await self.cli("crawl", self.folder, "--type", resources.ENCOUNTER)

        self.assert_folder(
            {
                ".metadata": None,
                "log.ndjson": None,
                f"{resources.ENCOUNTER}.ndjson.gz": [enc1],
                f"{resources.PATIENT}.ndjson.gz": None,
                "error": {
                    f"{resources.OPERATION_OUTCOME}.ndjson.gz": [outcome1, outcome2, outcome3],
                },
            }
        )

    async def test_skip_done(self):
        """Confirm we skip already done resources"""
        pat1 = {"resourceType": resources.PATIENT, "id": "pat1"}
        self.write_res(resources.PATIENT, [pat1])
        with open(f"{self.folder}/.metadata", "w", encoding="utf8") as f:
            json.dump(
                {
                    "done": {
                        resources.DEVICE: utils.FROZEN_TIMESTAMP,
                        resources.PATIENT: utils.FROZEN_TIMESTAMP,
                    }
                },
                f,
            )

        await self.cli(
            "crawl",
            self.folder,
            f"--type={resources.DEVICE},{resources.PATIENT}",
            "--group=my-group",
        )

        # We would have errored out if any network attempts were made.
        # Just confirm we only wrote out a new log.ndjson.

        self.assert_folder(
            {
                ".metadata": None,
                f"{resources.PATIENT}.ndjson.gz": None,
                "log.ndjson": None,
            }
        )

    async def test_since(self):
        """Test default behavior of --since (uses _lastUpdated)"""
        pat1 = {"resourceType": resources.PATIENT, "id": "pat1"}
        self.write_res(resources.PATIENT, [pat1])

        params = {
            resources.ENCOUNTER: [
                httpx.QueryParams(patient="pat1", identifier="ENC1", _lastUpdated="gt2022-01-05"),
                httpx.QueryParams(patient="pat1", type="ADMS", _lastUpdated="gt2022-01-05"),
            ],
            resources.IMMUNIZATION: [
                httpx.QueryParams(patient="pat1", _lastUpdated="gt2022-01-05"),
            ],
        }

        missing = self.set_resource_search_queries(params)

        await self.cli(
            "crawl",
            self.folder,
            "--group=my-group",
            "--since=2022-01-05",
            f"--type={resources.ENCOUNTER},{resources.IMMUNIZATION}",
            "--type-filter=Encounter?identifier=ENC1",
            "--type-filter=Encounter?type=ADMS",
        )

        self.assertEqual(missing, [])

        self.assert_folder(
            {
                ".metadata": {
                    "kind": "output",
                    "timestamp": utils.FROZEN_TIMESTAMP,
                    "version": utils.version,
                    "done": {
                        resources.ENCOUNTER: utils.FROZEN_TIMESTAMP,
                        resources.IMMUNIZATION: utils.FROZEN_TIMESTAMP,
                    },
                    "filters": {
                        resources.ENCOUNTER: ["identifier=ENC1", "type=ADMS"],
                        resources.IMMUNIZATION: [],
                    },
                    "since": "2022-01-05",
                    "sinceMode": "updated",
                },
                f"{resources.PATIENT}.ndjson.gz": None,
                "log.ndjson": None,
            }
        )

    async def test_since_created(self):
        """Test --since-mode=created"""
        pat1 = {"resourceType": resources.PATIENT, "id": "pat1"}
        self.write_res(resources.PATIENT, [pat1])
        with open(f"{self.folder}/.metadata", "w", encoding="utf8") as f:
            json.dump({"done": {resources.PATIENT: utils.FROZEN_TIMESTAMP}}, f)

        params = {
            resources.ALLERGY_INTOLERANCE: [httpx.QueryParams(patient="pat1", date="gt2022-01-05")],
            resources.CONDITION: [
                httpx.QueryParams(patient="pat1", **{"recorded-date": "gt2022-01-05"})
            ],
            resources.DEVICE: [httpx.QueryParams(patient="pat1")],  # no extra param
            resources.DIAGNOSTIC_REPORT: [httpx.QueryParams(patient="pat1", issued="gt2022-01-05")],
            resources.DOCUMENT_REFERENCE: [httpx.QueryParams(patient="pat1", date="gt2022-01-05")],
            resources.ENCOUNTER: [httpx.QueryParams(patient="pat1", date="gt2022-01-05")],
            resources.IMMUNIZATION: [httpx.QueryParams(patient="pat1", date="gt2022-01-05")],
            resources.MEDICATION_REQUEST: [
                httpx.QueryParams(patient="pat1", authoredon="gt2022-01-05")
            ],
            resources.OBSERVATION: [
                httpx.QueryParams(
                    patient="pat1",
                    category="social-history,vital-signs,imaging,laboratory,survey,exam,"
                    "procedure,therapy,activity",
                    date="gt2022-01-05",
                ),
            ],
            resources.PROCEDURE: [httpx.QueryParams(patient="pat1", date="gt2022-01-05")],
            resources.SERVICE_REQUEST: [httpx.QueryParams(patient="pat1", authored="gt2022-01-05")],
        }

        missing = self.set_resource_search_queries(params)

        await self.cli(
            "crawl",
            self.folder,
            "--group=my-group",
            "--since=2022-01-05",
            "--since-mode=created",
        )

        self.assertEqual(missing, [])

    async def test_since_epic_uses_created(self):
        """Confirm that Epic servers get the correct since mode by default"""
        pat1 = {"resourceType": resources.PATIENT, "id": "pat1"}
        self.write_res(resources.PATIENT, [pat1])

        # We'll end up seeing date= instead of _lastUpdated= because of Epic
        params = {resources.ENCOUNTER: [httpx.QueryParams(patient="pat1", date="gt2022-01-05")]}

        missing = self.set_resource_search_queries(params)
        self.server.get("metadata").respond(200, json={"software": {"name": "Epic"}})

        await self.cli(
            "crawl",
            self.folder,
            "--group=my-group",
            "--since=2022-01-05",
            f"--type={resources.ENCOUNTER}",
        )

        self.assertEqual(missing, [])

    async def test_complain_if_no_patients(self):
        with self.assertRaisesRegex(SystemExit, "No cohort patients found"):
            await self.cli(
                "crawl",
                self.folder,
                f"--source-dir={self.folder}",
                f"--type={resources.ENCOUNTER}",
            )

    async def test_complain_if_no_mrn_header(self):
        mrn_file = self.tmp_file(suffix=".csv")
        with mrn_file:
            mrn_file.write("header1,header2\n")
            mrn_file.write("abc,def\nghi,jkl\n")

        with self.assertRaisesRegex(SystemExit, "has no 'mrn' header"):
            await self.cli(
                "crawl",
                self.folder,
                f"--mrn-file={mrn_file.name}",
                "--mrn-system=my-system",
                f"--type={resources.PATIENT}",
            )

    @ddt.data(
        # 2020 is before our frozen time of 2021
        ({"meta": {"lastUpdated": "2020-01-01T10:00:00-10"}}, "2020-01-01T10:00:00-10:00"),
        ({"period": {"start": "2020"}}, "2020-01-01T00:00:00+00:00"),
        ({"period": {"end": "2020-01"}}, "2020-01-01T00:00:00+00:00"),
        ({"period": {"start": "2020-01-01"}}, "2020-01-01T00:00:00+00:00"),
        # 2022 is after our frozen time of 2021 - it took a year to finish exporting!
        ({"period": {"end": "2022-01-01"}}, utils.FROZEN_TIMESTAMP),
    )
    @ddt.unpack
    async def test_transaction_time(self, enc_fields, expected_time):
        """Confirm we write out an older date if the server only has old data"""
        pat1 = {"resourceType": resources.PATIENT, "id": "pat1"}
        self.write_res(resources.PATIENT, [pat1])

        params = {
            resources.ALLERGY_INTOLERANCE: {
                httpx.QueryParams(patient="pat1"): [
                    {
                        "resourceType": resources.ALLERGY_INTOLERANCE,
                        "id": "1",
                        "recordedDate": "bogus",
                    }
                ]
            },
            resources.CONDITION: {
                httpx.QueryParams(patient="pat1"): [
                    {"resourceType": resources.CONDITION, "id": "1", "recordedDate": "2000"}
                ]
            },
            resources.DIAGNOSTIC_REPORT: {
                httpx.QueryParams(patient="pat1"): [
                    {"resourceType": resources.DIAGNOSTIC_REPORT, "id": "1", "issued": "2001"}
                ]
            },
            resources.DOCUMENT_REFERENCE: {
                httpx.QueryParams(patient="pat1"): [
                    {"resourceType": resources.DOCUMENT_REFERENCE, "id": "1", "date": "2002"}
                ]
            },
            resources.ENCOUNTER: {
                httpx.QueryParams(patient="pat1"): [
                    {"resourceType": resources.ENCOUNTER, "id": "1", **enc_fields}
                ]
            },
            resources.IMMUNIZATION: {
                httpx.QueryParams(patient="pat1"): [
                    {
                        "resourceType": resources.IMMUNIZATION,
                        "id": "1",
                        "occurrenceDateTime": "2003",
                    }
                ]
            },
            resources.MEDICATION_REQUEST: {
                httpx.QueryParams(patient="pat1"): [
                    {"resourceType": resources.MEDICATION_REQUEST, "id": "1", "authoredOn": "2004"}
                ]
            },
            resources.OBSERVATION: {
                httpx.QueryParams(patient="pat1", category=utils.DEFAULT_OBS_CATEGORIES): [
                    {"resourceType": resources.OBSERVATION, "id": "1", "effectiveDateTime": "2005"},
                    {"resourceType": resources.OBSERVATION, "id": "1", "effectiveInstant": "2006"},
                    {
                        "resourceType": resources.OBSERVATION,
                        "id": "1",
                        "effectivePeriod": {"start": "2007"},
                    },
                    {
                        "resourceType": resources.OBSERVATION,
                        "id": "1",
                        "effectivePeriod": {"end": "2008"},
                    },
                ]
            },
            resources.PROCEDURE: {
                httpx.QueryParams(patient="pat1"): [
                    {"resourceType": resources.PROCEDURE, "id": "1", "performedDateTime": "2009"},
                    {
                        "resourceType": resources.PROCEDURE,
                        "id": "1",
                        "performedPeriod": {"start": "2010"},
                    },
                    {
                        "resourceType": resources.PROCEDURE,
                        "id": "1",
                        "performedPeriod": {"end": "2011"},
                    },
                ]
            },
            resources.SERVICE_REQUEST: {
                httpx.QueryParams(patient="pat1"): [
                    {"resourceType": resources.SERVICE_REQUEST, "id": "1", "authoredOn": "2012"},
                ]
            },
        }
        missing = self.set_resource_search_queries(params)

        await self.cli(
            "crawl",
            self.folder,
            f"--type={','.join(resources.CREATED_SEARCH_FIELDS)}",
            "--group-nickname=foo",
        )

        self.assertEqual(missing, [])

        expected_log_transaction_time = "2000-01-01T00:00:00+00:00"
        self.assert_folder(
            {
                ".metadata": {
                    "kind": "output",
                    "timestamp": utils.FROZEN_TIMESTAMP,
                    "version": utils.version,
                    "done": {
                        resources.ALLERGY_INTOLERANCE: utils.FROZEN_TIMESTAMP,  # fallback
                        resources.CONDITION: "2000-01-01T00:00:00+00:00",
                        resources.DIAGNOSTIC_REPORT: "2001-01-01T00:00:00+00:00",
                        resources.DOCUMENT_REFERENCE: "2002-01-01T00:00:00+00:00",
                        resources.ENCOUNTER: expected_time,
                        resources.IMMUNIZATION: "2003-01-01T00:00:00+00:00",
                        resources.MEDICATION_REQUEST: "2004-01-01T00:00:00+00:00",
                        resources.OBSERVATION: "2008-01-01T00:00:00+00:00",
                        resources.PROCEDURE: "2011-01-01T00:00:00+00:00",
                        resources.SERVICE_REQUEST: "2012-01-01T00:00:00+00:00",
                    },
                    "filters": {
                        resources.ALLERGY_INTOLERANCE: [],
                        resources.CONDITION: [],
                        resources.DIAGNOSTIC_REPORT: [],
                        resources.DOCUMENT_REFERENCE: [],
                        resources.ENCOUNTER: [],
                        resources.IMMUNIZATION: [],
                        resources.MEDICATION_REQUEST: [],
                        resources.OBSERVATION: [f"category={utils.DEFAULT_OBS_CATEGORIES}"],
                        resources.PROCEDURE: [],
                        resources.SERVICE_REQUEST: [],
                    },
                    "since": None,
                    "sinceMode": None,
                },
                f"{resources.ALLERGY_INTOLERANCE}.ndjson.gz": None,
                f"{resources.CONDITION}.ndjson.gz": None,
                f"{resources.DIAGNOSTIC_REPORT}.ndjson.gz": None,
                f"{resources.DOCUMENT_REFERENCE}.ndjson.gz": None,
                f"{resources.ENCOUNTER}.ndjson.gz": None,
                f"{resources.IMMUNIZATION}.ndjson.gz": None,
                f"{resources.MEDICATION_REQUEST}.ndjson.gz": None,
                f"{resources.OBSERVATION}.ndjson.gz": None,
                f"{resources.PATIENT}.ndjson.gz": None,
                f"{resources.PROCEDURE}.ndjson.gz": None,
                f"{resources.SERVICE_REQUEST}.ndjson.gz": None,
                # Check the log to confirm that we use the earliest resource transaction time
                # as the overall transactionTime.
                "log.ndjson": [
                    {
                        "exportId": "fake-log",
                        "timestamp": utils.FROZEN_TIMESTAMP,
                        "eventId": "kickoff",
                        "eventDetail": {
                            "exportUrl": f"{self.url}/Group/foo/$export",
                            "softwareName": None,
                            "softwareVersion": None,
                            "softwareReleaseDate": None,
                            "fhirVersion": None,
                            "requestParameters": {},
                            "errorCode": None,
                            "errorBody": None,
                            "responseHeaders": {},
                        },
                        "_client": "smart-fetch",
                        "_clientVersion": utils.version,
                    },
                    {
                        "exportId": "fake-log",
                        "timestamp": utils.FROZEN_TIMESTAMP,
                        "eventId": "status_complete",
                        "eventDetail": {"transactionTime": expected_log_transaction_time},
                    },
                    {
                        "exportId": "fake-log",
                        "timestamp": utils.FROZEN_TIMESTAMP,
                        "eventId": "status_page_complete",
                        "eventDetail": {
                            "transactionTime": expected_log_transaction_time,
                            "outputFileCount": 0,
                            "deletedFileCount": 0,
                            "errorFileCount": 0,
                        },
                    },
                    {
                        "exportId": "fake-log",
                        "timestamp": utils.FROZEN_TIMESTAMP,
                        "eventId": "manifest_complete",
                        "eventDetail": {
                            "transactionTime": expected_log_transaction_time,
                            "totalOutputFileCount": 0,
                            "totalDeletedFileCount": 0,
                            "totalErrorFileCount": 0,
                            "totalManifests": 1,
                        },
                    },
                    {
                        "exportId": "fake-log",
                        "timestamp": utils.FROZEN_TIMESTAMP,
                        "eventId": "export_complete",
                        "eventDetail": {
                            "files": 0,
                            "resources": 0,
                            "bytes": 0,
                            "attachments": None,
                            "duration": 0,
                        },
                    },
                ],
            }
        )
