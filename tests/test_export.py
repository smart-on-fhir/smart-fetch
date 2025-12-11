import os
from unittest import mock

import ddt
import httpx

from smart_fetch import lifecycle, resources
from tests import utils


@ddt.ddt
class ExportTests(utils.TestCase):
    async def test_basic(self):
        pat1 = {"resourceType": resources.PATIENT, "id": "pat1"}
        con1 = {"resourceType": resources.CONDITION, "id": "con1"}

        self.mock_bulk(
            "group1",
            output=[pat1, con1],
            params={
                "_type": f"{resources.CONDITION},{resources.PATIENT}",
            },
        )

        await self.cli(
            "export",
            self.folder,
            "--group=group1",
            "--type=Condition",  # confirm we handle two type args
            "--type=Patient",
        )

        self.assert_folder(
            {
                "Condition.001.ndjson.gz": "001.2021-09-14/Condition.001.ndjson.gz",
                "Patient.001.ndjson.gz": "001.2021-09-14/Patient.001.ndjson.gz",
                "001.2021-09-14": {
                    ".metadata": None,
                    "log.ndjson": None,
                    "Condition.001.ndjson.gz": [con1],
                    "Patient.001.ndjson.gz": [pat1],
                },
                ".metadata": {
                    "kind": "managed",
                    "fhir-url": "http://example.invalid/R4",
                    "group": "group1",
                    "timestamp": utils.FROZEN_TIMESTAMP,
                    "version": utils.version,
                },
            }
        )

    async def test_crawls_by_default_for_epic(self):
        pat1 = {"resourceType": resources.PATIENT, "id": "pat1"}
        con1 = {"resourceType": resources.CONDITION, "id": "con1"}
        params = {resources.CONDITION: {httpx.QueryParams(patient="pat1"): [con1]}}
        missing = self.set_resource_search_queries(params)
        self.mock_bulk("group1", output=[pat1], params={"_type": resources.PATIENT})
        self.server.get("metadata").respond(200, json={"software": {"name": "Epic"}})

        await self.cli(
            "export",
            self.folder,
            "--group=group1",
            f"--type={resources.CONDITION},{resources.PATIENT}",
        )

        self.assertEqual(missing, [])
        self.assert_folder(
            {
                "Condition.001.ndjson.gz": None,
                "Patient.001.ndjson.gz": None,
                "001.2021-09-14": None,
                ".metadata": None,
            }
        )

    async def test_crawls_if_asked(self):
        pat1 = {"resourceType": resources.PATIENT, "id": "pat1"}
        con1 = {"resourceType": resources.CONDITION, "id": "con1"}
        params = {resources.CONDITION: {httpx.QueryParams(patient="pat1"): [con1]}}
        missing = self.set_resource_search_queries(params)
        self.mock_bulk(
            "group1",
            output=[pat1],
            params={
                "_type": resources.PATIENT,
            },
        )

        await self.cli(
            "export",
            self.folder,
            "--group=group1",
            "--export-mode=crawl",
            f"--type={resources.CONDITION},{resources.PATIENT}",
        )

        self.assertEqual(missing, [])
        self.assert_folder(
            {
                "Condition.001.ndjson.gz": None,
                "Patient.001.ndjson.gz": None,
                "001.2021-09-14": None,
                ".metadata": None,
            }
        )

    async def test_second_export(self):
        pat1 = {"resourceType": resources.PATIENT, "id": "pat1"}
        enc1 = {"resourceType": resources.ENCOUNTER, "id": "enc1"}
        self.mock_bulk(
            "group1",
            output=[pat1, enc1],
            params={
                "_type": f"{resources.ENCOUNTER},{resources.PATIENT}",
            },
        )
        await self.cli(
            "export",
            self.folder,
            "--group=group1",
            f"--type={resources.ENCOUNTER},{resources.PATIENT}",
        )

        pat2 = {"resourceType": resources.PATIENT, "id": "pat2"}
        enc2 = {"resourceType": resources.ENCOUNTER, "id": "enc2"}
        self.mock_bulk(
            "group1",
            output=[pat2, enc2],
            params={
                "_type": f"{resources.ENCOUNTER},{resources.PATIENT}",
                "_since": "2022-10-23",
            },
        )
        await self.cli(
            "export",
            self.folder,
            "--group=group1",
            f"--type={resources.ENCOUNTER},{resources.PATIENT}",
            "--since=2022-10-23",
        )

        self.assert_folder(
            {
                "Encounter.001.ndjson.gz": "001.2021-09-14/Encounter.001.ndjson.gz",
                "Encounter.002.ndjson.gz": "002.2021-09-14/Encounter.001.ndjson.gz",
                "Patient.001.ndjson.gz": "001.2021-09-14/Patient.001.ndjson.gz",
                "Patient.002.ndjson.gz": "002.2021-09-14/Patient.001.ndjson.gz",
                "001.2021-09-14": {
                    ".metadata": None,
                    "log.ndjson": None,
                    "Encounter.001.ndjson.gz": [enc1],
                    "Patient.001.ndjson.gz": [pat1],
                },
                "002.2021-09-14": {
                    ".metadata": None,
                    "log.ndjson": None,
                    "Encounter.001.ndjson.gz": [enc2],
                    "Patient.001.ndjson.gz": [pat2],
                },
                ".metadata": None,
            }
        )

    async def test_nickname(self):
        pat1 = {"resourceType": resources.PATIENT, "id": "pat1"}
        self.mock_bulk("group1", output=[pat1], params={"_type": resources.PATIENT})
        await self.cli(
            "export",
            self.folder,
            "--group=group1",
            "--nickname=my-export",
            f"--type={resources.PATIENT}",
        )

        self.assert_folder(
            {
                "Patient.001.ndjson.gz": "001.my-export/Patient.001.ndjson.gz",
                "001.my-export": {
                    ".metadata": None,
                    "log.ndjson": None,
                    "Patient.001.ndjson.gz": [pat1],
                },
                ".metadata": None,
            }
        )

    async def test_crawl_uses_existing_patients_if_available(self):
        pat1 = {"resourceType": resources.PATIENT, "id": "pat1"}
        self.mock_bulk("group1", output=[pat1], params={"_type": resources.PATIENT})
        await self.cli(
            "export",
            self.folder,
            "--group=group1",
            f"--type={resources.PATIENT}",
        )

        con1 = {"resourceType": resources.CONDITION, "id": "con1"}
        params = {resources.CONDITION: {httpx.QueryParams(patient="pat1"): [con1]}}
        missing = self.set_resource_search_queries(params)
        await self.cli(
            "export",
            self.folder,
            "--group=group1",
            "--export-mode=crawl",
            f"--type={resources.CONDITION}",
        )

        self.assertEqual(missing, [])
        self.assert_folder(
            {
                "Condition.001.ndjson.gz": "002.2021-09-14/Condition.ndjson.gz",
                "Patient.001.ndjson.gz": "001.2021-09-14/Patient.001.ndjson.gz",
                "001.2021-09-14": None,
                "002.2021-09-14": None,
                ".metadata": None,
            }
        )

    async def test_meds_hydration(self):
        """Confirm we automatically run med hydration"""
        pat1 = {"resourceType": resources.PATIENT, "id": "pat1"}
        medreq1 = {
            "resourceType": resources.MEDICATION_REQUEST,
            "id": "medreq1",
            "medicationReference": {"reference": "Medication/1"},
        }
        self.mock_bulk(
            "group1",
            output=[pat1, medreq1],
            params={"_type": f"{resources.MEDICATION_REQUEST},{resources.PATIENT}"},
        )
        self.set_basic_resource_route()
        await self.cli(
            "export",
            self.folder,
            "--group=group1",
            f"--type={resources.PATIENT},{resources.MEDICATION_REQUEST}",
        )

        self.assert_folder(
            {
                "Medication.001.ndjson.gz": "001.2021-09-14/Medication.ndjson.gz",
                "MedicationRequest.001.ndjson.gz": "001.2021-09-14/MedicationRequest.001.ndjson.gz",
                "Patient.001.ndjson.gz": "001.2021-09-14/Patient.001.ndjson.gz",
                "001.2021-09-14": {
                    ".metadata": None,
                    "log.ndjson": None,
                    "Medication.ndjson.gz": [{"resourceType": resources.MEDICATION, "id": "1"}],
                    "MedicationRequest.001.ndjson.gz": None,
                    "Patient.001.ndjson.gz": None,
                },
                ".metadata": None,
            }
        )

    async def test_dxr_results_hydration_together(self):
        """Confirm we correctly handle dxr-results hydration, when DxRs are exported at same time"""
        pat1 = {"resourceType": resources.PATIENT, "id": "pat1"}
        dxr1 = {
            "resourceType": resources.DIAGNOSTIC_REPORT,
            "id": "dxr1",
            "result": [{"reference": "Observation/obs2"}],
        }
        obs1 = {"resourceType": resources.OBSERVATION, "id": "obs1"}

        self.mock_bulk(
            "group1",
            output=[pat1, dxr1, obs1],
            params={
                "_type": f"{resources.DIAGNOSTIC_REPORT},{resources.OBSERVATION},"
                f"{resources.PATIENT}",
                "_typeFilter": utils.DEFAULT_OBS_FILTER,
            },
        )
        self.set_basic_resource_route()
        await self.cli(
            "export",
            self.folder,
            "--group=group1",
            f"--type={resources.PATIENT},{resources.OBSERVATION},{resources.DIAGNOSTIC_REPORT}",
        )

        self.assert_folder(
            {
                "DiagnosticReport.001.ndjson.gz": "001.2021-09-14/DiagnosticReport.001.ndjson.gz",
                "Observation.001.ndjson.gz": "001.2021-09-14/Observation.001.ndjson.gz",
                "Observation.002.ndjson.gz": "001.2021-09-14/Observation.results.ndjson.gz",
                "Patient.001.ndjson.gz": "001.2021-09-14/Patient.001.ndjson.gz",
                "001.2021-09-14": {
                    ".metadata": None,
                    "log.ndjson": None,
                    "DiagnosticReport.001.ndjson.gz": [dxr1],
                    "Observation.results.ndjson.gz": [
                        {"resourceType": resources.OBSERVATION, "id": "obs2"},
                    ],
                    "Observation.001.ndjson.gz": [obs1],
                    "Patient.001.ndjson.gz": [pat1],
                },
                ".metadata": None,
            }
        )

    async def test_dxr_results_hydration_together_via_crawl(self):
        """Run DxReports and then Observations and ensure the observations don't collide"""
        # These resources both download initial observations, but also have DxReport.result
        # and Observation.hasMember links to further observations. All of these should come through.
        pat1 = {"resourceType": resources.PATIENT, "id": "pat1"}
        dxr1 = {
            "resourceType": resources.DIAGNOSTIC_REPORT,
            "id": "dxr1",
            "result": [{"reference": "Observation/res-obs"}],
        }
        obs1 = {
            "resourceType": resources.OBSERVATION,
            "id": "obs1",
            "hasMember": [{"reference": f"{resources.OBSERVATION}/obs1.member"}],
        }
        res_obs = {
            "resourceType": resources.OBSERVATION,
            "id": "res-obs",
            "hasMember": [{"reference": f"{resources.OBSERVATION}/res-obs.member"}],
        }

        def respond(request: httpx.Request, res_type: str, res_id: str, **kwargs) -> httpx.Response:
            if res_type == resources.OBSERVATION and res_id == "res-obs":
                return httpx.Response(200, request=request, json=res_obs)
            else:
                return self.basic_resource(request, res_type, res_id)

        params = {
            resources.DIAGNOSTIC_REPORT: {httpx.QueryParams(patient="pat1"): [dxr1]},
            resources.OBSERVATION: {
                httpx.QueryParams(patient="pat1", category=utils.DEFAULT_OBS_CATEGORIES): [obs1]
            },
        }
        missing = self.set_resource_search_queries(params)
        self.set_resource_route(respond)
        self.mock_bulk("group1", output=[pat1], params={"_type": resources.PATIENT})

        await self.cli(
            "export",
            self.folder,
            "--group=group1",
            "--export-mode=crawl",
            f"--type={resources.PATIENT},{resources.OBSERVATION},{resources.DIAGNOSTIC_REPORT}",
        )

        self.assertEqual(missing, [])
        self.assert_folder(
            {
                "DiagnosticReport.001.ndjson.gz": "001.2021-09-14/DiagnosticReport.ndjson.gz",
                "Observation.001.ndjson.gz": "001.2021-09-14/Observation.members.ndjson.gz",
                "Observation.002.ndjson.gz": "001.2021-09-14/Observation.ndjson.gz",
                "Observation.003.ndjson.gz": "001.2021-09-14/Observation.results.ndjson.gz",
                "Patient.001.ndjson.gz": "001.2021-09-14/Patient.001.ndjson.gz",
                "001.2021-09-14": {
                    ".metadata": None,
                    "log.ndjson": None,
                    "DiagnosticReport.ndjson.gz": [dxr1],
                    "Observation.ndjson.gz": [obs1],
                    "Observation.members.ndjson.gz": [
                        {"resourceType": resources.OBSERVATION, "id": "obs1.member"},
                        {"resourceType": resources.OBSERVATION, "id": "res-obs.member"},
                    ],
                    "Observation.results.ndjson.gz": [res_obs],
                    "Patient.001.ndjson.gz": [pat1],
                },
                ".metadata": None,
            }
        )

    async def test_dxr_results_hydration_separately(self):
        """Confirm we correctly handle dxr-results hydration, with just a DxR export"""
        pat1 = {"resourceType": resources.PATIENT, "id": "pat1"}
        dxr1 = {
            "resourceType": resources.DIAGNOSTIC_REPORT,
            "id": "dxr1",
            "result": [{"reference": "Observation/obs1"}],
        }

        def respond(request: httpx.Request, res_type: str, res_id: str) -> httpx.Response:
            match res_id:
                case "obs1":
                    return self.basic_resource(
                        # Recursive - confirm that we then run obs-members on the results,
                        # despite this not being originally an observation export.
                        request,
                        res_type,
                        res_id,
                        hasMember=[{"reference": "Observation/obs2"}],
                    )
                case "obs2":
                    return self.basic_resource(request, res_type, res_id)
                case _:
                    assert False, f"Wrong res_id {res_id}"

        self.set_resource_route(respond)

        self.mock_bulk(
            "group1",
            output=[pat1, dxr1],
            params={"_type": f"{resources.DIAGNOSTIC_REPORT},{resources.PATIENT}"},
        )

        await self.cli(
            "export",
            self.folder,
            "--group=group1",
            f"--type={resources.PATIENT},{resources.DIAGNOSTIC_REPORT}",
        )

        self.assert_folder(
            {
                "DiagnosticReport.001.ndjson.gz": "001.2021-09-14/DiagnosticReport.001.ndjson.gz",
                "Observation.001.ndjson.gz": "001.2021-09-14/Observation.members.ndjson.gz",
                "Observation.002.ndjson.gz": "001.2021-09-14/Observation.results.ndjson.gz",
                "Patient.001.ndjson.gz": "001.2021-09-14/Patient.001.ndjson.gz",
                "001.2021-09-14": {
                    ".metadata": None,
                    "log.ndjson": None,
                    "DiagnosticReport.001.ndjson.gz": [dxr1],
                    "Observation.members.ndjson.gz": [
                        {"resourceType": resources.OBSERVATION, "id": "obs2"},
                    ],
                    "Observation.results.ndjson.gz": [
                        {
                            "resourceType": resources.OBSERVATION,
                            "id": "obs1",
                            "hasMember": [{"reference": "Observation/obs2"}],
                        },
                    ],
                    "Patient.001.ndjson.gz": [pat1],
                },
                ".metadata": None,
            }
        )

    async def test_wrong_context(self):
        metadata = lifecycle.ManagedMetadata(self.folder)
        metadata.note_context(fhir_url="old-url", group="group1")
        with self.assertRaisesRegex(SystemExit, "is for a different FHIR URL"):
            await self.cli("export", self.folder)

        os.unlink(f"{self.folder}/.metadata")
        metadata = lifecycle.ManagedMetadata(self.folder)
        metadata.note_context(fhir_url=self.url, group="group1")
        with self.assertRaisesRegex(SystemExit, "is for a different Group"):
            await self.cli("export", self.folder, "--group=new-group")

    @ddt.data("bulk", "crawl")
    async def test_interrupted_hydration_will_resume(self, export_mode):
        """Confirm we don't skip the whole resource if we resume from hydration"""
        # Setup
        pat1 = {"resourceType": resources.PATIENT, "id": "pat1"}
        doc1 = {
            "resourceType": resources.DOCUMENT_REFERENCE,
            "id": "doc1",
            "content": [{"attachment": {"url": "Binary/a", "contentType": "text/html"}}],
        }
        if export_mode == "crawl":
            self.mock_bulk("group1", output=[pat1])
        else:
            self.mock_bulk("group1", output=[pat1, doc1])

        params = {
            resources.DOCUMENT_REFERENCE: {httpx.QueryParams(patient="pat1"): [doc1]},
        }
        self.set_resource_search_queries(params)

        def respond(request: httpx.Request, res_type: str, res_id: str) -> httpx.Response:
            if res_type == resources.BINARY and res_id == "a":
                return httpx.Response(
                    200,
                    request=request,
                    content=b"hello",
                    headers={"Content-Type": "text/html; charset=utf8"},
                )
            assert False, f"Wrong res_id {res_id}"

        self.set_resource_route(respond)

        mocker = mock.patch("smart_fetch.tasks.inline.InlineTask.run", side_effect=RuntimeError)
        self.addCleanup(mocker.stop)
        mocker.start()

        if export_mode == "crawl":
            suffix = "ndjson.gz"
            res_time = utils.FROZEN_TIMESTAMP
            extra_files = {"Patient.001.ndjson.gz": "001.2021-09-14/Patient.001.ndjson.gz"}
        else:
            suffix = "001.ndjson.gz"
            res_time = utils.TRANSACTION_TIME
            extra_files = {}

        # First interrupted run
        with self.assertRaises(RuntimeError):
            await self.cli(
                "export",
                self.folder,
                "--group=group1",
                f"--export-mode={export_mode}",
                f"--type={resources.PATIENT},{resources.DOCUMENT_REFERENCE}",
            )
        self.assert_folder(
            {
                "001.2021-09-14": {
                    ".metadata": {
                        # Notice: no complete=true here
                        "done": {
                            resources.DOCUMENT_REFERENCE: res_time,
                            resources.PATIENT: utils.TRANSACTION_TIME,
                        },
                        "filters": {resources.DOCUMENT_REFERENCE: [], resources.PATIENT: []},
                        "since": None,
                        "kind": "output",
                        "timestamp": utils.FROZEN_TIMESTAMP,
                        "version": utils.version,
                    },
                    "log.ndjson": None,
                    f"{resources.PATIENT}.001.ndjson.gz": [pat1],
                    f"{resources.DOCUMENT_REFERENCE}.{suffix}": [doc1],
                },
                ".metadata": None,
                **extra_files,
            }
        )

        # Second run to finish up
        mocker.stop()
        await self.cli(
            "export",
            self.folder,
            "--group=group1",
            f"--export-mode={export_mode}",
            f"--type={resources.PATIENT},{resources.DOCUMENT_REFERENCE}",
        )
        self.assert_folder(
            {
                "001.2021-09-14": {
                    ".metadata": {
                        "complete": True,
                        "done": {
                            resources.DOCUMENT_REFERENCE: res_time,
                            resources.PATIENT: utils.TRANSACTION_TIME,
                        },
                        "filters": {resources.DOCUMENT_REFERENCE: [], resources.PATIENT: []},
                        "since": None,
                        "kind": "output",
                        "timestamp": utils.FROZEN_TIMESTAMP,
                        "version": utils.version,
                    },
                    "log.ndjson": None,
                    f"{resources.PATIENT}.001.ndjson.gz": [pat1],
                    f"{resources.DOCUMENT_REFERENCE}.{suffix}": [
                        {
                            "resourceType": resources.DOCUMENT_REFERENCE,
                            "id": "doc1",
                            "content": [
                                {
                                    "attachment": {
                                        "url": "Binary/a",
                                        "contentType": "text/html; charset=utf8",
                                        "data": "aGVsbG8=",
                                        "hash": "qvTGHdzF6KLavt4PO0gs2a6pQ00=",
                                        "size": 5,
                                    }
                                }
                            ],
                        }
                    ],
                },
                f"{resources.DOCUMENT_REFERENCE}.001.ndjson.gz": (
                    f"001.2021-09-14/{resources.DOCUMENT_REFERENCE}.{suffix}"
                ),
                f"{resources.PATIENT}.001.ndjson.gz": (
                    f"001.2021-09-14/{resources.PATIENT}.001.ndjson.gz"
                ),
                ".metadata": None,
            }
        )

    @ddt.data(
        # First (interrupted), Second (None means same arguments), Whether it should resume)
        (["--type=Condition"], None, True),
        (["--type=Condition"], ["--type=Device"], False),
        (["--type=Condition"], ["--type=Condition,Device"], False),
        (["--type=Condition,Device"], None, True),
        (["--type=Condition,Device"], ["--type=Device"], False),
        (["--type=Condition", "--since=2020-10-10"], None, True),
        (
            ["--type=Condition", "--since=2020-10-10"],
            ["--type=Condition", "--since=2020-10-11"],
            False,
        ),
        (
            ["--type=Condition", "--since=2020-10-10"],
            ["--type=Condition", "--since=2020-10-10", "--since-mode=created"],
            False,
        ),
        (["--type=Condition", "--type-filter=Condition?code=1234"], None, True),
        (
            ["--type=Condition", "--type-filter=Condition?code=1234"],
            [
                "--type=Condition",
                "--type-filter=Condition?code=1234",
                "--type-filter=Condition?code=5678",
            ],
            False,
        ),
        (
            ["--type=Condition", "--since=2020-10-10", "--type-filter=Condition?code=1234"],
            None,
            True,
        ),
        (
            ["--type=Condition", "--since=2020-10-10", "--type-filter=Condition?code=1234"],
            ["--type=Condition", "--since=2020-10-10", "--type-filter=Condition?code=5678"],
            False,
        ),
    )
    @ddt.unpack
    async def test_finds_prev_workdir_to_resume(self, first, second, resumed):
        """Confirm we grab the right old folder to resume"""
        # Make an initial red-herring export
        self.mock_bulk()
        await self.cli("export", self.folder, "--type=DocumentReference")

        # Interrupt our first of two test exports
        with (
            mock.patch("smart_fetch.bulk_utils.BulkExporter.export", side_effect=RuntimeError),
            self.assertRaises(RuntimeError),
        ):
            await self.cli("export", self.folder, *first)

        self.assert_folder(
            {
                "001.2021-09-14": {
                    ".metadata": None,
                    "log.ndjson": None,
                },
                "002.2021-09-14": {".metadata": None},
                ".metadata": None,
            }
        )

        # Now do our second test export, which might resume the previous export
        second_args = first if second is None else second
        await self.cli("export", self.folder, *second_args)

        if resumed:
            self.assert_folder(
                {
                    "001.2021-09-14": {
                        ".metadata": None,
                        "log.ndjson": None,
                    },
                    "002.2021-09-14": {
                        ".metadata": None,
                        "log.ndjson": None,
                    },
                    ".metadata": None,
                }
            )
        else:
            self.assert_folder(
                {
                    "001.2021-09-14": {
                        ".metadata": None,
                        "log.ndjson": None,
                    },
                    "002.2021-09-14": {
                        ".metadata": None,
                    },
                    "003.2021-09-14": {
                        ".metadata": None,
                        "log.ndjson": None,
                    },
                    ".metadata": None,
                }
            )

    async def test_finds_prev_workdir_to_resume_with_nicknames(self):
        """Confirm we grab the right old folder to resume when using nicknames"""
        # Make all exports fail until further notice
        mocker = mock.patch("smart_fetch.bulk_utils.BulkExporter.export", side_effect=RuntimeError)
        self.addCleanup(mocker.stop)
        mocker.start()

        # Make an interrupted export
        with self.assertRaises(RuntimeError):
            await self.cli("export", self.folder, "--type=Condition", "--nickname=test")

        self.assert_folder(
            {
                "001.test": {".metadata": None},
                ".metadata": None,
            }
        )

        # OK let's allow bulk again
        mocker.stop()
        self.mock_bulk()

        # First confirm that if we use the same nickname but with a different set of filters,
        # we correctly error out.
        with self.assertRaisesRegex(SystemExit, "is for a different set of types and/or filters"):
            await self.cli("export", self.folder, f"--type={resources.DEVICE}", "--nickname=test")

        # Actually resume the export.
        await self.cli("export", self.folder, f"--type={resources.CONDITION}", "--nickname=test")

        self.assert_folder(
            {
                "001.test": {
                    ".metadata": None,
                    "log.ndjson": None,  # indicates we finished
                },
                ".metadata": None,
            }
        )

    async def test_error_if_using_old_nicknames(self):
        """Confirm we bail if resuming an old nickname is attempted"""
        self.mock_bulk()

        await self.cli("export", self.folder, "--type=Device", "--nickname=test")
        await self.cli("export", self.folder, "--type=ServiceRequest")

        with self.assertRaisesRegex(SystemExit, "Choose a new nickname."):
            await self.cli("export", self.folder, "--type=Device", "--nickname=test")

    async def test_since_auto(self):
        """Confirm we calculate the right "since" value for since=auto"""
        # Do some initial (empty) exports, marking each with a different transaction time.
        self.mock_bulk(
            params={"_type": f"{resources.CONDITION},{resources.ENCOUNTER}"},
            transaction_time="2001-01-01T00:00:00+00:00",
        )
        await self.cli("export", self.folder, f"--type={resources.CONDITION},{resources.ENCOUNTER}")

        self.mock_bulk(
            params={
                "_type": resources.CONDITION,
                "_typeFilter": f"{resources.CONDITION}?code=1234",
            },
            transaction_time="2002-02-02T00:00:00+00:00",
        )
        await self.cli(
            "export",
            self.folder,
            f"--type={resources.CONDITION}",
            f"--type-filter={resources.CONDITION}?code=1234",
        )

        self.mock_bulk(
            params={
                "_type": resources.CONDITION,
                "_typeFilter": f"{resources.CONDITION}?code=1234,{resources.CONDITION}?code=5678",
            },
            transaction_time="2003-03-03T00:00:00+00:00",
        )
        await self.cli(
            "export",
            self.folder,
            f"--type={resources.CONDITION}",
            f"--type-filter={resources.CONDITION}?code=1234",
            f"--type-filter={resources.CONDITION}?code=5678",
        )

        # Now... Run some --since=auto exports, and we should be seeing the right since values.

        async def assert_since(since, *args):
            stdout, _stderr = await self.capture_cli(
                "export",
                self.folder,
                f"--type={resources.CONDITION}",
                "--since=auto",
                *args,
            )
            self.assertIn(f"Using since value of '{since}'".encode(), stdout)

        # First one is for code 1234 which will base off the 3rd example above (because that was
        # an "OR" search, it's a valid match).
        self.mock_bulk(
            params={
                "_type": resources.CONDITION,
                "_since": "2003-03-03T00:00:00+00:00",
                "_typeFilter": f"{resources.CONDITION}?code=1234",
            },
        )
        await assert_since(
            "2003-03-03T00:00:00+00:00", f"--type-filter={resources.CONDITION}?code=1234"
        )

        # Same thing for 5678
        self.mock_bulk(
            params={
                "_type": resources.CONDITION,
                "_since": "2003-03-03T00:00:00+00:00",
                "_typeFilter": f"{resources.CONDITION}?code=5678",
            },
        )
        await assert_since(
            "2003-03-03T00:00:00+00:00", f"--type-filter={resources.CONDITION}?code=5678"
        )

        # And try an unfiltered search, which should be based off the 1st export above.
        self.mock_bulk(
            params={"_type": resources.CONDITION, "_since": "2001-01-01T00:00:00+00:00"},
            transaction_time="2004-04-04T00:00:00+00:00",
        )
        await assert_since("2001-01-01T00:00:00+00:00")

        # And ANOTHER unfiltered search, which should be based off the export right above.
        self.mock_bulk(params={"_type": resources.CONDITION, "_since": "2004-04-04T00:00:00+00:00"})
        await assert_since("2004-04-04T00:00:00+00:00")

        # And ANOTHER unfiltered search, with a different mode. Should go back to first search.
        self.mock_bulk(
            params={
                "_type": resources.CONDITION,
                "_typeFilter": "Condition?recorded-date=gt2001-01-01T00:00:00+00:00",
            }
        )
        await assert_since(
            "2001-01-01T00:00:00+00:00", "--since-mode=created", "--nickname=created-mode"
        )

        # And one export that includes a new resource, which will trigger a lack of since value.
        self.mock_bulk(params={"_type": "Condition,Device"})
        stdout, _stderr = await self.capture_cli(
            "export",
            self.folder,
            "--type=Condition,Device",
            "--since=auto",
        )
        self.assertNotIn(b"Using since value of", stdout)

    async def test_crawl_since_auto(self):
        """Confirm we use individual since dates for each resource type"""
        # Do some initial (empty) exports, marking each with a different transaction time.
        self.mock_bulk(
            params={"_type": f"{resources.CONDITION},{resources.PATIENT}"},
            output=[{"resourceType": resources.PATIENT, "id": "pat1"}],
            transaction_time="2001-01-01T00:00:00+00:00",
        )
        await self.cli("export", self.folder, "--type=Condition,Patient")

        self.mock_bulk(
            params={"_type": f"{resources.ENCOUNTER}"},
            transaction_time="2002-02-02T00:00:00+00:00",
        )
        await self.cli("export", self.folder, f"--type={resources.ENCOUNTER}")

        # Now... Run a --since=auto export, and we should be seeing different since values.

        params = {
            resources.CONDITION: {
                httpx.QueryParams(patient="pat1", _lastUpdated="gt2001-01-01T00:00:00+00:00"): []
            },
            resources.ENCOUNTER: {
                httpx.QueryParams(patient="pat1", _lastUpdated="gt2002-02-02T00:00:00+00:00"): []
            },
            resources.PROCEDURE: {httpx.QueryParams(patient="pat1"): []},
        }
        self.set_resource_search_queries(params)

        stdout, _stderr = await self.capture_cli(
            "export",
            self.folder,
            f"--type={resources.CONDITION},{resources.ENCOUNTER},{resources.PROCEDURE}",
            "--since=auto",
            "--export-mode=crawl",
        )
        self.assertIn(b"Using since value of '2001-01-01T00:00:00+00:00' for Condition.", stdout)
        self.assertIn(b"Using since value of '2002-02-02T00:00:00+00:00' for Encounter.", stdout)

    async def test_since_auto_not_found(self):
        """Confirm bail if we can't find a previous since target"""
        with self.assertRaisesRegex(SystemExit, "Could not detect a since value to use"):
            await self.cli("export", self.folder, f"--type={resources.CONDITION}", "--since=auto")

    async def test_pointing_at_empty_dir(self):
        self.mock_bulk()
        await self.cli("export", self.folder / "nope", f"--type={resources.CONDITION}")
        self.assert_folder(
            {
                "nope": {
                    "001.2021-09-14": None,
                    ".metadata": None,
                },
            }
        )

    @ddt.data(
        ({}, False),
        ({"rest": [{"mode": "server", "resource": [{"type": "Condition"}]}]}, False),
        (
            {
                "rest": [
                    {
                        "mode": "server",
                        "resource": [
                            {"type": "Condition", "searchParam": [{"name": "recorded-date"}]}
                        ],
                    }
                ]
            },
            True,
        ),
        (
            {
                "rest": [
                    {
                        "mode": "server",
                        "resource": [
                            {"type": "Encounter", "searchParam": [{"name": "recorded-date"}]}
                        ],
                    }
                ]
            },
            False,
        ),
    )
    @ddt.unpack
    async def test_created_mode_detects_search_fields(self, metadata, uses_time):
        self.server.get("metadata").respond(200, json=metadata)

        params = {"_type": "Condition"}
        if uses_time:
            params["_typeFilter"] = "Condition?recorded-date=gt2001-01-01T00:00:00Z"
        self.mock_bulk(params=params)

        await self.cli(
            "export",
            self.folder,
            "--type=Condition",
            "--since-mode=created",
            "--since=2001-01-01T00:00:00Z",
        )

    @mock.patch("smart_fetch.hydrate_utils.process", side_effect=RuntimeError)
    async def test_hydration_tasks_none(self, mock_run):
        """Confirm we can skip all hydration tasks"""
        self.mock_bulk()

        await self.cli("export", self.folder, "--type=Condition", "--hydration-tasks=none")

        # For sanity checking, run one task and confirm we do hit the error
        with self.assertRaises(RuntimeError):
            await self.cli(
                "export", self.folder, "--type=Condition", "--hydration-tasks=practitioner"
            )

    async def test_disable_obs_filter(self):
        # First with the default filters
        self.mock_bulk(params={"_type": "Observation", "_typeFilter": utils.DEFAULT_OBS_FILTER})
        await self.cli("export", self.folder, "--type=Observation")

        # And confirm we can turn them off
        self.mock_bulk(params={"_type": "Observation"})
        await self.cli("export", self.folder, "--type=Observation", "--no-default-filters")

    async def test_bulk_no_compression(self):
        medreq1 = {
            "resourceType": "MedicationRequest",
            "id": "medreq1",
            "medicationReference": {"reference": "Medication/1"},
        }
        self.mock_bulk(output=[medreq1])
        self.set_basic_resource_route()
        await self.cli("export", self.folder, "--type=MedicationRequest", "--no-compression")
        self.assert_folder(
            {
                "Medication.001.ndjson": "001.2021-09-14/Medication.ndjson",
                "MedicationRequest.001.ndjson": "001.2021-09-14/MedicationRequest.001.ndjson",
                "001.2021-09-14": {
                    ".metadata": None,
                    "log.ndjson": None,
                    "Medication.ndjson": [{"resourceType": "Medication", "id": "1"}],
                    "MedicationRequest.001.ndjson": [medreq1],
                },
                ".metadata": None,
            }
        )

    async def test_crawl_no_compression(self):
        pat1 = {"resourceType": "Patient", "id": "pat1"}
        medreq1 = {
            "resourceType": "MedicationRequest",
            "id": "medreq1",
            "medicationReference": {"reference": "Medication/1"},
        }
        params = {"MedicationRequest": {httpx.QueryParams(patient="pat1"): [medreq1]}}
        missing = self.set_resource_search_queries(params)
        self.set_basic_resource_route()
        self.mock_bulk(output=[pat1])

        await self.cli(
            "export",
            self.folder,
            "--export-mode=crawl",
            "--type=MedicationRequest,Patient",
            "--no-compression",
        )

        self.assertEqual(missing, [])
        self.assert_folder(
            {
                "Medication.001.ndjson": "001.2021-09-14/Medication.ndjson",
                "MedicationRequest.001.ndjson": "001.2021-09-14/MedicationRequest.ndjson",
                "Patient.001.ndjson": "001.2021-09-14/Patient.001.ndjson",
                "001.2021-09-14": {
                    ".metadata": None,
                    "log.ndjson": None,
                    "Medication.ndjson": [{"resourceType": "Medication", "id": "1"}],
                    "MedicationRequest.ndjson": [medreq1],
                    "Patient.001.ndjson": [pat1],
                },
                ".metadata": None,
            }
        )
