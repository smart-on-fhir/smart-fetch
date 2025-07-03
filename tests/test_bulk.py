import contextlib
import io
from collections.abc import AsyncIterator
from unittest import mock

import ddt
import httpx

from smart_fetch import lifecycle, resources
from tests import utils


@ddt.ddt
class BulkTests(utils.TestCase):
    async def test_basic(self):
        pat1 = {"resourceType": resources.PATIENT, "id": "pat1"}
        err1 = {"resourceType": resources.OPERATION_OUTCOME, "id": "err1"}
        del1 = {
            "resourceType": resources.BUNDLE,
            "entry": [
                {"request": {"method": "DELETE", "url": f"{resources.PATIENT}/pat2"}},
            ],
        }

        self.mock_bulk(
            "group1",
            output=[pat1],
            error=[err1],
            deleted=[del1],
            params={
                "_type": resources.PATIENT,
            },
        )

        await self.cli("bulk", self.folder, "--group=group1", "--type", resources.PATIENT)

        self.assert_folder(
            {
                "log.ndjson": [
                    {
                        "exportId": f"{self.dlserver}/exports/1",
                        "timestamp": utils.FROZEN_TIMESTAMP,
                        "eventId": "kickoff",
                        "_client": "smart-fetch",
                        "_clientVersion": "1!0.0.0",
                        "eventDetail": {
                            "exportUrl": f"{self.url}/Group/group1/$export?"
                            f"_type={resources.PATIENT}",
                            "softwareName": None,
                            "softwareVersion": None,
                            "softwareReleaseDate": None,
                            "fhirVersion": None,
                            "requestParameters": {"_type": resources.PATIENT},
                            "errorCode": None,
                            "errorBody": None,
                            "responseHeaders": {"content-location": f"{self.dlserver}/exports/1"},
                        },
                    },
                    {
                        "exportId": f"{self.dlserver}/exports/1",
                        "timestamp": utils.FROZEN_TIMESTAMP,
                        "eventId": "status_complete",
                        "eventDetail": {
                            "transactionTime": utils.TRANSACTION_TIME,
                        },
                    },
                    {
                        "exportId": f"{self.dlserver}/exports/1",
                        "timestamp": utils.FROZEN_TIMESTAMP,
                        "eventId": "status_page_complete",
                        "eventDetail": {
                            "transactionTime": utils.TRANSACTION_TIME,
                            "outputFileCount": 1,
                            "deletedFileCount": 1,
                            "errorFileCount": 1,
                        },
                    },
                    {
                        "exportId": f"{self.dlserver}/exports/1",
                        "timestamp": utils.FROZEN_TIMESTAMP,
                        "eventId": "manifest_complete",
                        "eventDetail": {
                            "transactionTime": utils.TRANSACTION_TIME,
                            "totalOutputFileCount": 1,
                            "totalDeletedFileCount": 1,
                            "totalErrorFileCount": 1,
                            "totalManifests": 1,
                        },
                    },
                    {
                        "exportId": f"{self.dlserver}/exports/1",
                        "timestamp": utils.FROZEN_TIMESTAMP,
                        "eventId": "download_request",
                        "eventDetail": {
                            "fileUrl": f"{self.dlserver}/output/1.0",
                            "itemType": "output",
                            "resourceType": "Patient",
                        },
                    },
                    {
                        "exportId": f"{self.dlserver}/exports/1",
                        "timestamp": utils.FROZEN_TIMESTAMP,
                        "eventId": "download_complete",
                        "eventDetail": {
                            "fileUrl": f"{self.dlserver}/output/1.0",
                            "resourceCount": 1,
                            "fileSize": 41,
                        },
                    },
                    {
                        "exportId": f"{self.dlserver}/exports/1",
                        "timestamp": utils.FROZEN_TIMESTAMP,
                        "eventId": "download_request",
                        "eventDetail": {
                            "fileUrl": f"{self.dlserver}/error/1.0",
                            "itemType": "error",
                            "resourceType": "OperationOutcome",
                        },
                    },
                    {
                        "exportId": f"{self.dlserver}/exports/1",
                        "timestamp": utils.FROZEN_TIMESTAMP,
                        "eventId": "download_complete",
                        "eventDetail": {
                            "fileUrl": f"{self.dlserver}/error/1.0",
                            "resourceCount": 1,
                            "fileSize": 50,
                        },
                    },
                    {
                        "exportId": f"{self.dlserver}/exports/1",
                        "timestamp": utils.FROZEN_TIMESTAMP,
                        "eventId": "download_request",
                        "eventDetail": {
                            "fileUrl": f"{self.dlserver}/deleted/1.0",
                            "itemType": "deleted",
                            "resourceType": "Bundle",
                        },
                    },
                    {
                        "exportId": f"{self.dlserver}/exports/1",
                        "timestamp": utils.FROZEN_TIMESTAMP,
                        "eventId": "download_complete",
                        "eventDetail": {
                            "fileUrl": f"{self.dlserver}/deleted/1.0",
                            "resourceCount": 1,
                            "fileSize": 95,
                        },
                    },
                    {
                        "exportId": f"{self.dlserver}/exports/1",
                        "timestamp": utils.FROZEN_TIMESTAMP,
                        "eventId": "export_complete",
                        "eventDetail": {
                            "files": 3,
                            "resources": 3,
                            "bytes": 186,
                            "attachments": None,
                            "duration": 0,
                        },
                    },
                ],
                f"{resources.PATIENT}.001.ndjson.gz": [pat1],
                "error": {
                    f"{resources.OPERATION_OUTCOME}.001.ndjson.gz": [err1],
                },
                "deleted": {
                    f"{resources.BUNDLE}.001.ndjson.gz": [del1],
                },
                ".metadata": {
                    "kind": "output",
                    "timestamp": utils.FROZEN_TIMESTAMP,
                    "version": utils.version,
                    "done": {resources.PATIENT: utils.TRANSACTION_TIME},
                    "filters": {resources.PATIENT: []},
                    "since": None,
                    "sinceMode": None,
                },
            }
        )

    async def test_since_updated(self):
        # Just confirm we passed in the right parameters and the request is mocked
        self.mock_bulk("group1", params={"_since": "2022-03-23", "_type": resources.DEVICE})
        await self.cli(
            "bulk", self.folder, "--group=group1", "--since=2022-03-23", "--type", resources.DEVICE
        )

    async def test_since_created(self):
        # Just confirm we passed in the right parameters and the request is mocked
        types = ",".join(sorted(resources.PATIENT_TYPES))
        filters = [
            f"{resources.ALLERGY_INTOLERANCE}?date=gt2022-01-05",
            f"{resources.CONDITION}?recorded-date=gt2022-01-05",
            f"{resources.DIAGNOSTIC_REPORT}?issued=gt2022-01-05",
            f"{resources.DOCUMENT_REFERENCE}?date=gt2022-01-05",
            f"{resources.ENCOUNTER}?date=gt2022-01-05",
            f"{resources.IMMUNIZATION}?date=gt2022-01-05",
            f"{resources.MEDICATION_REQUEST}?authoredon=gt2022-01-05",
            f"{resources.OBSERVATION}?category=social-history,vital-signs,imaging,laboratory,"
            f"survey,exam,procedure,therapy,activity&date=gt2022-01-05",
            f"{resources.PROCEDURE}?date=gt2022-01-05",
            f"{resources.SERVICE_REQUEST}?authored=gt2022-01-05",
        ]
        type_filter = ",".join(f.replace(",", "%2C") for f in filters)

        self.mock_bulk(
            "group1",
            params={
                "_type": types,
                "_typeFilter": type_filter,
            },
        )

        await self.cli(
            "bulk", self.folder, "--group=group1", "--since=2022-01-05", "--since-mode=created"
        )

        # Confirm that we wrote out the right "since" metadata value (not encoded in the filters)
        self.assert_folder(
            {
                ".metadata": {
                    "done": {
                        "AllergyIntolerance": utils.TRANSACTION_TIME,
                        "Condition": utils.TRANSACTION_TIME,
                        "Device": utils.TRANSACTION_TIME,
                        "DiagnosticReport": utils.TRANSACTION_TIME,
                        "DocumentReference": utils.TRANSACTION_TIME,
                        "Encounter": utils.TRANSACTION_TIME,
                        "Immunization": utils.TRANSACTION_TIME,
                        "MedicationRequest": utils.TRANSACTION_TIME,
                        "Observation": utils.TRANSACTION_TIME,
                        "Patient": utils.TRANSACTION_TIME,
                        "Procedure": utils.TRANSACTION_TIME,
                        "ServiceRequest": utils.TRANSACTION_TIME,
                    },
                    "filters": {
                        "AllergyIntolerance": [],
                        "Condition": [],
                        "Device": [],
                        "DiagnosticReport": [],
                        "DocumentReference": [],
                        "Encounter": [],
                        "Immunization": [],
                        "MedicationRequest": [],
                        "Observation": [
                            "category=social-history,vital-signs,imaging,laboratory,survey,exam,"
                            "procedure,therapy,activity"
                        ],
                        "Patient": [],
                        "Procedure": [],
                        "ServiceRequest": [],
                    },
                    "kind": "output",
                    "since": "2022-01-05",
                    "sinceMode": "created",
                    "timestamp": utils.FROZEN_TIMESTAMP,
                    "version": utils.version,
                },
                "log.ndjson": None,
            }
        )

    async def test_custom_type_filter(self):
        self.mock_bulk(
            "group1",
            params={
                "_type": resources.PROCEDURE,
                "_typeFilter": f"{resources.PROCEDURE}?code=123",
            },
        )
        await self.cli(
            "bulk",
            self.folder,
            "--group=group1",
            f"--type={resources.PROCEDURE}",
            f"--type-filter={resources.PROCEDURE}?code=123",
        )

    async def test_export_warnings(self):
        self.mock_bulk(
            "group1",
            error=[
                {
                    "resourceType": "OperationOutcome",
                    "issue": [{"severity": "warning", "diagnostics": "warning1"}],
                },
                {
                    "resourceType": "OperationOutcome",
                    "issue": [{"severity": "error", "code": "error1"}],
                },
            ],
            params={"_type": resources.PROCEDURE},
        )

        stdout = io.StringIO()
        with self.assertRaisesRegex(SystemExit, "Errors occurred during export:\n - error1"):
            with contextlib.redirect_stdout(stdout):
                await self.cli(
                    "bulk",
                    self.folder,
                    "--group=group1",
                    f"--type={resources.PROCEDURE}",
                )

        self.assertIn("Messages from server:\n - warning1\n", stdout.getvalue())

    async def test_kickoff_error(self):
        self.mock_bulk("group1", kickoff_response=httpx.Response(404, json={"err": "404"}))
        with self.assertRaisesRegex(SystemExit, "An error occurred when connecting to"):
            await self.cli("bulk", self.folder, "--group=group1")

    @mock.patch("asyncio.sleep", side_effect=RuntimeError("insomnia"))
    @mock.patch("uuid.uuid4", new=lambda: "1234")
    async def test_log_non_network_kickoff_error(self, mock_sleep):
        """Confirm that we log unexpected code/runtime errors"""
        self.mock_bulk("group1", kickoff_response=httpx.Response(500))

        # These errors we don't catch, since they are unexpected programming errors.
        # But we do still log them.
        with self.assertRaisesRegex(RuntimeError, "insomnia"):
            await self.cli("bulk", self.folder, "--group=group1", "--type", resources.CONDITION)

        self.assert_folder(
            {
                ".metadata": None,
                "log.ndjson": [
                    {
                        "exportId": "1234",  # did not have time to get a status URL
                        "timestamp": utils.FROZEN_TIMESTAMP,
                        "eventId": "kickoff",
                        "_client": "smart-fetch",
                        "_clientVersion": "1!0.0.0",
                        "eventDetail": {
                            "exportUrl": f"{self.url}/Group/group1/$export?"
                            f"_type={resources.CONDITION}",
                            "softwareName": None,
                            "softwareVersion": None,
                            "softwareReleaseDate": None,
                            "fhirVersion": None,
                            "requestParameters": {"_type": resources.CONDITION},
                            "errorCode": None,
                            "errorBody": "insomnia",
                            "responseHeaders": None,
                        },
                    },
                ],
            }
        )

    async def test_download_error(self):
        self.mock_bulk("group1", output=[httpx.Response(400)])
        with self.assertRaisesRegex(SystemExit, "An error occurred when connecting to"):
            await self.cli("bulk", self.folder, "--group=group1")

    async def test_download_error_while_streaming(self):
        async def exploding_stream() -> AsyncIterator[bytes]:
            raise ZeroDivisionError("oops")
            yield b"hello"

        self.mock_bulk("group1", output=[httpx.Response(200, stream=exploding_stream())])

        with self.assertRaisesRegex(
            SystemExit, "Error downloading 'http://example.invalid/dl/output/1.0': oops"
        ):
            await self.cli("bulk", self.folder, "--group=group1")

    async def test_unexpected_status_code(self):
        self.mock_bulk("group1", status_response=httpx.Response(204))

        with self.assertRaisesRegex(
            SystemExit, "Unexpected status code 204 from the bulk FHIR export server"
        ):
            await self.cli(
                "bulk",
                self.folder,
                "--group=group1",
            )

    async def test_no_delete_if_interrupted(self):
        """Verify that we don't delete the export on the server if we bail during the export"""
        self.mock_bulk(
            "group1",
            output=[httpx.Response(400)],
            skip_delete=True,
        )

        with self.assertRaises(SystemExit):
            # Would complain about unmocked network call if we tried to delete
            await self.cli("bulk", self.folder, "--group=group1")

    async def test_delete_error_is_ignored(self):
        self.mock_bulk("group1", delete_response=httpx.Response(404))
        # No fatal error
        await self.cli("bulk", self.folder, "--group=group1")

    async def test_resume(self):
        self.mock_bulk("group1", skip_kickoff=True)
        metadata = lifecycle.OutputMetadata(self.folder)
        metadata.set_bulk_status_url(f"{self.dlserver}/exports/1")
        # No complaint about missing kickoff, we just go straight to status checking
        await self.cli("bulk", self.folder)

    async def test_cancel(self):
        self.mock_bulk("group1", skip_kickoff=True, skip_status=True)
        metadata = lifecycle.OutputMetadata(self.folder)
        metadata.set_bulk_status_url(f"{self.dlserver}/exports/1")
        await self.cli("bulk", self.folder, "--cancel")

    async def test_cancel_no_resume(self):
        with self.assertRaisesRegex(
            SystemExit, "You provided --cancel but no in-progress bulk export was found in"
        ):
            await self.cli("bulk", self.folder, "--cancel")

    @mock.patch("asyncio.sleep")
    async def test_timeout(self, mock_sleep):
        headers = [httpx.Response(202, headers={"Retry-After": "300"})] * 9000
        self.mock_bulk("group1", status_response=headers)
        with self.assertRaisesRegex(
            SystemExit, "Timed out waiting for the bulk FHIR export to finish."
        ):
            await self.cli("bulk", self.folder, "--group=group1")

    async def test_skip_done_resources(self):
        metadata = lifecycle.OutputMetadata(self.folder)
        metadata.mark_done(resources.PROCEDURE)
        self.mock_bulk("group1", params={"_type": resources.DEVICE})
        await self.cli(
            "bulk",
            self.folder,
            "--group=group1",
            f"--type={resources.DEVICE},{resources.PROCEDURE}",
        )

    async def test_skip_all_resources(self):
        metadata = lifecycle.OutputMetadata(self.folder)
        metadata.mark_done(resources.DEVICE)
        metadata.mark_done(resources.PROCEDURE)
        await self.cli("bulk", self.folder, f"--type={resources.DEVICE},{resources.PROCEDURE}")

    async def test_bogus_transaction_time(self):
        """Confirm we gracefully handle a bad server transaction time"""
        self.mock_bulk(transaction_time="blarg")
        await self.cli("bulk", self.folder, f"--type={resources.DEVICE}")

        self.assert_folder(
            {
                ".metadata": {
                    "done": {resources.DEVICE: utils.FROZEN_TIMESTAMP},  # <- current time used
                    "kind": "output",
                    "timestamp": utils.FROZEN_TIMESTAMP,
                    "version": utils.version,
                    "filters": {resources.DEVICE: []},
                    "since": None,
                    "sinceMode": None,
                },
                "log.ndjson": None,
            }
        )
