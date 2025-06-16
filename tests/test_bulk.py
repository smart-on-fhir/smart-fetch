import ddt
import urllib.parse

import smart_extract
from smart_extract import resources, timing
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
                        "exportId": f"{self.url}/exports/1",
                        "timestamp": timing.now().isoformat(),
                        "eventId": "kickoff",
                        "_client": "smart-extract",
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
                            "responseHeaders": {"content-location": f"{self.url}/exports/1"},
                        },
                    },
                    {
                        "exportId": f"{self.url}/exports/1",
                        "timestamp": timing.now().isoformat(),
                        "eventId": "status_complete",
                        "eventDetail": {
                            "transactionTime": timing.now().astimezone().isoformat(),
                        },
                    },
                    {
                        "exportId": f"{self.url}/exports/1",
                        "timestamp": timing.now().isoformat(),
                        "eventId": "status_page_complete",
                        "eventDetail": {
                            "transactionTime": timing.now().isoformat(),
                            "outputFileCount": 1,
                            "deletedFileCount": 1,
                            "errorFileCount": 1,
                        },
                    },
                    {
                        "exportId": f"{self.url}/exports/1",
                        "timestamp": timing.now().isoformat(),
                        "eventId": "manifest_complete",
                        "eventDetail": {
                            "transactionTime": timing.now().isoformat(),
                            "totalOutputFileCount": 1,
                            "totalDeletedFileCount": 1,
                            "totalErrorFileCount": 1,
                            "totalManifests": 1,
                        },
                    },
                    {
                        "exportId": f"{self.url}/exports/1",
                        "timestamp": timing.now().isoformat(),
                        "eventId": "download_request",
                        "eventDetail": {
                            "fileUrl": f"{self.url}/downloads/output/0",
                            "itemType": "output",
                            "resourceType": "Patient",
                        },
                    },
                    {
                        "exportId": f"{self.url}/exports/1",
                        "timestamp": timing.now().isoformat(),
                        "eventId": "download_complete",
                        "eventDetail": {
                            "fileUrl": f"{self.url}/downloads/output/0",
                            "resourceCount": 1,
                            "fileSize": 41,
                        },
                    },
                    {
                        "exportId": f"{self.url}/exports/1",
                        "timestamp": timing.now().isoformat(),
                        "eventId": "download_request",
                        "eventDetail": {
                            "fileUrl": f"{self.url}/downloads/error/0",
                            "itemType": "error",
                            "resourceType": "OperationOutcome",
                        },
                    },
                    {
                        "exportId": f"{self.url}/exports/1",
                        "timestamp": timing.now().isoformat(),
                        "eventId": "download_complete",
                        "eventDetail": {
                            "fileUrl": f"{self.url}/downloads/error/0",
                            "resourceCount": 1,
                            "fileSize": 50,
                        },
                    },
                    {
                        "exportId": f"{self.url}/exports/1",
                        "timestamp": timing.now().isoformat(),
                        "eventId": "download_request",
                        "eventDetail": {
                            "fileUrl": f"{self.url}/downloads/deleted/0",
                            "itemType": "deleted",
                            "resourceType": "Bundle",
                        },
                    },
                    {
                        "exportId": f"{self.url}/exports/1",
                        "timestamp": timing.now().isoformat(),
                        "eventId": "download_complete",
                        "eventDetail": {
                            "fileUrl": f"{self.url}/downloads/deleted/0",
                            "resourceCount": 1,
                            "fileSize": 95,
                        },
                    },
                    {
                        "exportId": f"{self.url}/exports/1",
                        "timestamp": timing.now().isoformat(),
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
                f"{resources.PATIENT}.000.ndjson": [pat1],
                "error": {
                    f"{resources.OPERATION_OUTCOME}.000.ndjson": [err1],
                },
                "deleted": {
                    f"{resources.BUNDLE}.000.ndjson": [del1],
                },
                ".metadata": {
                    "timestamp": timing.now().isoformat(),
                    "version": smart_extract.__version__,
                    "done": [resources.PATIENT],
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
        type_filter = ",".join(urllib.parse.quote(f) for f in filters)

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
