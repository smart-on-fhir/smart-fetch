import ddt
import httpx

import smart_extract
from smart_extract import resources, timing
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
            f"--type={resources.CONDITION},{resources.PATIENT}",
        )

        self.assert_folder(
            {
                "Condition.000.ndjson.gz": (
                    "429c626ef720f128eadc5f68eabc00b9/Condition.000.ndjson.gz"
                ),
                "Patient.000.ndjson.gz": "429c626ef720f128eadc5f68eabc00b9/Patient.000.ndjson.gz",
                "429c626ef720f128eadc5f68eabc00b9": {
                    ".metadata": None,
                    "log.ndjson": None,
                    "Condition.000.ndjson.gz": [con1],
                    "Patient.000.ndjson.gz": [pat1],
                },
                ".metadata": {
                    "kind": "managed",
                    "fhir-url": "http://example.invalid/R4",
                    "group": "group1",
                    "timestamp": timing.now().isoformat(),
                    "version": smart_extract.__version__,
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
                "Condition.000.ndjson.gz": None,
                "Patient.000.ndjson.gz": None,
                "429c626ef720f128eadc5f68eabc00b9": None,
                ".metadata": None,
            }
        )

    async def test_crawls_if_asked(self):
        pat1 = {"resourceType": resources.PATIENT, "id": "pat1"}
        con1 = {"resourceType": resources.CONDITION, "id": "con1"}
        params = {resources.CONDITION: {httpx.QueryParams(patient="pat1"): [con1]}}
        missing = self.set_resource_search_queries(params)
        self.mock_bulk("group1", output=[pat1], params={
            "_type": resources.PATIENT,
        })

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
                "Condition.000.ndjson.gz": None,
                "Patient.000.ndjson.gz": None,
                "429c626ef720f128eadc5f68eabc00b9": None,
                ".metadata": None,
            }
        )

    async def test_second_export(self):
        pat1 = {"resourceType": resources.PATIENT, "id": "pat1"}
        enc1 = {"resourceType": resources.ENCOUNTER, "id": "enc1"}
        self.mock_bulk("group1", output=[pat1, enc1], params={
            "_type": f"{resources.ENCOUNTER},{resources.PATIENT}",
        })
        await self.cli(
            "export",
            self.folder,
            "--group=group1",
            f"--type={resources.ENCOUNTER},{resources.PATIENT}",
        )

        pat2 = {"resourceType": resources.PATIENT, "id": "pat2"}
        enc2 = {"resourceType": resources.ENCOUNTER, "id": "enc2"}
        self.mock_bulk("group1", output=[pat2, enc2], params={
            "_type": f"{resources.ENCOUNTER},{resources.PATIENT}",
            "_since": "2022-10-23",
        })
        await self.cli(
            "export",
            self.folder,
            "--group=group1",
            f"--type={resources.ENCOUNTER},{resources.PATIENT}",
            "--since=2022-10-23",
        )

        self.assert_folder(
            {
                "Encounter.000.ndjson.gz": (
                    "238ec0e72fb1fd8dc9c4c0aa6a92459f/Encounter.000.ndjson.gz"
                ),
                "Encounter.001.ndjson.gz": (
                    "47d55de8ca7736dab2337f20611b8b4d/Encounter.000.ndjson.gz"
                ),
                "Patient.000.ndjson.gz": "238ec0e72fb1fd8dc9c4c0aa6a92459f/Patient.000.ndjson.gz",
                "Patient.001.ndjson.gz": "47d55de8ca7736dab2337f20611b8b4d/Patient.000.ndjson.gz",
                "238ec0e72fb1fd8dc9c4c0aa6a92459f": {
                    ".metadata": None,
                    "log.ndjson": None,
                    "Encounter.000.ndjson.gz": [enc1],
                    "Patient.000.ndjson.gz": [pat1],
                },
                "47d55de8ca7736dab2337f20611b8b4d": {
                    ".metadata": None,
                    "log.ndjson": None,
                    "Encounter.000.ndjson.gz": [enc2],
                    "Patient.000.ndjson.gz": [pat2],
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
                "Patient.000.ndjson.gz": "my-export/Patient.000.ndjson.gz",
                "my-export": {
                    ".metadata": None,
                    "log.ndjson": None,
                    "Patient.000.ndjson.gz": [pat1],
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
                "Condition.000.ndjson.gz": "c0364eeb431653e28217058258792f8b/Condition.ndjson.gz",
                "Patient.000.ndjson.gz": "6fd669b7904f3e1e63674456c6b2bac8/Patient.000.ndjson.gz",
                "6fd669b7904f3e1e63674456c6b2bac8": None,
                "c0364eeb431653e28217058258792f8b": None,
                ".metadata": None,
            }
        )
