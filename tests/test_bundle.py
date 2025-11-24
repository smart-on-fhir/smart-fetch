import gzip
import json

import httpx

from tests import utils


class BundleTests(utils.TestCase):
    """Tests for both the bundle subcommand and general bundle handling"""

    def setUp(self):
        super().setUp()
        self.bundle_file = f"{self.folder}/Bundle.json.gz"

    async def test_not_dir(self):
        with self.assertRaisesRegex(
            SystemExit, f"Local folder '{self.folder}/nope' does not exist."
        ):
            await self.local_cli("bundle", f"{self.folder}/nope")

    async def test_bundle_exists(self):
        with open(self.bundle_file, "w", encoding="utf8"):
            pass  # just create it

        with self.assertRaisesRegex(
            SystemExit, f"Bundle file '{self.bundle_file}' already exists."
        ):
            await self.local_cli("bundle", self.folder)

    async def test_no_input_files(self):
        with self.assertRaisesRegex(SystemExit, f"No FHIR files found in '{self.folder}'."):
            await self.local_cli("bundle", self.folder)

    async def test_bundle_happy_path(self):
        # Just make a couple dead-simple resources
        self.write_res("Condition", [{}])
        self.write_res("Patient", [{}])
        self.assert_folder(
            {
                "Condition.ndjson.gz": None,
                "Patient.ndjson.gz": None,
            }
        )

        # Bundle those two files up into one
        await self.local_cli("bundle", self.folder)
        self.assert_folder(
            {
                "Bundle.json.gz": None,
            }
        )

        # Confirm we did it well
        with gzip.open(f"{self.folder}/Bundle.json.gz", "rt", encoding="utf8") as f:
            bundle = json.load(f)
        self.assertEqual(
            bundle,
            {
                "resourceType": "Bundle",
                "meta": {"profile": ["http://hl7.org/fhir/R4/StructureDefinition/Bundle"]},
                "type": "collection",
                "timestamp": utils.FROZEN_TIMESTAMP,
                "entry": [
                    {"resource": {"resourceType": "Condition", "id": "0"}},
                    {"resource": {"resourceType": "Patient", "id": "0"}},
                ],
            },
        )

    async def test_crawl_can_bundle(self):
        pat = {"resourceType": "Patient", "id": "pat"}
        proc = {"resourceType": "Procedure", "id": "proc"}
        params = {"Procedure": {httpx.QueryParams(patient="pat"): [proc]}}
        missing = self.set_resource_search_queries(params)
        self.mock_bulk(output=[pat], params={"_type": "Patient"})

        await self.cli("crawl", self.folder, "--bundle", "--type=Patient,Procedure")

        self.assertEqual(missing, [])
        self.assert_folder(
            {
                ".metadata": None,
                "log.ndjson": None,
                "Bundle.json.gz": None,
            }
        )

    async def test_bulk_can_bundle(self):
        pat = {"resourceType": "Patient", "id": "pat"}
        proc = {"resourceType": "Procedure", "id": "proc"}
        self.mock_bulk(output=[pat, proc], params={"_type": "Patient,Procedure"})

        await self.cli("bulk", self.folder, "--bundle", "--type=Patient,Procedure")

        self.assert_folder(
            {
                ".metadata": None,
                "log.ndjson": None,
                "Bundle.json.gz": None,
            }
        )

    async def test_non_compressed(self):
        self.write_res("Condition", [{}])
        await self.local_cli("bundle", "--no-compression", self.folder)
        self.assert_folder({"Bundle.json": None})

    async def test_crawl_bundle_non_compressed(self):
        pat = {"resourceType": "Patient", "id": "pat"}
        self.mock_bulk(output=[pat], params={"_type": "Patient"})
        await self.cli("crawl", self.folder, "--bundle", "--type=Patient", "--no-compression")
        self.assert_folder(
            {
                ".metadata": None,
                "log.ndjson": None,
                "Bundle.json": None,
            }
        )

    async def test_bulk_bundle_non_compressed(self):
        pat = {"resourceType": "Patient", "id": "pat"}
        self.mock_bulk(output=[pat], params={"_type": "Patient"})
        await self.cli("bulk", self.folder, "--bundle", "--type=Patient", "--no-compression")
        self.assert_folder(
            {
                ".metadata": None,
                "log.ndjson": None,
                "Bundle.json": None,
            }
        )
