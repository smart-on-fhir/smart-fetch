from smart_fetch import resources
from tests import utils


class HydrateMedsTests(utils.TestCase):
    async def test_basic(self):
        """Simple meds hydration from scratch"""
        self.write_res(
            resources.MEDICATION_DISPENSE,
            [
                {"medicationReference": {"reference": "Medication/meddisp1"}},
            ],
        )
        self.write_res(
            resources.MEDICATION_REQUEST,
            [
                {"medicationReference": {"reference": "Medication/medreq1"}},
            ],
        )
        self.set_basic_resource_route()
        await self.cli("hydrate", self.folder, "--tasks=medication")

        self.assert_folder(
            {
                "Medication.referenced.ndjson.gz": [
                    {"resourceType": "Medication", "id": "meddisp1"},
                    {"resourceType": "Medication", "id": "medreq1"},
                ],
                "MedicationDispense.ndjson.gz": None,
                "MedicationRequest.ndjson.gz": None,
            }
        )
