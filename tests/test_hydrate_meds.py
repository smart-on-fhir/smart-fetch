from smart_fetch import resources
from tests import utils


class HydrateMedsTests(utils.TestCase):
    async def test_basic(self):
        """Simple meds hydration from scratch"""
        med_reqs = [
            {"medicationReference": {"reference": "Medication/1"}},
            {"medicationReference": {"reference": "Medication/2"}},
        ]
        self.write_res(resources.MEDICATION_REQUEST, med_reqs)
        self.set_basic_resource_route()
        await self.cli("hydrate", self.folder, "--hydration-tasks=meds")

        self.assert_folder(
            {
                f"{resources.MEDICATION}.ndjson.gz": [
                    {"resourceType": resources.MEDICATION, "id": "1"},
                    {"resourceType": resources.MEDICATION, "id": "2"},
                ],
                f"{resources.MEDICATION_REQUEST}.ndjson.gz": med_reqs,
            }
        )
