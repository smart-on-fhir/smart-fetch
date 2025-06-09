from smart_extract import resources
from tests import utils


class FixTests(utils.TestCase):
    async def test_fix_meds_basic(self):
        """Simple meds fix from scratch"""
        self.write_res(
            resources.MEDICATION_REQUEST,
            [
                {"medicationReference": {"reference": "Medication/1"}},
                {"medicationReference": {"reference": "Medication/2"}},
            ],
        )
        await self.cli("fix", self.folder, "meds")

        self.assert_folder(
            {
                resources.MEDICATION_REQUEST: {
                    ".fix.done": {
                        "timestamp": utils.FROZEN_TIMESTAMP,
                        "version": utils.version,
                        "fixes": ["meds"],
                    },
                    f"{resources.MEDICATION}.ndjson.gz": [
                        {"resourceType": resources.MEDICATION, "id": "1"},
                        {"resourceType": resources.MEDICATION, "id": "2"},
                    ],
                    f"{resources.MEDICATION_REQUEST}.ndjson.gz": None,
                },
            }
        )
