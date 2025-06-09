import httpx

from smart_extract import resources
from tests import utils


class FixMedsTests(utils.TestCase):
    async def test_basic(self):
        """Simple meds fix from scratch"""
        med_reqs = [
            {"medicationReference": {"reference": "Medication/1"}},
            {"medicationReference": {"reference": "Medication/2"}},
        ]
        self.write_res(resources.MEDICATION_REQUEST, med_reqs)
        self.set_basic_resource_route()
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
                    f"{resources.MEDICATION_REQUEST}.ndjson.gz": med_reqs,
                },
            }
        )

    async def test_edge_cases(self):
        """Odd ball issues"""
        self.write_res(
            resources.MEDICATION_REQUEST,
            [
                {"medicationReference": {"reference": "Medication/good"}},
                {"medicationReference": {"reference": "Medication/404-answer"}},
                {"medicationReference": {"reference": "Medication/wrong-type-answer"}},
                {"medicationReference": {"reference": "Binary/wrong-type"}},
            ],
        )

        def respond(request: httpx.Request, res_type: str, res_id: str) -> httpx.Response:
            match res_id:
                case "good":
                    return self.basic_resource(request, res_type, res_id)
                case "404-answer":
                    return httpx.Response(404)
                case "wrong-type-answer":
                    return self.basic_resource(request, resources.OPERATION_OUTCOME, "error")
                case _:
                    assert False, f"Wrong res_id {res_id}"

        self.set_resource_route(respond)
        await self.cli("fix", self.folder, "meds")

        self.assert_folder(
            {
                resources.MEDICATION_REQUEST: {
                    f"{resources.MEDICATION}.ndjson.gz": [
                        {"resourceType": resources.MEDICATION, "id": "good"},
                    ],
                    ".fix.done": None,
                    f"{resources.MEDICATION_REQUEST}.ndjson.gz": None,
                },
            }
        )
