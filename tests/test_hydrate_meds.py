import httpx

from smart_extract import resources
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
                ".metadata": {
                    "kind": "output",
                    "timestamp": utils.FROZEN_TIMESTAMP,
                    "version": utils.version,
                    "done": ["meds"],
                },
                f"{resources.MEDICATION}.ndjson.gz": [
                    {"resourceType": resources.MEDICATION, "id": "1"},
                    {"resourceType": resources.MEDICATION, "id": "2"},
                ],
                f"{resources.MEDICATION_REQUEST}.ndjson.gz": med_reqs,
            }
        )

    # This is a general "hydrate plumbing" test that is using MedReqs as an example
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
        await self.cli("hydrate", self.folder, "--hydration-tasks=meds")

        self.assert_folder(
            {
                f"{resources.MEDICATION}.ndjson.gz": [
                    {"resourceType": resources.MEDICATION, "id": "good"},
                ],
                ".metadata": None,
                f"{resources.MEDICATION_REQUEST}.ndjson.gz": None,
            }
        )

    # This is a general "hydrate plumbing" test that is using MedReqs as an example
    async def test_resuming(self):
        """Test that we can pick up where we left off"""
        self.write_res(
            resources.MEDICATION_REQUEST,
            [
                {"medicationReference": {"reference": "Medication/1"}},
                {"medicationReference": {"reference": "Medication/2"}},
            ],
        )
        self.write_res(resources.MEDICATION, [{"id": "1"}])

        def respond(request: httpx.Request, res_type: str, res_id: str) -> httpx.Response:
            if res_id == "2":
                return self.basic_resource(request, res_type, res_id)
            else:
                assert False, f"Wrong res_id {res_id}"

        self.set_resource_route(respond)
        await self.cli("hydrate", self.folder, "--hydration-tasks=meds")

        self.assert_folder(
            {
                f"{resources.MEDICATION}.ndjson.gz": [
                    {"resourceType": resources.MEDICATION, "id": "1"},
                    {"resourceType": resources.MEDICATION, "id": "2"},
                ],
                ".metadata": None,
                f"{resources.MEDICATION_REQUEST}.ndjson.gz": None,
            }
        )

    # This is a general "hydrate plumbing" test that is using MedReqs as an example
    async def test_no_med_reqs(self):
        """Test that we gracefully skip the task when missing MedReqs"""
        await self.cli("hydrate", self.folder, "--hydration-tasks=meds")
        self.assert_folder({})
