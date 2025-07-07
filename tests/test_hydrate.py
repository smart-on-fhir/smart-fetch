import contextlib
import io
from unittest import mock

import ddt
import httpx

from smart_fetch import resources
from tests import utils


@ddt.ddt
class HydrateTests(utils.TestCase):
    """General hydrate tests, not specific to a specific task"""

    @mock.patch("asyncio.sleep")
    async def test_edge_cases(self, mock_sleep):
        """Odd ball issues"""
        self.write_res(
            resources.MEDICATION_REQUEST,
            [
                {"medicationReference": {"reference": "Medication/good"}},
                {"medicationReference": {"reference": "Medication/404-answer"}},
                {"medicationReference": {"reference": "Medication/500-answer"}},
                {"medicationReference": {"reference": "Medication/wrong-type-answer"}},
                {"medicationReference": {"reference": "Binary/wrong-type"}},
                {"medicationReference": {"reference": "#contained-reference"}},
            ],
        )

        def respond(request: httpx.Request, res_type: str, res_id: str) -> httpx.Response:
            match res_id:
                case "good":
                    return self.basic_resource(request, res_type, res_id)
                case "404-answer":
                    return httpx.Response(404)
                case "500-answer":
                    return httpx.Response(500)
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
                f"{resources.MEDICATION_REQUEST}.ndjson.gz": None,
            }
        )

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
                f"{resources.MEDICATION_REQUEST}.ndjson.gz": None,
            }
        )

    async def test_no_input(self):
        """Test that we gracefully skip the task when missing input sources"""
        await self.cli("hydrate", self.folder, "--hydration-tasks=obs-members")
        self.assert_folder({})

    @ddt.data(
        ("help", 0),
        ("bogus", 2),
    )
    @ddt.unpack
    async def test_task_parsing_help(self, arg, exit_code):
        stdout = io.StringIO()
        with self.assertRaises(SystemExit) as cm:
            with contextlib.redirect_stdout(stdout):
                await self.cli("hydrate", self.folder, f"--hydration-tasks={arg}")
        self.assertEqual(cm.exception.code, exit_code)
        self.assertIn("These hydration tasks are supported:", stdout.getvalue())

    async def test_unexpected_error(self):
        """We should complain loudly about this - shouldn't happen"""
        # Write so many MedReqs out, so we can exercise our queue-draining code
        self.write_res(
            resources.MEDICATION_REQUEST,
            [
                {"medicationReference": {"reference": "Medication/0"}},
                {"medicationReference": {"reference": "Medication/1"}},
                {"medicationReference": {"reference": "Medication/2"}},
                {"medicationReference": {"reference": "Medication/3"}},
                {"medicationReference": {"reference": "Medication/4"}},
                {"medicationReference": {"reference": "Medication/5"}},
                {"medicationReference": {"reference": "Medication/6"}},
                {"medicationReference": {"reference": "Medication/7"}},
                {"medicationReference": {"reference": "Medication/8"}},
                {"medicationReference": {"reference": "Medication/9"}},
                {"medicationReference": {"reference": "Medication/10"}},
            ],
        )

        async def slow_explode(client, id_pool, reference, expected_type):
            raise RuntimeError("oops")

        with self.assertRaisesRegex(SystemExit, "oops"):
            with mock.patch("smart_fetch.hydrate_utils.download_reference", new=slow_explode):
                await self.cli("hydrate", self.folder, "--hydration-tasks=meds")
