import os

import httpx

from smart_fetch import resources
from tests import utils


class HydrateObsMemberTests(utils.TestCase):
    async def test_basic(self):
        """Simple obs-members task from scratch"""
        obs = [
            {"hasMember": [{"reference": "Observation/a1"}, {"reference": "Observation/a2"}]},
            {"hasMember": [{"reference": "Observation/b"}]},
        ]
        self.write_res(resources.OBSERVATION, obs)
        self.set_basic_resource_route()
        await self.cli("hydrate", self.folder, "--hydration-tasks=obs-members")

        self.assert_folder(
            {
                f"{resources.OBSERVATION}.ndjson.gz": obs,
                f"{resources.OBSERVATION}.members.ndjson.gz": [
                    {"resourceType": resources.OBSERVATION, "id": "a1"},
                    {"resourceType": resources.OBSERVATION, "id": "a2"},
                    {"resourceType": resources.OBSERVATION, "id": "b"},
                ],
            }
        )

    async def test_nested_members(self):
        """Confirm we go as deep as needed"""
        self.write_res(resources.OBSERVATION, [{"hasMember": [{"reference": "Observation/a"}]}])

        def respond(request: httpx.Request, res_type: str, res_id: str) -> httpx.Response:
            match res_id:
                case "a":
                    return self.basic_resource(
                        request,
                        res_type,
                        res_id,
                        hasMember=[{"reference": "Observation/b"}, {"reference": "Observation/c"}],
                    )
                case "b":
                    return self.basic_resource(
                        # Recursive - confirm this doesn't mess us up
                        request,
                        res_type,
                        res_id,
                        hasMember=[{"reference": "Observation/a"}],
                    )
                case "c":
                    return self.basic_resource(
                        request, res_type, res_id, hasMember=[{"reference": "Observation/d"}]
                    )
                case "d":
                    return self.basic_resource(request, res_type, res_id)
                case _:
                    assert False, f"Wrong res_id {res_id}"

        self.set_resource_route(respond)
        await self.cli("hydrate", self.folder, "--hydration-tasks=obs-members")

        self.assert_folder(
            {
                f"{resources.OBSERVATION}.ndjson.gz": [
                    {
                        "resourceType": resources.OBSERVATION,
                        "id": "0",
                        "hasMember": [{"reference": "Observation/a"}],
                    },
                ],
                f"{resources.OBSERVATION}.members.ndjson.gz": [
                    {
                        "resourceType": resources.OBSERVATION,
                        "id": "a",
                        "hasMember": [
                            {"reference": "Observation/b"},
                            {"reference": "Observation/c"},
                        ],
                    },
                    {
                        "resourceType": resources.OBSERVATION,
                        "id": "b",
                        "hasMember": [{"reference": "Observation/a"}],
                    },
                    {
                        "resourceType": resources.OBSERVATION,
                        "id": "c",
                        "hasMember": [{"reference": "Observation/d"}],
                    },
                    {
                        "resourceType": resources.OBSERVATION,
                        "id": "d",
                    },
                ],
            }
        )


class HydrateObsDxrTests(utils.TestCase):
    async def test_basic(self):
        """Simple dxr-results task from scratch"""
        dxr = [
            {"result": [{"reference": "Observation/a"}, {"reference": "Observation/b"}]},
            {"result": [{"reference": "Observation/c"}]},
        ]
        self.write_res(resources.DIAGNOSTIC_REPORT, dxr)
        self.set_basic_resource_route()
        await self.cli("hydrate", self.folder, "--hydration-tasks=dxr-results")

        self.assert_folder(
            {
                f"{resources.DIAGNOSTIC_REPORT}.ndjson.gz": dxr,
                f"{resources.OBSERVATION}.results.ndjson.gz": [
                    {"resourceType": resources.OBSERVATION, "id": "a"},
                    {"resourceType": resources.OBSERVATION, "id": "b"},
                    {"resourceType": resources.OBSERVATION, "id": "c"},
                ],
            }
        )

    async def test_separate_dirs(self):
        """Confirm we can read the DxReports from elsewhere"""
        dxr = [
            {"result": [{"reference": "Observation/a"}]},
        ]
        self.write_res(resources.DIAGNOSTIC_REPORT, dxr, subfolder="elsewhere")
        self.set_basic_resource_route()
        await self.cli(
            "hydrate",
            self.folder,
            "--hydration-tasks=dxr-results",
            "--source-dir",
            os.path.join(self.folder, "elsewhere"),
        )

        self.assert_folder(
            {
                "elsewhere": {
                    f"{resources.DIAGNOSTIC_REPORT}.ndjson.gz": dxr,
                },
                f"{resources.OBSERVATION}.results.ndjson.gz": [
                    {"resourceType": resources.OBSERVATION, "id": "a"},
                ],
            }
        )
