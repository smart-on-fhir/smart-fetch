import os

import httpx

from smart_extract import resources
from tests import utils


class FixObsMemberTests(utils.TestCase):
    async def test_basic(self):
        """Simple obs-members fix from scratch"""
        obs = [
            {"hasMember": [{"reference": "Observation/a1"}, {"reference": "Observation/a2"}]},
            {"hasMember": [{"reference": "Observation/b"}]},
        ]
        self.write_res(resources.OBSERVATION, obs)
        self.set_basic_resource_route()
        await self.cli("fix", self.folder, "obs-members")

        self.assert_folder(
            {
                ".metadata": {
                    "kind": "output",
                    "timestamp": utils.FROZEN_TIMESTAMP,
                    "version": utils.version,
                    "done": ["obs-members"],
                },
                f"{resources.OBSERVATION}.ndjson.gz": [
                    *obs,
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
        await self.cli("fix", self.folder, "obs-members")

        self.assert_folder(
            {
                f"{resources.OBSERVATION}.ndjson.gz": [
                    {
                        "resourceType": resources.OBSERVATION,
                        "id": "0",
                        "hasMember": [{"reference": "Observation/a"}],
                    },
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
                ".metadata": None,
            }
        )


class FixObsDxrTests(utils.TestCase):
    async def test_basic(self):
        """Simple obs-dxr fix from scratch"""
        dxr = [
            {"result": [{"reference": "Observation/a"}, {"reference": "Observation/b"}]},
            {"result": [{"reference": "Observation/c"}]},
        ]
        self.write_res(resources.DIAGNOSTIC_REPORT, dxr)
        self.set_basic_resource_route()
        await self.cli("fix", self.folder, "obs-dxr")

        self.assert_folder(
            {
                ".metadata": {
                    "kind": "output",
                    "timestamp": utils.FROZEN_TIMESTAMP,
                    "version": utils.version,
                    "done": ["obs-dxr"],
                },
                f"{resources.DIAGNOSTIC_REPORT}.ndjson.gz": dxr,
                f"{resources.OBSERVATION}.ndjson.gz": [
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
            "fix", self.folder, "obs-dxr", "--source-dir", os.path.join(self.folder, "elsewhere")
        )

        self.assert_folder(
            {
                "elsewhere": {
                    f"{resources.DIAGNOSTIC_REPORT}.ndjson.gz": dxr,
                },
                ".metadata": None,
                f"{resources.OBSERVATION}.ndjson.gz": [
                    {"resourceType": resources.OBSERVATION, "id": "a"},
                ],
            }
        )
