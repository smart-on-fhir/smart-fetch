from smart_extract import resources
from tests import utils


class CommandLineTests(utils.TestCase):
    """General CLI tests"""
    async def test_limit_resources_to_server(self):
        pat1 = {"resourceType": resources.PATIENT, "id": "pat1"}
        con1 = {"resourceType": resources.CONDITION, "id": "con1"}

        self.mock_bulk(
            "group1",
            output=[pat1, con1],
            params={"_type": f"{resources.CONDITION},{resources.PATIENT}"},
        )
        self.server.get("metadata").respond(200, json={
            "rest": [
                {
                    "mode": "server",
                    "resource": [{"type": resources.PATIENT}, {"type": resources.CONDITION}],
                },
            ],
        })

        await self.cli("bulk", self.folder, "--group=group1",
                       f"--type={resources.PATIENT},{resources.CONDITION},{resources.DEVICE}")

        self.assert_folder(
            {
                "log.ndjson": None,
                f"{resources.CONDITION}.000.ndjson.gz": None,
                f"{resources.PATIENT}.000.ndjson.gz": None,
                ".metadata": None,
            }
        )
