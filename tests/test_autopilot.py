import ddt

import smart_extract
from smart_extract import resources, timing
from tests import utils


@ddt.ddt
class AutopilotTests(utils.TestCase):
    async def test_basic(self):
        pat1 = {"resourceType": resources.PATIENT, "id": "pat1"}
        con1 = {"resourceType": resources.CONDITION, "id": "con1"}

        self.mock_bulk(
            "group1",
            output=[pat1, con1],
            params={
                "_type": f"{resources.CONDITION},{resources.PATIENT}",
            },
        )

        await self.cli(
            "autopilot",
            self.folder,
            "--group=group1",
            f"--type={resources.CONDITION},{resources.PATIENT}",
        )

        self.assert_folder(
            {
                "Condition.000.ndjson.gz": (
                    "429c626ef720f128eadc5f68eabc00b9/Condition.000.ndjson.gz"
                ),
                "Patient.000.ndjson.gz": (
                    "429c626ef720f128eadc5f68eabc00b9/Patient.000.ndjson.gz"
                ),
                "429c626ef720f128eadc5f68eabc00b9": {
                    ".metadata": None,
                    "log.ndjson": None,
                    "Condition.000.ndjson.gz": [con1],
                    "Patient.000.ndjson.gz": [pat1],
                },
                ".metadata": {
                    "kind": "managed",
                    "fhir-url": "http://example.invalid/R4",
                    "group": "group1",
                    "timestamp": timing.now().isoformat(),
                    "version": smart_extract.__version__,
                },
            }
        )
