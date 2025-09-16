from smart_fetch import resources
from tests import utils


class HydrateLocationTests(utils.TestCase):
    async def test_basic(self):
        self.write_res(resources.DEVICE, [{"location": {"reference": "Location/dev1"}}])
        self.write_res(resources.DIAGNOSTIC_REPORT, [{"subject": {"reference": "Location/dxr1"}}])
        self.write_res(
            resources.ENCOUNTER,
            [
                {
                    "hospitalization": {
                        "origin": {"reference": "Location/enc1"},
                        "destination": {"reference": "Location/enc2"},
                    },
                    "location": [{"location": {"reference": "Location/enc3"}}],
                }
            ],
        )
        self.write_res(resources.IMMUNIZATION, [{"location": {"reference": "Location/imm1"}}])
        self.write_res(resources.LOCATION, [{"partOf": {"reference": "Location/loc1"}}])
        self.write_res(resources.OBSERVATION, [{"subject": {"reference": "Location/obs1"}}])
        self.write_res(resources.PRACTITIONER_ROLE, [{"location": [{"reference": "Location/pr1"}]}])
        self.write_res(resources.PROCEDURE, [{"location": {"reference": "Location/proc1"}}])
        self.write_res(
            resources.SERVICE_REQUEST,
            [
                {
                    "subject": {"reference": "Location/servreq1"},
                    "locationReference": [{"reference": "Location/servreq2"}],
                }
            ],
        )
        self.set_basic_resource_route()

        # Do the task!
        await self.cli("hydrate", self.folder, "--tasks=location")

        self.assert_folder(
            {
                "Device.ndjson.gz": None,
                "DiagnosticReport.ndjson.gz": None,
                "Encounter.ndjson.gz": None,
                "Immunization.ndjson.gz": None,
                "Observation.ndjson.gz": None,
                "PractitionerRole.ndjson.gz": None,
                "Procedure.ndjson.gz": None,
                "ServiceRequest.ndjson.gz": None,
                "Location.ndjson.gz": [
                    {
                        "resourceType": "Location",
                        "id": "0",
                        "partOf": {"reference": "Location/loc1"},
                    },
                ],
                "Location.referenced.ndjson.gz": [
                    {"resourceType": "Location", "id": "dev1"},
                    {"resourceType": "Location", "id": "dxr1"},
                    {"resourceType": "Location", "id": "enc1"},
                    {"resourceType": "Location", "id": "enc2"},
                    {"resourceType": "Location", "id": "enc3"},
                    {"resourceType": "Location", "id": "imm1"},
                    {"resourceType": "Location", "id": "loc1"},
                    {"resourceType": "Location", "id": "obs1"},
                    {"resourceType": "Location", "id": "pr1"},
                    {"resourceType": "Location", "id": "proc1"},
                    {"resourceType": "Location", "id": "servreq1"},
                    {"resourceType": "Location", "id": "servreq2"},
                ],
            }
        )
