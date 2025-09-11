import httpx

from smart_fetch import resources
from tests import utils


class HydratePractitionerTests(utils.TestCase):
    async def test_basic(self):
        self.write_res(
            resources.ALLERGY_INTOLERANCE,
            [
                {
                    "recorder": {"reference": "Practitioner/allergy1"},
                    "asserter": {"reference": "Practitioner/allergy2"},
                },
                {
                    "recorder": {"reference": "PractitionerRole/allergy1"},
                    "asserter": {"reference": "PractitionerRole/allergy2"},
                },
            ],
        )
        self.write_res(
            resources.CONDITION,
            [
                {
                    "recorder": {"reference": "Practitioner/cond1"},
                    "asserter": {"reference": "Practitioner/cond2"},
                },
                {
                    "recorder": {"reference": "PractitionerRole/cond1"},
                    "asserter": {"reference": "PractitionerRole/cond2"},
                },
            ],
        )
        self.write_res(
            resources.DIAGNOSTIC_REPORT,
            [
                {
                    "performer": [{"reference": "Practitioner/dxr1"}],
                    "resultsInterpreter": [{"reference": "Practitioner/dxr2"}],
                },
                {
                    "performer": [{"reference": "PractitionerRole/dxr1"}],
                    "resultsInterpreter": [{"reference": "PractitionerRole/dxr2"}],
                },
            ],
        )
        self.write_res(
            resources.DOCUMENT_REFERENCE,
            [
                {
                    "subject": {"reference": "Practitioner/doc1"},
                    "author": [{"reference": "Practitioner/doc2"}],
                    "authenticator": {"reference": "Practitioner/doc3"},
                },
                {
                    "subject": {"reference": "PractitionerRole/doc1"},
                    "author": [{"reference": "PractitionerRole/doc2"}],
                    "authenticator": {"reference": "PractitionerRole/doc3"},
                },
            ],
        )
        self.write_res(
            resources.ENCOUNTER,
            [
                {
                    "participant": [{"individual": {"reference": "Practitioner/enc1"}}],
                },
                {
                    "participant": [{"individual": {"reference": "PractitionerRole/enc1"}}],
                },
            ],
        )
        self.write_res(
            resources.IMMUNIZATION,
            [
                {
                    "performer": [{"actor": {"reference": "Practitioner/imm1"}}],
                },
                {
                    "performer": [{"actor": {"reference": "PractitionerRole/imm1"}}],
                },
            ],
        )
        self.write_res(
            resources.MEDICATION_REQUEST,
            [
                {
                    "reportedReference": {"reference": "Practitioner/medreq1"},
                    "requester": {"reference": "Practitioner/medreq2"},
                    "performer": {"reference": "Practitioner/medreq3"},
                    "recorder": {"reference": "Practitioner/medreq4"},
                },
                {
                    "reportedReference": {"reference": "PractitionerRole/medreq1"},
                    "requester": {"reference": "PractitionerRole/medreq2"},
                    "performer": {"reference": "PractitionerRole/medreq3"},
                    "recorder": {"reference": "PractitionerRole/medreq4"},
                },
            ],
        )
        self.write_res(
            resources.OBSERVATION,
            [
                {
                    "performer": [{"reference": "Practitioner/obs1"}],
                },
                {
                    "performer": [{"reference": "PractitionerRole/obs1"}],
                },
            ],
        )
        self.write_res(
            resources.PATIENT,
            [
                {
                    "generalPractitioner": [{"reference": "Practitioner/pat1"}],
                },
                {
                    "generalPractitioner": [{"reference": "PractitionerRole/pat1"}],
                },
            ],
        )
        self.write_res(
            resources.PRACTITIONER_ROLE,
            [
                {
                    "practitioner": {"reference": "Practitioner/practrole1"},
                },
            ],
        )
        self.write_res(
            resources.PROCEDURE,
            [
                {
                    "recorder": {"reference": "Practitioner/proc1"},
                    "asserter": {"reference": "Practitioner/proc2"},
                    "performer": [{"actor": {"reference": "Practitioner/proc3"}}],
                },
                {
                    "recorder": {"reference": "PractitionerRole/proc1"},
                    "asserter": {"reference": "PractitionerRole/proc2"},
                    "performer": [{"actor": {"reference": "PractitionerRole/proc3"}}],
                },
            ],
        )
        self.write_res(
            resources.SERVICE_REQUEST,
            [
                {
                    "requester": {"reference": "Practitioner/servreq1"},
                    "performer": [{"reference": "Practitioner/servreq2"}],
                },
                {
                    "requester": {"reference": "PractitionerRole/servreq1"},
                    "performer": [{"reference": "PractitionerRole/servreq2"}],
                },
            ],
        )
        self.set_basic_resource_route()

        # Handle searching for roles from practitioners
        def respond(request: httpx.Request, res_type: str) -> httpx.Response:
            if request.url.params == httpx.QueryParams(practitioner="proc1"):
                entries = [
                    {"resource": {"resourceType": resources.PRACTITIONER_ROLE, "id": "searched"}}
                ]
            else:
                entries = []
            return httpx.Response(200, json={"resourceType": resources.BUNDLE, "entry": entries})

        self.set_resource_search_route(respond)

        # Do the task!
        await self.cli("hydrate", self.folder, "--tasks=practitioner")

        self.assert_folder(
            {
                "AllergyIntolerance.ndjson.gz": None,
                "Condition.ndjson.gz": None,
                "DiagnosticReport.ndjson.gz": None,
                "DocumentReference.ndjson.gz": None,
                "Encounter.ndjson.gz": None,
                "Immunization.ndjson.gz": None,
                "Observation.ndjson.gz": None,
                "MedicationRequest.ndjson.gz": None,
                "Patient.ndjson.gz": None,
                "Procedure.ndjson.gz": None,
                "ServiceRequest.ndjson.gz": None,
                "PractitionerRole.ndjson.gz": [
                    {
                        "resourceType": "PractitionerRole",
                        "id": "0",
                        "practitioner": {"reference": "Practitioner/practrole1"},
                    },
                ],
                "Practitioner.referenced.ndjson.gz": [
                    {"resourceType": "Practitioner", "id": "allergy1"},
                    {"resourceType": "Practitioner", "id": "allergy2"},
                    {"resourceType": "Practitioner", "id": "cond1"},
                    {"resourceType": "Practitioner", "id": "cond2"},
                    {"resourceType": "Practitioner", "id": "dxr1"},
                    {"resourceType": "Practitioner", "id": "dxr2"},
                    {"resourceType": "Practitioner", "id": "doc1"},
                    {"resourceType": "Practitioner", "id": "doc2"},
                    {"resourceType": "Practitioner", "id": "doc3"},
                    {"resourceType": "Practitioner", "id": "enc1"},
                    {"resourceType": "Practitioner", "id": "imm1"},
                    {"resourceType": "Practitioner", "id": "medreq1"},
                    {"resourceType": "Practitioner", "id": "medreq2"},
                    {"resourceType": "Practitioner", "id": "medreq3"},
                    {"resourceType": "Practitioner", "id": "medreq4"},
                    {"resourceType": "Practitioner", "id": "obs1"},
                    {"resourceType": "Practitioner", "id": "pat1"},
                    {"resourceType": "Practitioner", "id": "practrole1"},  # unique here
                    {"resourceType": "Practitioner", "id": "proc1"},
                    {"resourceType": "Practitioner", "id": "proc2"},
                    {"resourceType": "Practitioner", "id": "proc3"},
                    {"resourceType": "Practitioner", "id": "servreq1"},
                    {"resourceType": "Practitioner", "id": "servreq2"},
                ],
                "PractitionerRole.referenced.ndjson.gz": [
                    {"resourceType": "PractitionerRole", "id": "allergy1"},
                    {"resourceType": "PractitionerRole", "id": "allergy2"},
                    {"resourceType": "PractitionerRole", "id": "cond1"},
                    {"resourceType": "PractitionerRole", "id": "cond2"},
                    {"resourceType": "PractitionerRole", "id": "dxr1"},
                    {"resourceType": "PractitionerRole", "id": "dxr2"},
                    {"resourceType": "PractitionerRole", "id": "doc1"},
                    {"resourceType": "PractitionerRole", "id": "doc2"},
                    {"resourceType": "PractitionerRole", "id": "doc3"},
                    {"resourceType": "PractitionerRole", "id": "enc1"},
                    {"resourceType": "PractitionerRole", "id": "imm1"},
                    {"resourceType": "PractitionerRole", "id": "medreq1"},
                    {"resourceType": "PractitionerRole", "id": "medreq2"},
                    {"resourceType": "PractitionerRole", "id": "medreq3"},
                    {"resourceType": "PractitionerRole", "id": "medreq4"},
                    {"resourceType": "PractitionerRole", "id": "obs1"},
                    {"resourceType": "PractitionerRole", "id": "pat1"},
                    {"resourceType": "PractitionerRole", "id": "proc1"},
                    {"resourceType": "PractitionerRole", "id": "proc2"},
                    {"resourceType": "PractitionerRole", "id": "proc3"},
                    {"resourceType": "PractitionerRole", "id": "servreq1"},
                    {"resourceType": "PractitionerRole", "id": "servreq2"},
                    {"resourceType": "PractitionerRole", "id": "searched"},  # unique here
                ],
            }
        )

    async def test_searching_edge_cases(self):
        self.write_res(
            resources.PRACTITIONER,
            [{"id": "repeat1"}, {"id": "repeat2"}, {"id": "error"}],
        )

        # Handle searching for roles from practitioners
        def respond(request: httpx.Request, res_type: str) -> httpx.Response:
            if request.url.params.get("practitioner") in {"repeat1", "repeat2"}:
                entries = [{"resource": {"resourceType": "PractitionerRole", "id": "repeated"}}]
            elif request.url.params.get("practitioner") in {"error"}:
                return httpx.Response(400)
            else:
                entries = []
            return httpx.Response(200, json={"resourceType": resources.BUNDLE, "entry": entries})

        self.set_resource_search_route(respond)

        # Do the task!
        await self.cli("hydrate", self.folder, "--tasks=practitioner")

        self.assert_folder(
            {
                "PractitionerRole.referenced.ndjson.gz": [
                    {"resourceType": "PractitionerRole", "id": "repeated"},
                ],
                "Practitioner.ndjson.gz": None,
            }
        )
