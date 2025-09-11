from smart_fetch import resources
from tests import utils


class HydrateOrganizationTests(utils.TestCase):
    async def test_basic(self):
        self.write_res(resources.DEVICE, [{"owner": {"reference": "Organization/dev1"}}])
        self.write_res(
            resources.DIAGNOSTIC_REPORT,
            [
                {
                    "performer": [{"reference": "Organization/dxr1"}],
                    "resultsInterpreter": [{"reference": "Organization/dxr2"}],
                }
            ],
        )
        self.write_res(
            resources.DOCUMENT_REFERENCE,
            [
                {
                    "author": [{"reference": "Organization/doc1"}],
                    "authenticator": {"reference": "Organization/doc2"},
                    "custodian": {"reference": "Organization/doc3"},
                }
            ],
        )
        self.write_res(
            resources.ENCOUNTER,
            [
                {
                    "hospitalization": {
                        "origin": {"reference": "Organization/enc1"},
                        "destination": {"reference": "Organization/enc2"},
                    },
                    "serviceProvider": {"reference": "Organization/enc3"},
                }
            ],
        )
        self.write_res(
            resources.IMMUNIZATION,
            [
                {
                    "manufacturer": {"reference": "Organization/imm1"},
                    "performer": [{"actor": {"reference": "Organization/imm2"}}],
                    "protocolApplied": [{"authority": {"reference": "Organization/imm3"}}],
                }
            ],
        )
        self.write_res(
            resources.LOCATION, [{"managingOrganization": {"reference": "Organization/loc1"}}]
        )
        self.write_res(
            resources.MEDICATION_REQUEST,
            [
                {
                    "reportedReference": {"reference": "Organization/medreq1"},
                    "requester": {"reference": "Organization/medreq2"},
                    "performer": {"reference": "Organization/medreq3"},
                    "dispenseRequest": {"performer": {"reference": "Organization/medreq4"}},
                },
                {
                    # Repeat of later patient line, to confirm it is ignored
                    "requester": {"reference": "Organization/pat3"},
                },
            ],
        )
        self.write_res(resources.OBSERVATION, [{"performer": [{"reference": "Organization/obs1"}]}])
        self.write_res(resources.ORGANIZATION, [{"partOf": {"reference": "Organization/org1"}}])
        self.write_res(
            resources.PATIENT,
            [
                {
                    "contact": [{"organization": {"reference": "Organization/pat1"}}],
                    "generalPractitioner": [{"reference": "Organization/pat2"}],
                    "managingOrganization": {"reference": "Organization/pat3"},
                },
                {
                    # Repeat, to confirm it is ignored
                    "managingOrganization": {"reference": "Organization/pat3"},
                },
            ],
        )
        self.write_res(
            resources.PRACTITIONER,
            [{"qualification": [{"issuer": {"reference": "Organization/pract1"}}]}],
        )
        self.write_res(
            resources.PRACTITIONER_ROLE, [{"organization": {"reference": "Organization/pr1"}}]
        )
        self.write_res(
            resources.PROCEDURE,
            [
                {
                    "performer": [
                        {
                            "actor": {"reference": "Organization/proc1"},
                            "onBehalfOf": {"reference": "Organization/proc2"},
                        }
                    ]
                }
            ],
        )
        self.write_res(
            resources.SERVICE_REQUEST,
            [
                {
                    "requester": {"reference": "Organization/servreq1"},
                    "performer": [{"reference": "Organization/servreq2"}],
                }
            ],
        )
        self.set_basic_resource_route()

        # Do the task!
        await self.cli("hydrate", self.folder, "--tasks=organization")

        self.assert_folder(
            {
                "Device.ndjson.gz": None,
                "DiagnosticReport.ndjson.gz": None,
                "DocumentReference.ndjson.gz": None,
                "Encounter.ndjson.gz": None,
                "Immunization.ndjson.gz": None,
                "Location.ndjson.gz": None,
                "MedicationRequest.ndjson.gz": None,
                "Observation.ndjson.gz": None,
                "Patient.ndjson.gz": None,
                "Practitioner.ndjson.gz": None,
                "PractitionerRole.ndjson.gz": None,
                "Procedure.ndjson.gz": None,
                "ServiceRequest.ndjson.gz": None,
                "Organization.ndjson.gz": [
                    {
                        "resourceType": "Organization",
                        "id": "0",
                        "partOf": {"reference": "Organization/org1"},
                    },
                ],
                "Organization.referenced.ndjson.gz": [
                    {"resourceType": "Organization", "id": "dev1"},
                    {"resourceType": "Organization", "id": "dxr1"},
                    {"resourceType": "Organization", "id": "dxr2"},
                    {"resourceType": "Organization", "id": "doc1"},
                    {"resourceType": "Organization", "id": "doc2"},
                    {"resourceType": "Organization", "id": "doc3"},
                    {"resourceType": "Organization", "id": "enc1"},
                    {"resourceType": "Organization", "id": "enc2"},
                    {"resourceType": "Organization", "id": "enc3"},
                    {"resourceType": "Organization", "id": "imm1"},
                    {"resourceType": "Organization", "id": "imm2"},
                    {"resourceType": "Organization", "id": "imm3"},
                    {"resourceType": "Organization", "id": "loc1"},
                    {"resourceType": "Organization", "id": "medreq1"},
                    {"resourceType": "Organization", "id": "medreq2"},
                    {"resourceType": "Organization", "id": "medreq3"},
                    {"resourceType": "Organization", "id": "medreq4"},
                    {"resourceType": "Organization", "id": "obs1"},
                    {"resourceType": "Organization", "id": "pat1"},
                    {"resourceType": "Organization", "id": "pat2"},
                    {"resourceType": "Organization", "id": "pat3"},
                    {"resourceType": "Organization", "id": "pract1"},
                    {"resourceType": "Organization", "id": "pr1"},
                    {"resourceType": "Organization", "id": "proc1"},
                    {"resourceType": "Organization", "id": "proc2"},
                    {"resourceType": "Organization", "id": "servreq1"},
                    {"resourceType": "Organization", "id": "servreq2"},
                    {"resourceType": "Organization", "id": "org1"},
                ],
            }
        )
