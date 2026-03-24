from tests import utils


class HydrateSpecTests(utils.TestCase):
    async def test_basic(self):
        """Simple Specimen hydration from scratch"""
        self.write_res(
            "DiagnosticReport",
            [{"specimen": [{"reference": "Specimen/dxr1"}]}],
        )
        self.write_res("Observation", [{"specimen": {"reference": "Specimen/obs1"}}])
        self.write_res("ServiceRequest", [{"specimen": [{"reference": "Specimen/servreq1"}]}])
        self.set_basic_resource_route()
        await self.cli("hydrate", self.folder, "--tasks=SPECIMEN")

        self.assert_folder(
            {
                "DiagnosticReport.ndjson.gz": None,
                "Observation.ndjson.gz": None,
                "ServiceRequest.ndjson.gz": None,
                "Specimen.referenced.ndjson.gz": [
                    {"resourceType": "Specimen", "id": "dxr1"},
                    {"resourceType": "Specimen", "id": "obs1"},
                    {"resourceType": "Specimen", "id": "servreq1"},
                ],
            }
        )
