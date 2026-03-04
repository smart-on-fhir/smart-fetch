from tests import utils


class HydrateEpOfCareTests(utils.TestCase):
    async def test_basic(self):
        """Simple EpisodeOfCare hydration from scratch"""
        self.write_res(
            "DocumentReference",
            [{"context": {"encounter": [{"reference": "EpisodeOfCare/docref1"}]}}],
        )
        self.write_res("Encounter", [{"episodeOfCare": [{"reference": "EpisodeOfCare/enc1"}]}])
        self.write_res("MedicationDispense", [{"context": {"reference": "EpisodeOfCare/meddisp1"}}])
        self.set_basic_resource_route()
        await self.cli("hydrate", self.folder, "--tasks=EpisodeOfCare")

        self.assert_folder(
            {
                "EpisodeOfCare.referenced.ndjson.gz": [
                    {"resourceType": "EpisodeOfCare", "id": "docref1"},
                    {"resourceType": "EpisodeOfCare", "id": "enc1"},
                    {"resourceType": "EpisodeOfCare", "id": "meddisp1"},
                ],
                "DocumentReference.ndjson.gz": None,
                "Encounter.ndjson.gz": None,
                "MedicationDispense.ndjson.gz": None,
            }
        )
