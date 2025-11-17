import glob
import os

from tests import utils


class SymlinkTests(utils.TestCase):
    """Tests for both reset-symlinks and general symlink handling"""

    async def test_not_a_managed_dir(self):
        with self.assertRaisesRegex(
            SystemExit,
            f"Folder '{self.folder}' does not look like a SMART Fetch managed export folder.",
        ):
            await self.local_cli("reset-symlinks", self.folder)

    async def test_basic_reset_symlinks(self):
        enc = {"resourceType": "Encounter", "id": "con"}
        self.mock_bulk(output=[enc], params={"_type": "Encounter"})
        await self.cli("export", self.folder, "--type=Encounter")

        # Delete all current symlinks
        for path in glob.glob(f"{self.folder}/*.ndjson.gz"):
            os.unlink(path)
        self.assert_folder(
            {
                "001.2021-09-14": None,
                ".metadata": None,
            }
        )

        # Make them again
        await self.local_cli("reset-symlinks", self.folder)
        self.assert_folder(
            {
                "Encounter.001.ndjson.gz": "001.2021-09-14/Encounter.001.ndjson.gz",
                "001.2021-09-14": None,
                ".metadata": None,
            }
        )

    async def test_filters_are_independent(self):
        """Verify that different filters for the same resource are independent"""
        con = {"resourceType": "Condition", "id": "con"}

        # First full export with code=1234
        self.mock_bulk(
            output=[con],
            params={"_type": "Condition", "_typeFilter": "Condition?code=1234"},
        )
        await self.cli(
            "export", self.folder, "--type=Condition", "--type-filter=Condition?code=1234"
        )
        self.assert_folder(
            {
                "Condition.001.ndjson.gz": "001.2021-09-14/Condition.001.ndjson.gz",
                "001.2021-09-14": None,
                ".metadata": None,
            }
        )

        # Second full export but with severity=abcd this time
        self.mock_bulk(
            output=[con],
            params={"_type": "Condition", "_typeFilter": "Condition?severity=abcd"},
        )
        await self.cli(
            "export", self.folder, "--type=Condition", "--type-filter=Condition?severity=abcd"
        )
        self.assert_folder(
            {
                "Condition.001.ndjson.gz": "001.2021-09-14/Condition.001.ndjson.gz",
                "Condition.002.ndjson.gz": "002.2021-09-14/Condition.001.ndjson.gz",
                "001.2021-09-14": None,
                "002.2021-09-14": None,
                ".metadata": None,
            }
        )

        # An incremental export with code=1234 this time
        self.mock_bulk(
            output=[con],
            params={
                "_type": "Condition",
                "_typeFilter": "Condition?code=1234",
                "_since": "2021-10-10",
            },
        )
        await self.cli(
            "export",
            self.folder,
            "--type=Condition",
            "--type-filter=Condition?code=1234",
            "--since=2021-10-10",
        )
        self.assert_folder(
            {
                "Condition.001.ndjson.gz": "001.2021-09-14/Condition.001.ndjson.gz",
                "Condition.002.ndjson.gz": "002.2021-09-14/Condition.001.ndjson.gz",
                "Condition.003.ndjson.gz": "003.2021-09-14/Condition.001.ndjson.gz",
                "001.2021-09-14": None,
                "002.2021-09-14": None,
                "003.2021-09-14": None,
                ".metadata": None,
            }
        )

        # Another full for severity=abcd
        self.mock_bulk(
            output=[con],
            params={"_type": "Condition", "_typeFilter": "Condition?severity=abcd"},
        )
        await self.cli(
            "export", self.folder, "--type=Condition", "--type-filter=Condition?severity=abcd"
        )
        self.assert_folder(
            {
                # Note the missing link for the 002.* export
                "Condition.001.ndjson.gz": "001.2021-09-14/Condition.001.ndjson.gz",
                "Condition.002.ndjson.gz": "003.2021-09-14/Condition.001.ndjson.gz",
                "Condition.003.ndjson.gz": "004.2021-09-14/Condition.001.ndjson.gz",
                "001.2021-09-14": None,
                "002.2021-09-14": None,
                "003.2021-09-14": None,
                "004.2021-09-14": None,
                ".metadata": None,
            }
        )

        # And finally, clear all with a new export without a type filter
        self.mock_bulk(output=[con], params={"_type": "Condition"})
        await self.cli("export", self.folder, "--type=Condition")
        self.assert_folder(
            {
                "Condition.001.ndjson.gz": "005.2021-09-14/Condition.001.ndjson.gz",
                "001.2021-09-14": None,
                "002.2021-09-14": None,
                "003.2021-09-14": None,
                "004.2021-09-14": None,
                "005.2021-09-14": None,
                ".metadata": None,
            }
        )

    async def test_hydration_from_multiple_sources(self):
        """Verify that we notice hydration results from everywhere they could come from"""
        obs = {
            "resourceType": "Observation",
            "id": "obs",
            "hasMember": [{"reference": "Observation/obs2"}],
            "subject": {"reference": "Location/loc"},
        }
        proc = {
            "resourceType": "Procedure",
            "id": "proc",
            "location": {"reference": "Location/loc"},
        }
        self.set_basic_resource_route()  # to cover all the reference downloads

        # One full obs export
        self.mock_bulk(
            output=[obs], params={"_type": "Observation", "_typeFilter": utils.DEFAULT_OBS_FILTER}
        )
        await self.cli("export", self.folder, "--type=Observation")

        # And a full proc export
        self.mock_bulk(output=[proc], params={"_type": "Procedure"})
        await self.cli("export", self.folder, "--type=Procedure")

        # And a second full obs export, obsoleting any Location results from the first obs export
        self.mock_bulk(
            output=[obs], params={"_type": "Observation", "_typeFilter": utils.DEFAULT_OBS_FILTER}
        )
        await self.cli("export", self.folder, "--type=Observation")

        self.assert_folder(
            {
                "Location.001.ndjson.gz": "002.2021-09-14/Location.referenced.ndjson.gz",
                "Location.002.ndjson.gz": "003.2021-09-14/Location.referenced.ndjson.gz",
                "Observation.001.ndjson.gz": "003.2021-09-14/Observation.001.ndjson.gz",
                "Observation.002.ndjson.gz": "003.2021-09-14/Observation.members.ndjson.gz",
                "Procedure.001.ndjson.gz": "002.2021-09-14/Procedure.001.ndjson.gz",
                "001.2021-09-14": None,
                "002.2021-09-14": None,
                "003.2021-09-14": None,
                ".metadata": None,
            }
        )
