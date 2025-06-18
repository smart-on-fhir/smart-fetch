import contextlib
import io
import os

import ddt

from smart_extract import lifecycle, resources
from tests import utils


@ddt.ddt
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
        self.server.get("metadata").respond(
            200,
            json={
                "rest": [
                    {
                        "mode": "server",
                        "resource": [{"type": resources.PATIENT}, {"type": resources.CONDITION}],
                    },
                ],
            },
        )

        await self.cli(
            "bulk",
            self.folder,
            "--group=group1",
            f"--type={resources.PATIENT},{resources.CONDITION},{resources.DEVICE}",
        )

        self.assert_folder(
            {
                "log.ndjson": None,
                f"{resources.CONDITION}.000.ndjson.gz": None,
                f"{resources.PATIENT}.000.ndjson.gz": None,
                ".metadata": None,
            }
        )

    @ddt.data(
        ("help", 0),
        ("bogus", 2),
    )
    @ddt.unpack
    async def test_type_parsing_help(self, arg, exit_code):
        stdout = io.StringIO()
        with self.assertRaises(SystemExit) as cm:
            with contextlib.redirect_stdout(stdout):
                await self.cli("export", self.folder, f"--type={arg}")
        self.assertEqual(cm.exception.code, exit_code)
        self.assertIn("These types are supported:", stdout.getvalue())

    async def test_type_parsing_is_case_insensitive(self):
        self.mock_bulk("group1", params={"_type": f"{resources.CONDITION},{resources.PATIENT}"})
        await self.cli("export", self.folder, "--group=group1", "--type=condITION,PATient")

    async def test_type_filter_parsing_bad_format(self):
        with self.assertRaisesRegex(
            SystemExit, r"Type filter arguments must be in the format 'Resource\?params'."
        ):
            await self.cli(
                "export", self.folder, "--type", resources.CONDITION, "--type-filter=bogus"
            )

        with self.assertRaisesRegex(
            SystemExit, "Type filter for Patient but that type is not included in --type."
        ):
            await self.cli(
                "export",
                self.folder,
                f"--type={resources.CONDITION}",
                f"--type-filter={resources.PATIENT}?active=true",
            )

    async def test_loading_config(self):
        config = self.tmp_file()
        with config:
            config.write(f"""
                group = "group1"
                type = "{resources.CONDITION},{resources.PATIENT}"
                type-filter = "{resources.PATIENT}?active=false"
            """)
        self.mock_bulk(
            "group1",
            params={
                "_type": f"{resources.CONDITION},{resources.PATIENT}",
                "_typeFilter": f"{resources.PATIENT}?active=false",
            },
        )
        await self.cli("export", self.folder, f"--config={config.name}")

    async def test_wrong_metadata(self):
        metadata = lifecycle.OutputMetadata(self.folder)
        metadata.mark_done(resources.CONDITION)
        with self.assertRaisesRegex(SystemExit, "is not a managed folder, but an output folder"):
            await self.cli("export", self.folder)

        os.unlink(f"{self.folder}/.metadata")
        metadata = lifecycle.ManagedMetadata(self.folder)
        metadata.note_context(fhir_url=self.url, group="group1")
        with self.assertRaisesRegex(SystemExit, "is not an output folder, but a managed folder"):
            await self.cli("bulk", self.folder)
