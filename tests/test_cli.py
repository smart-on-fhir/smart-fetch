import contextlib
import io
import os
from unittest import mock

import ddt

from smart_fetch import cli_utils, lifecycle, resources
from smart_fetch.cli import main
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
                f"{resources.CONDITION}.001.ndjson.gz": None,
                f"{resources.PATIENT}.001.ndjson.gz": None,
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

    async def test_wrong_metadata_kind(self):
        metadata = lifecycle.OutputMetadata(self.folder)
        metadata.mark_done(resources.CONDITION)
        with self.assertRaisesRegex(SystemExit, "is not a managed folder, but an output folder"):
            await self.cli("export", self.folder)

        os.unlink(f"{self.folder}/.metadata")
        metadata = lifecycle.ManagedMetadata(self.folder)
        metadata.note_context(fhir_url=self.url, group="group1")
        with self.assertRaisesRegex(SystemExit, "is not an output folder, but a managed folder"):
            await self.cli("bulk", self.folder)

    async def test_wrong_metadata_context(self):
        metadata = lifecycle.OutputMetadata(self.folder)
        metadata.note_context(
            filters={resources.CONDITION: {"code=1234"}},
            since="2013-10-30",
            since_mode=cli_utils.SinceMode.CREATED,
        )

        # Wrong type filter
        with self.assertRaises(SystemExit) as cm:
            await self.cli("bulk", self.folder, f"--type={resources.CONDITION}")
        self.assertEqual(
            str(cm.exception),
            f"""Folder {self.folder} is for a different set of types and/or filters. Expected:
  Condition (no filter)

but found:
  Condition?code=1234""",
        )

        # Wrong types
        with self.assertRaises(SystemExit) as cm:
            await self.cli(
                "bulk",
                self.folder,
                f"--type={resources.CONDITION},{resources.DEVICE}",
                f"--type-filter={resources.CONDITION}?code=1234",
            )
        self.assertEqual(
            str(cm.exception),
            f"""Folder {self.folder} is for a different set of types and/or filters. Expected:
  Condition?code=1234
  Device (no filter)

but found:
  Condition?code=1234""",
        )

        # Wrong since
        with self.assertRaisesRegex(
            SystemExit,
            f"Folder {self.folder} is for a different --since time. "
            "Expected 2020-03-23 but found 2013-10-30.",
        ):
            await self.cli(
                "bulk",
                self.folder,
                "--since=2020-03-23",
                f"--type={resources.CONDITION}",
                f"--type-filter={resources.CONDITION}?code=1234",
            )

        # Wrong since mode
        with self.assertRaisesRegex(
            SystemExit,
            f"Folder {self.folder} is for a different --since-mode. "
            "Expected 'updated' but found 'created'.",
        ):
            await self.cli(
                "bulk",
                self.folder,
                "--since=2013-10-30",
                "--since-mode=updated",
                f"--type={resources.CONDITION}",
                f"--type-filter={resources.CONDITION}?code=1234",
            )

    async def test_no_fhir_url(self):
        with self.assertRaisesRegex(SystemExit, "--fhir-url is required"):
            await main.main(["bulk", str(self.folder)])

    @mock.patch("cumulus_fhir_support.FhirClient.create_for_cli")
    async def test_fallback_clients(self, mock_client):
        # Confirm we use rest auth for bulk if needed
        with self.assertRaises(SystemExit):
            await self.cli(
                "export",
                self.folder,
                "--type=help",
                "--smart-client-id=rest",
                "--smart-key=rest.jwks",
            )
        self.assertEqual(mock_client.call_count, 2)
        self.assertEqual(mock_client.call_args_list[0].kwargs["smart_client_id"], "rest")
        self.assertEqual(mock_client.call_args_list[0].kwargs["smart_key"], "rest.jwks")
        self.assertEqual(mock_client.call_args_list[1].kwargs["smart_client_id"], "rest")
        self.assertEqual(mock_client.call_args_list[1].kwargs["smart_key"], "rest.jwks")

        mock_client.reset_mock()

        # And the reverse
        with self.assertRaises(SystemExit):
            await self.cli(
                "export",
                self.folder,
                "--type=help",
                "--bulk-smart-client-id=bulk",
                "--bulk-smart-key=bulk.jwks",
            )
        self.assertEqual(mock_client.call_count, 2)
        self.assertEqual(mock_client.call_args_list[0].kwargs["smart_client_id"], "bulk")
        self.assertEqual(mock_client.call_args_list[0].kwargs["smart_key"], "bulk.jwks")
        self.assertEqual(mock_client.call_args_list[1].kwargs["smart_client_id"], "bulk")
        self.assertEqual(mock_client.call_args_list[1].kwargs["smart_key"], "bulk.jwks")

        mock_client.reset_mock()

        # And finally, confirm we can provide both
        with self.assertRaises(SystemExit):
            await self.cli(
                "export",
                self.folder,
                "--type=help",
                "--smart-client-id=rest",
                "--smart-key=rest.jwks",
                "--bulk-smart-client-id=bulk",
                "--bulk-smart-key=bulk.jwks",
            )
        self.assertEqual(mock_client.call_count, 2)
        self.assertEqual(mock_client.call_args_list[0].kwargs["smart_client_id"], "rest")
        self.assertEqual(mock_client.call_args_list[0].kwargs["smart_key"], "rest.jwks")
        self.assertEqual(mock_client.call_args_list[1].kwargs["smart_client_id"], "bulk")
        self.assertEqual(mock_client.call_args_list[1].kwargs["smart_key"], "bulk.jwks")

    @ddt.data(
        (12288, "12KB"),
        (12024036, "11.5MB"),
        (12024036048, "11.2GB"),
    )
    @ddt.unpack
    def test_unit_human_file_size(self, size, expected_string):
        self.assertEqual(cli_utils.human_file_size(size), expected_string)

    def test_unit_metadata_no_earliest_done(self):
        """Confirm we return None if there is no earliest done date"""
        self.assertIsNone(lifecycle.OutputMetadata(self.folder).get_earliest_done_date())
