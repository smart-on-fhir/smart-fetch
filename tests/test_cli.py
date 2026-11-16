import io
import os
from unittest import mock

import cumulus_fhir_support as cfs
import ddt

import smart_fetch
from smart_fetch import cli_utils, filtering, lifecycle, resources
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
            await self.capture_cli("export", self.folder, f"--type={arg}", stdout=stdout)
        self.assertEqual(cm.exception.code, exit_code)
        self.assertIn("These resource types are supported:", stdout.getvalue())

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
            filtering.Filters(
                ["Condition"],
                type_filters=["Condition?code=1234"],
                since="2013-10-30",
                since_mode=filtering.SinceMode.CREATED,
            )
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

    async def test_input_folder_does_not_exist(self):
        with self.assertRaisesRegex(SystemExit, "does not exist"):
            await self.cli("hydrate", self.folder, "--source-dir=/does/not/exist")

    async def test_input_folder_is_url(self):
        with self.assertRaisesRegex(SystemExit, "looks like a URL"):
            await self.cli("hydrate", self.folder, "--source-dir=s3://bucket/")

    async def test_output_folder_is_s3_bucket(self):
        with self.assertRaisesRegex(SystemExit, "Expected a subfolder"):
            await self.cli("hydrate", "s3://bucket/")

    async def test_output_folder_is_non_s3_url(self):
        with self.assertRaisesRegex(SystemExit, "looks like an unsupported URL"):
            await self.cli("hydrate", "https://bucket/")

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

    @ddt.data(
        None,
        "bulk",
        "bundle",
        "crawl",
        "export",
        "hydrate",
        "reset-symlinks",
        "single",
    )
    async def test_help(self, subcommand):
        """Proof of life for --help"""
        args = [subcommand, "--help"] if subcommand else ["--help"]
        stdout = io.StringIO()
        stderr = io.StringIO()
        with self.assertRaises(SystemExit) as cm:
            await self.capture_cli(*args, stdout=stdout, stderr=stderr)

        self.assertEqual(cm.exception.code, 0)
        self.assertIn("options:\n", stdout.getvalue())
        self.assertEqual(stderr.getvalue(), "")

    async def test_version(self):
        stdout = io.StringIO()
        stderr = io.StringIO()
        with self.assertRaises(SystemExit) as cm:
            await self.capture_cli("--version", stdout=stdout, stderr=stderr)

        self.assertEqual(cm.exception.code, 0)
        self.assertEqual(stdout.getvalue(), f"smart-fetch {smart_fetch.__version__}\n")
        self.assertEqual(stderr.getvalue(), "")

    @ddt.data("bulk", "crawl", "export", "hydrate")
    async def test_prepare_register_s3_options(self, subcommand):
        with (
            mock.patch.object(cfs.FsPath, "register_options") as mock_register,
            mock.patch.object(cfs.FsPath, "rm"),
            mock.patch.object(cfs.FsPath, "write_text"),
            self.assertRaisesRegex(SystemExit, "--fhir-url is required"),
        ):
            await self.local_cli(
                subcommand, f"{self.s3_folder}/test", "--kms-key=foo", "--s3-region=bar"
            )
        mock_register.assert_called_once()

    @ddt.data("bulk", "crawl", "export", "hydrate")
    async def test_prepare_validate_s3_connection(self, subcommand):
        with (
            mock.patch.object(cfs.FsPath, "register_options") as mock_register,
            self.assertRaisesRegex(SystemExit, "Could not write to S3 folder"),
            mock.patch.object(cfs.FsPath, "rm"),
            mock.patch.object(cfs.FsPath, "write_text", side_effect=PermissionError("Forbidden")),
        ):
            await self.local_cli(
                subcommand, f"{self.s3_folder}/test", "--kms-key=foo", "--s3-region=bar"
            )
        mock_register.assert_called_once()

    @ddt.data("bulk", "crawl", "export", "hydrate")
    def test_download_calls_copy(self, subcommand):
        args = [subcommand, f"{self.s3_folder}/test"]
        with (
            mock.patch.object(cfs.FsPath, "exists", return_value=True),
            mock.patch.object(cfs.FsPath, "copy") as mock_copy,
        ):
            folder, _, _ = cli_utils.download_from_s3(main.define_parser().parse_args(args))

        self.assertEqual(folder, cfs.FsPath(f"{self.s3_folder}/test"))
        mock_copy.assert_called_once()

    def test_upload_returns_early_when_missing_workdir(self):
        with (
            mock.patch.object(cfs.FsPath, "exists", return_value=False),
            mock.patch.object(cfs.FsPath, "copy") as mock_copy,
        ):
            cli_utils.upload_to_s3(
                f"{self.s3_folder}/test", cfs.FsPath(f"{self.folder}/nope"), False
            )

        mock_copy.assert_not_called()

    @ddt.data(True, False)
    def test_upload_calls_copy_when_workdir_exists(self, should_replace_files):
        with (
            mock.patch.object(cfs.FsPath, "exists", return_value=True),
            mock.patch.object(cfs.FsPath, "copy") as mock_copy,
            mock.patch.object(cfs.FsPath, "rm") as mock_remove,
        ):
            cli_utils.upload_to_s3(
                cfs.FsPath(f"{self.s3_folder}/test"),
                cfs.FsPath(f"{self.folder}/nope"),
                should_replace_files,
            )

        if should_replace_files:
            self.assertEqual(mock_remove.call_count, 2)
        else:
            mock_remove.assert_called_once()

        mock_copy.assert_called_once_with(cfs.FsPath(f"{self.s3_folder}/test"))
