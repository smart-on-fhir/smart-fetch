import base64
import json

from tests import utils


class SingleTests(utils.TestCase):
    async def test_basic(self):
        self.server.get("Patient/alice").respond(
            200,
            json={
                "resourceType": "Patient",
                "id": "alice",
            },
        )

        stdout, stderr = await self.capture_cli("single", "Patient/alice")

        self.assertEqual(
            stdout,
            b"""{
  "resourceType": "Patient",
  "id": "alice"
}
""",
        )
        self.assertEqual(stderr, "")

    async def test_internal_newlines(self):
        self.server.get("Patient/alice").respond(
            200,
            json={"resourceType": "Patient", "id": "alice", "text": {"div": "long\ntext\nfield"}},
        )

        # We should be escaping the newlines in all cases, but we especially want to confirm that
        # we only emit one line in contact mode.
        stdout, stderr = await self.capture_cli("single", "Patient/alice", "--compact")

        self.assertEqual(
            stdout,
            b"""{"resourceType":"Patient","id":"alice","text":{"div":"long\\ntext\\nfield"}}\n""",
        )
        self.assertEqual(stderr, "")

    async def test_compact(self):
        self.server.get("Patient/alice").respond(
            200,
            json={
                "resourceType": "Patient",
                "id": "alice",
            },
        )
        stdout, stderr = await self.capture_cli("single", "Patient/alice", "--compact")
        self.assertEqual(stdout, b'{"resourceType":"Patient","id":"alice"}\n')
        self.assertEqual(stderr, "")

    async def test_binary(self):
        # Use non-utf8 to confirm we print without trying to decode to a string
        invalid_utf8 = b"\xc3\x28"
        # sanity check that we have a non-utf8-string value
        with self.assertRaises(UnicodeDecodeError):
            invalid_utf8.decode("utf8")

        self.server.get("Binary/test").respond(
            200,
            json={
                "resourceType": "Binary",
                "contentType": "application/custom",
                "data": base64.standard_b64encode(invalid_utf8).decode("ascii"),
            },
        )
        stdout, stderr = await self.capture_cli("single", "Binary/test", "--binary")
        self.assertEqual(stdout, invalid_utf8)  # note the (correct) lack of a newline
        self.assertEqual(stderr, "")

    async def test_binary_no_data(self):
        """Just confirm we don't blow up, mostly"""
        self.server.get("Binary/test").respond(
            200,
            json={
                "resourceType": "Binary",
                "contentType": "application/custom",
            },
        )
        stdout, stderr = await self.capture_cli("single", "Binary/test", "--binary")
        self.assertEqual(stdout, b"")
        self.assertEqual(stderr, "")

    @staticmethod
    def make_error() -> dict:
        return {
            "resourceType": "OperationOutcome",
            "issue": [{"details": {"text": "bad request, bruh", "diagnostics": "code 1234"}}],
        }

    async def test_error(self):
        self.server.get("Patient/alice").respond(400, json=self.make_error())
        with self.assertRaisesRegex(SystemExit, ": \\[400\\] bad request, bruh$"):
            await self.capture_cli("single", "Patient/alice")

    async def test_verbose_error(self):
        self.server.get("Patient/alice").respond(400, json=self.make_error())
        with self.assertRaises(SystemExit) as cm:
            await self.capture_cli("single", "Patient/alice", "--verbose")
        self.assertEqual(json.loads(cm.exception.code), self.make_error())
