"""Unit tests for ndjson helpers"""

import gzip
import json
import os

import ddt

from smart_fetch import ndjson
from tests import utils


@ddt.ddt
class NdjsonTests(utils.TestCase):
    @ddt.data(
        # append, exception, whether file is expected to be updated
        (True, ndjson.NdjsonWriter.FakeSuddenTermination, False),
        (True, KeyboardInterrupt, True),
        (False, ndjson.NdjsonWriter.FakeSuddenTermination, False),
        (False, KeyboardInterrupt, True),
    )
    @ddt.unpack
    async def test_interruption(self, append, exc, expect_updated):
        target = f"{self.folder}/target.gz"
        tmp = f"{self.folder}/target.gz.tmp"

        # Start us off with a little content
        with gzip.open(target, "wt", encoding="utf8") as f:
            json.dump({"k": 1}, f)
            f.write("\n")

        # Use a big enough string that we force a buffer write in middle of file
        buf_len = 150_000 * 1024
        big_str = "@" * buf_len
        try:
            with ndjson.NdjsonWriter(target, append=append) as writer:
                writer.write({"k": big_str})
                raise exc()
        except exc:
            pass

        if expect_updated:
            prefix = '{"k": 1}\n' if append else ""
            expected_result = f'{prefix}{{"k":"{big_str}"}}\n'

            self.assertEqual(gzip.open(target, "rt", encoding="utf8").read(), expected_result)
            self.assertFalse(os.path.exists(tmp))
        else:
            self.assertEqual(gzip.open(target, "rt", encoding="utf8").read(), '{"k": 1}\n')
            with self.assertRaisesRegex(EOFError, "Compressed file ended before"):
                gzip.open(tmp, "rt", encoding="utf8").read()

    @ddt.data(
        "",
        ".gz",
    )
    async def test_ensures_newline(self, suffix):
        """If provided a file without a trailing newline, we insert one"""
        target = f"{self.folder}/target.ndjson{suffix}"

        # Start us off with a little content
        with ndjson.open_file(target, "w") as f:
            json.dump({"k": 1}, f)

        with ndjson.NdjsonWriter(target, append=True) as writer:
            writer.write({"k": 2})

        self.assertEqual(
            ndjson.open_file(target, "r").read(),
            '{"k": 1}\n{"k":2}\n',
        )
