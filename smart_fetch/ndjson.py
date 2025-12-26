import gzip
import itertools
import json
import os
import shutil
import sys
from functools import partial
from typing import BinaryIO, TextIO

import cumulus_fhir_support as cfs

from smart_fetch import timing


class NdjsonWriter:
    """
    Convenience context manager to write multiple objects to a ndjson file.

    This is safe against interruption:
    - a separate scratch file is used for the writing
    - if interrupted by an exception, what has been written so far will be written out to disk
      and the target file atomically replaced
    - if interrupted by process termination, the target file is not changed at all

    We assume that we're the only process/writer using this file (i.e. this is not safe for
    concurrent access).
    """

    class FakeSuddenTermination(Exception):
        """If encountered, we don't write to disk. Used only in tests."""

    def __init__(self, path: str, append: bool = False):
        self._path = path
        self._write_path = path + ".tmp"
        self._append = append
        self._file = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is NdjsonWriter.FakeSuddenTermination:
            return  # this path is only hit in tests

        if self._file:
            self._file.flush()  # write out file buffers
            os.fsync(self._file.fileno())  # write out system buffers to disk
            self._file.close()
            self._file = None

            os.replace(self._write_path, self._path)

    def _ensure_file(self):
        if self._file:
            return

        if self._append:
            # Start our write file off with the real content
            try:
                shutil.copy2(self._path, self._write_path)
            except FileNotFoundError:
                # OK nothing to start with, remove any previous temp file
                try:
                    os.remove(self._write_path)
                except FileNotFoundError:
                    pass

            # Check for newline, in case this file came from a third party that didn't write it
            try:
                with open_file_bytes(self._write_path, "r") as f:
                    f.seek(-1, os.SEEK_END)
                    needs_newline = f.read(1) != b"\n"
            except (FileNotFoundError, OSError):
                needs_newline = False

            # And finally, open the file for actual writing
            self._file = open_file(self._write_path, "a")
            if needs_newline:
                self._file.write("\n")
        else:
            self._file = open_file(self._write_path, "w")

    def write(self, obj: dict) -> None:
        # lazily create the file, to avoid 0-line ndjson files
        self._ensure_file()

        self._file.write(compact_json(obj) + "\n")


def read_local_line_count(path) -> int:
    """Reads a local file and provides the count of new line characters."""
    # From https://stackoverflow.com/a/27517681/239668 with some modifications
    # Copyright Michael Bacon, licensed CC-BY-SA 3.0
    count = 0
    buf = None
    with open_file_bytes(path, "r") as f:
        bufgen = itertools.takewhile(
            lambda x: x, (f.read(1024 * 1024) for _ in itertools.repeat(None))
        )
        for buf in bufgen:
            count += buf.count(b"\n")
    if buf and buf[-1] != ord("\n"):  # catch a final line without a trailing newline
        count += 1
    return count


def compact_json(obj: dict) -> str:
    """Formats JSON with no whitespace"""
    # Specify separators for the most compact (no whitespace) representation saves disk space.
    return json.dumps(obj, separators=(",", ":"))


def bundle_folder(folder: str, *, compress: bool = False, exist_ok: bool = False) -> str | None:
    """
    Converts a folder into a single Bundle file.

    Note: all source NDJSON files used will be deleted!

    Returns:
        Filename if a bundle was created else None (it won't be if there are no input files).
    """
    output_path = filename(folder, "Bundle.json", compress=compress)
    if not exist_ok and os.path.exists(output_path):
        sys.exit(f"Bundle file '{output_path}' already exists.")

    filenames = [
        path for path, res_type in cfs.list_multiline_json_in_dir(folder).items() if res_type
    ]
    if not filenames:
        return None

    with open_file(output_path, "w") as f:
        # Start the Bundle
        f.write(
            "{\n"
            '  "resourceType": "Bundle",\n'
            '  "meta": {\n'
            '    "profile": ["http://hl7.org/fhir/R4/StructureDefinition/Bundle"]\n'
            "  },\n"
            '  "type": "collection",\n'
            f'  "timestamp": "{timing.now().isoformat()}",\n'
            '  "entry": ['
        )

        # Now read each resource and add into our Bundle
        first = True
        for path in filenames:
            for res in cfs.read_multiline_json(path):
                if not first:
                    f.write(",")
                f.write('\n    {"resource": ' + compact_json(res) + "}")
                first = False

        # And finish up
        f.write(
            "\n"  # end the last resource
            "  ]\n"
            "}\n"
        )

    # Now delete all the source files
    for path in filenames:
        os.unlink(path)

    return output_path


def filename(stem: str, *extra, compress: bool = False) -> str:
    """Returns an appropriate output filename to use"""
    stem = os.path.join(stem, *extra)
    return f"{stem}.gz" if compress else stem


def is_compressed(path: str) -> bool:
    path = path.casefold().removesuffix(".tmp")
    return path.endswith(".gz")


def open_file(path: str, mode: str) -> TextIO:
    open_func = gzip.open if is_compressed(path) else open
    return open_func(path, mode + "t", encoding="utf8")


def open_file_bytes(path: str, mode: str) -> BinaryIO:
    open_func = gzip.open if is_compressed(path) else partial(open, buffering=0)
    return open_func(path, mode + "b")
