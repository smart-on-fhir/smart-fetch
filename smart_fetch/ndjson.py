import gzip
import itertools
import json
import os
import sys
from functools import partial
from typing import TextIO

import cumulus_fhir_support as cfs

from smart_fetch import timing


class NdjsonWriter:
    """
    Convenience context manager to write multiple objects to a ndjson file.

    Note that this is not atomic - partial writes will make it to the target file.
    """

    def __init__(self, path: str, append: bool = False):
        self._path = path
        self._write_path = path if append or not os.path.exists(path) else path + ".tmp"
        self._append = append
        self._file = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self._file:
            self._file.flush()  # write out file buffers
            os.fsync(self._file.fileno())  # write out system buffers to disk
            self._file.close()
            self._file = None

            if self._write_path != self._path:
                os.replace(self._write_path, self._path)

    def _ensure_file(self):
        if not self._file:
            mode = "a" if self._append else "w"
            self._file = open_file(self._write_path, mode)

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
    open_func = gzip.open if is_compressed(path) else partial(open, buffering=0)
    with open_func(path, "rb") as f:
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
