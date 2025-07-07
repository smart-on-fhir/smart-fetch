import gzip
import itertools
import json
import os
from functools import partial


class NdjsonWriter:
    """
    Convenience context manager to write multiple objects to a ndjson file.

    Note that this is not atomic - partial writes will make it to the target file.
    """

    def __init__(self, path: str, append: bool = False):
        self._path = path
        self._write_path = path if append or not os.path.exists(path) else path + ".tmp"
        self._append = append
        self._compressed = path.endswith(".gz")
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
            open_func = gzip.open if self._compressed else open
            self._file = open_func(self._write_path, mode + "t", encoding="utf8")

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
    open_func = gzip.open if path.casefold().endswith(".gz") else partial(open, buffering=0)
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
