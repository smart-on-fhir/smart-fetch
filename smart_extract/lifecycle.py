import json
import os

import smart_extract
from smart_extract import timing


class Metadata:
    def __init__(self, folder: str):
        self._path = os.path.join(folder, ".metadata")
        self._contents = self._read()

    def is_done(self, tag: str) -> bool:
        return tag in self._contents.get("done", [])

    def mark_done(self, tag: str) -> None:
        if not self.is_done(tag):
            self._contents.setdefault("done", []).append(tag)
        self._write()

    @staticmethod
    def _basic_metadata() -> dict:
        return {
            "timestamp": timing.now().isoformat(),
            "version": smart_extract.__version__,
        }

    def _read(self) -> dict:
        try:
            with open(self._path, encoding="utf8") as f:
                return json.load(f)
        except FileNotFoundError:
            return {}

    def _write(self) -> None:
        self._contents.update(self._basic_metadata())
        os.makedirs(os.path.dirname(self._path), exist_ok=True)

        tmp_path = f"{self._path}.tmp"
        with open(tmp_path, "w", encoding="utf8") as f:
            json.dump(self._contents, f, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, self._path)
