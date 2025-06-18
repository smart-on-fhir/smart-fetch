import enum
import json
import os
import sys

import smart_extract
from smart_extract import timing


class MetadataKind(enum.StrEnum):
    MANAGED = enum.auto()
    OUTPUT = enum.auto()

    @classmethod
    def pretty(cls, kind: "MetadataKind") -> str:
        if kind == MetadataKind.MANAGED:
            return f"a {kind} folder"
        else:
            return f"an {kind} folder"


class Metadata:
    def __init__(self, folder: str, kind: MetadataKind):
        self._path = os.path.join(folder, ".metadata")
        self._kind = kind
        self._contents = self._read()
        if found_kind := self._contents.get("kind"):
            if found_kind != self._kind:
                sys.exit(
                    f"Folder {folder} is not {MetadataKind.pretty(self._kind)},"
                    f" but {MetadataKind.pretty(found_kind)}"
                )

    def _basic_metadata(self) -> dict:
        return {
            "kind": self._kind,
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


class OutputMetadata(Metadata):
    def __init__(self, folder: str):
        super().__init__(folder, MetadataKind.OUTPUT)

    def is_done(self, tag: str) -> bool:
        return tag in self._contents.get("done", [])

    def mark_done(self, tag: str) -> None:
        if not self.is_done(tag):
            self._contents.setdefault("done", []).append(tag)
        self._write()


class ManagedMetadata(Metadata):
    def __init__(self, folder: str):
        super().__init__(folder, MetadataKind.MANAGED)

    def note_context(self, *, fhir_url: str, group: str | None) -> None:
        if "fhir-url" not in self._contents:
            self._contents["fhir-url"] = fhir_url
            self._contents["group"] = group
            self._write()
            return

        # Check FHIR URL, with some extremely basic normalization
        found_url = self._contents["fhir-url"]
        found_url = found_url.removesuffix("/")
        fhir_url = fhir_url.removesuffix("/")
        if found_url != fhir_url:
            sys.exit(
                f"Target folder {os.path.basename(self._path)} is for a different FHIR URL.\n"
                f"Expected {fhir_url}\n"
                f"But found {found_url}"
            )

        # Check group
        found_group = self._contents["group"]
        if found_group != group:
            sys.exit(
                f"Target folder {os.path.basename(self._path)} is for a different Group.\n"
                f"Expected {group}, but found {found_group}."
            )
