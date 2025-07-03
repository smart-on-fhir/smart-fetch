import datetime
import enum
import json
import os
import sys

import smart_fetch
from smart_fetch import cli_utils, timing


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
        self._folder = folder
        self._path = os.path.join(self._folder, ".metadata")
        self._kind = kind
        self._contents = self._read()
        if found_kind := self._contents.get("kind"):
            if found_kind != self._kind:
                sys.exit(
                    f"Folder {self._folder} is not {MetadataKind.pretty(self._kind)},"
                    f" but {MetadataKind.pretty(found_kind)}"
                )

    def _basic_metadata(self) -> dict:
        return {
            "kind": self._kind,
            "timestamp": timing.now().isoformat(),
            "version": smart_fetch.__version__,
        }

    def _read(self) -> dict:
        try:
            with open(self._path, encoding="utf8") as f:
                return json.load(f)
        except FileNotFoundError:
            return {}

    def _write(self) -> None:
        self._contents.update(self._basic_metadata())
        os.makedirs(self._folder, exist_ok=True)

        tmp_path = f"{self._path}.tmp"
        with open(tmp_path, "w", encoding="utf8") as f:
            json.dump(self._contents, f, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, self._path)


class OutputMetadata(Metadata):
    def __init__(self, folder: str):
        super().__init__(folder, MetadataKind.OUTPUT)

    def note_context(
        self, *, filters: cli_utils.Filters, since: str | None, since_mode: cli_utils.SinceMode
    ) -> None:
        ordered_filters = {res: sorted(params) for res, params in filters.items()}
        if "filters" not in self._contents:
            self._contents["filters"] = ordered_filters
            self._contents["since"] = since
            self._contents["sinceMode"] = str(since_mode) if since else None
            self._write()
            return

        found_filters = self._contents.get("filters")
        if found_filters != ordered_filters:
            sys.exit(
                f"Folder {self._folder} is for a different set of types and/or filters. "
                f"Expected:\n{self._pretty_filters(ordered_filters)}\n\nbut found:\n"
                f"{self._pretty_filters(found_filters)}"
            )

        found_since = self._contents.get("since")
        if found_since != since:
            sys.exit(
                f"Folder {self._folder} is for a different --since time. "
                f"Expected {since} but found {found_since}."
            )

        found_since_mode = self._contents.get("sinceMode")
        if since and found_since_mode != since_mode:
            sys.exit(
                f"Folder {self._folder} is for a different --since-mode. "
                f"Expected '{since_mode}' but found '{found_since_mode}'."
            )

    def has_same_context(
        self, *, filters: cli_utils.Filters, since: str | None, since_mode: cli_utils.SinceMode
    ) -> bool:
        """Determines if this folder is the same exact context (filters and since)"""
        ordered_filters = {res: sorted(params) for res, params in filters.items()}
        found_filters = self._contents.get("filters")
        found_since = self._contents.get("since")
        found_since_mode = self._contents.get("sinceMode")
        return (
            found_filters == ordered_filters
            and found_since == since
            and (not since or found_since_mode == since_mode)
        )

    def get_matching_timestamps(
        self, filters: cli_utils.Filters, since_mode: str
    ) -> dict[str, datetime.datetime]:
        """
        Tells if we have a superset of any resources from `filters`

        Returns the resources that are a match, and that resource's finished timestamp.

        This is used to find previous export timestamps to calculate a new automatic "since" value.
        """
        matches = {}

        # We only match on the same type of "since"
        if self._contents.get("sinceMode") and self._contents.get("sinceMode") != since_mode:
            return matches

        for res_type, params_list in self._contents.get("filters", {}).items():
            if res_type not in filters:
                continue

            found_params = set(params_list)
            both_empty = not filters[res_type] and not found_params
            # We look for a subset here, because multiple type filters is an OR - so if we searched
            # for A OR B before, but now are looking for previous A timestamps, that's a match.
            target_is_subset = filters[res_type] and filters[res_type].issubset(found_params)
            if both_empty or target_is_subset:
                # OK this folder is a valid match for our current set of filters.
                # Let's find the timestamps.
                done = self._contents.get("done", {})
                if res_type in done:
                    matches[res_type] = datetime.datetime.fromisoformat(done[res_type])

        return matches

    @staticmethod
    def _pretty_filters(filters: dict[str, list[str]]) -> str:
        lines = []
        for res_type, params_list in sorted(filters.items()):
            if params_list:
                lines.extend([f"  {res_type}?{params}" for params in params_list])
            else:
                lines.append(f"  {res_type} (no filter)")
        return "\n".join(lines)

    def is_done(self, tag: str) -> bool:
        return tag in self._contents.get("done", {})

    def mark_done(self, tag: str, timestamp: datetime.datetime | None = None) -> None:
        """
        Marks the given resource or task as done.

        The timestamp is ideally a "transactionTime" style stamp (like in bulk export).
        If not provided, it simply uses now().
        """
        timestamp = timestamp or timing.now()
        done = self._contents.setdefault("done", {})
        done[tag] = timestamp.isoformat()
        self._write()

    def get_earliest_done_date(self) -> datetime.datetime | None:
        if done := self._contents.get("done"):
            return min(datetime.datetime.fromisoformat(time) for time in done.values())
        return None

    def set_bulk_status_url(self, status_url: str | None) -> None:
        if status_url:
            self._contents["bulk-status"] = status_url
        else:
            del self._contents["bulk-status"]
        self._write()

    def get_bulk_status_url(self) -> str | None:
        return self._contents.get("bulk-status")


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
