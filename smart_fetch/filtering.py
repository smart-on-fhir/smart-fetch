"""Support for filtering parameters"""

import datetime
import enum
import sys
from collections.abc import Iterable

import cumulus_fhir_support as cfs
import rich

from smart_fetch import resources

TypeFilters = dict[str, set[str]]


class SinceMode(enum.StrEnum):
    AUTO = enum.auto()
    UPDATED = enum.auto()
    CREATED = enum.auto()


class Filters:
    def __init__(
        self,
        res_types: Iterable[str],
        *,
        type_filters: list[str] | None = None,
        server_type: cfs.ServerType = cfs.ServerType.UNKNOWN,
        since: str | None = None,
        since_mode: SinceMode | None = None,
    ):
        """Parses the incoming --type-filter arguments and adds in default filters."""
        self.server_type = server_type
        self.since = since
        self.since_mode = self._calculate_since_mode(since_mode) if since else None
        self.detailed_since: dict[str, datetime.datetime | None] | None = None

        # First, ensure each type has a filter entry
        self._filters: TypeFilters = {}
        for res_type in res_types:
            self._filters[res_type] = set()

        # Next, add in the manually provided filters
        for type_filter in type_filters or []:
            if "?" not in type_filter:
                sys.exit("Type filter arguments must be in the format 'Resource?params'.")
            res_type, params = type_filter.split("?", 1)
            if res_type not in self._filters:
                sys.exit(f"Type filter for {res_type} but that type is not included in --type.")
            self._filters[res_type].add(params)

        if self._filters.get(resources.OBSERVATION) == set():
            # Add some basic default filters for Observation, because the volume of Observations
            # gets overwhelming quickly. So we limit to the nine basic US Core categories.
            categories = "category=social-history,vital-signs,imaging,laboratory,survey,exam"
            if server_type != cfs.ServerType.EPIC:
                # As of June 2025, Epic does not support these types and will error out
                categories += ",procedure,therapy,activity"

            self._filters[resources.OBSERVATION] = {categories}

    def resources(self) -> set[str]:
        return set(self._filters)

    def params(self, *, with_since: bool = True, bulk: bool = False) -> TypeFilters:
        """
        Returns search / _typeFilter parameters for this set of filters.

        With `with_since=True`, the since value will be added to the filters, if appropriate.
        This is not bulk export's _since parameter, but rather a way to emulate it.

        In "updated" mode, _since is faked by just searching for _lastUpdated.

        In "created" mode, _since is faked by searching for per-resource fields (when they are
        available) for a date that is as close to creation date as possible. This is necessary for
        servers that don't offer a meta.lastUpdated field (like Epic).
        """
        filters = dict(self._filters)

        if not self.since or not with_since:
            return filters

        if self.since_mode == SinceMode.CREATED:
            for res_type, field in resources.CREATED_SEARCH_FIELDS.items():
                self._add_filter(filters, res_type, field)
        elif not bulk:  # UPDATED mode, non bulk
            for res_type in filters:
                self._add_filter(filters, res_type, "_lastUpdated")

        # else if SinceMode.UPDATED and in bulk mode, we use Bulk Export's _since param instead,
        # which is better than faking it with _lastUpdated, because _since has extra logic around
        # older resources of patients added to the group after _since.

        return filters

    def get_bulk_since(self) -> str | None:
        """Coalesce our various since options down to a single _since value"""
        if self.since_mode == SinceMode.CREATED:
            return None  # we don't use a _since value in this mode, we use params instead

        if self.detailed_since is not None:
            if any(val is None for val in self.detailed_since.values()):
                return None
            else:
                return min(val for val in self.detailed_since.values()).isoformat()
        else:
            return self.since

    def print_since(self, bulk: bool = False) -> None:
        if bulk and self.since_mode == SinceMode.UPDATED:
            if bulk_since := self.get_bulk_since():
                rich.print(f"Using since value of '{bulk_since}'.")
        elif self.detailed_since is not None:
            for res_type, timestamp in sorted(self.detailed_since.items()):
                if timestamp:
                    rich.print(f"Using since value of '{timestamp.isoformat()}' for {res_type}.")
        elif self.since:
            rich.print(f"Using since value of '{self.since}'.")

    def _add_filter(self, filters: dict, res_type: str, field: str) -> None:
        if res_type not in filters:
            return

        if self.detailed_since is not None:
            if not self.detailed_since.get(res_type):
                return
            else:
                res_since = self.detailed_since[res_type].isoformat()
        else:
            res_since = self.since
        new_param = f"{field}=gt{res_since}"

        if filters[res_type]:
            filters[res_type] = {f"{params}&{new_param}" for params in filters[res_type]}
        else:
            filters[res_type] = {new_param}

    def _calculate_since_mode(self, since_mode: SinceMode | None) -> SinceMode:
        """Converts "auto" into created or updated based on whether the server supports created."""
        if not since_mode or since_mode == SinceMode.AUTO:
            # Epic does not support meta.lastUpdated, so we have to fall back to created time here.
            # Otherwise, prefer to grab any resource updated since this time, to get all the latest
            # and greatest edits.
            if self.server_type == cfs.ServerType.EPIC:
                rich.print(
                    "Epic server detected. Defaulting to 'created' instead of 'updated' since mode."
                )
                return SinceMode.CREATED
            else:
                return SinceMode.UPDATED

        return since_mode
