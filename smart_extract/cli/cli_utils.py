import argparse
import os
import sys
import tomllib

import rich.progress
from cumulus_etl import common, fhir, store
from cumulus_etl.fhir.fhir_client import ServerType

from smart_extract import resources

# RESOURCE SELECTION

ALLOWED_TYPES = {
    "all",
    "help",
    *resources.PATIENT_TYPES,
}
ALLOWED_CASE_MAP: dict[str, str] = {res_type.casefold(): res_type for res_type in ALLOWED_TYPES}


def add_type_selection(parser: argparse.ArgumentParser) -> None:
    group = parser.add_argument_group("resource selection")
    group.add_argument(
        "--type",
        default="all",
        help="only consider these resource types (comma separated, "
        "default is all supported FHIR resources, "
        "use '--type help' to see full list)",
    )
    group.add_argument(
        "--type-filter",
        metavar="FILTER",
        action="append",
        help="search filter to apply to the export (_typeFilter), can be specified multiple times",
    )


# TODO: don't hardcode any supported resources, just use whatever we have access to from scopes
def parse_resource_selection(types: str) -> list[str]:
    orig_types = set(types.split(","))
    lower_types = {t.casefold() for t in orig_types}

    def print_help():
        print("These types are supported:")
        print("  all")
        for pat_type in resources.PATIENT_TYPES:
            print(f"  {pat_type}")

    # Check if any provided types are bogus
    for orig_type in orig_types:
        if orig_type.casefold() not in ALLOWED_CASE_MAP:
            print(f"Unknown resource type provided: {orig_type}")
            print()
            print_help()
            sys.exit(2)

    if "help" in lower_types:
        print_help()
        sys.exit(0)

    if "all" in lower_types:
        return resources.PATIENT_TYPES

    # Keep our internal preferred order by iterating on PATIENT_TYPES, not lower_types
    return [pat_type for pat_type in resources.PATIENT_TYPES if pat_type.casefold() in lower_types]


def parse_type_filters(client, type_filters: list[str] | None) -> dict[str, set[str]]:
    # First, break out what the user provided on the CLI
    filters = {}
    for type_filter in type_filters or []:
        res_type, params = type_filter.split("?", 1)
        filters.setdefault(res_type, set()).add(params)

    if resources.OBSERVATION not in filters:
        # Add some basic default filters for Observation, because the volume of Observations gets
        # overwhelming quickly. So we limit to the nine basic US Core categories.
        categories = "category=social-history,vital-signs,imaging,laboratory,survey,exam"
        if client._server_type != ServerType.EPIC:
            # As of June 2025, Epic does not support these types and will error out
            categories += ",procedure,therapy,activity"

        filters[resources.OBSERVATION] = {categories}

        # Cerner doesn't seem to provide a category for Smoking Status observations, so we search
        # on the US Core required loinc code to pick those up.
        if client._server_type == ServerType.CERNER:
            filters[resources.OBSERVATION].add("code=http://loinc.org|72166-2")

    return filters


# AUTHENTICATION


def add_auth(parser: argparse.ArgumentParser):
    group = parser.add_argument_group("authentication")
    group.add_argument("--smart-client-id", metavar="ID", help="client ID for SMART authentication")
    group.add_argument(
        "--smart-key", metavar="PATH", help="JWKS or PEM file for SMART authentication"
    )
    group.add_argument("--basic-user", metavar="USER", help="username for Basic authentication")
    group.add_argument(
        "--basic-passwd", metavar="PATH", help="password file for Basic authentication"
    )
    group.add_argument(
        "--bearer-token", metavar="PATH", help="token file for Bearer authentication"
    )
    group.add_argument(
        "--fhir-url",
        metavar="URL",
        help="FHIR server base URL",
    )
    group.add_argument(
        "--token-url",
        metavar="URL",
        help="FHIR server token URL, only needed if server does not provide it",
    )


# GENERAL


def add_general(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--config", "-c", metavar="PATH", help="config file")


def load_config(args) -> None:
    if args.config:
        with open(args.config, "rb") as f:
            data = tomllib.load(f)

        for key in data:
            prop = key.replace("-", "_")
            if prop in args and getattr(args, prop) is None:
                setattr(args, prop, data[key])


def prepare(args):
    load_config(args)

    if not args.fhir_url:
        print("--fhir-url is required")
        sys.exit(2)

    common.print_header()

    return fhir.create_fhir_client_for_cli(args, store.Root(args.fhir_url), ["*"])


def confirm_dir_is_empty(folder: str, allow: set[str] | None = None) -> None:
    """Errors out if the dir exists with contents"""
    os.makedirs(folder, exist_ok=True)

    files = {os.path.basename(p) for p in os.listdir(folder)}
    if allow:
        files -= allow
    if files:
        sys.exit(
            f"The target folder '{folder}' already has contents. Please provide an empty folder.",
        )


def make_progress_bar() -> rich.progress.Progress:
    # The default columns use time remaining, which has felt inaccurate/less useful than a simple
    # elapsed counter.
    # - The estimation logic seems rough (often jumping time around).
    # - For indeterminate bars, the estimate shows nothing.
    columns = [
        rich.progress.TextColumn("[progress.description]{task.description}"),
        rich.progress.BarColumn(),
        rich.progress.TaskProgressColumn(),
        rich.progress.TimeElapsedColumn(),
    ]
    return rich.progress.Progress(*columns)
