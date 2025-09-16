import argparse
import sys
import tomllib

import cumulus_fhir_support as cfs
import rich.progress

from smart_fetch import resources

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


def limit_to_server_resources(client: cfs.FhirClient, res_types: list[str]) -> list[str]:
    """
    Returns a subset of `res_types` based on what the server supports.

    For example, the demo SMART bulk export server does not support ServiceRequest.
    """
    for rest in client.capabilities.get("rest", []):
        if rest.get("mode") == "server" and "resource" in rest:
            break
    else:
        return res_types

    server_types = {res["type"] for res in rest["resource"] if "type" in res}
    for res_type in sorted(res_types):
        if res_type not in server_types:
            rich.print(f"Skipping {res_type} because the server does not support it.")

    return [x for x in res_types if x in server_types]


def parse_resource_selection(types: str) -> list[str]:
    """
    Determines the list of resource types based on the requested CLI values.

    Handles "help", "all", and case-insensitivity.
    """
    orig_types = set(types.split(",")) if types else {"all"}
    lower_types = {t.casefold() for t in orig_types}

    def print_help():
        rich.get_console().print("These types are supported:")
        rich.get_console().print("  all")
        for pat_type in resources.PATIENT_TYPES:
            rich.get_console().print(f"  {pat_type}")

    # Check if any provided types are bogus
    for orig_type in orig_types:
        if orig_type.casefold() not in ALLOWED_CASE_MAP:
            rich.get_console().print(f"Unknown resource type provided: {orig_type}")
            rich.get_console().print()
            print_help()
            sys.exit(2)

    if "help" in lower_types:
        print_help()
        sys.exit(0)

    if "all" in lower_types:
        return resources.PATIENT_TYPES

    # Keep our internal preferred order by iterating on PATIENT_TYPES, not lower_types
    return [pat_type for pat_type in resources.PATIENT_TYPES if pat_type.casefold() in lower_types]


# COHORT SELECTION


def add_cohort_selection(parser: argparse.ArgumentParser):
    group = parser.add_argument_group("cohort selection")
    group.add_argument(
        "--group",
        metavar="GROUP",
        help="a FHIR Group to export (default is whole system)",
    )
    group.add_argument(
        "--group-nickname",
        metavar="NAME",
        help="a human-friendly name for the cohort, used in log files and such",
    )
    group.add_argument("--mrn-system", metavar="SYSTEM", help="system identifier for MRNs")
    group.add_argument(
        "--mrn-file",
        metavar="PATH",
        help="file with MRNs to export (instead of a Group), one per line "
        "(or a .csv with an 'mrn' column)",
    )
    return group


# AUTHENTICATION


def add_auth(parser: argparse.ArgumentParser):
    group = parser.add_argument_group("authentication")
    group.add_argument("--smart-client-id", metavar="ID", help="client ID for SMART authentication")
    group.add_argument(
        "--smart-key", metavar="PATH", help="JWKS or PEM file for SMART authentication"
    )
    group.add_argument(
        "--bulk-smart-client-id",
        metavar="ID",
        help="client ID for bulk export SMART authentication, "
        "only needed if your EHR uses separate bulk credentials",
    )
    group.add_argument(
        "--bulk-smart-key",
        metavar="PATH",
        help="JWKS or PEM file for bulk export SMART authentication, "
        "only needed if your EHR uses separate bulk credentials",
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


def add_general(parser: argparse.ArgumentParser, root: bool = False) -> None:
    default = None if root else argparse.SUPPRESS
    parser.add_argument("--config", "-c", metavar="PATH", help="config file", default=default)


def load_config(args) -> None:
    """Loads a config file and injects the contents into our CLI argument list"""
    if not args.config:
        return

    with open(args.config, "rb") as f:
        data = tomllib.load(f)

    for key in data:
        prop = key.replace("-", "_")
        if prop in args and getattr(args, prop) is None:
            if prop in {"type_filter"}:
                # Special handling for "append" types, to upgrade to list
                if isinstance(data[key], str):
                    data[key] = [data[key]]
            setattr(args, prop, data[key])


def create_client_for_cli(
    args, smart_client_id: str | None, smart_key: str | None
) -> cfs.FhirClient:
    return cfs.FhirClient.create_for_cli(
        args.fhir_url,
        resources.SCOPE_TYPES,
        token_url=args.token_url,
        smart_client_id=smart_client_id or args.smart_client_id,
        smart_key=smart_key or args.smart_key,
        basic_user=args.basic_user,
        basic_password=args.basic_passwd,
        bearer_token=args.bearer_token,
    )


def prepare(args) -> tuple[cfs.FhirClient, cfs.FhirClient]:
    """Returns (REST client, bulk client), which may be same client"""
    load_config(args)

    if not args.fhir_url:
        sys.exit("--fhir-url is required")

    rest_id = args.smart_client_id
    rest_key = args.smart_key
    bulk_id = args.bulk_smart_client_id
    bulk_key = args.bulk_smart_key

    # Have rest and bulk keys fall back to the other one, if only one is provided.
    if rest_id and rest_key and not bulk_id and not bulk_key:
        bulk_id = rest_id
        bulk_key = rest_key
    elif bulk_id and bulk_key and not rest_id and not rest_key:
        rest_id = bulk_id
        rest_key = bulk_key

    rest_client = create_client_for_cli(args, smart_client_id=rest_id, smart_key=rest_key)
    bulk_client = create_client_for_cli(args, smart_client_id=bulk_id, smart_key=bulk_key)

    return rest_client, bulk_client


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


def _pretty_float(num: float, precision: int = 1) -> str:
    """
    Returns a formatted float with trailing zeros chopped off.

    Could not find a cleaner builtin solution.
    Prior art: https://stackoverflow.com/questions/2440692/formatting-floats-without-trailing-zeros
    """
    return f"{num:.{precision}f}".rstrip("0").rstrip(".")


def human_file_size(count: int) -> str:
    """
    Returns a human-readable version of a count of bytes.

    I couldn't find a version of this that's sitting in a library we use. Very annoying.
    """
    for suffix in ("KB", "MB"):
        count /= 1024
        if count < 1024:
            return f"{_pretty_float(count)}{suffix}"
    return f"{_pretty_float(count / 1024)}GB"


def human_time_offset(seconds: int) -> str:
    """
    Returns a (fuzzy) human-readable version of a count of seconds.

    Examples:
      49 => "49s"
      90 => "1.5m"
      18000 => "5h"
    """
    if seconds < 60:
        return f"{seconds}s"

    minutes = seconds / 60
    if minutes < 60:
        return f"{_pretty_float(minutes)}m"

    hours = minutes / 60
    return f"{_pretty_float(hours)}h"


def print_done() -> None:
    rich.get_console().rule()
    rich.print("✨ Done ✨")
