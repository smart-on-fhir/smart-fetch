import argparse
import itertools
import os.path
import sys
import tomllib
import urllib.parse

import cumulus_fhir_support as cfs
import rich.progress

import smart_fetch
from smart_fetch import hydrate_utils, resources, tasks

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
        action="append",
        help="only consider these resource types (comma separated, "
        "default is all supported FHIR resources, "
        "use '--type help' to see full list)",
    )
    group.add_argument(
        "--type-filter",
        metavar="FILTER",
        action="append",
        help="search filter to apply to the export; "
        "can be specified multiple times; looks like 'Condition?code=1234'; "
        "see FHIR docs for details on _typeFilter",
    )
    group.add_argument(
        "--no-default-filters",
        dest="default_filters",
        action="store_false",
        help="disable any default type filters when none are provided "
        "(like Observation categories)",
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


def parse_resource_selection(types: list[str] | None) -> list[str]:
    """
    Determines the list of resource types based on the requested CLI values.

    Handles "help", "all", and case-insensitivity.
    """
    types = types or ["all"]
    orig_types = set(itertools.chain.from_iterable(x.split(",") for x in types))
    lower_types = {t.casefold() for t in orig_types}

    def print_help():
        rich.print("These resource types are supported:")
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
    group.add_argument(
        "--id-list",
        metavar="IDS",
        help="list of IDs/MRNs to export (instead of a Group), comma separated",
    )
    group.add_argument(
        "--id-file",
        metavar="PATH",
        help="file with IDs/MRNs to export (instead of a Group), one per line "
        "(or a .csv with an 'id' or 'mrn' column)",
    )
    group.add_argument(
        "--id-system",
        metavar="SYSTEM",
        help="system URI of the identifier to look for; if not set, FHIR IDs will be used",
    )
    # Deprecated aliases for the above (deprecated in Nov '25)
    group.add_argument("--mrn-file", dest="id_file", help=argparse.SUPPRESS)
    group.add_argument("--mrn-system", dest="id_system", help=argparse.SUPPRESS)
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


# HYDRATION


def _print_hydration_help():
    rich.print("These hydration tasks are supported:")
    rich.print("  all")
    rich.print("  none")
    for task_name in sorted(tasks.all_tasks.keys()):
        rich.print(f"  {task_name}")


def parse_hydration_tasks(user_tasks: str | None) -> list[type[hydrate_utils.Task]]:
    user_tasks = set(user_tasks.casefold().split(",")) if user_tasks else {"all"}

    if "help" in user_tasks:
        _print_hydration_help()
        sys.exit(0)
    elif "none" in user_tasks:
        return []
    elif "all" in user_tasks:
        return list(itertools.chain.from_iterable(tasks.all_tasks.values()))

    for task_name in user_tasks:
        if task_name not in tasks.all_tasks:
            rich.print(f"Unknown hydration task provided: {task_name}")
            rich.print()
            _print_hydration_help()
            sys.exit(2)

    return list(
        itertools.chain.from_iterable(
            task_list for task_name, task_list in tasks.all_tasks.items() if task_name in user_tasks
        )
    )


# VALIDATION / ERROR HANDLING


verbose = False  # global indicator of verbose mode (rather than trying to pass down to all funcs)


def maybe_print_error(exc: Exception | str) -> None:
    if verbose:
        rich.print(str(exc))


def maybe_print_type_mismatch(expected: str, got: dict) -> None:
    got_type = got.get("resourceType")
    message = f"Error: expected resource type of '{expected}', but got '{got_type}'."

    # We could have received an OperationOutcome, which will hold a real error message we could
    # show. So check for that.
    if text := text_from_operation_outcome(got):
        message = text

    maybe_print_error(message)


def text_from_operation_outcome(resource: dict) -> str | None:
    if resource.get("resourceType") == resources.OPERATION_OUTCOME:
        if issue := resource.get("issue"):
            issue = issue[0]  # just grab first issue
            return issue.get("details", {}).get("text") or issue.get("diagnostics")
    return None


def validate_output_folder(path: str | None) -> None:
    scheme = path and urllib.parse.urlparse(path).scheme
    if scheme:
        sys.exit(f"Expected a local folder, but '{path}' looks like a URL.")


def validate_input_folder(path: str | None) -> None:
    validate_output_folder(path)

    if path and not os.path.exists(path):
        sys.exit(f"Local folder '{path}' does not exist.")


# GENERAL


def add_general(parser: argparse.ArgumentParser, root: bool = False) -> None:
    parser.add_argument(
        "--version", action="version", version=f"smart-fetch {smart_fetch.__version__}"
    )
    default = None if root else argparse.SUPPRESS
    parser.add_argument("--config", "-c", metavar="PATH", help="config file", default=default)
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="show more output (like network errors)"
    )


def add_compression(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--no-compression",
        dest="compress",
        action="store_false",
        help="turns off gzip compression of files (which saves ~90%% of file size, "
        "but may not be supported in some downstream tools)",
    )


def load_config(args) -> None:
    """Loads a config file and injects the contents into our CLI argument list"""
    if not args.config:
        return

    with open(args.config, "rb") as f:
        data = tomllib.load(f)

    for key in data:
        prop = key.replace("-", "_")
        if prop in args and getattr(args, prop) is None:
            if prop in {"type", "type_filter"}:
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
