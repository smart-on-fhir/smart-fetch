import itertools
import os

import cumulus_fhir_support as cfs

from smart_fetch import lifecycle, ndjson, resources, tasks


def reset_all_links(managed_dir: str) -> None:
    for res_type in resources.SCOPE_TYPES:
        reset_res_links(managed_dir, res_type)


def reset_res_links(managed_dir: str, res_type: str) -> None:
    # Remove all current links for this resource type
    for entry in os.scandir(managed_dir):
        if entry.name.startswith(f"{res_type}.") and entry.is_symlink():
            os.remove(entry.path)

    # Add links for all "active" files for this resource type, and consider all possible ways
    # the resource could appear (either direct export or hydration).
    files = _find_active_resource_files(managed_dir, res_type)
    for index, path in enumerate(files, 1):
        compressed = ndjson.is_compressed(path)
        link_name = ndjson.filename(f"{res_type}.{index:03}.ndjson", compress=compressed)
        os.symlink(path, os.path.join(managed_dir, link_name))


def _export_types_for_res_type(res_type: str) -> set[str]:
    task_list = list(itertools.chain.from_iterable(tasks.all_tasks.values()))
    possible = {task.INPUT_RES_TYPE for task in task_list if task.OUTPUT_RES_TYPE == res_type}
    possible.add(res_type)
    return possible & set(resources.PATIENT_TYPES)


def _find_active_resource_files(managed_dir: str, res_type: str) -> list[str]:
    """
    Reports all NDJSON files for active resources in the managed dir.

    See `_find_active_resource_workdirs` for more explanation of what "active" means.

    Will include all possible ways to generate the provided `res_type` -- i.e. if res_type can be
    gotten by hydration, we look for all active export folders that could have generated it.

    Args:
        managed_dir: The toplevel export dir.
        res_type: The resource type to find.

    Returns:
        A list of filenames, relative to `managed_dir`, with the oldest ones first.
    """
    workdirs = set()
    for export_type in _export_types_for_res_type(res_type):
        workdirs.update(_find_active_resource_workdirs(managed_dir, export_type))

    # Add all res type filenames from the workdirs
    filenames = []
    for workdir in sorted(workdirs):
        filenames.extend(cfs.list_multiline_json_in_dir(workdir, res_type))

    return [os.path.relpath(f, start=managed_dir) for f in filenames]


def _find_active_resource_workdirs(managed_dir: str, res_type: str) -> list[str]:
    """
    Reports all workdirs (managed dir subfolders) for active resources in the managed dir.

    "Active" here means "currently valid" - like if two back to back full exports were made,
    we don't care about the resources in the first full export - it's entirely obsoleted by the
    second export. But we *do* want to include "since" exports that have occurred after a full.

    This gets trickier with filters. All non-subsumed filtered resources are kept around.
    i.e. if you filter on Patient?active=true and Patient?gender=female, we'll accept both of those,
    until we get to a bare Patient resource without a filter.

    Overall, we don't try SUPER hard to avoid duplicates. Like, if there are two "since" exports
    with the same "since" value, we don't notice that - we're mostly just on the hunt for full
    exports.

    Args:
        managed_dir: The toplevel export dir.
        res_type: The resource type to find.

    Returns:
        A list of workdir folder names, most recent first.
    """
    full_export_filters = set()
    workdirs = []

    # Check each folder from newest to oldest
    for folder in lifecycle.list_workdirs(managed_dir):
        workdir = os.path.join(managed_dir, folder)
        metadata = lifecycle.OutputMetadata(workdir)
        filters = metadata.get_res_filters(res_type)
        if filters is None:
            continue  # it's not even about us
        if filters and filters.issubset(full_export_filters):
            continue  # don't need these, we already have a newer export of them

        # This workdir is active for this resource! Add it.
        workdirs.append(workdir)

        # If this was a full export (not a "since" export), note the filters down
        if res_type not in metadata.get_since_resources():
            if not filters:
                break  # no filter at all - we're done! this was a full complete export
            full_export_filters |= metadata.get_res_filters(res_type)

    return workdirs
