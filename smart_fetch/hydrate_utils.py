import dataclasses
import enum
import logging
import os
from collections.abc import AsyncIterable, Callable
from functools import partial

import cumulus_fhir_support as cfs
import rich.progress
import rich.table

from smart_fetch import iter_utils, ndjson


class TaskResultReason(enum.Enum):
    ALREADY_DONE = enum.auto()
    NEWLY_DONE = enum.auto()
    FATAL_ERROR = enum.auto()
    RETRY_ERROR = enum.auto()
    IGNORED = enum.auto()


SingleResult = tuple[dict | None, TaskResultReason]
Result = list[SingleResult]


@dataclasses.dataclass()
class TaskStats:
    total: int = 0
    total_resources: int = 0
    already_done: int = 0
    already_done_resources: int = 0
    newly_done: int = 0
    newly_done_resources: int = 0
    fatal_errors: int = 0
    fatal_errors_resources: int = 0
    retry_errors: int = 0
    retry_errors_resources: int = 0

    def _add(self, reason: TaskResultReason) -> None:
        self.total += 1
        match reason:
            case TaskResultReason.ALREADY_DONE:
                self.already_done += 1
            case TaskResultReason.NEWLY_DONE:
                self.newly_done += 1
            case TaskResultReason.FATAL_ERROR:
                self.fatal_errors += 1
            case TaskResultReason.RETRY_ERROR:
                self.retry_errors += 1

    def add_resource_reasons(self, reasons: list[TaskResultReason]) -> None:
        self.total_resources += 1
        if any(r == TaskResultReason.ALREADY_DONE for r in reasons):
            self.already_done_resources += 1
        if any(r == TaskResultReason.NEWLY_DONE for r in reasons):
            self.newly_done_resources += 1
        if any(r == TaskResultReason.FATAL_ERROR for r in reasons):
            self.fatal_errors_resources += 1
        if any(r == TaskResultReason.RETRY_ERROR for r in reasons):
            self.retry_errors_resources += 1

        for reason in reasons:
            self._add(reason)

    def print(self, adjective: str, resource_header: str, item_header: str | None = None):
        table = rich.table.Table(
            "",
            rich.table.Column(header=resource_header, justify="right"),
            box=None,
        )
        if item_header:
            table.add_column(header=item_header, justify="right")
            table.add_row("Total examined", f"{self.total_resources:,}", f"{self.total:,}")
            if self.already_done:
                table.add_row(
                    f"Already {adjective}",
                    f"{self.already_done_resources:,}",
                    f"{self.already_done:,}",
                )
            table.add_row(
                f"Newly {adjective}", f"{self.newly_done_resources:,}", f"{self.newly_done:,}"
            )
            if self.fatal_errors:
                table.add_row(
                    "Fatal errors", f"{self.fatal_errors_resources:,}", f"{self.fatal_errors:,}"
                )
            if self.retry_errors:
                table.add_row(
                    "Retried but gave up",
                    f"{self.retry_errors_resources:,}",
                    f"{self.retry_errors:,}",
                )
        else:
            table.add_row("Total examined", f"{self.total:,}")
            if self.already_done:
                table.add_row(f"Already {adjective}", f"{self.already_done:,}")
            table.add_row(f"Newly {adjective}", f"{self.newly_done:,}")
            if self.fatal_errors:
                table.add_row("Fatal errors", f"{self.fatal_errors:,}")
            if self.retry_errors:
                table.add_row("Retried but gave up", f"{self.retry_errors:,}")
        rich.get_console().print(table)


async def _read(res_file: str) -> AsyncIterable[dict]:
    for res in cfs.read_multiline_json(res_file):
        yield res


async def _write(
    callback: Callable,
    client: cfs.FhirClient,
    id_pool: set[str],
    stats: TaskStats,
    res_type: str,
    writer: ndjson.NdjsonWriter,
    resource: dict,
    **kwargs,
) -> None:
    del res_type

    results = await callback(client, resource, id_pool)

    for result in results:
        if result[0]:
            writer.write(result[0])
            # This might have already been added previously, but guarantee it's added here,
            # regardless of whether the callback already did it.
            id_pool.add(f"{result[0]['resourceType']}/{result[0]['id']}")

    stats.add_resource_reasons([result[1] for result in results])


async def process(
    *,
    client: cfs.FhirClient,
    task_name: str,
    desc: str,
    workdir: str | None,
    input_type: str,
    source_dir: str | None = None,
    output_type: str | None = None,
    append: bool = True,
    file_slug: str | None = None,
    callback: Callable,
    progress: rich.progress.Progress | None = None,
) -> TaskStats | None:
    """
    Reads resources from a folder, and calls `callback` on each one.

    This implements the guts of our hydration task workflow.
    """
    output_type = output_type or input_type
    source_dir = source_dir or workdir

    if not append and output_type != input_type:
        raise ValueError(  # pragma: no cover
            "Must use same input and output type when re-writing resources"
        )
    if not append and file_slug:
        raise ValueError(  # pragma: no cover
            "Cannot provide a file slug when re-writing resources"
        )
    if not append:
        # Cannot use a separate source dir when re-writing resources, so enforce that here
        source_dir = workdir

    rich.get_console().rule()

    # Calculate total progress needed
    found_files = cfs.list_multiline_json_in_dir(source_dir, input_type)

    if not found_files:
        logging.warning(f"Skipping {task_name}, no {input_type} resources found.")
        return None

    # See what is already present
    downloaded_ids = set()
    if append:
        for resource in cfs.read_multiline_json_from_dir(workdir, output_type):
            downloaded_ids.add(f"{output_type}/{resource['id']}")

    # Iterate through inputs
    stats = TaskStats()
    writer = partial(_write, callback, client, downloaded_ids, stats)
    processor = iter_utils.ResourceProcessor(
        workdir, desc, writer, append=append, progress=progress
    )
    for res_file in cfs.list_multiline_json_in_dir(source_dir, input_type):
        if not append:
            output_file = res_file
        elif file_slug:
            output_file = os.path.join(workdir, f"{output_type}.{file_slug}.ndjson.gz")
        else:
            output_file = None
        total_lines = ndjson.read_local_line_count(res_file)
        processor.add_source(output_type, _read(res_file), total_lines, output_file=output_file)
    await processor.run()

    return stats


async def download_reference(
    client, id_pool: set[str], reference: str | None, expected_type: str
) -> SingleResult:
    if not reference or reference.startswith("#"):
        return None, TaskResultReason.IGNORED
    elif not reference.startswith(f"{expected_type}/"):
        return None, TaskResultReason.IGNORED
    elif reference in id_pool:
        return None, TaskResultReason.ALREADY_DONE

    try:
        response = await client.request("GET", reference)
        resource = response.json()
    except cfs.FatalNetworkError:
        return None, TaskResultReason.FATAL_ERROR
    except cfs.TemporaryNetworkError:
        return None, TaskResultReason.RETRY_ERROR

    if resource.get("resourceType") != expected_type:
        # Hmm, wrong type. Could be OperationOutcome? Mark as fatal.
        return None, TaskResultReason.FATAL_ERROR

    # Add this immediately, to help avoid any re-downloading during a hydrate operation
    id_pool.add(f"{resource['resourceType']}/{resource['id']}")
    return resource, TaskResultReason.NEWLY_DONE
