import abc
import dataclasses
import enum
import itertools
from collections.abc import AsyncIterable, Callable, Iterator
from functools import partial

import cumulus_fhir_support as cfs
import rich.progress
import rich.table

from smart_fetch import cli_utils, iter_utils, ndjson


class TaskResultReason(enum.Enum):
    ALREADY_DONE = enum.auto()
    NEWLY_DONE = enum.auto()
    FATAL_ERROR = enum.auto()
    RETRY_ERROR = enum.auto()
    IGNORED = enum.auto()


SingleResult = tuple[dict | None, TaskResultReason]
Result = list[SingleResult]


class Task(abc.ABC):
    NAME: str  # name of task
    INPUT_RES_TYPE: str  # resource type to read in
    OUTPUT_RES_TYPE: str  # resource type to write out

    def __init__(self, client: cfs.FhirClient, compress: bool = False):
        self.client = client
        self.compress = compress

    @abc.abstractmethod
    async def run(self, workdir: str, **kwargs) -> None:
        """Runs the task"""

    @abc.abstractmethod
    async def process_one(self, resource: dict, id_pool: set[str], **kwargs) -> Result:
        """Handles one resource"""


class ReferenceDownloadTask(Task):
    """A task that simply downloads referenced resources (e.g. linked Observations)"""

    # This is a tuple of reference field names.
    # Examples:
    #    "performer" (simple 0..1 field)
    #    "performer*" (0..* field)
    #    "performer.actor" (nested field)
    #    "performer*.actor" (nested field inside an array)
    REFS: tuple[str] = ()
    FILE_SLUG = "referenced"

    async def run(self, workdir: str, source_dir: str | None = None, **kwargs) -> None:
        rich.print(f"Downloading referenced {self.OUTPUT_RES_TYPE}s from {self.INPUT_RES_TYPE}s.")
        stats = await process(
            task_name=self.NAME,
            desc="Downloading",
            workdir=workdir,
            source_dir=source_dir or workdir,
            input_type=self.INPUT_RES_TYPE,
            output_type=self.OUTPUT_RES_TYPE,
            callback=self.process_one,
            file_slug=self.FILE_SLUG,
            compress=self.compress,
        )
        if stats:
            stats.print("downloaded", f"{self.INPUT_RES_TYPE}s", f"{self.OUTPUT_RES_TYPE}s")

    @classmethod
    def _resolve_ref_field(cls, resource: dict, field: str) -> list[dict]:
        parts = field.split(".", 1)
        cur_field = parts[0].removesuffix("*")
        if parts[0].endswith("*"):
            children = resource.get(cur_field, [])
        else:
            children = [resource.get(cur_field, {})]
        if len(parts) == 1:
            return children
        else:
            return itertools.chain.from_iterable(
                cls._resolve_ref_field(child, parts[1]) for child in children
            )

    @classmethod
    def resolve_ref_fields(cls, resource: dict) -> Iterator[str]:
        refs = itertools.chain.from_iterable(
            cls._resolve_ref_field(resource, field) for field in cls.REFS
        )
        return filter(None, [ref.get("reference") for ref in refs])

    async def process_one(self, resource: dict, id_pool: set[str], **kwargs) -> Result:
        refs = self.resolve_ref_fields(resource)
        results = [
            await download_reference(self.client, id_pool, ref, self.OUTPUT_RES_TYPE)
            for ref in refs
        ]
        # Recurse on results if input and output res types are the same.
        # This avoids loops because the ID pool prevents us from visiting a resource twice.
        if self.INPUT_RES_TYPE == self.OUTPUT_RES_TYPE:
            for result in results:
                if result[0]:
                    results.extend(await self.process_one(result[0], id_pool))
        return results


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

    def print(self, adjective: str, resource_header: str, item_header: str):
        if resource_header == item_header:
            resource_header = f"Source {resource_header}"
            item_header = f"Target {item_header}"
        table = rich.table.Table(
            "",
            rich.table.Column(header=resource_header, justify="right"),
            box=None,
        )
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
        rich.get_console().print(table)


async def _read(res_file: str) -> AsyncIterable[dict]:
    for res in cfs.read_multiline_json(res_file):
        yield res


async def _write(
    callback: Callable,
    id_pool: set[str],
    stats: TaskStats,
    res_type: str,
    writer: ndjson.NdjsonWriter,
    resource: dict,
    **kwargs,
) -> None:
    del res_type

    results = await callback(resource, id_pool)

    for result in results:
        if result[0]:
            writer.write(result[0])
            # This might have already been added previously, but guarantee it's added here,
            # regardless of whether the callback already did it.
            id_pool.add(f"{result[0]['resourceType']}/{result[0]['id']}")

    stats.add_resource_reasons([result[1] for result in results])


async def process(
    *,
    task_name: str,
    desc: str,
    workdir: str | None,
    input_type: str,
    source_dir: str | None = None,
    output_type: str | None = None,
    append: bool = True,
    compress: bool = False,
    file_slug: str | None = None,
    callback: Callable,
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

    # Calculate total progress needed
    found_files = cfs.list_multiline_json_in_dir(source_dir, input_type)

    if not found_files:
        rich.print(f"Skipping {task_name}, no {input_type} resources found.")
        return None

    # See what is already present
    downloaded_ids = set()
    if append:
        for resource in cfs.read_multiline_json_from_dir(workdir, output_type):
            downloaded_ids.add(f"{output_type}/{resource['id']}")

    # Iterate through inputs
    stats = TaskStats()
    writer = partial(_write, callback, downloaded_ids, stats)
    processor = iter_utils.ResourceProcessor(workdir, desc, writer, append=append)
    for res_file in cfs.list_multiline_json_in_dir(source_dir, input_type):
        if not append:
            output_file = res_file
        elif file_slug:
            output_file = ndjson.filename(f"{output_type}.{file_slug}.ndjson", compress=compress)
        else:
            output_file = ndjson.filename(f"{output_type}.ndjson", compress=compress)
        total_lines = ndjson.read_local_line_count(res_file)
        processor.add_source(
            output_type, _read(res_file), total=total_lines, output_file=output_file
        )
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

    # Add this immediately, to help avoid any re-downloading during a hydrate operation.
    # Notice that we want to add this before "awaiting", so that another fiber and us don't both
    # try to download this.
    id_pool.add(reference)

    try:
        response = await client.request("GET", reference)
        resource = response.json()
    except cfs.FatalNetworkError as exc:
        cli_utils.maybe_print_error(exc)
        return None, TaskResultReason.FATAL_ERROR
    except cfs.TemporaryNetworkError as exc:
        cli_utils.maybe_print_error(exc)
        return None, TaskResultReason.RETRY_ERROR

    if resource.get("resourceType") != expected_type:
        # Hmm, wrong type. Could be OperationOutcome? Mark as fatal.
        cli_utils.maybe_print_type_mismatch(expected_type, resource)
        return None, TaskResultReason.FATAL_ERROR

    return resource, TaskResultReason.NEWLY_DONE
