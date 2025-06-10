import dataclasses
import enum
import os
import sys
from collections.abc import AsyncIterable, Callable
from functools import partial

import cumulus_fhir_support as cfs
import rich.table
from cumulus_etl import common, errors, fhir

from smart_extract import iter_utils


class FixResultReason(enum.Enum):
    ALREADY_DONE = enum.auto()
    NEWLY_DONE = enum.auto()
    FATAL_ERROR = enum.auto()
    RETRY_ERROR = enum.auto()
    IGNORED = enum.auto()


SingleResult = tuple[dict | None, FixResultReason]
Result = list[SingleResult]


@dataclasses.dataclass()
class FixStats:
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

    def _add(self, reason: FixResultReason) -> None:
        self.total += 1
        match reason:
            case FixResultReason.ALREADY_DONE:
                self.already_done += 1
            case FixResultReason.NEWLY_DONE:
                self.newly_done += 1
            case FixResultReason.FATAL_ERROR:
                self.fatal_errors += 1
            case FixResultReason.RETRY_ERROR:
                self.retry_errors += 1

    def add_resource_reasons(self, reasons: list[FixResultReason]) -> None:
        self.total_resources += 1
        if any(r == FixResultReason.ALREADY_DONE for r in reasons):
            self.already_done_resources += 1
        if any(r == FixResultReason.NEWLY_DONE for r in reasons):
            self.newly_done_resources += 1
        if any(r == FixResultReason.FATAL_ERROR for r in reasons):
            self.fatal_errors_resources += 1
        if any(r == FixResultReason.RETRY_ERROR for r in reasons):
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


async def _read(folder: dir, res_type: str) -> AsyncIterable[dict]:
    for res in cfs.read_multiline_json_from_dir(folder, res_type):
        yield res


async def _write(
    callback: Callable,
    client,
    id_pool: set[str],
    stats: FixStats,
    writer: common.NdjsonWriter,
    resource: dict,
) -> None:
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
    client,
    folder: str,
    input_type: str,
    fix_name: str,
    desc: str,
    callback: Callable,
    output_folder: str | None = None,
    output_type: str | None = None,
    append: bool = True,
) -> FixStats:
    output_type = output_type or input_type
    output_folder = output_folder or output_type
    input_subfolder = os.path.join(folder, input_type)
    output_subfolder = os.path.join(folder, output_folder)

    if not append and output_type != input_type:
        raise ValueError("Must use same input and output type if re-writing the resources")

    # Calculate total progress needed
    found_files = cfs.list_multiline_json_in_dir(input_subfolder, input_type)
    total_lines = sum(common.read_local_line_count(path) for path in found_files)

    if not total_lines:
        sys.exit(f"Cannot run the {fix_name} fix, no {input_type} resources found.")

    # See what is already present
    downloaded_ids = set()
    if append:
        for resource in cfs.read_multiline_json_from_dir(output_subfolder, output_type):
            downloaded_ids.add(f"{output_type}/{resource['id']}")

    # Iterate through inputs
    stats = FixStats()
    writer = partial(_write, callback, client, downloaded_ids, stats)
    processor = iter_utils.ResourceProcessor(folder, f"fix:{fix_name}", desc, writer, append=append)
    processor.add(
        output_type, _read(input_subfolder, input_type), total_lines, res_folder=output_folder
    )
    await processor.run()

    return stats


async def download_reference(
    client, id_pool: set[str], reference: str, expected_type: str
) -> SingleResult:
    if not reference.startswith(f"{expected_type}/"):
        return None, FixResultReason.IGNORED
    elif reference in id_pool:
        return None, FixResultReason.ALREADY_DONE

    try:
        resource = await fhir.download_reference(client, reference)
    except errors.FatalNetworkError:
        return None, FixResultReason.FATAL_ERROR
    except errors.TemporaryNetworkError:
        return None, FixResultReason.RETRY_ERROR

    if not resource:
        return None, FixResultReason.IGNORED

    if resource["resourceType"] != expected_type:
        # Hmm, wrong type. Could be OperationOutcome? Mark as fatal.
        return None, FixResultReason.FATAL_ERROR

    # Add this immediately, to help avoid any re-downloading during a fix operation
    id_pool.add(f"{resource['resourceType']}/{resource['id']}")
    return resource, FixResultReason.NEWLY_DONE
