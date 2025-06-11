import asyncio
import datetime
import os
from collections.abc import AsyncIterable, Awaitable, Callable
from functools import partial
from typing import TypeVar

from cumulus_etl import common, errors, fhir

from smart_extract import cli_utils, lifecycle

Item = TypeVar("Item")


def _drain_queue(queue: asyncio.Queue) -> None:
    """
    Used to empty the queue if we are early exiting.

    Once we depend on Python 3.13, we can just use Queue.shutdown() instead.
    """
    while not queue.empty():
        queue.get_nowait()
        queue.task_done()


async def _worker(
    queue: asyncio.Queue, shutdown: asyncio.Event, processor: Callable[[Item], Awaitable[None]]
) -> None:
    while True:
        item = await queue.get()
        try:
            await processor(item)
        except Exception:
            shutdown.set()  # flag other tasks to stop
            raise
        finally:
            queue.task_done()
            if shutdown.is_set():
                _drain_queue(queue)


async def _reader(
    queue: asyncio.Queue, shutdown: asyncio.Event, iterable: AsyncIterable[Item]
) -> None:
    async for item in iterable:
        await queue.put(item)
        if shutdown.is_set():
            _drain_queue(queue)
            break


async def peek_ahead_processor(
    iterable: AsyncIterable[Item],
    processor: Callable[[Item], Awaitable[None]],
    *,
    peek_at: int,
) -> None:
    """Processes items in sequence, but always looks at some in parallel"""
    queue = asyncio.Queue(peek_at)
    shutdown = asyncio.Event()  # poor substitute for Python 3.13's wonderful Queue.shutdown()
    reader = asyncio.create_task(_reader(queue, shutdown, iterable), name="peek-ahead-reader")
    tasks = [
        asyncio.create_task(_worker(queue, shutdown, processor), name=f"peek-ahead-worker-{i}")
        for i in range(peek_at)
    ]

    try:
        await reader  # read all the input, processing as we go
        await queue.join()  # finish up final batch of workers
    finally:
        # Close out the tasks
        for task in tasks:
            task.cancel()

        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Check for an early exit in the tasks
        for result in results:
            if result and not isinstance(result, asyncio.CancelledError):
                errors.fatal(str(result), errors.INLINE_TASK_FAILED)


class ResourceProcessor:
    def __init__(
        self,
        folder: str,
        tag: str,
        desc: str,
        callback: Callable[[str, common.NdjsonWriter, Item], Awaitable[None]],
        finish_callback: Callable[[str, datetime.datetime], Awaitable[None]] | None = None,
        append: bool = True,
    ):
        self._iterables: dict[str, AsyncIterable | None] = {}
        self._folders: dict[str, str] = {}
        self._totals: dict[str, int] = {}
        self._callback = callback
        self._finish_callback = finish_callback
        self._progress = cli_utils.make_progress_bar()
        self._progress_task = None
        self._folder = folder
        self._desc = desc
        self._tag = tag
        self._append = append

    def add(
        self,
        res_type: str,
        iterable: AsyncIterable[Item],
        total: int,
        *,
        res_folder: str | None = None,
    ):
        if res_type in self._iterables:
            raise ValueError(f"Can't have two iterables for same resource type {res_type}")

        self._iterables[res_type] = iterable
        self._folders[res_type] = os.path.join(self._folder, res_folder or res_type)
        self._totals[res_type] = total

    async def run(self):
        with self._progress:
            for res_type, iterable in self._iterables.items():
                if lifecycle.should_skip(self._folders[res_type], self._tag):
                    print(f"Skipping {res_type}, already done.")
                    continue
                with lifecycle.mark_done(self._folders[res_type], self._tag) as start_time:
                    self._progress_task = self._progress.add_task(
                        f"{self._desc} {res_type}s…", total=self._totals[res_type]
                    )

                    os.makedirs(self._folders[res_type], exist_ok=True)
                    final_file = os.path.join(self._folders[res_type], f"{res_type}.ndjson.gz")
                    output_file = final_file
                    if not self._append:
                        output_file += ".tmp"

                    writer = common.NdjsonWriter(output_file, append=self._append, compressed=True)
                    with writer:
                        await peek_ahead_processor(
                            iterable,
                            partial(self._process_wrapper, writer, res_type),
                            peek_at=fhir.FhirClient.MAX_CONNECTIONS * 2,
                        )

                    if output_file != final_file and os.path.exists(output_file):
                        os.replace(output_file, final_file)

                    if self._finish_callback:
                        await self._finish_callback(res_type, start_time)

        # Reset iterables, so we can be run again
        self._iterables = {}

    async def _process_wrapper(
        self, writer: common.NdjsonWriter, res_type: str, item: Item
    ) -> None:
        await self._callback(res_type, writer, item)
        self._progress.update(self._progress_task, advance=1)
