import asyncio
import os
from collections.abc import AsyncIterable, Awaitable, Callable
from functools import partial
from typing import TypeVar

from cumulus_etl import common, errors, fhir

import smart_extract
from smart_extract.cli import cli_utils

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


async def _reader(queue: asyncio.Queue, shutdown: asyncio.Event, iterable: AsyncIterable[Item]) -> None:
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
        process: Callable[[common.NdjsonWriter, Item], Awaitable[None]],
        append: bool = True,
    ):
        self._iterables: dict[str, AsyncIterable | None] = {}
        self._totals: dict[str, int] = {}
        self._progress = cli_utils.make_progress_bar()
        self._progress_task = None
        self._folder = folder
        self._desc = desc
        self._tag = tag
        self._callback = process
        self._append = append

    def add(self, res_type: str, iterable: AsyncIterable[Item], total: int):
        if res_type in self._iterables:
            raise ValueError(f"Can't have two iterables for same resource type {res_type}")

        if os.path.exists(self._done_file(res_type)):
            iterable = None

        self._iterables[res_type] = iterable
        self._totals[res_type] = total

    async def run(self):
        with self._progress:
            for res_type, iterable in self._iterables.items():
                if iterable is None:
                    print(f"Skipping {res_type}, already done.")
                    continue

                self._progress_task = self._progress.add_task(
                    f"{self._desc} {res_type}…", total=self._totals[res_type]
                )

                res_file = self._res_file(res_type, f"{res_type}.ndjson.gz")
                os.makedirs(self._res_folder(res_type), exist_ok=True)

                with common.NdjsonWriter(res_file, append=self._append, compressed=True) as writer:
                    await peek_ahead_processor(
                        iterable,
                        partial(self._process_wrapper, writer),
                        peek_at=fhir.FhirClient.MAX_CONNECTIONS * 2,
                    )

                self._finish(res_type)

        # Reset iterables, so we can be run again
        self._iterables = {}

    def _res_folder(self, res_type: str) -> str:
        return os.path.join(self._folder, res_type)

    def _res_file(self, res_type: str, filename: str) -> str:
        return os.path.join(self._res_folder(res_type), filename)

    def _done_file(self, res_type: str) -> str:
        return self._res_file(res_type, f".{self._tag}.done")

    async def _process_wrapper(self, writer: common.NdjsonWriter, item: Item) -> None:
        await self._callback(writer, item)
        self._progress.update(self._progress_task, advance=1)

    def _finish(self, res_type: str) -> None:
        with open(self._done_file(res_type), "w", encoding="utf8") as f:
            f.write(f"{smart_extract.__version__}\n")
