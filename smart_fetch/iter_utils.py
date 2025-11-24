import asyncio
import dataclasses
import os
import sys
from collections.abc import AsyncIterable, Awaitable, Callable
from functools import partial
from typing import TypeVar

import cumulus_fhir_support as cfs
import rich.progress

from smart_fetch import cli_utils, ndjson, timing

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
                sys.exit(str(result))


@dataclasses.dataclass
class _SourceDetails:
    iterable: AsyncIterable
    total: int
    output_file: str | None = None


class ResourceProcessor:
    def __init__(
        self,
        folder: str,
        desc: str,
        callback: Callable[[str, ndjson.NdjsonWriter, Item], Awaitable[None]],
        finish_callback: Callable[[str], Awaitable[None]] | None = None,
        append: bool = True,
        compress: bool = False,
    ):
        self.sources: dict[str, list[_SourceDetails]] = {}
        self._callback = callback
        self._finish_callback = finish_callback
        self._folder = folder
        self._desc = desc
        self._append = append
        self._compress = compress

    def add_source(
        self,
        res_type: str,
        iterable: AsyncIterable[Item],
        *,
        total: int,
        output_file: str,
    ):
        output_file = os.path.join(self._folder, output_file)
        source = _SourceDetails(iterable, total, output_file)
        self.sources.setdefault(res_type, []).append(source)

    async def run(self):
        for res_type, sources in self.sources.items():
            timestamp = timing.now()

            with cli_utils.make_progress_bar() as progress:
                res_total = sum(src.total for src in sources)
                task = progress.add_task(f"{self._desc} {res_type}s…", total=res_total)

                for source in sources:
                    writer = ndjson.NdjsonWriter(source.output_file, append=self._append)
                    with writer:
                        await peek_ahead_processor(
                            source.iterable,
                            partial(self._process_wrapper, writer, res_type, progress, task),
                            peek_at=cfs.FhirClient.MAX_CONNECTIONS * 2,
                        )

            if self._finish_callback:
                await self._finish_callback(res_type, timestamp=timestamp)

        # Reset sources, so we can be run again
        self.sources = {}

    async def _process_wrapper(
        self,
        writer: ndjson.NdjsonWriter,
        res_type: str,
        progress: rich.progress.Progress,
        task: rich.progress.TaskID,
        item: Item,
    ) -> None:
        await self._callback(res_type, writer, item)
        progress.update(task, advance=1)
