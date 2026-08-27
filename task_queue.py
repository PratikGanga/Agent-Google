"""
core/task_queue.py

A bounded async task queue that runs multiple agents/jobs concurrently with a
worker-pool pattern. This is what lets the system "handle the heavy lifting"
without blocking the main thread or overwhelming memory/CPU.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable

logger = logging.getLogger("background_agent.queue")

JobFn = Callable[..., Awaitable[Any]]


@dataclass(order=True)
class Job:
    priority: int
    fn: JobFn = field(compare=False)
    args: tuple = field(default_factory=tuple, compare=False)
    kwargs: dict = field(default_factory=dict, compare=False)
    name: str = field(default="job", compare=False)


class TaskQueue:
    """
    Priority-aware async queue with a fixed-size worker pool.

    Usage:
        queue = TaskQueue(concurrency=8)
        await queue.start()
        await queue.submit(my_coro_fn, arg1, arg2, priority=1, name="chunk-0")
        await queue.join()
        await queue.shutdown()
    """

    def __init__(self, concurrency: int = 8, max_queue_size: int = 0):
        self.concurrency = concurrency
        self._queue: asyncio.PriorityQueue[Job] = asyncio.PriorityQueue(maxsize=max_queue_size)
        self._workers: list[asyncio.Task] = []
        self._results: dict[str, Any] = {}
        self._errors: dict[str, BaseException] = {}
        self._running = False

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._workers = [
            asyncio.create_task(self._worker_loop(i), name=f"worker-{i}")
            for i in range(self.concurrency)
        ]
        logger.info("TaskQueue started with %d workers", self.concurrency)

    async def submit(
        self,
        fn: JobFn,
        *args: Any,
        priority: int = 5,
        name: str = "job",
        **kwargs: Any,
    ) -> None:
        """Lower `priority` value = runs sooner. Applies backpressure if the
        queue is bounded and full — the caller will await until there's room."""
        await self._queue.put(Job(priority=priority, fn=fn, args=args, kwargs=kwargs, name=name))

    async def _worker_loop(self, worker_id: int) -> None:
        while True:
            job = await self._queue.get()
            try:
                logger.debug("worker-%d picked up %s", worker_id, job.name)
                result = await job.fn(*job.args, **job.kwargs)
                self._results[job.name] = result
            except Exception as exc:  # noqa: BLE001
                logger.exception("Job %s failed", job.name)
                self._errors[job.name] = exc
            finally:
                self._queue.task_done()

    async def join(self) -> None:
        """Wait until every submitted job has been processed."""
        await self._queue.join()

    async def shutdown(self) -> None:
        for worker in self._workers:
            worker.cancel()
        await asyncio.gather(*self._workers, return_exceptions=True)
        self._running = False
        logger.info("TaskQueue shut down")

    @property
    def results(self) -> dict[str, Any]:
        return self._results

    @property
    def errors(self) -> dict[str, BaseException]:
        return self._errors
