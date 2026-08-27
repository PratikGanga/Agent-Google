"""
core/dataset_processor.py

Handles "the heavy lifting of massive datasets" by:
  1. Never loading the full dataset into memory — streams it in chunks.
  2. Processing chunks concurrently via the TaskQueue.
  3. Checkpointing progress so a crash/restart can resume instead of
     reprocessing everything from scratch.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from pathlib import Path
from typing import Any, AsyncIterator, Callable, Iterable

from .task_queue import TaskQueue

logger = logging.getLogger("background_agent.dataset")

ChunkFn = Callable[[list[Any]], "asyncio.Future[Any]"]


class DatasetProcessor:
    """
    Streams a large iterable/file source in fixed-size chunks and processes
    each chunk concurrently, with checkpointing for resumability.
    """

    def __init__(
        self,
        chunk_size: int = 1000,
        concurrency: int = 8,
        checkpoint_path: str | None = None,
    ):
        self.chunk_size = chunk_size
        self.checkpoint_path = Path(checkpoint_path) if checkpoint_path else None
        self.queue = TaskQueue(concurrency=concurrency)
        self._last_completed_chunk = self._load_checkpoint()

    # ------------------------------------------------------------------ #
    # Checkpointing
    # ------------------------------------------------------------------ #

    def _load_checkpoint(self) -> int:
        if self.checkpoint_path and self.checkpoint_path.exists():
            try:
                data = json.loads(self.checkpoint_path.read_text())
                return int(data.get("last_completed_chunk", -1))
            except (json.JSONDecodeError, ValueError):
                logger.warning("Corrupt checkpoint file, starting from scratch")
        return -1

    def _save_checkpoint(self, chunk_index: int) -> None:
        if not self.checkpoint_path:
            return
        self.checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.checkpoint_path.with_suffix(".tmp")
        tmp.write_text(json.dumps({"last_completed_chunk": chunk_index}))
        os.replace(tmp, self.checkpoint_path)  # atomic on POSIX

    # ------------------------------------------------------------------ #
    # Streaming source helpers
    # ------------------------------------------------------------------ #

    @staticmethod
    async def iter_jsonl(path: str) -> AsyncIterator[dict]:
        """Stream a large JSON-Lines file one record at a time (no full load)."""
        loop = asyncio.get_event_loop()

        def _read_lines():
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    if line.strip():
                        yield json.loads(line)

        # Run the blocking file iteration in a thread, yielding batches back
        # to the event loop so we don't stall other coroutines.
        gen = _read_lines()
        while True:
            item = await loop.run_in_executor(None, lambda: next(gen, None))
            if item is None:
                break
            yield item

    @staticmethod
    def _chunk(iterable: Iterable[Any], size: int) -> Iterable[list[Any]]:
        batch: list[Any] = []
        for item in iterable:
            batch.append(item)
            if len(batch) >= size:
                yield batch
                batch = []
        if batch:
            yield batch

    # ------------------------------------------------------------------ #
    # Main entry point
    # ------------------------------------------------------------------ #

    async def process(
        self,
        source: AsyncIterator[Any],
        process_chunk: Callable[[list[Any]], Any],
    ) -> dict[str, Any]:
        """
        Consume `source` in chunks and run `process_chunk` on each one
        concurrently, skipping any chunks already completed per checkpoint.
        """
        await self.queue.start()

        buffer: list[Any] = []
        chunk_index = -1

        async def submit_chunk(idx: int, data: list[Any]) -> None:
            if idx <= self._last_completed_chunk:
                logger.info("Skipping already-completed chunk %d", idx)
                return

            async def _wrapped():
                result = await process_chunk(data)
                self._save_checkpoint(idx)
                return result

            await self.queue.submit(_wrapped, priority=5, name=f"chunk-{idx}")

        async for item in source:
            buffer.append(item)
            if len(buffer) >= self.chunk_size:
                chunk_index += 1
                await submit_chunk(chunk_index, buffer)
                buffer = []

        if buffer:
            chunk_index += 1
            await submit_chunk(chunk_index, buffer)

        await self.queue.join()
        await self.queue.shutdown()

        return {
            "total_chunks": chunk_index + 1,
            "results": self.queue.results,
            "errors": self.queue.errors,
        }
