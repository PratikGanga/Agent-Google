"""
core/agent.py

Base class for a next-generation background agent.

Design goals:
- Runs fully asynchronously (asyncio), so it doesn't block the caller.
- Can be started, paused, resumed, and cancelled cleanly.
- Emits structured status/progress events that a UI or orchestrator can subscribe to.
- Handles failures with retry + backoff instead of crashing the whole process.
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Awaitable, Callable, Optional

logger = logging.getLogger("background_agent")


class AgentStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class AgentEvent:
    agent_id: str
    status: AgentStatus
    message: str
    progress: float = 0.0          # 0.0 - 1.0
    timestamp: float = field(default_factory=time.time)
    data: Optional[dict[str, Any]] = None


EventCallback = Callable[[AgentEvent], Awaitable[None]]


class BackgroundAgent:
    """
    A long-running, cancellable, resumable unit of work.

    Subclass this and implement `run_step()` (called repeatedly) or override
    `execute()` entirely for custom control flow.
    """

    def __init__(
        self,
        name: str,
        max_retries: int = 3,
        retry_backoff_base: float = 1.5,
        on_event: Optional[EventCallback] = None,
    ):
        self.id = str(uuid.uuid4())
        self.name = name
        self.status = AgentStatus.PENDING
        self.max_retries = max_retries
        self.retry_backoff_base = retry_backoff_base
        self._on_event = on_event

        self._pause_event = asyncio.Event()
        self._pause_event.set()  # not paused by default
        self._cancel_requested = False
        self._task: Optional[asyncio.Task] = None

    # ------------------------------------------------------------------ #
    # Lifecycle controls
    # ------------------------------------------------------------------ #

    def start(self) -> asyncio.Task:
        """Schedule the agent to run in the background and return its Task."""
        self._task = asyncio.create_task(self._run_with_supervision(), name=self.name)
        return self._task

    def pause(self) -> None:
        self._pause_event.clear()
        self.status = AgentStatus.PAUSED

    def resume(self) -> None:
        self._pause_event.set()
        if self.status == AgentStatus.PAUSED:
            self.status = AgentStatus.RUNNING

    def cancel(self) -> None:
        self._cancel_requested = True
        self._pause_event.set()  # unblock if paused, so cancellation can propagate

    async def wait(self) -> None:
        if self._task:
            await self._task

    # ------------------------------------------------------------------ #
    # Internal supervision: retries, backoff, pause/cancel checks
    # ------------------------------------------------------------------ #

    async def _run_with_supervision(self) -> None:
        attempt = 0
        self.status = AgentStatus.RUNNING
        await self._emit("Agent started", progress=0.0)

        while True:
            try:
                await self.execute()
                self.status = AgentStatus.COMPLETED
                await self._emit("Agent completed", progress=1.0)
                return
            except asyncio.CancelledError:
                self.status = AgentStatus.CANCELLED
                await self._emit("Agent cancelled")
                raise
            except Exception as exc:  # noqa: BLE001
                attempt += 1
                if attempt > self.max_retries:
                    self.status = AgentStatus.FAILED
                    await self._emit(f"Agent failed permanently: {exc}")
                    raise
                delay = self.retry_backoff_base ** attempt
                logger.warning(
                    "Agent %s failed (attempt %d/%d): %s — retrying in %.1fs",
                    self.name, attempt, self.max_retries, exc, delay,
                )
                await self._emit(
                    f"Retrying after error: {exc}", progress=None, data={"attempt": attempt}
                )
                await asyncio.sleep(delay)

    async def _checkpoint(self) -> None:
        """
        Call this frequently inside execute()/run_step() implementations.
        Honors pause requests and raises CancelledError on cancellation.
        """
        if self._cancel_requested:
            raise asyncio.CancelledError()
        await self._pause_event.wait()
        if self._cancel_requested:
            raise asyncio.CancelledError()

    async def _emit(
        self, message: str, progress: Optional[float] = None, data: Optional[dict] = None
    ) -> None:
        event = AgentEvent(
            agent_id=self.id,
            status=self.status,
            message=message,
            progress=progress if progress is not None else -1.0,
            data=data,
        )
        logger.info("[%s] %s (progress=%s)", self.name, message, progress)
        if self._on_event:
            await self._on_event(event)

    # ------------------------------------------------------------------ #
    # Override this
    # ------------------------------------------------------------------ #

    async def execute(self) -> None:
        """Override with the agent's actual work. Call `await self._checkpoint()`
        periodically inside loops so pause/cancel are respected."""
        raise NotImplementedError
