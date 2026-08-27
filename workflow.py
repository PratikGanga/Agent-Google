"""
core/workflow.py

Automates "complex workflows" by modeling them as a DAG of steps with
dependencies, and executing independent steps concurrently while respecting
ordering constraints. Includes cycle detection and per-step error isolation.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable

logger = logging.getLogger("background_agent.workflow")

StepFn = Callable[["WorkflowContext"], Awaitable[Any]]


@dataclass
class WorkflowContext:
    """Shared state passed between workflow steps; step outputs are stored here."""
    outputs: dict[str, Any] = field(default_factory=dict)

    def get(self, step_name: str) -> Any:
        return self.outputs[step_name]


@dataclass
class Step:
    name: str
    fn: StepFn
    depends_on: list[str] = field(default_factory=list)


class WorkflowError(Exception):
    pass


class Workflow:
    """
    Build with `.add_step(...)`, then `.run()`.

    Example:
        wf = Workflow("etl-pipeline")
        wf.add_step("extract", extract_fn)
        wf.add_step("transform", transform_fn, depends_on=["extract"])
        wf.add_step("load", load_fn, depends_on=["transform"])
        results = await wf.run()
    """

    def __init__(self, name: str):
        self.name = name
        self.steps: dict[str, Step] = {}

    def add_step(self, name: str, fn: StepFn, depends_on: list[str] | None = None) -> "Workflow":
        if name in self.steps:
            raise WorkflowError(f"Step '{name}' already defined")
        self.steps[name] = Step(name=name, fn=fn, depends_on=depends_on or [])
        return self

    def _validate(self) -> None:
        for step in self.steps.values():
            for dep in step.depends_on:
                if dep not in self.steps:
                    raise WorkflowError(f"Step '{step.name}' depends on unknown step '{dep}'")
        self._topological_order()  # raises on cycles

    def _topological_order(self) -> list[str]:
        visited: dict[str, int] = {}  # 0=unvisited,1=visiting,2=done
        order: list[str] = []

        def visit(name: str) -> None:
            state = visited.get(name, 0)
            if state == 2:
                return
            if state == 1:
                raise WorkflowError(f"Cycle detected involving step '{name}'")
            visited[name] = 1
            for dep in self.steps[name].depends_on:
                visit(dep)
            visited[name] = 2
            order.append(name)

        for step_name in self.steps:
            visit(step_name)
        return order

    async def run(self, max_concurrency: int = 8) -> dict[str, Any]:
        self._validate()
        ctx = WorkflowContext()
        semaphore = asyncio.Semaphore(max_concurrency)
        done: set[str] = set()
        errors: dict[str, BaseException] = {}
        in_flight: dict[str, asyncio.Task] = {}
        lock = asyncio.Lock()

        async def run_step(step: Step) -> None:
            async with semaphore:
                logger.info("Running step '%s'", step.name)
                try:
                    result = await step.fn(ctx)
                    ctx.outputs[step.name] = result
                    logger.info("Step '%s' completed", step.name)
                except Exception as exc:  # noqa: BLE001
                    logger.exception("Step '%s' failed", step.name)
                    errors[step.name] = exc
                finally:
                    async with lock:
                        done.add(step.name)

        async def scheduler() -> None:
            remaining = set(self.steps.keys())
            while remaining or in_flight:
                ready = [
                    name
                    for name in remaining
                    if name not in in_flight
                    and all(dep in done for dep in self.steps[name].depends_on)
                    and all(dep not in errors for dep in self.steps[name].depends_on)
                ]
                # Steps whose deps failed are marked failed-by-dependency and dropped.
                blocked = [
                    name
                    for name in remaining
                    if any(dep in errors for dep in self.steps[name].depends_on)
                ]
                for name in blocked:
                    errors[name] = WorkflowError(f"Skipped: upstream dependency failed")
                    remaining.discard(name)
                    done.add(name)

                for name in ready:
                    task = asyncio.create_task(run_step(self.steps[name]))
                    in_flight[name] = task
                    remaining.discard(name)

                if not in_flight:
                    if remaining:
                        raise WorkflowError(f"Deadlock: cannot schedule {remaining}")
                    break

                finished, _ = await asyncio.wait(
                    in_flight.values(), return_when=asyncio.FIRST_COMPLETED
                )
                for t in finished:
                    finished_name = next(n for n, task in in_flight.items() if task is t)
                    del in_flight[finished_name]

        await scheduler()

        if errors:
            logger.warning("Workflow '%s' finished with errors: %s", self.name, list(errors))

        return {"outputs": ctx.outputs, "errors": errors}
