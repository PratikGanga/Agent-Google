"""
example_usage.py

Runnable demo:
  1. A BackgroundAgent that streams progress events while it works.
  2. A DatasetProcessor that "handles the heavy lifting" of a large dataset
     in chunks, concurrently, with checkpointing.
  3. A Workflow that automates a multi-step pipeline (extract -> process -> report)
     built on top of the agent + dataset processor.

Run with:  python -m background_agent.example_usage
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
import tempfile
from pathlib import Path

from .core.agent import AgentEvent, BackgroundAgent
from .core.dataset_processor import DatasetProcessor
from .core.workflow import Workflow, WorkflowContext

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


# --------------------------------------------------------------------- #
# 1. A concrete background agent
# --------------------------------------------------------------------- #

class DataSyncAgent(BackgroundAgent):
    """Example agent: simulates syncing N records with periodic checkpoints."""

    def __init__(self, total_records: int, **kwargs):
        super().__init__(name="DataSyncAgent", **kwargs)
        self.total_records = total_records

    async def execute(self) -> None:
        for i in range(self.total_records):
            await self._checkpoint()  # respects pause()/cancel()
            await asyncio.sleep(0.01)  # simulate I/O (network call, DB write, etc.)
            if i % max(1, self.total_records // 10) == 0:
                await self._emit(f"Synced {i}/{self.total_records}", progress=i / self.total_records)


async def print_event(event: AgentEvent) -> None:
    bar_progress = f"{event.progress:.0%}" if event.progress >= 0 else "n/a"
    print(f"  [agent] {event.status.value:>9} | {bar_progress:>5} | {event.message}")


# --------------------------------------------------------------------- #
# 2. Massive dataset processing
# --------------------------------------------------------------------- #

def make_fake_dataset(path: Path, n_records: int = 5000) -> None:
    """Write a fake JSONL dataset to disk to simulate a 'massive dataset'."""
    with path.open("w", encoding="utf-8") as f:
        for i in range(n_records):
            f.write(json.dumps({"id": i, "value": random.random()}) + "\n")


async def process_chunk(records: list[dict]) -> dict:
    """Simulate CPU/IO work on a chunk: e.g. transform + aggregate."""
    await asyncio.sleep(0.02)  # simulate work
    total = sum(r["value"] for r in records)
    return {"count": len(records), "sum": total}


# --------------------------------------------------------------------- #
# 3. A multi-step workflow tying it together
# --------------------------------------------------------------------- #

async def step_run_agent(ctx: WorkflowContext) -> str:
    agent = DataSyncAgent(total_records=200, on_event=print_event)
    agent.start()
    await agent.wait()
    return "sync_complete"


async def step_process_dataset(ctx: WorkflowContext) -> dict:
    with tempfile.TemporaryDirectory() as tmp:
        dataset_path = Path(tmp) / "dataset.jsonl"
        checkpoint_path = Path(tmp) / "checkpoint.json"
        make_fake_dataset(dataset_path, n_records=5000)

        processor = DatasetProcessor(
            chunk_size=500, concurrency=6, checkpoint_path=str(checkpoint_path)
        )
        source = DatasetProcessor.iter_jsonl(str(dataset_path))
        summary = await processor.process(source, process_chunk)
        return summary


async def step_report(ctx: WorkflowContext) -> str:
    dataset_summary = ctx.get("process_dataset")
    total_records = sum(r["count"] for r in dataset_summary["results"].values())
    total_sum = sum(r["sum"] for r in dataset_summary["results"].values())
    report = (
        f"Processed {total_records} records across "
        f"{dataset_summary['total_chunks']} chunks. Aggregate sum: {total_sum:.2f}"
    )
    print(f"\n[report] {report}")
    return report


async def main() -> None:
    wf = Workflow("agentic-pipeline")
    wf.add_step("sync_agent", step_run_agent)
    wf.add_step("process_dataset", step_process_dataset)  # runs concurrently with sync_agent
    wf.add_step("report", step_report, depends_on=["process_dataset"])

    result = await wf.run(max_concurrency=4)

    if result["errors"]:
        print("Workflow finished with errors:", result["errors"])
    else:
        print("\nWorkflow finished successfully.")


if __name__ == "__main__":
    asyncio.run(main())
