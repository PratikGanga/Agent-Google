# Background Agent Framework

A minimal, dependency-free (stdlib-only) Python framework for building agents that:

- **Run in the background** — non-blocking, cancellable, pausable, with retry/backoff.
- **Handle massive datasets** — stream data in chunks instead of loading it all
  into memory, process chunks concurrently, and checkpoint progress so you can
  resume after a crash instead of starting over.
- **Automate complex workflows asynchronously** — a DAG-based workflow engine
  runs independent steps concurrently while respecting dependencies.

## Structure

```
background_agent/
├── core/
│   ├── agent.py             # BackgroundAgent base class (lifecycle, retries, events)
│   ├── task_queue.py        # Bounded async worker pool with priorities
│   ├── dataset_processor.py # Chunked/streaming processing + checkpointing
│   └── workflow.py          # DAG workflow engine (dependency-aware scheduling)
├── example_usage.py         # End-to-end runnable demo
└── README.md
```

## Quick start

```bash
python -m background_agent.example_usage
```

This spins up:
1. A `DataSyncAgent` (a `BackgroundAgent` subclass) that reports progress as it runs.
2. A `DatasetProcessor` that streams a 5,000-record JSONL file in chunks of 500,
   processes each chunk concurrently, and checkpoints after each chunk.
3. A `Workflow` that runs the agent and the dataset processing concurrently,
   then a `report` step that depends on both finishing.

## Building your own agent

```python
from background_agent import BackgroundAgent

class MyAgent(BackgroundAgent):
    async def execute(self):
        for item in big_list:
            await self._checkpoint()          # respects pause()/cancel()
            await do_work(item)
            await self._emit("progress update", progress=0.5)

agent = MyAgent(name="my-agent", on_event=my_event_handler)
agent.start()          # returns immediately, runs in the background
await agent.wait()     # await completion whenever you're ready
```

## Processing a massive dataset

```python
from background_agent import DatasetProcessor

processor = DatasetProcessor(chunk_size=1000, concurrency=8, checkpoint_path="ckpt.json")
source = DatasetProcessor.iter_jsonl("huge_file.jsonl")

async def handle_chunk(records):
    return {"count": len(records)}

summary = await processor.process(source, handle_chunk)
```

If the process crashes or is killed, re-running with the same `checkpoint_path`
skips chunks that were already completed.

## Automating a workflow

```python
from background_agent import Workflow

wf = Workflow("etl")
wf.add_step("extract", extract_fn)
wf.add_step("transform", transform_fn, depends_on=["extract"])
wf.add_step("load", load_fn, depends_on=["transform"])

result = await wf.run(max_concurrency=4)
```

Steps with no shared dependencies run concurrently automatically. Cycles and
unknown dependencies are detected before execution starts.

## Extending this for production

- **Distributed execution**: swap `TaskQueue` for Celery, Ray, or a Redis-backed
  queue if you need work spread across multiple machines.
- **Durable checkpoints**: replace the JSON checkpoint file with a database
  row/table so multiple processes can coordinate safely.
- **Observability**: pipe `AgentEvent`s to a metrics/log aggregator (e.g. push
  to a Prometheus pushgateway or a structured logging sink) instead of `print`.
- **LLM-driven steps**: a workflow step's `fn` can just as easily be a call to
  an LLM API — the DAG engine doesn't care what a step *does*, only its
  dependencies.
