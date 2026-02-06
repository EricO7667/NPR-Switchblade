# runner/runner_local.py

"""
======================================================================
RunnerLocal — Executes the Lego Brick Pipeline Locally
======================================================================

Responsible for:
- Reading pipeline.yaml
- Executing bricks sequentially
- Injecting metadata automatically per step
- Optionally saving payload snapshots

Each brick stays pure; runner handles history and logging.
======================================================================
"""

import os, yaml
from datetime import datetime
from npr_tool.runner.payload import Payload
from npr_tool.core.registry import registry
# ensure all bricks are loaded
import npr_tool.bricks

class LocalRunner:
    def __init__(self, config_path: str, enable_snapshots=True, snapshot_dir="./data/snapshots"):
        self.config_path = config_path
        self.enable_snapshots = enable_snapshots
        self.snapshot_dir = snapshot_dir

        os.makedirs(self.snapshot_dir, exist_ok=True)
        self._load_pipeline()

    # ----------------------------------------------------
    # Load pipeline YAML
    # ----------------------------------------------------
    def _load_pipeline(self):
        with open(self.config_path, "r") as f:
            cfg = yaml.safe_load(f)
        self.pipeline = cfg.get("pipeline", [])
        self.global_meta = cfg.get("meta", {"environment": "local"})

    # ----------------------------------------------------
    # Main execution
    # ----------------------------------------------------
    def execute(self):
        """Run each brick sequentially, enriching metadata automatically."""
        payload = Payload(data=None, schema="empty", metadata=self.global_meta)

        for i, step in enumerate(self.pipeline):
            name = step["name"]
            cfg = step.get("config", {})

            print(f"\n Running brick [{i+1}/{len(self.pipeline)}]: {name}")

            # Run brick logic
            brick_cls = registry.get(name)
            brick = brick_cls(cfg)

            prev_schema = payload.schema
            start_time = datetime.now()

            payload = brick.run(payload)  # returns new Payload
            end_time = datetime.now()

            # ----------------------------------------------------
            # Inject runtime metadata automatically
            # ----------------------------------------------------
            runtime_meta = {
                "step_index": i,
                "brick_name": name,
                "schema_before": prev_schema,
                "schema_after": payload.schema,
                "duration_sec": round((end_time - start_time).total_seconds(), 3),
                "timestamp": end_time.isoformat(),
            }

            # Append metadata (immutably)
            payload = payload.with_update(
                metadata={
                    "runner": runtime_meta
                }
            )


            # ----------------------------------------------------
            # Save snapshot (optional)
            # ----------------------------------------------------
            if self.enable_snapshots:
                snapshot_path = f"{self.snapshot_dir}/step_{i:02d}_{name}.json"
                payload.save_snapshot(snapshot_path)
                print(f"   Snapshot saved → {snapshot_path}")

        print("\n Pipeline complete!")
        print(f"   Final schema: {payload.schema}")
        return payload