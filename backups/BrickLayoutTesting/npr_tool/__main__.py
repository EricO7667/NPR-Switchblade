"""
====================================================================
MAIN ENTRYPOINT — CLI Launch Script
====================================================================

PURPOSE:
Allows you to run the configured pipeline directly from terminal.

Command:
    python -m npr_tool

Data Flow:
  pipeline.yaml → LocalRunner → Sequential Bricks → Final Payload
====================================================================
"""

from npr_tool.runner.runner_local import LocalRunner
from pathlib import Path

if __name__ == "__main__":
    # Resolve path relative to the npr_tool package directory
    BASE_DIR = Path(__file__).resolve().parent
    config_path = BASE_DIR / "configs" / "pipeline.yaml"

    print(f"Using pipeline config: {config_path}")

    runner = LocalRunner(config_path)
    result = runner.execute()

    print("\n PIPELINE COMPLETE")
    print(f"Schema: {result.schema}")
    print(f"Rows Processed: {result.metadata.get('rows')}")
    print(f"Source File: {result.metadata.get('source')}")
