"""
====================================================================
SOURCE BRICK — importer_excel
====================================================================

Purpose:
--------
Load a spreadsheet from disk and introduce it into the pipeline as
raw, uninterpreted data.

This brick performs NO semantic interpretation.
====================================================================
"""

import os
import pandas as pd

from npr_tool.runner.payload import Payload
from npr_tool.core.registry import registry


@registry.register("importer_excel")
class ImporterExcel:
    """
    Source Brick:
    Loads an Excel file and emits raw row dictionaries.
    """

    def __init__(self, config: dict):
        self.path = config.get("path")
        if not self.path:
            raise ValueError("importer_excel requires 'path' in config")
        if not os.path.exists(self.path):
            raise FileNotFoundError(f"Excel file not found: {self.path}")

    def run(self, payload: Payload) -> Payload:
        # ------------------------------------------------------------
        # Trust incoming schema
        # ------------------------------------------------------------
        # Expected schema: "empty"
        # No defensive checks required

        # ------------------------------------------------------------
        # Load Excel
        # ------------------------------------------------------------
        df = pd.read_excel(self.path, dtype=str).fillna("")

        records = df.to_dict(orient="records")

        # ------------------------------------------------------------
        # Emit new payload
        # ------------------------------------------------------------
        return payload.with_update(
            data=records,
            schema="raw_excel",
            metadata={
                "source": {
                    "path": self.path,
                    "rows": len(records),
                    "columns": list(df.columns),
                }
            }
        )
