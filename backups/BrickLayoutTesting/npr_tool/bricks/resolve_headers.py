"""
====================================================================
TRANSFORM BRICK — resolver_headers
====================================================================

Purpose:
--------
Normalize inconsistent spreadsheet headers into canonical internal
field names without interpreting data meaning.
====================================================================
"""

from npr_tool.runner.payload import Payload
from npr_tool.core.registry import registry
from npr_tool.core.utils import clean_excel_str


@registry.register("resolver_headers")
class ResolverHeaders:
    """
    Transform Brick:
    Maps raw Excel headers to canonical internal names.
    """

    def __init__(self, config: dict):
        self.header_map = config.get("header_map", {})

    def run(self, payload: Payload) -> Payload:
        # ------------------------------------------------------------
        # Trust incoming schema
        # ------------------------------------------------------------
        # Expected schema: "raw_excel"

        records = payload.data
        if not records:
            return payload.with_update(
                schema="normalized",
                metadata={
                    "transform": {
                        "column_map": {},
                        "dropped_columns": []
                    }
                }
            )

        # ------------------------------------------------------------
        # Build normalized records
        # ------------------------------------------------------------
        normalized_records = []
        column_map = {}
        dropped_columns = set()

        for row in records:
            new_row = {}
            for original_key, value in row.items():
                clean_key = clean_excel_str(original_key)

                canonical_key = self.header_map.get(clean_key)
                if canonical_key:
                    new_row[canonical_key] = value
                    column_map[clean_key] = canonical_key
                else:
                    dropped_columns.add(clean_key)

            normalized_records.append(new_row)

        # ------------------------------------------------------------
        # Emit new payload
        # ------------------------------------------------------------
        return payload.with_update(
            data=normalized_records,
            schema="normalized",
            metadata={
                "transform": {
                    "column_map": column_map,
                    "dropped_columns": sorted(dropped_columns)
                }
            }
        )
