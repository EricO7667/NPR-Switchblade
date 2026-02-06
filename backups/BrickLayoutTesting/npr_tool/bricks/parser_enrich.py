"""
====================================================================
ENRICH BRICK — parser_enrich
====================================================================

Purpose:
--------
Parse normalized row data to extract structured component meaning.
====================================================================
"""

from npr_tool.runner.payload import Payload
from npr_tool.core.registry import registry
from npr_tool.core.parsing_engine import ParsingEngine


@registry.register("parser_enrich")
class ParserEnrich:
    """
    Enrich Brick:
    Adds parsed component attributes to each row.
    """

    def __init__(self, config: dict):
        self.description_field = config.get("description_field", "description")
        self.engine = ParsingEngine()

    def run(self, payload: Payload) -> Payload:
        # Expected schema: normalized
        records = payload.data or []

        enriched = []
        warning_count = 0

        for row in records:
            parsed = self.engine.parse(row, self.description_field)
            if "parsing_warnings" in parsed:
                warning_count += 1
            enriched.append(parsed)

        return payload.with_update(
            data=enriched,
            schema="parsed_parts",
            metadata={
                "enrich": {
                    "parsed_rows": len(enriched),
                    "warnings_count": warning_count,
                }
            }
        )
