"""
====================================================================
PAYLOAD MODULE — The Immutable Data Carrier Between Bricks
====================================================================

This defines the `Payload` object — a *read-only* container that
travels between bricks during pipeline execution. Each brick reads
from it, transforms data, and emits a *new* payload snapshot.

Data flow is 100% explicit — there's no hidden global state.
====================================================================
"""

from dataclasses import dataclass, field
from typing import Any, Dict
import copy
import uuid
from datetime import datetime
import os, json


@dataclass(frozen=True)
class Payload:
    """
    Represents one "snapshot" of data at any point in the pipeline.

    🔹 data:      The actual dataset 
    🔹 schema:    Logical schema tag 
    🔹 metadata:  Extra context, where it came from, stats, config, etc.
    🔹 id:        Unique UUID for traceability
    🔹 created_at: UTC timestamp for provenance tracking

    `frozen=True` ensures immutability, once created, cannot be altered.
    """

    data: Any = None
    schema: str = "empty"
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __init__(self, data=None, schema="empty", metadata=None):
        object.__setattr__(self, "data", data)
        object.__setattr__(self, "schema", schema)
        object.__setattr__(self, "metadata", metadata or {})

    # ==============================================================
    # with_update()
    # --------------------------------------------------------------
    # Creates a *new* Payload with optional changes. Used by bricks
    # to return a modified version without mutating existing data.
    # ==============================================================
    def with_update(self, **kwargs) -> "Payload":
        new_data = kwargs.get("data", self.data)
        new_schema = kwargs.get("schema", self.schema)
        new_metadata = {**self.metadata, **kwargs.get("metadata", {})}
        return Payload(data=new_data, schema=new_schema, metadata=new_metadata)
    
    # ----------------------------------------------------
    # Save snapshot to disk
    # ----------------------------------------------------
    def save_snapshot(self, path):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as f:
            json.dump({
                "schema": self.schema,
                "metadata": self.metadata,
                "preview": str(self.data)[:500]
            }, f, indent=2)
