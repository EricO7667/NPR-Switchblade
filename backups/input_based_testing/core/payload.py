from dataclasses import dataclass, field
from typing import Any, Dict
from datetime import datetime
import copy, uuid

@dataclass(frozen=True)
class Payload:
    data: Any = None
    schema: str = "empty"
    metadata: Dict[str, Any] = field(default_factory=dict)
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    created_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())

    def with_update(self, **kwargs) -> "Payload":
        new_data = kwargs.get("data", self.data)
        new_schema = kwargs.get("schema", self.schema)
        new_meta = copy.deepcopy(self.metadata)
        new_meta.update(kwargs.get("metadata", {}))
        return Payload(data=new_data, schema=new_schema, metadata=new_meta)
