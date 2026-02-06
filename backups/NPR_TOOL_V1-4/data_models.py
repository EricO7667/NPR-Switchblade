from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Literal


# =========================================================
# MATCH TYPES ENUM
# =========================================================
class MatchType(Enum):
    EXACT_MFG_PN = "Exact MFG Part #"
    EXACT_ITEMNUM = "Exact Item Number"
    PREFIX_FAMILY = "MPN Family Prefix Match"
    SUBSTITUTE = "Substitute Match"
    PARSED_MATCH = "Parsed Engineering Match"
    API_ASSISTED = "API Assisted Match"
    NO_MATCH = "No Match"



# =========================================================
# SUBSTITUTES & API DATA
# =========================================================
@dataclass
class SubstitutePart:
    """
    Represents an alternate or equivalent part for an Inventory item.

    NOTE:
    - This is *not* automatically used unless the matching engine is told to.
    - It exists to support a scalable substitute graph later (base->subs).
    """
    base_itemnum: str
    sub_itemnum: str
    description: str
    mfgpn: str
    notes: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "base_itemnum": self.base_itemnum,
            "sub_itemnum": self.sub_itemnum,
            "description": self.description,
            "mfgpn": self.mfgpn,
            "notes": self.notes,
        }


@dataclass
class DigiKeyData:
    """
    Represents manufacturer API data for a given part number.

    This is a placeholder container; the tool can populate it later.
    """
    mfgpn: str
    url: str = ""
    specs: Dict[str, str] = field(default_factory=dict)
    availability: str = ""
    price: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "mfgpn": self.mfgpn,
            "url": self.url,
            "specs": self.specs,
            "availability": self.availability,
            "price": self.price,
        }


# =========================================================
# PART MODELS
# =========================================================
@dataclass
class NPRPart:
    """
    Represents a new part request entry (BOM/NPR row).

    raw_fields: normalized dictionary of *all* Excel columns for debugging/export
    parsed: parsed engineering dictionary from parsing_engine
    """
    partnum: str
    desc: str
    mfgname: str
    mfgpn: str
    supplier: str
    raw_fields: Dict[str, str] = field(default_factory=dict)
    parsed: Dict[str, Any] = field(default_factory=dict)

    # --------------------------
    # Convenience shortcuts
    # --------------------------
    @property
    def description(self) -> str:
        return self.desc or ""

    @property
    def part_type(self) -> str:
        return str(self.parsed.get("type") or "OTHER")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "partnum": self.partnum,
            "description": self.description,
            "mfgname": self.mfgname,
            "mfgpn": self.mfgpn,
            "supplier": self.supplier,
            "parsed": self.parsed,
            "raw_fields": self.raw_fields,
        }


@dataclass
class InventoryPart:
    """
    Represents an inventory record (ERP / stock list row).

    substitutes + api_data exist specifically to support the upcoming features
    called out in issues.txt (substitute equivalence & API assisted matching).
    """
    itemnum: str
    desc: str
    mfgid: str
    mfgname: str
    vendoritem: str
    raw_fields: Dict[str, str] = field(default_factory=dict)
    parsed: Dict[str, Any] = field(default_factory=dict)
    substitutes: List[SubstitutePart] = field(default_factory=list)
    api_data: Optional[DigiKeyData] = None

    @property
    def description(self) -> str:
        return self.desc or ""

    @property
    def part_type(self) -> str:
        return str(self.parsed.get("type") or "OTHER")

    def add_substitute(self, sub: SubstitutePart) -> None:
        self.substitutes.append(sub)

    def set_api_data(self, data: DigiKeyData) -> None:
        self.api_data = data

    def to_dict(self) -> Dict[str, Any]:
        return {
            "itemnum": self.itemnum,
            "description": self.description,
            "mfgid": self.mfgid,
            "mfgname": self.mfgname,
            "vendoritem": self.vendoritem,
            "parsed": self.parsed,
            "raw_fields": self.raw_fields,
            "substitutes": [s.to_dict() for s in self.substitutes],
            "api_data": self.api_data.to_dict() if self.api_data else None,
        }


# =========================================================
# MATCH RESULT
# =========================================================
@dataclass
class MatchResult:
    """
    A single match outcome.

    inventory_part is Optional because NO_MATCH returns None.
    explain is a structured breakdown (useful for UI tooltips later).
    """
    match_type: MatchType
    confidence: float
    inventory_part: Optional[InventoryPart] = None
    notes: str = ""
    explain: Dict[str, Any] = field(default_factory=dict)

    def __repr__(self) -> str:
        inv = self.inventory_part.itemnum if self.inventory_part else "None"
        return f"<MatchResult {self.match_type.value} ({self.confidence:.2f}) inv={inv}>"




@dataclass
class NPRAlternateRow:
    """
    Page 3 — Alternate proposal row.
    This is the ONLY place alternates live.
    """

    # Cross-reference
    parent: ParentRef

    # Context (derived from parent when needed)
    exists_in_inventory: bool

    # Manufacturer identity (core proposal)
    manufacturer_name: str
    manufacturer_part_number: str
    supplier: str

    # Classification flags (engineering-owned)
    flags: Dict[str, bool] = field(default_factory=dict)
    # e.g. {"SMT": True, "PCB": False, "Mechanical": False}

    # Commercial (engineering researched)
    unit_cost: Optional[float] = None
    stock_unit: Optional[str] = None
    lead_time_weeks: Optional[int] = None

    # Compliance (engineering suggested)
    qc_required: Optional[bool] = None
    tariff_code: Optional[str] = None

    # Metadata
    source: str = ""          # "inventory", "digikey", "mouser", "manual"
    notes: Optional[str] = None

    # UI control
    include_in_npr: bool = True
    include_in_approval: bool = True



@dataclass(frozen=True)
class ParentRef:
    """
    Cross-sheet reference to the owning item.
    """
    parent_type: Literal["NEW", "EXISTS"]
    parent_id: str   # BOM ID (NEW) or internal PN (EXISTS)
