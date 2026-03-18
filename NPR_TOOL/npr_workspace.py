from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Iterable, Sequence, Type

class NPRRowKind(str, Enum):
    """
    Page 2 contains two kinds of rows:
      - CONTEXT_EXISTS: inventory parts that exist (blank details in final NPR)
      - ALTERNATE_PROPOSAL: alternates we propose (fillable engineering fields)
    """
    CONTEXT_EXISTS = "CONTEXT_EXISTS"
    ALTERNATE_PROPOSAL = "ALTERNATE_PROPOSAL"

@dataclass
class NPRPrimaryNewItem:
    """
    Page 1 row (NEW / unmatched BOM part).
    This is a work-item: "find/select alternates for this new BOM part".
    """
    bom_uid: str
    bom_mpn: str
    description: str
    component_type: str
    populated: bool

    include_in_export: bool = True
    notes: str = ""

    # Secondary row ids that this Page 1 item "owns" (alternate proposals / context links)
    secondary_row_ids: List[str] = field(default_factory=list)


@dataclass
class NPRSecondaryRow:
    """
    Page 2 row. This is what can appear in the final export.
    """
    row_id: str
    kind: NPRRowKind

    # Link back to BOM context
    parent_bom_uid: str
    parent_bom_mpn: str
    parent_description: str

    # Context row fields
    internal_part_number: str = ""
    exists_in_inventory: bool = False

    # Engineering-owned alternate fields (proposal rows only)
    manufacturer_name: str = ""
    manufacturer_part_number: str = ""
    supplier: str = ""
    unit_cost: Optional[float] = None
    stock_unit: str = ""
    lead_time_weeks: Optional[int] = None
    qc_required: Optional[bool] = None
    tariff_code: str = ""

    # Process flags / custom fields
    flags: Dict[str, bool] = field(default_factory=dict)  # SMT/TH/Process/Assembly/PCB/Mechanical

    # UI controls
    include_in_export: bool = True
    source: str = ""  # inventory/digikey/mouser/manual
    notes: str = ""


@dataclass
class NPRWorkspace:
    """
    The NPR Workspace backing state.
    One-way flow:
      matching results -> Page1/Page2 -> Page3 preview -> export
    """
    primary_new_items: List[NPRPrimaryNewItem] = field(default_factory=list)
    secondary_rows: List[NPRSecondaryRow] = field(default_factory=list)

    def get_export_selected_secondary_rows(self) -> List[NPRSecondaryRow]:
        return [r for r in self.secondary_rows if r.include_in_export]
    

# -----------------------------------------------------------------------------
# External alternate helpers
# -----------------------------------------------------------------------------

def _clean_text(value: object) -> str:
    return str(value or "").strip()


def _next_external_index(node) -> int:
    max_idx = 0
    for alt in (getattr(node, "alternates", []) or []):
        alt_id = _clean_text(getattr(alt, "id", ""))
        if not alt_id.startswith("EXT-"):
            continue
        try:
            max_idx = max(max_idx, int(alt_id.rsplit("-", 1)[-1]))
        except Exception:
            continue
    return max_idx + 1


def _external_key(source: str, manufacturer_part_number: str, internal_part_number: str = "") -> tuple[str, str, str]:
    return (
        _clean_text(source).lower(),
        _clean_text(manufacturer_part_number).upper(),
        _clean_text(internal_part_number).upper(),
    )


def external_alt_specs_from_node(node) -> List[dict]:
    """Serialize non-inventory alternates attached to a node."""
    rows: List[dict] = []
    for alt in (getattr(node, "alternates", []) or []):
        source = _clean_text(getattr(alt, "source", ""))
        if not source or source.lower() == "inventory":
            continue
        rows.append(
            {
                "id": _clean_text(getattr(alt, "id", "")),
                "source": source,
                "manufacturer": _clean_text(getattr(alt, "manufacturer", "")),
                "manufacturer_part_number": _clean_text(getattr(alt, "manufacturer_part_number", "")),
                "internal_part_number": _clean_text(getattr(alt, "internal_part_number", "")),
                "description": _clean_text(getattr(alt, "description", "")),
                "confidence": float(getattr(alt, "confidence", 0.0) or 0.0),
                "relationship": _clean_text(getattr(alt, "relationship", "External Alternate")) or "External Alternate",
                "selected": bool(getattr(alt, "selected", False)),
                "rejected": bool(getattr(alt, "rejected", False)),
                "supplier": _clean_text(getattr(alt, "supplier", "")),
                "stock": getattr(alt, "stock", None),
                "unit_cost": getattr(alt, "unit_cost", None),
                "meta": dict(getattr(alt, "meta", {}) or {}),
            }
        )
    return rows


def restore_external_alternates(node, alternate_cls, rows: Sequence[dict] | None) -> List[object]:
    """Rebuild external Alternate objects from persisted rows and attach them to the node."""
    restored: List[object] = []
    existing = {_clean_text(getattr(a, "id", "")) for a in (getattr(node, "alternates", []) or [])}
    for row in (rows or []):
        if not isinstance(row, dict):
            continue
        source = _clean_text(row.get("source"))
        if not source or source.lower() == "inventory":
            continue
        alt_id = _clean_text(row.get("id")) or f"EXT-{_clean_text(getattr(node, 'id', 'NODE'))}-{len(restored)+1}"
        if alt_id in existing:
            continue
        alt = alternate_cls(
            id=alt_id,
            source=source,
            manufacturer=_clean_text(row.get("manufacturer")),
            manufacturer_part_number=_clean_text(row.get("manufacturer_part_number")),
            internal_part_number=_clean_text(row.get("internal_part_number")),
            description=_clean_text(row.get("description")),
            confidence=float(row.get("confidence", 0.0) or 0.0),
            relationship=_clean_text(row.get("relationship")) or "External Alternate",
            selected=bool(row.get("selected", False)),
            rejected=bool(row.get("rejected", False)),
            raw=None,
            meta=dict(row.get("meta") or {}),
        )
        if hasattr(alt, "supplier"):
            try:
                alt.supplier = _clean_text(row.get("supplier"))
            except Exception:
                pass
        if hasattr(alt, "stock"):
            try:
                alt.stock = row.get("stock", None)
            except Exception:
                pass
        if hasattr(alt, "unit_cost"):
            try:
                alt.unit_cost = row.get("unit_cost", None)
            except Exception:
                pass
        node.alternates.append(alt)
        existing.add(alt_id)
        restored.append(alt)
    return restored


def build_fake_external_alt_specs(node) -> List[dict]:
    """Create stable test/demo external alternates for a node."""
    bom_mpn = _clean_text(getattr(node, "bom_mpn", "")) or "UNKNOWN-MPN"
    desc = _clean_text(getattr(node, "description", "")) or "External alternate"
    manufacturer = "External Supplier"
    next_idx = _next_external_index(node)
    rows: List[dict] = []
    base_variants = [
        (f"{bom_mpn}-EXTA", f"{desc} [External Alt A]", "External Proposal"),
        (f"{bom_mpn}-EXTB", f"{desc} [External Alt B]", "External Proposal"),
        (f"{bom_mpn}-EXTC", f"{desc} [External Alt C]", "External Proposal"),
    ]
    existing_keys = {
        _external_key(getattr(a, "source", ""), getattr(a, "manufacturer_part_number", ""), getattr(a, "internal_part_number", ""))
        for a in (getattr(node, "alternates", []) or [])
        if _clean_text(getattr(a, "source", "")).lower() != "inventory"
    }
    for offset, (mpn, description, relationship) in enumerate(base_variants, start=0):
        key = _external_key("external", mpn, "")
        if key in existing_keys:
            continue
        rows.append(
            {
                "id": f"EXT-{_clean_text(getattr(node, 'id', 'NODE'))}-{next_idx + offset}",
                "source": "external",
                "manufacturer": manufacturer,
                "manufacturer_part_number": mpn,
                "internal_part_number": "",
                "description": description,
                "confidence": 0.0,
                "relationship": relationship,
                "selected": False,
                "rejected": False,
                "meta": {"seeded_external": True},
            }
        )
    return rows


def append_external_alternates(node, alternate_cls, rows: Sequence[dict] | None) -> List[object]:
    """Append new external alternates, deduped against existing non-inventory cards."""
    created: List[object] = []
    existing_keys = {
        _external_key(getattr(a, "source", ""), getattr(a, "manufacturer_part_number", ""), getattr(a, "internal_part_number", ""))
        for a in (getattr(node, "alternates", []) or [])
        if _clean_text(getattr(a, "source", "")).lower() != "inventory"
    }
    for row in (rows or []):
        if not isinstance(row, dict):
            continue
        key = _external_key(row.get("source"), row.get("manufacturer_part_number"), row.get("internal_part_number"))
        if key in existing_keys:
            continue
        created.extend(restore_external_alternates(node, alternate_cls, [row]))
        existing_keys.add(key)
    return created
