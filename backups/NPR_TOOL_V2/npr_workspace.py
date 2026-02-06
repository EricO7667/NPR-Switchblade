from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional
from .data_models import NPRPart, InventoryPart, MatchResult, MatchType

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
    