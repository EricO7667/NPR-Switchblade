from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional
from .data_models import NPRPart, InventoryPart, MatchResult, MatchType

class DecisionStatus(str, Enum):
    UNREVIEWED = "UNREVIEWED"
    AUTO_ACCEPTED = "AUTO_ACCEPTED"
    NEEDS_DECISION = "NEEDS_DECISION"
    NEEDS_ALTERNATE = "NEEDS_ALTERNATE"
    READY_FOR_EXPORT = "READY_FOR_EXPORT"

@dataclass
class DecisionNode:
    """
    One NPR decision unit.
    Exactly one exists per BOM line.
    """

    # Identity
    bom_uid: str
    description: str

    # Base context
    base_type: str  # "NEW" or "EXISTS"
    base_bom_part: NPRPart
    base_inventory_part: Optional[InventoryPart]

    # Matching signal (input only, not UI truth)
    match_type: MatchType
    confidence: float

    # Alternates (grow over time, API-safe)
    alternate_candidates: List[InventoryPart] = field(default_factory=list)
    selected_alternates: List[InventoryPart] = field(default_factory=list)

    # Workflow
    status: DecisionStatus = DecisionStatus.UNREVIEWED
    locked: bool = False


def build_decision_node(
    npr_part: NPRPart,
    match: MatchResult,
) -> DecisionNode:
    """
    Create a DecisionNode from matcher output.
    Matching is treated as a signal, not a decision.
    """

    inventory_part = match.inventory_part

    # Determine base type
    if inventory_part is None:
        base_type = "NEW"
        status = DecisionStatus.NEEDS_ALTERNATE
    else:
        base_type = "EXISTS"

        # Initial status depends on match quality
        if match.match_type in (
            MatchType.EXACT_MFG_PN,
            MatchType.EXACT_ITEMNUM,
        ):
            status = DecisionStatus.AUTO_ACCEPTED
            base_type = "EXISTS"
        
        elif match.match_type in (
            MatchType.PREFIX_FAMILY,
            MatchType.PARSED_MATCH,
            MatchType.SUBSTITUTE,
            MatchType.API_ASSISTED,
        ):
            status = DecisionStatus.NEEDS_DECISION
            base_type = "EXISTS"
        
        else:  # MatchType.NO_MATCH
            status = DecisionStatus.NEEDS_ALTERNATE
            base_type = "NEW"
        

    node = DecisionNode(
        bom_uid=npr_part.partnum,
        description=npr_part.description,
        base_type=base_type,
        base_bom_part=npr_part,
        base_inventory_part=match.inventory_part,
        match_type=match.match_type,
        confidence=match.confidence,
        status=status,
    )

    return node


def build_decision_nodes(
    parts: List[NPRPart],
    match_results: List[MatchResult],
) -> List[DecisionNode]:
    if len(parts) != len(match_results):
        raise ValueError("Parts and match results length mismatch")

    nodes: List[DecisionNode] = []

    for part, match in zip(parts, match_results):
        node = build_decision_node(part, match)
        nodes.append(node)

    return nodes


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
    