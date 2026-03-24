from __future__ import annotations
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional
import uuid

# =========================================================
# NEW CORE DATA MODEL (v2 schema: canonical BOM input + mutable state + inventory_company)
# =========================================================

@dataclass
class InvItem:
    """Item-level purchasing option under a company part (CPN).

    Pricing is item-level; stock is pooled at the CompanyPart level.
    """
    mfgname: str = ""
    mfgid: str = ""
    mpn: str = ""  # manufacturer part number

    unit_price: Optional[float] = None
    last_unit_price: Optional[float] = None
    standard_cost: Optional[float] = None
    average_cost: Optional[float] = None

    tariff_code: str = ""
    tariff_rate: Optional[float] = None

    supplier: str = ""
    lead_time_days: Optional[int] = None
    meta: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "mfgname": self.mfgname,
            "mfgid": self.mfgid,
            "mpn": self.mpn,
            "unit_price": self.unit_price,
            "last_unit_price": self.last_unit_price,
            "standard_cost": self.standard_cost,
            "average_cost": self.average_cost,
            "tariff_code": self.tariff_code,
            "tariff_rate": self.tariff_rate,
            "supplier": self.supplier,
            "lead_time_days": self.lead_time_days,
            "meta": self.meta,
        }


@dataclass
class CompanyPart:
    """Company Part Number (CPN) level inventory record (inventory_company table)."""
    cpn: str
    canonical_desc: str = ""
    stock_total: int = 0
    alternates: List[InvItem] = field(default_factory=list)
    raw_fields: Dict[str, Any] = field(default_factory=dict)

    def to_repo_dict(self) -> Dict[str, Any]:
        return {
            "cpn": self.cpn,
            "canonical_desc": self.canonical_desc,
            "stock_total": int(self.stock_total or 0),
            "alternates": [a.to_dict() for a in (self.alternates or [])],
        }


@dataclass
class BomLineInput:
    """Canonical imported BOM line (bom_line_input)."""
    input_line_id: int
    partnum: str = ""
    description: str = ""
    qty: Optional[float] = None
    refdes: str = ""
    item_type: str = ""
    mfgname: str = ""
    mfgpn: str = ""
    supplier: str = ""
    raw_json: Dict[str, Any] = field(default_factory=dict)

    def to_repo_dict(self) -> Dict[str, Any]:
        return {
            "input_line_id": int(self.input_line_id),
            "partnum": self.partnum,
            "description": self.description,
            "qty": self.qty,
            "refdes": self.refdes,
            "item_type": self.item_type,
            "mfgname": self.mfgname,
            "mfgpn": self.mfgpn,
            "supplier": self.supplier,
            "raw_json": self.raw_json,
        }


@dataclass
class BomLineState:
    """Mutable export-ready BOM line (bom_line_state)."""
    line_id: int
    cpn: str = ""
    needs_new_cpn: bool = False

    desc: str = ""
    qty: Optional[float] = None
    refdes: str = ""
    item_type: str = ""

    selected_mfg: str = ""
    selected_mpn: str = ""

    unit_price: Optional[float] = None
    ext_price: Optional[float] = None

    supplier: str = ""
    lead_time_days: Optional[int] = None
    qc_required: bool = False

    tariff_code: str = ""
    tariff_rate: Optional[float] = None

    quote_num: str = ""
    npr_num_used_in: str = ""

    stock_unit: str = ""
    purchase_unit: str = ""
    per_unit_qty: Optional[float] = None

    notes: str = ""




# =========================================================
# MATCH TYPES ENUM
# =========================================================
class MatchType(Enum):
    EXACT_MFG_PN = "Exact MFG Part #"
    PARTIAL_ITEMNUM = "Patial Item Number"
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
class CNSRecord:
    prefix: str
    body: str
    suffix: str
    description: str

    sheet_name: str = ""
    category: str = ""          # e.g. "00" .. "99" 
    date: str = ""
    initials: str = ""

    raw_fields: Dict[str, str] = field(default_factory=dict)
    parsed: Dict[str, str] = field(default_factory=dict)

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
    candidates: List[InventoryPart] = field(default_factory=list) 
    notes: str = ""
    explain: Dict[str, Any] = field(default_factory=dict)

    def __repr__(self) -> str:
        inv = self.inventory_part.itemnum if self.inventory_part else "None"
        return f"<MatchResult {self.match_type.value} ({self.confidence:.2f}) inv={inv}>"


@dataclass
class Alternate:
    """
    A selectable alternate part.
    This is UI-facing, export-facing, and approval-facing.
    """

    # ---- Identity ----
    id: str                              # stable, unique
    source: str                          # "inventory" | "digikey" | "manual" | "api"

    # ---- Part identity ----
    manufacturer: str = ""
    manufacturer_part_number: str = ""
    internal_part_number: str = ""       # itemnum if inventory-backed

    # ---- Description ----
    description: str = ""

    # ---- Electrical / parsed attributes ----
    value: str = ""
    package: str = ""
    tolerance: str = ""
    voltage: str = ""
    wattage: str = ""

    # ---- Commercial ----
    stock: int = 0
    unit_cost: Optional[float] = None
    supplier: str = ""

    # ---- Matching metadata ----
    confidence: float = 0.0
    relationship: str = ""               # "Exact", "Parsed", "Family", "Alternate"
    matched_mpn: str = ""                #  the BOM/customer MPN that caused this card to be shown/selected

    # ---- UI / workflow flags ----
    selected: bool = False
    rejected: bool = False

    # ---- Raw backing object (optional, NEVER used by UI) ----
    raw: Optional[object] = None

    # ---- Extra extensibility ----
    meta: Dict[str, Any] = field(default_factory=dict)

    @staticmethod
    def new_id(prefix: str = "ALT") -> str:
        return f"{prefix}-{uuid.uuid4().hex[:10]}"


class CardSection(str, Enum):
    """Stable UI buckets for rendering card groups without inferring from widgets."""
    INTERNAL_MATCHES = "INTERNAL_MATCHES"
    EXTERNAL_ALTERNATES = "EXTERNAL_ALTERNATES"
    REJECTED = "REJECTED"


@dataclass
class CardState:
    """Mutable UI-facing workflow state for a rendered card."""
    selected: bool = False
    rejected: bool = False
    pinned: bool = False
    locked: bool = False
    visible: bool = True
    actionable: bool = True


@dataclass
class CardDisplay:
    """Read-only presentation snapshot derived from the backing Alternate."""
    title: str = ""
    subtitle: str = ""
    description: str = ""
    source_label: str = ""
    stock_label: str = "-"
    confidence_ratio: float = 0.0
    confidence_text: str = "0%"
    border_role: str = "default"
    badges: List[str] = field(default_factory=list)
    mfgpn_count: int = 0


@dataclass
class DecisionCard:
    """
    UI/state-driven card object.

    This becomes the owned representation of a rendered card. The Alternate remains
    the export/workflow object, while DecisionCard is the explicit renderable state.
    """

    card_id: str
    node_id: str
    alt_id: str = ""
    section: CardSection = CardSection.INTERNAL_MATCHES
    source: str = ""
    is_inventory: bool = False

    company_part_number: str = ""
    manufacturer_part_number: str = ""
    manufacturer: str = ""
    relationship: str = ""

    state: CardState = field(default_factory=CardState)
    display: CardDisplay = field(default_factory=CardDisplay)

    alternate: Optional[Alternate] = None
    meta: Dict[str, Any] = field(default_factory=dict)

    @property
    def is_rejected(self) -> bool:
        return bool(self.state.rejected)

    @property
    def is_selected(self) -> bool:
        return bool(self.state.selected)

    @property
    def is_pinned(self) -> bool:
        return bool(self.state.pinned)



@dataclass
class HeaderControlState:
    """Mutable enable/disable state for the upper-panel controls."""
    company_pn_editable: bool = False
    apply_pn_enabled: bool = False
    description_editable: bool = False
    bom_section_editable: bool = False
    approval_editable: bool = False
    load_external_enabled: bool = False
    mark_ready_enabled: bool = True
    unmark_ready_enabled: bool = False
    auto_reject_enabled: bool = True


@dataclass
class NodeHeaderState:
    """
    Controller-owned upper-panel state.

    This object now supports both:
      - committed node-backed values
      - mutable UI draft values

    The UI should render from this object rather than reconstructing header values
    from selected alternates, scattered node fields, or widget text.
    """

    node_id: str
    title_text: str = "Company PN: —"
    subtitle_text: str = "BOM MPN: —"
    status_text: str = ""

    # Current mutable/UI-facing values
    company_part_number: str = ""
    suggested_company_part_number: str = ""
    description_text: str = ""
    bom_mpn: str = ""
    bom_section: str = "SURFACE MOUNT"
    include_approval: bool = False

    # Committed node-backed values
    committed_company_part_number: str = ""
    committed_description_text: str = ""
    committed_bom_section: str = "SURFACE MOUNT"
    committed_include_approval: bool = False

    # Dirty flags for staged edits
    dirty_company_part_number: bool = False
    dirty_description: bool = False
    dirty_bom_section: bool = False
    dirty_approval: bool = False

    selected_alt_id: str = ""
    selected_card_id: str = ""

    has_selected_alt: bool = False
    selected_is_internal: bool = False
    all_rejected: bool = False
    locked: bool = False
    is_ready: bool = False

    controls: HeaderControlState = field(default_factory=HeaderControlState)


@dataclass
class CardDetailState:
    """
    Controller-owned right-panel/detail-panel state for a node/card selection.

    The specs panel should render strictly from this object rather than inferring
    which alternate is being viewed from widget history or ad hoc UI logic.
    """

    node_id: str
    card_id: str = ""
    alt_id: str = ""
    title_text: str = "Information"
    specs: Dict[str, Any] = field(default_factory=dict)
    export_mfgpn_options: List[str] = field(default_factory=list)
    selected_export_mfgpn: str = ""
    has_card: bool = False
    is_inventory: bool = False

@dataclass
class CommittedExportState:
    """Committed export-facing snapshot derived from a node's durable state."""

    node_id: str
    line_id: int = 0
    company_part_number: str = ""
    description_text: str = ""
    bom_mpn: str = ""
    bom_section: str = "SURFACE MOUNT"
    bucket: str = "SURFACE MOUNT"
    type_value: str = "SMD"
    manufacturer_name: str = ""
    manufacturer_part_number: str = ""
    selected_alt_id: str = ""
    selected_alt_source: str = ""
    include_approval: bool = False
    has_internal: bool = False
    has_selected_external: bool = False
    preferred_inventory_mfgpn: str = ""
    exclude_customer_part_number_in_npr: bool = False
    notes: str = ""


class DecisionStatus(str, Enum):
    FULL_MATCH = "FULL_MATCH"
    NEEDS_DECISION = "NEEDS_DECISION"
    NEEDS_ALTERNATE = "NEEDS_ALTERNATE"
    READY_FOR_EXPORT = "READY_FOR_EXPORT"
    EXISTS = "EXISTS"              # Anchored internal PN
    NEEDS_REVIEW = "NEEDS_REVIEW"  # External or manual alt pending check


@dataclass
class DecisionNode:
    """
    A single NPR decision task.
    This is the PRIMARY unit rendered by the UI.
    """

    # ---- Identity ---- 
    id: str                              # stable, immutable
    base_type: str                       # "NEW" | "EXISTS"

    # ---- Base context ----
    bom_uid: str = ""
    bom_mpn: str = ""
    description: str = ""

    internal_part_number: str = ""       # EXISTS only
    inventory_mpn: str = ""              # EXISTS only
    assigned_part_number: str = ""       # committed manual/company PN override
    preferred_inventory_mfgpn: str = ""  # committed chosen MFG PN under the company PN
    bom_section: str = "SURFACE MOUNT"   # committed export section/bucket
    focused_alt_id: str = ""             # committed focused alternate identity for restore
    exclude_customer_part_number_in_npr: bool = False  # external-only NPR option

    # ---- Matching metadata ----
    match_type: str = ""
    confidence: float = 0.0

    # ---- Alternates ----
    alternates: List[Alternate] = field(default_factory=list)
    cards: List[DecisionCard] = field(default_factory=list)
    focused_card_id: str = ""

    # ---- Workflow state ----
    status: DecisionStatus = DecisionStatus.NEEDS_DECISION
    locked: bool = False
    needs_approval: bool = False

    # ---- Notes / explainability ----
    notes: str = ""
    explain: dict = field(default_factory=dict)

    # ---- Convenience helpers ----
    def selected_alternates(self) -> List[Alternate]:
        return [a for a in self.alternates if a.selected and not a.rejected]

    def candidate_alternates(self) -> List[Alternate]:
        return [a for a in self.alternates if not a.rejected]

    def set_cards(self, cards: List[DecisionCard]) -> None:
        self.cards = list(cards or [])

    def get_card(self, card_id: str) -> Optional[DecisionCard]:
        for card in (self.cards or []):
            if getattr(card, "card_id", "") == card_id:
                return card
        return None

    def get_card_by_alt_id(self, alt_id: str) -> Optional[DecisionCard]:
        for card in (self.cards or []):
            if getattr(card, "alt_id", "") == alt_id:
                return card
        return None

    def set_focused_card(self, card_id: str, alt_id: str = "") -> None:
        self.focused_card_id = str(card_id or "").strip()
        if alt_id:
            self.focused_alt_id = str(alt_id or "").strip()

    def clear_focused_card(self) -> None:
        self.focused_card_id = ""
        self.focused_alt_id = ""

    def has_selection(self) -> bool:
        return len(self.selected_alternates()) > 0
