from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional
import uuid

# =========================================================
# INVENTORY INGEST MODELS
# These replace the old legacy inventory-loading models.
# They mirror the SQL ingest pipeline directly.
# =========================================================


@dataclass(slots=True)
class ERPInventoryRow:
    """Normalized ERP row produced from the ERP Excel file."""

    item_number: str
    description: str = ""
    primary_vendor_number: str = ""
    vendor_item: str = ""
    manufacturer_id: str = ""
    manufacturer_name: str = ""
    manufacturer_item_count: Optional[float] = None
    last_cost: Optional[float] = None
    standard_cost: Optional[float] = None
    average_cost: Optional[float] = None
    revision: str = ""
    item_lead_time: Optional[float] = None
    default_whse: str = ""
    total_qty: Optional[float] = None
    source_row_key: str = ""
    row_hash: str = ""
    raw_fields: Dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class AlternateMasterRow:
    """Normalized master/alternate row produced from the master Excel file."""

    item_number: str
    description: str = ""
    active: str = ""
    manufacturer_id: str = ""
    manufacturer_name: str = ""
    manufacturer_part_number: str = ""
    tariff_code: str = ""
    tariff_rate: Optional[float] = None
    last_cost: Optional[float] = None
    standard_cost: Optional[float] = None
    average_cost: Optional[float] = None
    source_row_key: str = ""
    row_hash: str = ""
    raw_fields: Dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class ManufacturerPartRecord:
    """Canonical manufacturer part under a company part number."""

    manufacturer_part_id: Optional[int] = None
    company_part_id: Optional[int] = None
    company_part_number: str = ""
    manufacturer_part_number: str = ""
    manufacturer_id: str = ""
    manufacturer_name: str = ""
    description: str = ""
    active: str = ""
    item_lead_time: Optional[float] = None
    tariff_code: str = ""
    tariff_rate: Optional[float] = None
    last_cost: Optional[float] = None
    standard_cost: Optional[float] = None
    average_cost: Optional[float] = None
    is_erp_primary: bool = False
    erp_source_row_key: Optional[str] = None
    master_source_row_key: Optional[str] = None
    updated_at: str = ""
    last_seen_at: str = ""
    raw_fields: Dict[str, Any] = field(default_factory=dict)

    def to_repo_dict(self) -> Dict[str, Any]:
        """Dictionary shape expected by repository/database write code."""
        return {
            "manufacturer_part_number": self.manufacturer_part_number,
            "manufacturer_id": self.manufacturer_id,
            "manufacturer_name": self.manufacturer_name,
            "description": self.description,
            "active": self.active,
            "item_lead_time": self.item_lead_time,
            "tariff_code": self.tariff_code,
            "tariff_rate": self.tariff_rate,
            "last_cost": self.last_cost,
            "standard_cost": self.standard_cost,
            "average_cost": self.average_cost,
            "is_erp_primary": 1 if self.is_erp_primary else 0,
            "erp_source_row_key": self.erp_source_row_key,
            "master_source_row_key": self.master_source_row_key,
            "raw_json": self.raw_fields,
        }


@dataclass(slots=True)
class CompanyPartRecord:
    """Canonical company part composed from ERP and master inventory data."""

    company_part_id: Optional[int] = None
    company_part_number: str = ""
    description: str = ""
    default_whse: str = ""
    total_qty: Optional[float] = None
    revision: str = ""
    primary_vendor_number: str = ""
    updated_at: str = ""
    last_seen_at: str = ""
    manufacturer_parts: List[ManufacturerPartRecord] = field(default_factory=list)
    raw_fields: Dict[str, Any] = field(default_factory=dict)

    @property
    def cpn(self) -> str:
        return self.company_part_number

    @property
    def canonical_desc(self) -> str:
        return self.description or ""

    @property
    def stock_total(self) -> int:
        try:
            return int(float(self.total_qty or 0))
        except Exception:
            return 0

    @property
    def alternates(self) -> List[ManufacturerPartRecord]:
        return self.manufacturer_parts

    def to_repo_dict(self) -> Dict[str, Any]:
        """Dictionary shape expected by repository/database write code."""
        return {
            "company_part_number": self.company_part_number,
            "description": self.description,
            "default_whse": self.default_whse,
            "total_qty": self.total_qty,
            "revision": self.revision,
            "primary_vendor_number": self.primary_vendor_number,
            "manufacturer_parts": [m.to_repo_dict() for m in self.manufacturer_parts],
            "raw_json": self.raw_fields,
        }


# Existing code commonly imports CompanyPart. Keep that name pointed at the new model.
CompanyPart = CompanyPartRecord
InvItem = ManufacturerPartRecord


# =========================================================
# BOM / WORKFLOW MODELS
# These are not the legacy inventory loaders. Keep them.
# =========================================================


@dataclass
class BomLineInput:
    """Canonical imported BOM line."""

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
    """Mutable export-ready BOM line."""

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




@dataclass
class SubstitutePart:
    """Represents a substitute/alias manufacturer part tied to a base inventory part."""

    base_itemnum: str
    sub_itemnum: str = ""
    description: str = ""
    mfgpn: str = ""
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
class InventoryPart:
    """
    Matching/UI-facing inventory view object.

    Why this still exists even after the new canonical schema:
    - CompanyPartRecord / ManufacturerPartRecord are the authoritative DB ingest models.
    - MatchingEngine and parts of DecisionController still operate on a flatter runtime object.
    - This class is the compatibility runtime shape for matching, cards, and explain output.
    """

    itemnum: str
    desc: str = ""
    mfgid: str = ""
    mfgname: str = ""
    vendoritem: str = ""
    supplier: str = ""
    stock: int = 0
    lead_time_days: Optional[int] = None
    raw_fields: Dict[str, Any] = field(default_factory=dict)
    parsed: Dict[str, Any] = field(default_factory=dict)
    substitutes: List[SubstitutePart] = field(default_factory=list)
    api_data: Optional["DigiKeyData"] = None

    @property
    def description(self) -> str:
        return self.desc or ""

    @property
    def part_type(self) -> str:
        return str((self.parsed or {}).get("type") or "OTHER")

    def add_substitute(self, sub: SubstitutePart) -> None:
        self.substitutes.append(sub)

    def set_api_data(self, data: "DigiKeyData") -> None:
        self.api_data = data

    def to_dict(self) -> Dict[str, Any]:
        return {
            "itemnum": self.itemnum,
            "description": self.description,
            "mfgid": self.mfgid,
            "mfgname": self.mfgname,
            "vendoritem": self.vendoritem,
            "supplier": self.supplier,
            "stock": self.stock,
            "lead_time_days": self.lead_time_days,
            "parsed": self.parsed,
            "raw_fields": self.raw_fields,
            "substitutes": [s.to_dict() for s in self.substitutes],
            "api_data": self.api_data.to_dict() if self.api_data else None,
        }


@dataclass
class NPRPart:
    """
    Canonical BOM/NPR input object used by the matching engine.

    This is the runtime model for one imported BOM line after normalization and before matching.
    """

    partnum: str
    desc: str = ""
    qty: Optional[float] = None
    refdes: str = ""
    item_type: str = ""
    mfgname: str = ""
    mfgpn: str = ""
    supplier: str = ""
    raw_fields: Dict[str, Any] = field(default_factory=dict)
    parsed: Dict[str, Any] = field(default_factory=dict)

    @property
    def description(self) -> str:
        return self.desc or ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "partnum": self.partnum,
            "description": self.description,
            "qty": self.qty,
            "refdes": self.refdes,
            "item_type": self.item_type,
            "mfgname": self.mfgname,
            "mfgpn": self.mfgpn,
            "supplier": self.supplier,
            "raw_fields": self.raw_fields,
            "parsed": self.parsed,
        }

class MatchType(Enum):
    EXACT_MFG_PN = "Exact MFG Part #"
    PARTIAL_ITEMNUM = "Patial Item Number"
    PREFIX_FAMILY = "MPN Family Prefix Match"
    SUBSTITUTE = "Substitute Match"
    PARSED_MATCH = "Parsed Engineering Match"
    API_ASSISTED = "API Assisted Match"
    NO_MATCH = "No Match"


@dataclass
class CNSRecord:
    prefix: str
    body: str
    suffix: str
    description: str
    sheet_name: str = ""
    category: str = ""
    date: str = ""
    initials: str = ""
    raw_fields: Dict[str, str] = field(default_factory=dict)
    parsed: Dict[str, str] = field(default_factory=dict)


@dataclass
class DigiKeyData:
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


@dataclass
class MatchResult:
    """Single match outcome against a canonical inventory manufacturer part."""

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
    """Selectable alternate shown in the UI."""

    id: str
    source: str
    manufacturer: str = ""
    manufacturer_part_number: str = ""
    internal_part_number: str = ""
    description: str = ""
    value: str = ""
    package: str = ""
    tolerance: str = ""
    voltage: str = ""
    wattage: str = ""
    stock: int = 0
    unit_cost: Optional[float] = None
    supplier: str = ""
    confidence: float = 0.0
    relationship: str = ""
    matched_mpn: str = ""
    selected: bool = False
    rejected: bool = False
    raw: Optional[object] = None
    meta: Dict[str, Any] = field(default_factory=dict)

    @staticmethod
    def new_id(prefix: str = "ALT") -> str:
        return f"{prefix}-{uuid.uuid4().hex[:10]}"


class CardSection(str, Enum):
    INTERNAL_MATCHES = "INTERNAL_MATCHES"
    EXTERNAL_ALTERNATES = "EXTERNAL_ALTERNATES"
    REJECTED = "REJECTED"


@dataclass
class CardState:
    selected: bool = False
    rejected: bool = False
    pinned: bool = False
    locked: bool = False
    visible: bool = True
    actionable: bool = True


@dataclass
class CardDisplay:
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
    node_id: str
    title_text: str = "Company PN: —"
    subtitle_text: str = "BOM MPN: —"
    status_text: str = ""
    company_part_number: str = ""
    suggested_company_part_number: str = ""
    description_text: str = ""
    bom_mpn: str = ""
    bom_section: str = "SURFACE MOUNT"
    include_approval: bool = False
    committed_company_part_number: str = ""
    committed_description_text: str = ""
    committed_bom_section: str = "SURFACE MOUNT"
    committed_include_approval: bool = False
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
    EXISTS = "EXISTS"
    NEEDS_REVIEW = "NEEDS_REVIEW"


@dataclass
class DecisionNode:
    id: str
    base_type: str
    bom_uid: str = ""
    bom_mpn: str = ""
    description: str = ""
    internal_part_number: str = ""
    inventory_mpn: str = ""
    assigned_part_number: str = ""
    preferred_inventory_mfgpn: str = ""
    bom_section: str = "SURFACE MOUNT"
    focused_alt_id: str = ""
    exclude_customer_part_number_in_npr: bool = False
    match_type: str = ""
    confidence: float = 0.0
    alternates: List[Alternate] = field(default_factory=list)
    cards: List[DecisionCard] = field(default_factory=list)
    focused_card_id: str = ""
    status: DecisionStatus = DecisionStatus.NEEDS_DECISION
    locked: bool = False
    needs_approval: bool = False
    notes: str = ""
    explain: dict = field(default_factory=dict)

    def selected_alternates(self) -> List[Alternate]:
        return [a for a in self.alternates if a.selected and not a.rejected]

    def candidate_alternates(self) -> List[Alternate]:
        return [a for a in self.alternates if not a.rejected]

    def set_cards(self, cards: List[DecisionCard]) -> None:
        self.cards = list(cards or [])

    def get_card(self, card_id: str) -> Optional[DecisionCard]:
        for card in self.cards:
            if getattr(card, "card_id", "") == card_id:
                return card
        return None

    def get_card_by_alt_id(self, alt_id: str) -> Optional[DecisionCard]:
        for card in self.cards:
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
