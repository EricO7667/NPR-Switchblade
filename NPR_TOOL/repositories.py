# repositories.py
from __future__ import annotations

import json
import sqlite3
import threading
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple

from .data_models import CompanyPartRecord, ManufacturerPartRecord, MatchResult, MatchType, NPRPart, InventoryPart

_DB_LOCK = threading.RLock()

"""
Repository layer responsibilities:
- DataLoader reads Excel and produces normalized Python objects.
- db.py defines the SQLite schema.
- repositories.py is the only layer that turns those Python objects into SQL
  writes and turns SQL rows back into Python objects.

Why keep this separate:
- The loader should not know SQL.
- The schema file should not contain business read/write logic.
- The controller/UI can call repository methods without hand-writing SQL.
"""

# -----------------------------
# helpers
# -----------------------------
def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def new_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


def json_dumps(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"), default=str)


def json_loads(s: Any, *, default: Any):
    try:
        if s is None:
            return default
        if isinstance(s, (dict, list)):
            return s
        s = str(s)
        if not s.strip():
            return default
        return json.loads(s)
    except Exception:
        return default


def _as_float_or_none(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        if isinstance(v, str) and not v.strip():
            return None
        return float(v)
    except Exception:
        return None


def _as_int_or_none(v: Any) -> Optional[int]:
    if v is None:
        return None
    try:
        if isinstance(v, str) and not v.strip():
            return None
        return int(float(v))
    except Exception:
        return None


# =============================================================================
# WorkspaceRepo
# =============================================================================
class WorkspaceRepo:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def create(
        self,
        *,
        name: str,
        label: str = "",
        bom_source_path: str = "",
        inventory_master_path: str = "",
        inventory_erp_path: str = "",
        cns_path: str = "",
        notes: str = "",
        status: str = "ACTIVE",
    ) -> str:
        ws_id = new_id("WS")
        now = _now_iso()
        with _DB_LOCK, self.conn:
            self.conn.execute(
                """
                INSERT INTO workspace (
                    workspace_id, name, status, created_at, updated_at,
                    label, bom_source_path, inventory_master_path, inventory_erp_path, cns_path, notes
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                """,
                (
                    ws_id, name, status, now, now,
                    label, bom_source_path, inventory_master_path, inventory_erp_path, cns_path, notes
                ),
            )
        return ws_id

    def get(self, workspace_id: str) -> Optional[Dict[str, Any]]:
        row = self.conn.execute(
            "SELECT * FROM workspace WHERE workspace_id = ?;",
            (workspace_id,),
        ).fetchone()
        return dict(row) if row else None

    def list(self, *, status: Optional[str] = None) -> List[Dict[str, Any]]:
        q = "SELECT * FROM workspace"
        args: Tuple[Any, ...] = ()
        if status:
            q += " WHERE status = ?"
            args = (status,)
        q += " ORDER BY updated_at DESC;"
        rows = self.conn.execute(q, args).fetchall()
        return [dict(r) for r in rows]

    def update_meta(
        self,
        workspace_id: str,
        *,
        name: Optional[str] = None,
        label: Optional[str] = None,
        bom_source_path: Optional[str] = None,
        inventory_master_path: Optional[str] = None,
        inventory_erp_path: Optional[str] = None,
        cns_path: Optional[str] = None,
        notes: Optional[str] = None,
        status: Optional[str] = None,
    ) -> None:
        patch: Dict[str, Any] = {}
        if name is not None:
            patch["name"] = name
        if label is not None:
            patch["label"] = label
        if bom_source_path is not None:
            patch["bom_source_path"] = bom_source_path
        if inventory_master_path is not None:
            patch["inventory_master_path"] = inventory_master_path
        if inventory_erp_path is not None:
            patch["inventory_erp_path"] = inventory_erp_path
        if cns_path is not None:
            patch["cns_path"] = cns_path
        if notes is not None:
            patch["notes"] = notes
        if status is not None:
            patch["status"] = status

        if not patch:
            return

        patch["updated_at"] = _now_iso()

        cols = list(patch.keys())
        set_clause = ", ".join([f"{c} = ?" for c in cols])
        args = [patch[c] for c in cols] + [workspace_id]

        with _DB_LOCK, self.conn:
            self.conn.execute(
                f"UPDATE workspace SET {set_clause} WHERE workspace_id = ?;",
                args,
            )

    def touch(self, workspace_id: str) -> None:
        with _DB_LOCK, self.conn:
            self.conn.execute(
                "UPDATE workspace SET updated_at = ? WHERE workspace_id = ?;",
                (_now_iso(), workspace_id),
            )


# =============================================================================
# BomRepo
# =============================================================================
class BomRepo:
    """
    Canonical: bom_line_input
    Mutable:   bom_line_state  (minimal schema in db.py)
    """

    _STATE_COLS = {
        "updated_at",
        "cpn",
        "selected_mpn",
        "selected_mfg",
        "confidence",
        "match_type",
        "needs_new_cpn",
        "locked",
        "needs_approval",
        "notes",
        "explain_json",
    }

    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def upsert_inputs(
        self,
        workspace_id: str,
        inputs: Sequence[Dict[str, Any]],
        *,
        imported_at: Optional[str] = None,
    ) -> None:
        imported_at = imported_at or _now_iso()

        with _DB_LOCK, self.conn:
            for row in inputs:
                if "input_line_id" not in row:
                    raise ValueError("Each input row must include input_line_id")

                input_line_id = int(row["input_line_id"])
                raw = row.get("raw_json")
                if raw is None:
                    raw = {k: v for k, v in row.items() if k != "raw_json"}

                self.conn.execute(
                    """
                    INSERT INTO bom_line_input (
                        workspace_id, input_line_id, imported_at, raw_json,
                        partnum, description, qty, refdes, item_type, mfgname, mfgpn, supplier
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(workspace_id, input_line_id) DO UPDATE SET
                        imported_at=excluded.imported_at,
                        raw_json=excluded.raw_json,
                        partnum=excluded.partnum,
                        description=excluded.description,
                        qty=excluded.qty,
                        refdes=excluded.refdes,
                        item_type=excluded.item_type,
                        mfgname=excluded.mfgname,
                        mfgpn=excluded.mfgpn,
                        supplier=excluded.supplier;
                    """,
                    (
                        workspace_id,
                        input_line_id,
                        imported_at,
                        json_dumps(raw),
                        str(row.get("partnum", "") or ""),
                        str(row.get("description", "") or ""),
                        _as_float_or_none(row.get("qty")) or 0.0,
                        str(row.get("refdes", "") or ""),
                        str(row.get("item_type", "") or ""),
                        str(row.get("mfgname", "") or ""),
                        str(row.get("mfgpn", "") or ""),
                        str(row.get("supplier", "") or ""),
                    ),
                )

    def list_inputs(self, workspace_id: str) -> List[Dict[str, Any]]:
        rows = self.conn.execute(
            """
            SELECT *
            FROM bom_line_input
            WHERE workspace_id = ?
            ORDER BY input_line_id ASC;
            """,
            (workspace_id,),
        ).fetchall()

        out: List[Dict[str, Any]] = []
        for r in rows:
            d = dict(r)
            d["raw_json"] = json_loads(d.get("raw_json"), default={})
            out.append(d)
        return out



    def list_input_parts(self, workspace_id: str) -> List[NPRPart]:
        """Hydrate bom_line_input rows into NPRPart runtime objects for matching."""
        parts: List[NPRPart] = []
        for row in self.list_inputs(workspace_id):
            raw_json = row.get("raw_json") or {}
            raw_fields = dict(raw_json.get("raw_fields") or {}) if isinstance(raw_json, dict) else {}
            parsed = dict(raw_json.get("parsed") or {}) if isinstance(raw_json, dict) else {}
            parts.append(
                NPRPart(
                    partnum=str(row.get("partnum", "") or ""),
                    desc=str(row.get("description", "") or ""),
                    qty=_as_float_or_none(row.get("qty")),
                    refdes=str(row.get("refdes", "") or ""),
                    item_type=str(row.get("item_type", "") or ""),
                    mfgname=str(row.get("mfgname", "") or ""),
                    mfgpn=str(row.get("mfgpn", "") or ""),
                    supplier=str(row.get("supplier", "") or ""),
                    raw_fields=raw_fields,
                    parsed=parsed,
                )
            )
        return parts

    def get_input(self, workspace_id: str, input_line_id: int) -> Optional[Dict[str, Any]]:
        row = self.conn.execute(
            """
            SELECT *
            FROM bom_line_input
            WHERE workspace_id = ? AND input_line_id = ?;
            """,
            (workspace_id, int(input_line_id)),
        ).fetchone()
        if not row:
            return None
        d = dict(row)
        d["raw_json"] = json_loads(d.get("raw_json"), default={})
        return d

    def bootstrap_state_from_inputs(
        self,
        workspace_id: str,
        *,
        overwrite_existing: bool = False,
    ) -> None:
        """
        Ensures bom_line_state exists for every bom_line_input row.
        Minimal schema: only inserts workspace_id, line_id, updated_at.
        """
        now = _now_iso()
        rows = self.conn.execute(
            """
            SELECT input_line_id
            FROM bom_line_input
            WHERE workspace_id = ?
            ORDER BY input_line_id ASC;
            """,
            (workspace_id,),
        ).fetchall()

        with _DB_LOCK, self.conn:
            for r in rows:
                line_id = int(r["input_line_id"])
                if overwrite_existing:
                    self.conn.execute(
                        """
                        INSERT INTO bom_line_state (workspace_id, line_id, updated_at)
                        VALUES (?, ?, ?)
                        ON CONFLICT(workspace_id, line_id) DO UPDATE SET
                            updated_at=excluded.updated_at;
                        """,
                        (workspace_id, line_id, now),
                    )
                else:
                    self.conn.execute(
                        """
                        INSERT OR IGNORE INTO bom_line_state (workspace_id, line_id, updated_at)
                        VALUES (?, ?, ?);
                        """,
                        (workspace_id, line_id, now),
                    )

    def load_joined_lines(self, workspace_id: str) -> List[Dict[str, Any]]:
        rows = self.conn.execute(
            """
            SELECT
                i.workspace_id,
                i.input_line_id,
                i.imported_at,
                i.raw_json AS input_raw_json,
                i.partnum AS input_partnum,
                i.description AS input_description,
                i.qty AS input_qty,
                i.refdes AS input_refdes,
                i.item_type AS input_item_type,
                i.mfgname AS input_mfgname,
                i.mfgpn AS input_mfgpn,
                i.supplier AS input_supplier,

                s.updated_at,
                s.cpn,
                s.selected_mpn,
                s.selected_mfg,
                s.confidence,
                s.match_type,
                s.needs_new_cpn,
                s.locked,
                s.needs_approval,
                s.notes,
                s.explain_json

            FROM bom_line_input i
            LEFT JOIN bom_line_state s
                ON s.workspace_id = i.workspace_id
               AND s.line_id = i.input_line_id
            WHERE i.workspace_id = ?
            ORDER BY i.input_line_id ASC;
            """,
            (workspace_id,),
        ).fetchall()

        out: List[Dict[str, Any]] = []
        for r in rows:
            d = dict(r)
            d["input_raw_json"] = json_loads(d.get("input_raw_json"), default={})
            d["explain_json"] = json_loads(d.get("explain_json"), default={})
            out.append(d)
        return out

    def patch_state(self, workspace_id: str, line_id: int, patch: Dict[str, Any]) -> None:
        if not patch:
            return

        safe: Dict[str, Any] = {}
        for k, v in patch.items():
            if k not in self._STATE_COLS:
                raise ValueError(f"Illegal bom_line_state column in patch: {k}")
            safe[k] = v

        safe.setdefault("updated_at", _now_iso())

        cols = list(safe.keys())
        set_clause = ", ".join([f"{c} = ?" for c in cols])
        args = [safe[c] for c in cols] + [workspace_id, int(line_id)]

        with _DB_LOCK, self.conn:
            cur = self.conn.execute(
                "SELECT 1 FROM bom_line_state WHERE workspace_id = ? AND line_id = ?;",
                (workspace_id, int(line_id)),
            ).fetchone()

            if not cur:
                self.conn.execute(
                    "INSERT INTO bom_line_state (workspace_id, line_id, updated_at) VALUES (?, ?, ?);",
                    (workspace_id, int(line_id), safe["updated_at"]),
                )

            self.conn.execute(
                f"UPDATE bom_line_state SET {set_clause} WHERE workspace_id = ? AND line_id = ?;",
                args,
            )

    def set_match(
        self,
        workspace_id: str,
        line_id: int,
        *,
        cpn: str,
        selected_mfg: str = "",
        selected_mpn: str = "",
        match_type: str = "",
        confidence: float = 0.0,
        needs_new_cpn: bool = False,
        explain: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.patch_state(
            workspace_id,
            line_id,
            {
                "cpn": str(cpn or ""),
                "selected_mfg": str(selected_mfg or ""),
                "selected_mpn": str(selected_mpn or ""),
                "match_type": str(match_type or ""),
                "confidence": float(confidence or 0.0),
                "needs_new_cpn": 1 if needs_new_cpn else 0,
                "explain_json": json_dumps(explain or {}),
            },
        )

    def flag_needs_new_cpn(self, workspace_id: str, line_id: int, *, note: str = "") -> None:
        patch: Dict[str, Any] = {"needs_new_cpn": 1}
        if note:
            patch["notes"] = note
        self.patch_state(workspace_id, line_id, patch)



# =============================================================================
# Inventory repositories
# =============================================================================
# This is the active inventory persistence layer.
#
# The flow is:
#   Excel -> DataLoader normalized objects -> repository writes -> SQLite tables
#
# These classes own SQL for the new normalized inventory schema only.
# The old compatibility repository aliases were removed on purpose.
# =============================================================================
class CompanyPartRepo:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    @staticmethod
    def _row_to_company(row: sqlite3.Row) -> CompanyPartRecord:
        return CompanyPartRecord(
            company_part_id=row['company_part_id'],
            company_part_number=str(row['company_part_number'] or ''),
            description=str(row['description'] or ''),
            default_whse=str(row['default_whse'] or ''),
            total_qty=_as_float_or_none(row['total_qty']),
            revision=str(row['revision'] or ''),
            primary_vendor_number=str(row['primary_vendor_number'] or ''),
            updated_at=str(row['updated_at'] or ''),
            last_seen_at=str(row['imported_at'] or ''),
            raw_fields=json_loads(row['raw_json'], default={}),
        )

    def get(self, workspace_id: str, company_part_number: str) -> Optional[CompanyPartRecord]:
        row = self.conn.execute(
            "SELECT * FROM company_part WHERE workspace_id = ? AND company_part_number = ?;",
            (workspace_id, str(company_part_number or '')),
        ).fetchone()
        if not row:
            return None
        company = self._row_to_company(row)
        company.manufacturer_parts = ManufacturerPartRepo(self.conn).list_for_company_part_id(workspace_id, company.company_part_id)
        return company

    def list(self, workspace_id: str) -> List[CompanyPartRecord]:
        rows = self.conn.execute(
            "SELECT * FROM company_part WHERE workspace_id = ? ORDER BY company_part_number ASC;",
            (workspace_id,),
        ).fetchall()
        out = [self._row_to_company(r) for r in rows]
        part_repo = ManufacturerPartRepo(self.conn)
        for company in out:
            company.manufacturer_parts = part_repo.list_for_company_part_id(workspace_id, company.company_part_id)
        return out

    def replace_for_workspace(
        self,
        workspace_id: str,
        company_parts: Sequence[CompanyPartRecord],
        *,
        imported_at: Optional[str] = None,
    ) -> Dict[str, int]:
        imported_at = imported_at or _now_iso()
        stats = {'company_parts': 0, 'manufacturer_parts': 0}
        with _DB_LOCK, self.conn:
            self.conn.execute('DELETE FROM manufacturer_part WHERE workspace_id = ?;', (workspace_id,))
            self.conn.execute('DELETE FROM company_part WHERE workspace_id = ?;', (workspace_id,))
            for cp in (company_parts or []):
                cpn = str(getattr(cp, 'company_part_number', '') or '').strip()
                if not cpn:
                    continue
                cur = self.conn.execute(
                    """
                    INSERT INTO company_part (
                        workspace_id, company_part_number, description, default_whse,
                        total_qty, revision, primary_vendor_number, raw_json,
                        imported_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                    """,
                    (
                        workspace_id,
                        cpn,
                        str(getattr(cp, 'description', '') or ''),
                        str(getattr(cp, 'default_whse', '') or ''),
                        _as_float_or_none(getattr(cp, 'total_qty', None)),
                        str(getattr(cp, 'revision', '') or ''),
                        str(getattr(cp, 'primary_vendor_number', '') or ''),
                        json_dumps(getattr(cp, 'raw_fields', {}) or {}),
                        imported_at,
                        imported_at,
                    ),
                )
                company_part_id = int(cur.lastrowid)
                stats['company_parts'] += 1
                for mp in (getattr(cp, 'manufacturer_parts', None) or []):
                    self.conn.execute(
                        """
                        INSERT INTO manufacturer_part (
                            workspace_id, company_part_id, manufacturer_part_number,
                            manufacturer_id, manufacturer_name, description,
                            active, item_lead_time, tariff_code, tariff_rate,
                            last_cost, standard_cost, average_cost,
                            is_erp_primary, erp_source_row_key, master_source_row_key,
                            raw_json, imported_at, updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                        """,
                        (
                            workspace_id,
                            company_part_id,
                            str(getattr(mp, 'manufacturer_part_number', '') or ''),
                            str(getattr(mp, 'manufacturer_id', '') or ''),
                            str(getattr(mp, 'manufacturer_name', '') or ''),
                            str(getattr(mp, 'description', '') or ''),
                            str(getattr(mp, 'active', '') or ''),
                            _as_float_or_none(getattr(mp, 'item_lead_time', None)),
                            str(getattr(mp, 'tariff_code', '') or ''),
                            _as_float_or_none(getattr(mp, 'tariff_rate', None)),
                            _as_float_or_none(getattr(mp, 'last_cost', None)),
                            _as_float_or_none(getattr(mp, 'standard_cost', None)),
                            _as_float_or_none(getattr(mp, 'average_cost', None)),
                            1 if bool(getattr(mp, 'is_erp_primary', False)) else 0,
                            str(getattr(mp, 'erp_source_row_key', '') or ''),
                            str(getattr(mp, 'master_source_row_key', '') or ''),
                            json_dumps(getattr(mp, 'raw_fields', {}) or {}),
                            imported_at,
                            imported_at,
                        ),
                    )
                    stats['manufacturer_parts'] += 1
        return stats


class ManufacturerPartRepo:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    @staticmethod
    def _row_to_manufacturer(row: sqlite3.Row) -> ManufacturerPartRecord:
        return ManufacturerPartRecord(
            manufacturer_part_id=row['manufacturer_part_id'],
            company_part_id=row['company_part_id'],
            company_part_number=str(row['company_part_number']) if 'company_part_number' in row.keys() else '',
            manufacturer_part_number=str(row['manufacturer_part_number'] or ''),
            manufacturer_id=str(row['manufacturer_id'] or ''),
            manufacturer_name=str(row['manufacturer_name'] or ''),
            description=str(row['description'] or ''),
            active=str(row['active'] or ''),
            item_lead_time=_as_float_or_none(row['item_lead_time']),
            tariff_code=str(row['tariff_code'] or ''),
            tariff_rate=_as_float_or_none(row['tariff_rate']),
            last_cost=_as_float_or_none(row['last_cost']),
            standard_cost=_as_float_or_none(row['standard_cost']),
            average_cost=_as_float_or_none(row['average_cost']),
            is_erp_primary=bool(row['is_erp_primary']),
            erp_source_row_key=str(row['erp_source_row_key'] or ''),
            master_source_row_key=str(row['master_source_row_key'] or ''),
            updated_at=str(row['updated_at'] or ''),
            last_seen_at=str(row['imported_at'] or ''),
            raw_fields=json_loads(row['raw_json'], default={}),
        )

    def list_for_company_part_id(self, workspace_id: str, company_part_id: Optional[int]) -> List[ManufacturerPartRecord]:
        if company_part_id is None:
            return []
        rows = self.conn.execute(
            """
            SELECT mp.*, cp.company_part_number
            FROM manufacturer_part mp
            JOIN company_part cp ON cp.company_part_id = mp.company_part_id
            WHERE mp.workspace_id = ? AND mp.company_part_id = ?
            ORDER BY mp.is_erp_primary DESC, mp.manufacturer_name ASC, mp.manufacturer_part_number ASC;
            """,
            (workspace_id, int(company_part_id)),
        ).fetchall()
        return [self._row_to_manufacturer(r) for r in rows]

    def list_for_cpn(self, workspace_id: str, company_part_number: str) -> List[ManufacturerPartRecord]:
        rows = self.conn.execute(
            """
            SELECT mp.*, cp.company_part_number
            FROM manufacturer_part mp
            JOIN company_part cp ON cp.company_part_id = mp.company_part_id
            WHERE cp.workspace_id = ? AND cp.company_part_number = ?
            ORDER BY mp.is_erp_primary DESC, mp.manufacturer_name ASC, mp.manufacturer_part_number ASC;
            """,
            (workspace_id, str(company_part_number or '')),
        ).fetchall()
        return [self._row_to_manufacturer(r) for r in rows]


class InventoryImportRepo:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn
        self.company_parts = CompanyPartRepo(conn)

    def replace_inventory_snapshot(
        self,
        workspace_id: str,
        *,
        erp_rows: Sequence[Dict[str, Any]] | None = None,
        master_rows: Sequence[Dict[str, Any]] | None = None,
        company_parts: Sequence[CompanyPartRecord] | None = None,
        imported_at: Optional[str] = None,
    ) -> Dict[str, int]:
        imported_at = imported_at or _now_iso()
        erp_rows = list(erp_rows or [])
        master_rows = list(master_rows or [])
        company_parts = list(company_parts or [])
        with _DB_LOCK, self.conn:
            self.conn.execute('DELETE FROM erp_inventory_raw WHERE workspace_id = ?;', (workspace_id,))
            self.conn.execute('DELETE FROM alternate_master_raw WHERE workspace_id = ?;', (workspace_id,))
            for row in erp_rows:
                self.conn.execute(
                    """
                    INSERT INTO erp_inventory_raw (
                        workspace_id, source_row_key, item_number, description,
                        primary_vendor_number, vendor_item, manufacturer_id,
                        manufacturer_name, manufacturer_item_count, last_cost,
                        standard_cost, average_cost, revision, item_lead_time,
                        default_whse, total_qty, raw_json, imported_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                    """,
                    (
                        workspace_id,
                        str(row.get('source_row_key', '') or ''),
                        str(row.get('item_number', '') or ''),
                        str(row.get('description', '') or ''),
                        str(row.get('primary_vendor_number', '') or ''),
                        str(row.get('vendor_item', '') or ''),
                        str(row.get('manufacturer_id', '') or ''),
                        str(row.get('manufacturer_name', '') or ''),
                        _as_float_or_none(row.get('manufacturer_item_count')),
                        _as_float_or_none(row.get('last_cost')),
                        _as_float_or_none(row.get('standard_cost')),
                        _as_float_or_none(row.get('average_cost')),
                        str(row.get('revision', '') or ''),
                        _as_float_or_none(row.get('item_lead_time')),
                        str(row.get('default_whse', '') or ''),
                        _as_float_or_none(row.get('total_qty')),
                        json_dumps(row.get('raw_fields', {}) or row),
                        imported_at,
                    ),
                )
            for row in master_rows:
                self.conn.execute(
                    """
                    INSERT INTO alternate_master_raw (
                        workspace_id, source_row_key, item_number, description,
                        active, manufacturer_id, manufacturer_name,
                        manufacturer_part_number, tariff_code, tariff_rate,
                        last_cost, standard_cost, average_cost, raw_json, imported_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                    """,
                    (
                        workspace_id,
                        str(row.get('source_row_key', '') or ''),
                        str(row.get('item_number', '') or ''),
                        str(row.get('description', '') or ''),
                        str(row.get('active', '') or ''),
                        str(row.get('manufacturer_id', '') or ''),
                        str(row.get('manufacturer_name', '') or ''),
                        str(row.get('manufacturer_part_number', '') or ''),
                        str(row.get('tariff_code', '') or ''),
                        _as_float_or_none(row.get('tariff_rate')),
                        _as_float_or_none(row.get('last_cost')),
                        _as_float_or_none(row.get('standard_cost')),
                        _as_float_or_none(row.get('average_cost')),
                        json_dumps(row.get('raw_fields', {}) or row),
                        imported_at,
                    ),
                )
        stats = self.company_parts.replace_for_workspace(workspace_id, company_parts, imported_at=imported_at)
        stats.update({'erp_rows': len(erp_rows), 'master_rows': len(master_rows)})
        return stats



# =============================================================================
# ExportLogRepo
# =============================================================================
class ExportLogRepo:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def add(
        self,
        workspace_id: str,
        *,
        export_type: str,
        path: str,
        meta: Optional[Dict[str, Any]] = None,
        created_at: Optional[str] = None,
        export_id: Optional[str] = None,
    ) -> str:
        export_id = export_id or new_id("EXP")
        created_at = created_at or _now_iso()
        meta = meta or {}

        with _DB_LOCK, self.conn:
            self.conn.execute(
                """
                INSERT INTO export_log (
                    export_id, workspace_id, created_at,
                    export_type, path, meta_json
                ) VALUES (?, ?, ?, ?, ?, ?);
                """,
                (
                    export_id,
                    workspace_id,
                    created_at,
                    str(export_type or ""),
                    str(path or ""),
                    json_dumps(meta),
                ),
            )
        return export_id

    def list(self, workspace_id: str) -> List[Dict[str, Any]]:
        rows = self.conn.execute(
            """
            SELECT *
            FROM export_log
            WHERE workspace_id = ?
            ORDER BY created_at DESC;
            """,
            (workspace_id,),
        ).fetchall()

        out: List[Dict[str, Any]] = []
        for r in rows:
            d = dict(r)
            d["meta_json"] = json_loads(d.get("meta_json"), default={})
            out.append(d)
        return out


# =============================================================================
# MatchRunRepo / MatchNodeRepo / MatchAltRepo
# =============================================================================
class MatchRunRepo:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def create(
        self,
        workspace_id: str,
        *,
        engine_name: str = "MatchingEngine",
        engine_version: str = "",
        config: Optional[Dict[str, Any]] = None,
        summary: Optional[Dict[str, Any]] = None,
        created_at: Optional[str] = None,
        run_id: Optional[str] = None,
    ) -> str:
        run_id = run_id or new_id("RUN")
        created_at = created_at or _now_iso()
        config = config or {}
        summary = summary or {}

        with _DB_LOCK, self.conn:
            self.conn.execute(
                """
                INSERT INTO match_run (
                    run_id, workspace_id, created_at,
                    engine_name, engine_version,
                    config_json, summary_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?);
                """,
                (
                    run_id,
                    workspace_id,
                    created_at,
                    engine_name,
                    engine_version,
                    json_dumps(config),
                    json_dumps(summary),
                ),
            )
        return run_id

    def latest_run_id(self, workspace_id: str) -> Optional[str]:
        row = self.conn.execute(
            """
            SELECT run_id
            FROM match_run
            WHERE workspace_id = ?
            ORDER BY created_at DESC
            LIMIT 1;
            """,
            (workspace_id,),
        ).fetchone()
        return row["run_id"] if row else None


class MatchNodeRepo:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def replace_for_run(
        self,
        workspace_id: str,
        run_id: str,
        nodes: Sequence[Dict[str, Any]],
        *,
        created_at: Optional[str] = None,
    ) -> None:
        created_at = created_at or _now_iso()
        now = _now_iso()

        with _DB_LOCK, self.conn:
            self.conn.execute(
                "DELETE FROM match_node WHERE workspace_id = ? AND run_id = ?;",
                (workspace_id, run_id),
            )

            for n in (nodes or []):
                node_id = str(n.get("node_id", "") or "").strip()
                if not node_id:
                    continue

                self.conn.execute(
                    """
                    INSERT INTO match_node (
                        workspace_id, run_id, node_id,
                        line_id,
                        base_type, bom_uid, bom_mpn, description,
                        internal_part_number, inventory_mpn,
                        match_type, confidence,
                        status, locked, needs_approval,
                        notes, explain_json,
                        created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                    """,
                    (
                        workspace_id,
                        run_id,
                        node_id,
                        int(n.get("line_id", 0) or 0),
                        str(n.get("base_type", "") or ""),
                        str(n.get("bom_uid", "") or ""),
                        str(n.get("bom_mpn", "") or ""),
                        str(n.get("description", "") or ""),
                        str(n.get("internal_part_number", "") or ""),
                        str(n.get("inventory_mpn", "") or ""),
                        str(n.get("match_type", "") or ""),
                        float(n.get("confidence", 0.0) or 0.0),
                        str(n.get("status", "NEEDS_DECISION") or "NEEDS_DECISION"),
                        int(n.get("locked", 0) or 0),
                        int(n.get("needs_approval", 0) or 0),
                        str(n.get("notes", "") or ""),
                        json_dumps(n.get("explain_json", {}) or n.get("explain", {}) or {}),
                        created_at,
                        now,
                    ),
                )

    def list_for_run(self, workspace_id: str, run_id: str) -> List[Dict[str, Any]]:
        rows = self.conn.execute(
            """
            SELECT *
            FROM match_node
            WHERE workspace_id = ? AND run_id = ?
            ORDER BY line_id ASC;
            """,
            (workspace_id, run_id),
        ).fetchall()

        out: List[Dict[str, Any]] = []
        for r in rows:
            d = dict(r)
            d["explain_json"] = json_loads(d.get("explain_json"), default={})
            out.append(d)
        return out


class MatchAltRepo:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def replace_for_node(
        self,
        workspace_id: str,
        run_id: str,
        node_id: str,
        alts: Sequence[Dict[str, Any]],
        *,
        created_at: Optional[str] = None,
    ) -> None:
        created_at = created_at or _now_iso()
        now = _now_iso()

        with _DB_LOCK, self.conn:
            self.conn.execute(
                "DELETE FROM match_alt WHERE workspace_id = ? AND run_id = ? AND node_id = ?;",
                (workspace_id, run_id, node_id),
            )

            for a in (alts or []):
                alt_id = str(a.get("alt_id", "") or "").strip() or new_id("ALT")

                self.conn.execute(
                    """
                    INSERT INTO match_alt (
                        workspace_id, run_id, node_id, alt_id,
                        source,
                        manufacturer, manufacturer_part_number, internal_part_number,
                        description,
                        value, package, tolerance, voltage, wattage,
                        stock, unit_cost,
                        supplier,
                        confidence, relationship, matched_mpn,
                        selected, rejected,
                        meta_json, raw_json,
                        created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                    """,
                    (
                        workspace_id,
                        run_id,
                        node_id,
                        alt_id,
                        str(a.get("source", "inventory") or "inventory"),
                        str(a.get("manufacturer", "") or ""),
                        str(a.get("manufacturer_part_number", "") or ""),
                        str(a.get("internal_part_number", "") or ""),
                        str(a.get("description", "") or ""),
                        str(a.get("value", "") or ""),
                        str(a.get("package", "") or ""),
                        str(a.get("tolerance", "") or ""),
                        str(a.get("voltage", "") or ""),
                        str(a.get("wattage", "") or ""),
                        int(a.get("stock", 0) or 0),
                        _as_float_or_none(a.get("unit_cost")),
                        str(a.get("supplier", "") or ""),
                        float(a.get("confidence", 0.0) or 0.0),
                        str(a.get("relationship", "") or ""),
                        str(a.get("matched_mpn", "") or ""),
                        int(a.get("selected", 0) or 0),
                        int(a.get("rejected", 0) or 0),
                        json_dumps(a.get("meta_json", {}) or a.get("meta", {}) or {}),
                        json_dumps(a.get("raw_json", {}) or a.get("raw", {}) or {}),
                        created_at,
                        now,
                    ),
                )

    def list_for_node(self, workspace_id: str, run_id: str, node_id: str) -> List[Dict[str, Any]]:
        rows = self.conn.execute(
            """
            SELECT *
            FROM match_alt
            WHERE workspace_id = ? AND run_id = ? AND node_id = ? AND rejected = 0
            ORDER BY selected DESC, confidence DESC;
            """,
            (workspace_id, run_id, node_id),
        ).fetchall()

        out: List[Dict[str, Any]] = []
        for r in rows:
            d = dict(r)
            d["meta_json"] = json_loads(d.get("meta_json"), default={})
            d["raw_json"] = json_loads(d.get("raw_json"), default={})
            out.append(d)
        return out


# =============================================================================
# DecisionNodeRepo / DecisionAltRepo
# =============================================================================
class DecisionNodeRepo:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def save_node(
        self,
        workspace_id: str,
        node: Dict[str, Any],
        *,
        created_at: Optional[str] = None,
    ) -> None:
        created_at = created_at or _now_iso()
        now = _now_iso()
        with _DB_LOCK, self.conn:
            self.conn.execute(
                """
                INSERT INTO decision_node (
                    workspace_id, node_id, line_id,
                    base_type, bom_uid, bom_mpn, description,
                    internal_part_number, assigned_part_number, inventory_mpn, preferred_inventory_mfgpn, bom_section,
                    match_type, confidence,
                    status, locked, needs_approval,
                    focused_alt_id, exclude_customer_part_number_in_npr,
                    notes, explain_json,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(workspace_id, node_id) DO UPDATE SET
                    line_id=excluded.line_id,
                    base_type=excluded.base_type,
                    bom_uid=excluded.bom_uid,
                    bom_mpn=excluded.bom_mpn,
                    description=excluded.description,
                    internal_part_number=excluded.internal_part_number,
                    assigned_part_number=excluded.assigned_part_number,
                    inventory_mpn=excluded.inventory_mpn,
                    preferred_inventory_mfgpn=excluded.preferred_inventory_mfgpn,
                    bom_section=excluded.bom_section,
                    match_type=excluded.match_type,
                    confidence=excluded.confidence,
                    status=excluded.status,
                    locked=excluded.locked,
                    needs_approval=excluded.needs_approval,
                    focused_alt_id=excluded.focused_alt_id,
                    exclude_customer_part_number_in_npr=excluded.exclude_customer_part_number_in_npr,
                    notes=excluded.notes,
                    explain_json=excluded.explain_json,
                    updated_at=excluded.updated_at;
                """,
                (
                    workspace_id,
                    str(node.get("node_id", "") or ""),
                    int(node.get("line_id", 0) or 0),
                    str(node.get("base_type", "") or ""),
                    str(node.get("bom_uid", "") or ""),
                    str(node.get("bom_mpn", "") or ""),
                    str(node.get("description", "") or ""),
                    str(node.get("internal_part_number", "") or ""),
                    str(node.get("assigned_part_number", "") or ""),
                    str(node.get("inventory_mpn", "") or ""),
                    str(node.get("preferred_inventory_mfgpn", "") or ""),
                    str(node.get("bom_section", "SURFACE MOUNT") or "SURFACE MOUNT"),
                    str(node.get("match_type", "") or ""),
                    float(node.get("confidence", 0.0) or 0.0),
                    str(node.get("status", "NEEDS_DECISION") or "NEEDS_DECISION"),
                    int(node.get("locked", 0) or 0),
                    int(node.get("needs_approval", 0) or 0),
                    str(node.get("focused_alt_id", "") or ""),
                    int(node.get("exclude_customer_part_number_in_npr", 0) or 0),
                    str(node.get("notes", "") or ""),
                    json_dumps(node.get("explain_json", {}) or node.get("explain", {}) or {}),
                    created_at,
                    now,
                ),
            )

    def save_nodes(self, workspace_id: str, nodes: Sequence[Dict[str, Any]]) -> None:
        for node in (nodes or []):
            self.save_node(workspace_id, node)

    def get_node(self, workspace_id: str, node_id: str) -> Optional[Dict[str, Any]]:
        row = self.conn.execute(
            """
            SELECT *
            FROM decision_node
            WHERE workspace_id = ? AND node_id = ?;
            """,
            (workspace_id, str(node_id or "")),
        ).fetchone()
        if not row:
            return None
        d = dict(row)
        d["explain_json"] = json_loads(d.get("explain_json"), default={})
        return d

    def list_nodes(self, workspace_id: str) -> List[Dict[str, Any]]:
        rows = self.conn.execute(
            """
            SELECT *
            FROM decision_node
            WHERE workspace_id = ?
            ORDER BY line_id ASC, node_id ASC;
            """,
            (workspace_id,),
        ).fetchall()

        out: List[Dict[str, Any]] = []
        for r in rows:
            d = dict(r)
            d["explain_json"] = json_loads(d.get("explain_json"), default={})
            out.append(d)
        return out

    def delete_workspace_nodes(self, workspace_id: str) -> None:
        with _DB_LOCK, self.conn:
            self.conn.execute(
                "DELETE FROM decision_node WHERE workspace_id = ?;",
                (workspace_id,),
            )


class DecisionAltRepo:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def save_node_alternates(
        self,
        workspace_id: str,
        node_id: str,
        alts: Sequence[Dict[str, Any]],
        *,
        created_at: Optional[str] = None,
    ) -> None:
        created_at = created_at or _now_iso()
        now = _now_iso()
        with _DB_LOCK, self.conn:
            self.conn.execute(
                "DELETE FROM decision_alt WHERE workspace_id = ? AND node_id = ?;",
                (workspace_id, str(node_id or "")),
            )
            for alt in (alts or []):
                self.conn.execute(
                    """
                    INSERT INTO decision_alt (
                        workspace_id, node_id, alt_id,
                        source,
                        manufacturer, manufacturer_part_number, internal_part_number,
                        description,
                        value, package, tolerance, voltage, wattage,
                        stock, unit_cost, supplier,
                        confidence, relationship, matched_mpn,
                        selected, rejected,
                        meta_json, raw_json,
                        created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                    """,
                    (
                        workspace_id,
                        str(node_id or ""),
                        str(alt.get("alt_id", "") or ""),
                        str(alt.get("source", "inventory") or "inventory"),
                        str(alt.get("manufacturer", "") or ""),
                        str(alt.get("manufacturer_part_number", "") or ""),
                        str(alt.get("internal_part_number", "") or ""),
                        str(alt.get("description", "") or ""),
                        str(alt.get("value", "") or ""),
                        str(alt.get("package", "") or ""),
                        str(alt.get("tolerance", "") or ""),
                        str(alt.get("voltage", "") or ""),
                        str(alt.get("wattage", "") or ""),
                        int(alt.get("stock", 0) or 0),
                        _as_float_or_none(alt.get("unit_cost")),
                        str(alt.get("supplier", "") or ""),
                        float(alt.get("confidence", 0.0) or 0.0),
                        str(alt.get("relationship", "") or ""),
                        str(alt.get("matched_mpn", "") or ""),
                        int(alt.get("selected", 0) or 0),
                        int(alt.get("rejected", 0) or 0),
                        json_dumps(alt.get("meta_json", {}) or alt.get("meta", {}) or {}),
                        json_dumps(alt.get("raw_json", {}) or alt.get("raw", {}) or {}),
                        created_at,
                        now,
                    ),
                )

    def save_alternate(
        self,
        workspace_id: str,
        node_id: str,
        alt: Dict[str, Any],
        *,
        created_at: Optional[str] = None,
    ) -> None:
        existing = self.list_node_alternates(workspace_id, node_id, include_rejected=True)
        existing = [a for a in existing if str(a.get("alt_id", "")) != str(alt.get("alt_id", ""))]
        existing.append(dict(alt))
        self.save_node_alternates(workspace_id, node_id, existing, created_at=created_at)

    def list_node_alternates(
        self,
        workspace_id: str,
        node_id: str,
        *,
        include_rejected: bool = True,
    ) -> List[Dict[str, Any]]:
        q = """
            SELECT *
            FROM decision_alt
            WHERE workspace_id = ? AND node_id = ?
        """
        args: List[Any] = [workspace_id, str(node_id or "")]
        if not include_rejected:
            q += " AND rejected = 0"
        q += " ORDER BY selected DESC, rejected ASC, confidence DESC, alt_id ASC;"
        rows = self.conn.execute(q, tuple(args)).fetchall()

        out: List[Dict[str, Any]] = []
        for r in rows:
            d = dict(r)
            d["meta_json"] = json_loads(d.get("meta_json"), default={})
            d["raw_json"] = json_loads(d.get("raw_json"), default={})
            out.append(d)
        return out

    def list_workspace_alternates(
        self,
        workspace_id: str,
        *,
        include_rejected: bool = True,
    ) -> List[Dict[str, Any]]:
        q = """
            SELECT *
            FROM decision_alt
            WHERE workspace_id = ?
        """
        args: List[Any] = [workspace_id]
        if not include_rejected:
            q += " AND rejected = 0"
        q += " ORDER BY node_id ASC, selected DESC, rejected ASC, confidence DESC, alt_id ASC;"
        rows = self.conn.execute(q, tuple(args)).fetchall()

        out: List[Dict[str, Any]] = []
        for r in rows:
            d = dict(r)
            d["meta_json"] = json_loads(d.get("meta_json"), default={})
            d["raw_json"] = json_loads(d.get("raw_json"), default={})
            out.append(d)
        return out

    def delete_node_alternates(self, workspace_id: str, node_id: str) -> None:
        with _DB_LOCK, self.conn:
            self.conn.execute(
                "DELETE FROM decision_alt WHERE workspace_id = ? AND node_id = ?;",
                (workspace_id, str(node_id or "")),
            )

    def delete_workspace_alternates(self, workspace_id: str) -> None:
        with _DB_LOCK, self.conn:
            self.conn.execute(
                "DELETE FROM decision_alt WHERE workspace_id = ?;",
                (workspace_id,),
            )

# =============================================================================
# SpladeCacheRepo
# =============================================================================
class SpladeCacheRepo:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def load_rows(
        self,
        *,
        model_name: str,
        preprocess_version: int,
        max_len: int,
        top_terms: int,
        row_keys: Sequence[str],
    ) -> Dict[str, Dict[str, Any]]:
        keys = [str(k or "").strip() for k in (row_keys or []) if str(k or "").strip()]
        if not keys:
            return {}

        out: Dict[str, Dict[str, Any]] = {}
        chunk = 500
        for i in range(0, len(keys), chunk):
            subset = keys[i:i+chunk]
            qmarks = ",".join("?" for _ in subset)
            rows = self.conn.execute(
                f"""
                SELECT row_key, row_hash, term_ids_blob, term_wts_blob, doc_norm, updated_at
                FROM splade_doc_cache
                WHERE model_name = ?
                  AND preprocess_version = ?
                  AND max_len = ?
                  AND top_terms = ?
                  AND row_key IN ({qmarks});
                """,
                [model_name, int(preprocess_version), int(max_len), int(top_terms), *subset],
            ).fetchall()
            for r in rows:
                out[str(r["row_key"])] = dict(r)
        return out

    def upsert_rows(
        self,
        *,
        model_name: str,
        preprocess_version: int,
        max_len: int,
        top_terms: int,
        rows: Sequence[Dict[str, Any]],
        updated_at: Optional[str] = None,
    ) -> int:
        updated_at = updated_at or _now_iso()
        count = 0
        with _DB_LOCK, self.conn:
            for row in (rows or []):
                row_key = str((row or {}).get("row_key", "") or "").strip()
                if not row_key:
                    continue
                self.conn.execute(
                    """
                    INSERT INTO splade_doc_cache (
                        model_name, preprocess_version, max_len, top_terms,
                        row_key, row_hash,
                        term_ids_blob, term_wts_blob, doc_norm, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(model_name, preprocess_version, max_len, top_terms, row_key) DO UPDATE SET
                        row_hash=excluded.row_hash,
                        term_ids_blob=excluded.term_ids_blob,
                        term_wts_blob=excluded.term_wts_blob,
                        doc_norm=excluded.doc_norm,
                        updated_at=excluded.updated_at;
                    """,
                    (
                        model_name,
                        int(preprocess_version),
                        int(max_len),
                        int(top_terms),
                        row_key,
                        str((row or {}).get("row_hash", "") or ""),
                        sqlite3.Binary((row or {}).get("term_ids_blob", b"")),
                        sqlite3.Binary((row or {}).get("term_wts_blob", b"")),
                        float((row or {}).get("doc_norm", 1.0) or 1.0),
                        updated_at,
                    ),
                )
                count += 1
        return count

    def prune_missing_row_keys(
        self,
        *,
        model_name: str,
        preprocess_version: int,
        max_len: int,
        top_terms: int,
        active_row_keys: Sequence[str],
    ) -> int:
        active = [str(k or "").strip() for k in (active_row_keys or []) if str(k or "").strip()]
        with _DB_LOCK, self.conn:
            if active:
                qmarks = ",".join("?" for _ in active)
                cur = self.conn.execute(
                    f"""
                    DELETE FROM splade_doc_cache
                    WHERE model_name = ?
                      AND preprocess_version = ?
                      AND max_len = ?
                      AND top_terms = ?
                      AND row_key NOT IN ({qmarks});
                    """,
                    [model_name, int(preprocess_version), int(max_len), int(top_terms), *active],
                )
            else:
                cur = self.conn.execute(
                    """
                    DELETE FROM splade_doc_cache
                    WHERE model_name = ?
                      AND preprocess_version = ?
                      AND max_len = ?
                      AND top_terms = ?;
                    """,
                    (model_name, int(preprocess_version), int(max_len), int(top_terms)),
                )
        return int(getattr(cur, "rowcount", 0) or 0)


# =============================================================================
# Runtime hydration helpers for matching/UI compatibility
# =============================================================================
def build_runtime_inventory_part(company_part: CompanyPartRecord, manufacturer_part: ManufacturerPartRecord) -> InventoryPart:
    """Flatten canonical inventory records into the runtime object expected by matching/UI code."""
    raw = dict(company_part.raw_fields or {})
    raw.update(manufacturer_part.raw_fields or {})
    raw.setdefault("totalqty", company_part.total_qty)
    raw.setdefault("total_qty", company_part.total_qty)
    raw.setdefault("company_part_number", company_part.company_part_number)
    return InventoryPart(
        itemnum=str(company_part.company_part_number or ""),
        desc=str(manufacturer_part.description or company_part.description or ""),
        mfgid=str(manufacturer_part.manufacturer_id or ""),
        mfgname=str(manufacturer_part.manufacturer_name or ""),
        vendoritem=str(manufacturer_part.manufacturer_part_number or ""),
        supplier=str(company_part.primary_vendor_number or ""),
        stock=_as_int_or_none(company_part.total_qty) or 0,
        lead_time_days=_as_int_or_none(manufacturer_part.item_lead_time),
        raw_fields=raw,
    )


def match_result_to_dict(match: MatchResult) -> Dict[str, Any]:
    """Serialize a MatchResult into a plain dictionary for DB logging/debug storage."""
    inv = match.inventory_part
    return {
        "match_type": match.match_type.value if hasattr(match.match_type, "value") else str(match.match_type or ""),
        "confidence": float(match.confidence or 0.0),
        "notes": str(match.notes or ""),
        "explain": dict(match.explain or {}),
        "inventory_part": inv.to_dict() if hasattr(inv, "to_dict") and inv is not None else None,
        "candidate_count": len(match.candidates or []),
    }


def match_result_from_dict(payload: Dict[str, Any]) -> MatchResult:
    """Rehydrate a minimal MatchResult from a stored dictionary payload."""
    payload = payload or {}
    inv_payload = payload.get("inventory_part") or None
    inv = None
    if isinstance(inv_payload, dict):
        inv = InventoryPart(
            itemnum=str(inv_payload.get("itemnum", "") or ""),
            desc=str(inv_payload.get("description", inv_payload.get("desc", "")) or ""),
            mfgid=str(inv_payload.get("mfgid", "") or ""),
            mfgname=str(inv_payload.get("mfgname", "") or ""),
            vendoritem=str(inv_payload.get("vendoritem", "") or ""),
            supplier=str(inv_payload.get("supplier", "") or ""),
            stock=_as_int_or_none(inv_payload.get("stock")) or 0,
            lead_time_days=_as_int_or_none(inv_payload.get("lead_time_days")),
            raw_fields=dict(inv_payload.get("raw_fields") or {}),
            parsed=dict(inv_payload.get("parsed") or {}),
        )
    mt_raw = str(payload.get("match_type", "") or "")
    try:
        mt = next(m for m in MatchType if m.value == mt_raw)
    except StopIteration:
        mt = MatchType.NO_MATCH
    return MatchResult(
        match_type=mt,
        confidence=float(payload.get("confidence", 0.0) or 0.0),
        inventory_part=inv,
        candidates=[inv] if inv is not None else [],
        notes=str(payload.get("notes", "") or ""),
        explain=dict(payload.get("explain") or {}),
    )
