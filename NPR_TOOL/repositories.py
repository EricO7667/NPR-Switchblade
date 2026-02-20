# repositories.py
from __future__ import annotations

import json
import sqlite3
import threading
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple

_DB_LOCK = threading.RLock()


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
# InventoryCompanyRepo + InventoryCompanyItemRepo
# =============================================================================
class InventoryCompanyRepo:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def upsert_company_parts(
        self,
        workspace_id: str,
        parts: Sequence[Dict[str, Any]],
        *,
        imported_at: Optional[str] = None,
    ) -> None:
        """
        parts: [{cpn, canonical_desc, stock_total, alternates}] where alternates is list[dict]
        Also maintains inventory_company_item (row-per-alt) for UI.
        """
        imported_at = imported_at or _now_iso()

        with _DB_LOCK, self.conn:
            for p in parts:
                cpn = str(p.get("cpn", "") or "").strip()
                if not cpn:
                    continue

                canonical_desc = str(p.get("canonical_desc", "") or "")
                stock_total = int(p.get("stock_total", 0) or 0)
                alternates = p.get("alternates", []) or []
                if not isinstance(alternates, list):
                    alternates = []

                # base snapshot
                self.conn.execute(
                    """
                    INSERT INTO inventory_company (
                        workspace_id, cpn,
                        canonical_desc, stock_total,
                        alternates_json,
                        imported_at
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(workspace_id, cpn) DO UPDATE SET
                        canonical_desc=excluded.canonical_desc,
                        stock_total=excluded.stock_total,
                        alternates_json=excluded.alternates_json,
                        imported_at=excluded.imported_at;
                    """,
                    (
                        workspace_id,
                        cpn,
                        canonical_desc,
                        stock_total,
                        json_dumps(alternates),
                        imported_at,
                    ),
                )

                # normalized view (blow away and rebuild rows for this cpn)
                self.conn.execute(
                    "DELETE FROM inventory_company_item WHERE workspace_id = ? AND cpn = ?;",
                    (workspace_id, cpn),
                )

                for a in alternates:
                    if not isinstance(a, dict):
                        continue

                    mpn = str(a.get("mpn") or a.get("mfgpn") or "").strip()
                    if not mpn:
                        continue

                    self.conn.execute(
                        """
                        INSERT INTO inventory_company_item (
                            workspace_id, cpn,
                            mfgname, mfgid, mpn,
                            unit_price, last_unit_price, standard_cost, average_cost,
                            tariff_code, tariff_rate, supplier, lead_time_days,
                            meta_json, imported_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(workspace_id, cpn, mfgname, mpn) DO UPDATE SET
                            mfgid=excluded.mfgid,
                            unit_price=excluded.unit_price,
                            last_unit_price=excluded.last_unit_price,
                            standard_cost=excluded.standard_cost,
                            average_cost=excluded.average_cost,
                            tariff_code=excluded.tariff_code,
                            tariff_rate=excluded.tariff_rate,
                            supplier=excluded.supplier,
                            lead_time_days=excluded.lead_time_days,
                            meta_json=excluded.meta_json,
                            imported_at=excluded.imported_at;
                        """,
                        (
                            workspace_id,
                            cpn,
                            str(a.get("mfgname", "") or ""),
                            str(a.get("mfgid", "") or ""),
                            mpn,
                            _as_float_or_none(a.get("unit_price")),
                            _as_float_or_none(a.get("last_unit_price")),
                            _as_float_or_none(a.get("standard_cost")),
                            _as_float_or_none(a.get("average_cost")),
                            str(a.get("tariff_code", "") or ""),
                            _as_float_or_none(a.get("tariff_rate")),
                            str(a.get("supplier", "") or ""),
                            _as_int_or_none(a.get("lead_time_days")),
                            json_dumps(a.get("meta", {}) or {}),
                            imported_at,
                        ),
                    )


    def sync_company_parts_incremental(
        self,
        workspace_id: str,
        parts: Sequence[Dict[str, Any]],
        *,
        imported_at: Optional[str] = None,
    ) -> Dict[str, int]:
        """
        Incrementally sync inventory_company / inventory_company_item for a workspace.

        Behavior:
        - Upserts only rows that are NEW or CHANGED (cpn-level diff on desc/stock/alternates_json)
        - Leaves unchanged rows untouched (prevents write amplification / DB growth)
        - Prunes CPN rows that no longer exist in the incoming master snapshot
        - Rebuilds inventory_company_item ONLY for changed/new CPNs
        """
        imported_at = imported_at or _now_iso()

        def _norm_alt(a: Dict[str, Any]) -> Dict[str, Any]:
            if not isinstance(a, dict):
                return {}
            out = dict(a)
            # normalize nested meta so JSON compare is stable
            out["meta"] = out.get("meta", {}) or {}
            return out

        def _alts_json(v: Any) -> str:
            alts = v or []
            if not isinstance(alts, list):
                alts = []
            alts = [_norm_alt(a) for a in alts if isinstance(a, dict)]
            return json.dumps(alts, sort_keys=True, separators=(",", ":"))

        incoming: Dict[str, Dict[str, Any]] = {}
        for p in (parts or []):
            cpn = str((p or {}).get("cpn", "") or "").strip()
            if not cpn:
                continue
            incoming[cpn] = {
                "canonical_desc": str((p or {}).get("canonical_desc", "") or ""),
                "stock_total": int((p or {}).get("stock_total", 0) or 0),
                "alternates": ((p or {}).get("alternates") or (p or {}).get("alternates_json") or []),
            }

        stats = {"new": 0, "changed": 0, "unchanged": 0, "pruned": 0}

        with _DB_LOCK, self.conn:
            existing_rows = self.conn.execute(
                "SELECT cpn, canonical_desc, stock_total, alternates_json FROM inventory_company WHERE workspace_id = ?;",
                (workspace_id,),
            ).fetchall()

            existing_by_cpn = {}
            for r in existing_rows:
                d = dict(r)
                existing_by_cpn[str(d.get("cpn") or "")] = {
                    "canonical_desc": str(d.get("canonical_desc") or ""),
                    "stock_total": int(d.get("stock_total") or 0),
                    "alternates_json": d.get("alternates_json") or "[]",
                }

            incoming_cpns = set(incoming.keys())
            existing_cpns = set(existing_by_cpn.keys())

            # prune rows that no longer exist in the incoming master snapshot
            stale_cpns = sorted(existing_cpns - incoming_cpns)
            if stale_cpns:
                qmarks = ",".join("?" for _ in stale_cpns)
                self.conn.execute(
                    f"DELETE FROM inventory_company_item WHERE workspace_id = ? AND cpn IN ({qmarks});",
                    [workspace_id, *stale_cpns],
                )
                self.conn.execute(
                    f"DELETE FROM inventory_company WHERE workspace_id = ? AND cpn IN ({qmarks});",
                    [workspace_id, *stale_cpns],
                )
                stats["pruned"] = len(stale_cpns)

            for cpn in sorted(incoming_cpns):
                p = incoming[cpn]
                canonical_desc = p["canonical_desc"]
                stock_total = p["stock_total"]
                alternates = p["alternates"] if isinstance(p["alternates"], list) else []
                alt_json = _alts_json(alternates)

                ex = existing_by_cpn.get(cpn)
                changed = True
                is_new = ex is None
                if ex is not None:
                    ex_alt_json = ex.get("alternates_json") or "[]"
                    # compare canonical serialized forms to avoid whitespace/order noise
                    try:
                        ex_alt_json_cmp = json.dumps(json.loads(ex_alt_json), sort_keys=True, separators=(",", ":"))
                    except Exception:
                        ex_alt_json_cmp = ex_alt_json
                    changed = not (
                        ex.get("canonical_desc", "") == canonical_desc and
                        int(ex.get("stock_total", 0)) == int(stock_total) and
                        ex_alt_json_cmp == alt_json
                    )

                if not changed:
                    stats["unchanged"] += 1
                    continue

                self.conn.execute(
                    """
                    INSERT INTO inventory_company (
                        workspace_id, cpn,
                        canonical_desc, stock_total,
                        alternates_json,
                        imported_at
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(workspace_id, cpn) DO UPDATE SET
                        canonical_desc=excluded.canonical_desc,
                        stock_total=excluded.stock_total,
                        alternates_json=excluded.alternates_json,
                        imported_at=excluded.imported_at;
                    """,
                    (workspace_id, cpn, canonical_desc, stock_total, alt_json, imported_at),
                )

                # rebuild normalized items only for this changed/new CPN
                self.conn.execute(
                    "DELETE FROM inventory_company_item WHERE workspace_id = ? AND cpn = ?;",
                    (workspace_id, cpn),
                )
                for a in alternates:
                    if not isinstance(a, dict):
                        continue
                    mpn = str(a.get("mpn") or a.get("mfgpn") or a.get("vendoritem") or "").strip()
                    if not mpn:
                        continue
                    self.conn.execute(
                        """
                        INSERT INTO inventory_company_item (
                            workspace_id, cpn,
                            mfgname, mfgid, mpn,
                            unit_price, last_unit_price, standard_cost, average_cost,
                            tariff_code, tariff_rate, supplier, lead_time_days,
                            meta_json, imported_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(workspace_id, cpn, mfgname, mpn) DO UPDATE SET
                            mfgid=excluded.mfgid,
                            unit_price=excluded.unit_price,
                            last_unit_price=excluded.last_unit_price,
                            standard_cost=excluded.standard_cost,
                            average_cost=excluded.average_cost,
                            tariff_code=excluded.tariff_code,
                            tariff_rate=excluded.tariff_rate,
                            supplier=excluded.supplier,
                            lead_time_days=excluded.lead_time_days,
                            meta_json=excluded.meta_json,
                            imported_at=excluded.imported_at;
                        """,
                        (
                            workspace_id,
                            cpn,
                            str(a.get("mfgname", "") or ""),
                            str(a.get("mfgid", "") or ""),
                            mpn,
                            _as_float_or_none(a.get("unit_price")),
                            _as_float_or_none(a.get("last_unit_price")),
                            _as_float_or_none(a.get("standard_cost")),
                            _as_float_or_none(a.get("average_cost")),
                            str(a.get("tariff_code", "") or ""),
                            _as_float_or_none(a.get("tariff_rate")),
                            str(a.get("supplier", "") or ""),
                            _as_int_or_none(a.get("lead_time_days")),
                            json_dumps(a.get("meta", {}) or {}),
                            imported_at,
                        ),
                    )

                if is_new:
                    stats["new"] += 1
                else:
                    stats["changed"] += 1

        return stats

    def get(self, workspace_id: str, cpn: str) -> Optional[Dict[str, Any]]:
        row = self.conn.execute(
            """
            SELECT *
            FROM inventory_company
            WHERE workspace_id = ? AND cpn = ?;
            """,
            (workspace_id, str(cpn or "")),
        ).fetchone()

        if not row:
            return None

        d = dict(row)
        d["alternates_json"] = json_loads(d.get("alternates_json"), default=[])
        return d

    def list(self, workspace_id: str) -> List[Dict[str, Any]]:
        rows = self.conn.execute(
            """
            SELECT *
            FROM inventory_company
            WHERE workspace_id = ?
            ORDER BY cpn ASC;
            """,
            (workspace_id,),
        ).fetchall()

        out: List[Dict[str, Any]] = []
        for r in rows:
            d = dict(r)
            d["alternates_json"] = json_loads(d.get("alternates_json"), default=[])
            out.append(d)
        return out


class InventoryCompanyItemRepo:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def list_for_cpn(self, workspace_id: str, cpn: str) -> List[Dict[str, Any]]:
        rows = self.conn.execute(
            """
            SELECT *
            FROM inventory_company_item
            WHERE workspace_id = ? AND cpn = ?
            ORDER BY mfgname ASC, mpn ASC;
            """,
            (workspace_id, str(cpn or "")),
        ).fetchall()

        out: List[Dict[str, Any]] = []
        for r in rows:
            d = dict(r)
            d["meta_json"] = json_loads(d.get("meta_json"), default={})
            out.append(d)
        return out


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
