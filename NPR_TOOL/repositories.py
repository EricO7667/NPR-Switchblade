# repositories.py
from __future__ import annotations

import hashlib
import json
import sqlite3
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

from .data_models import DecisionNode, Alternate, DecisionStatus
import threading
from enum import Enum

_DB_LOCK = threading.RLock()

def _status_to_db(v) -> str:
    # Save enums as their .value, not "DecisionStatus.X"
    try:
        if isinstance(v, Enum):
            return str(v.value)
    except Exception:
        pass
    return str(v or "")

def _status_from_db(s: str):
    s = str(s or "")
    # Normalize legacy forms like "DecisionStatus.READY_FOR_EXPORT"
    if "DecisionStatus." in s:
        s = s.split("DecisionStatus.", 1)[1]
    # Convert to DecisionStatus if possible; otherwise return the string
    try:
        return DecisionStatus(s)
    except Exception:
        return s



def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def new_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def json_dumps(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"), default=str)


def json_loads(s: str) -> Any:
    try:
        return json.loads(s or "{}")
    except Exception:
        return {}


def _parse_decision_status(raw: Any) -> DecisionStatus:
    """
    DB may contain:
      - 'READY_FOR_EXPORT' (preferred)
      - 'DecisionStatus.READY_FOR_EXPORT' (older/buggy)
      - 'READY_FOR_EXPORT' via Enum.name (rare)
    Normalize to DecisionStatus enum.
    """
    if isinstance(raw, DecisionStatus):
        return raw

    s = (str(raw or "")).strip()
    if not s:
        return DecisionStatus.NEEDS_DECISION

    if s.startswith("DecisionStatus."):
        s = s.split(".", 1)[1].strip()

    # Prefer enum VALUE parsing (DecisionStatus('READY_FOR_EXPORT'))
    try:
        return DecisionStatus(s)
    except Exception:
        pass

    # Fallback: enum NAME parsing (DecisionStatus['READY_FOR_EXPORT'])
    try:
        return DecisionStatus[s]
    except Exception:
        return DecisionStatus.NEEDS_DECISION


class WorkspaceRepo:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def create_workspace(
        self,
        *,
        name: str,
        bom_id: Optional[str] = None,
        inventory_snapshot_id: Optional[str] = None,
        parse_config_hash: str = "",
        match_config_hash: str = "",
    ) -> str:
        ws_id = new_id("WS")
        now = _now_iso()
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO workspace (
                    workspace_id, name, status, created_at, updated_at,
                    bom_id, inventory_snapshot_id, parse_config_hash, match_config_hash
                ) VALUES (?, ?, 'ACTIVE', ?, ?, ?, ?, ?, ?);
                """,
                (ws_id, name, now, now, bom_id, inventory_snapshot_id, parse_config_hash, match_config_hash),
            )
        return ws_id

    def get_workspace(self, workspace_id: str) -> Optional[Dict[str, Any]]:
        row = self.conn.execute(
            "SELECT * FROM workspace WHERE workspace_id = ?;",
            (workspace_id,),
        ).fetchone()
        return dict(row) if row else None

    def list_workspaces(self, *, status: Optional[str] = None) -> List[Dict[str, Any]]:
        q = "SELECT * FROM workspace"
        args: Tuple[Any, ...] = ()
        if status:
            q += " WHERE status = ?"
            args = (status,)
        q += " ORDER BY updated_at DESC"
        rows = self.conn.execute(q, args).fetchall()
        return [dict(r) for r in rows]

    def update_links(
        self,
        workspace_id: str,
        *,
        bom_id: Optional[str] = None,
        inventory_snapshot_id: Optional[str] = None,
    ) -> None:
        now = _now_iso()
        with self.conn:
            self.conn.execute(
                """
                UPDATE workspace
                SET bom_id = COALESCE(?, bom_id),
                    inventory_snapshot_id = COALESCE(?, inventory_snapshot_id),
                    updated_at = ?
                WHERE workspace_id = ?
                """,
                (bom_id, inventory_snapshot_id, now, workspace_id),
            )


class BomRepo:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def create_bom(self, *, source_path: Optional[str], file_bytes: bytes) -> str:
        bom_id = new_id("BOM")
        now = _now_iso()
        h = sha256_bytes(file_bytes)
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO bom (bom_id, source_path, source_hash, imported_at, row_count)
                VALUES (?, ?, ?, ?, 0);
                """,
                (bom_id, source_path, h, now),
            )
        return bom_id

    def set_row_count(self, bom_id: str, row_count: int) -> None:
        with self.conn:
            self.conn.execute(
                "UPDATE bom SET row_count = ? WHERE bom_id = ?;",
                (int(row_count), bom_id),
            )

    def get_bom(self, bom_id: str) -> Optional[Dict[str, Any]]:
        row = self.conn.execute(
            "SELECT * FROM bom WHERE bom_id = ?;",
            (bom_id,),
        ).fetchone()
        return dict(row) if row else None

    def upsert_bom_rows(self, bom_id: str, rows: Iterable[Dict[str, Any]]) -> List[str]:
        """
        Batch helper. Keeps DecisionController stable.
    
        If you already have upsert_bom_row(...), we loop and call it.
        Otherwise we do direct inserts.
        Returns bom_row_ids in the same order as input.
        """
        out_ids: List[str] = []
        with self.conn:
            for i, row in enumerate(rows):
                row_index = int(row.get("row_index", i))
                partnum = str(row.get("partnum", "") or "")
                mfgpn = str(row.get("mfgpn", "") or "")
                mfgname = str(row.get("mfgname", "") or "")
                supplier = str(row.get("supplier", "") or "")
                description = str(row.get("description", "") or "")
                raw_fields = row.get("raw_fields", {}) or {}
                parsed = row.get("parsed", {}) or {}
    
                # If your repo has upsert_bom_row, use it
                if hasattr(self, "upsert_bom_row"):
                    bom_row_id = self.upsert_bom_row(
                        bom_id,
                        row_index=row_index,
                        partnum=partnum,
                        mfgpn=mfgpn,
                        mfgname=mfgname,
                        supplier=supplier,
                        description=description,
                        raw_fields=raw_fields,
                        parsed=parsed,
                    )
                    out_ids.append(bom_row_id)
                    continue
                
                # Otherwise, insert new (your schema requires these columns)
                bom_row_id = new_id("BR")
                out_ids.append(bom_row_id)
    
                self.conn.execute(
                    """
                    INSERT INTO bom_row (
                        bom_row_id, bom_id, row_index,
                        partnum, mfgpn, mfgname, supplier, description,
                        raw_fields_json, parsed_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                    """,
                    (
                        bom_row_id, bom_id, row_index,
                        partnum, mfgpn, mfgname, supplier, description,
                        json_dumps(raw_fields),
                        json_dumps(parsed),
                    ),
                )
    
            # Update BOM row_count if that column exists
            try:
                self.conn.execute(
                    "UPDATE bom SET row_count = (SELECT COUNT(*) FROM bom_row WHERE bom_id = ?) WHERE bom_id = ?;",
                    (bom_id, bom_id),
                )
            except Exception:
                pass
            
        return out_ids
    

    def load_bom_rows(self, bom_id: str) -> List[Dict[str, Any]]:
        rows = self.conn.execute(
            """
            SELECT *
            FROM bom_row
            WHERE bom_id = ?
            ORDER BY row_index ASC;
            """,
            (bom_id,),
        ).fetchall()
        return [dict(r) for r in rows]


class InventoryRepo:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def create_snapshot(self, *, source_path: Optional[str], file_bytes: bytes, row_count: int, notes: str = "") -> str:
        snap_id = new_id("INV")
        now = _now_iso()
        h = sha256_bytes(file_bytes)
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO inventory_snapshot (inventory_snapshot_id, source_path, source_hash, loaded_at, row_count, notes)
                VALUES (?, ?, ?, ?, ?, ?);
                """,
                (snap_id, source_path, h, now, int(row_count), notes),
            )
        return snap_id

    def upsert_resolved(
        self,
        inventory_snapshot_id: str,
        *,
        itemnum: str,
        vendoritem: str = "",
        mfgpn: str = "",
        mfgname: str = "",
        description: str = "",
        raw_fields: Optional[Dict[str, Any]] = None,
        parsed: Optional[Dict[str, Any]] = None,
    ) -> None:
        raw_fields = raw_fields or {}
        parsed = parsed or {}
        rid = new_id("IR")

        with _DB_LOCK:
            with self.conn:
                cur = self.conn.execute(
                    """
                    SELECT inventory_resolved_id
                    FROM inventory_resolved
                    WHERE inventory_snapshot_id = ? AND itemnum = ?;
                    """,
                    (inventory_snapshot_id, itemnum),
                ).fetchone()

                if cur:
                    self.conn.execute(
                        """
                        UPDATE inventory_resolved
                        SET vendoritem=?, mfgpn=?, mfgname=?, description=?,
                            raw_fields_json=?, parsed_json=?
                        WHERE inventory_snapshot_id=? AND itemnum=?;
                        """,
                        (
                            vendoritem, mfgpn, mfgname, description,
                            json_dumps(raw_fields), json_dumps(parsed),
                            inventory_snapshot_id, itemnum,
                        ),
                    )
                else:
                    self.conn.execute(
                        """
                        INSERT INTO inventory_resolved (
                            inventory_resolved_id, inventory_snapshot_id,
                            itemnum, vendoritem, mfgpn, mfgname, description,
                            raw_fields_json, parsed_json
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
                        """,
                        (
                            rid, inventory_snapshot_id,
                            itemnum, vendoritem, mfgpn, mfgname, description,
                            json_dumps(raw_fields), json_dumps(parsed),
                        ),
                    )

class DecisionRepo:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn

    def upsert_node(self, workspace_id: str, node: DecisionNode, *, bom_row_id: Optional[str] = None) -> None:
        now = _now_iso()
        explain = node.explain or {}
        status_db = _status_to_db(node.status)

        with _DB_LOCK, self.conn:
            existing = self.conn.execute(
                "SELECT node_id FROM decision_node WHERE node_id = ?;",
                (node.id,),
            ).fetchone()

            if existing:
                self.conn.execute(
                    """
                    UPDATE decision_node
                    SET workspace_id=?,
                        bom_row_id=COALESCE(?, bom_row_id),

                        base_type=?,
                        bom_uid=?,
                        bom_mpn=?,
                        description=?,

                        internal_part_number=?,
                        inventory_mpn=?,

                        match_type=?,
                        confidence=?,

                        status=?,
                        locked=?,
                        needs_approval=?,

                        notes=?,
                        explain_json=?,
                        updated_at=?
                    WHERE node_id=?;
                    """,
                    (
                        workspace_id,
                        bom_row_id,

                        node.base_type,
                        node.bom_uid,
                        node.bom_mpn,
                        node.description,

                        node.internal_part_number,
                        node.inventory_mpn,

                        node.match_type,
                        float(node.confidence or 0.0),

                        status_db,
                        int(node.locked),
                        int(node.needs_approval),

                        node.notes or "",
                        json_dumps(explain),
                        now,
                        node.id,
                    ),
                )
            else:
                self.conn.execute(
                    """
                    INSERT INTO decision_node (
                        node_id, workspace_id, bom_row_id,

                        base_type,
                        bom_uid, bom_mpn, description,

                        internal_part_number, inventory_mpn,

                        match_type, confidence,

                        status, locked, needs_approval,

                        notes, explain_json,
                        created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                    """,
                    (
                        node.id, workspace_id, bom_row_id,

                        node.base_type,
                        node.bom_uid,
                        node.bom_mpn,
                        node.description,

                        node.internal_part_number,
                        node.inventory_mpn,

                        node.match_type,
                        float(node.confidence or 0.0),

                        status_db,
                        int(node.locked),
                        int(node.needs_approval),

                        node.notes or "",
                        json_dumps(explain),
                        now, now,
                    ),
                )


    def load_nodes(self, workspace_id: str) -> List[DecisionNode]:
        rows = self.conn.execute(
            """
            SELECT *
            FROM decision_node
            WHERE workspace_id = ?
            ORDER BY created_at ASC;
            """,
            (workspace_id,),
        ).fetchall()

        out: List[DecisionNode] = []
        for r in rows:
            explain = json_loads(r["explain_json"])

            node = DecisionNode(
                id=r["node_id"],                       # <-- FIX
                base_type=r["base_type"],
                bom_uid=r["bom_uid"],
                bom_mpn=r["bom_mpn"],
                description=r["description"],
                internal_part_number=r["internal_part_number"],
                inventory_mpn=r["inventory_mpn"],
                match_type=r["match_type"],
                confidence=float(r["confidence"] or 0.0),
                status=_status_from_db(r["status"]),   # <-- normalize
                locked=bool(int(r["locked"] or 0)),
                needs_approval=bool(int(r["needs_approval"] or 0)),
                notes=r["notes"] or "",
                explain=explain if isinstance(explain, dict) else {},
            )
            out.append(node)
        return out

    def replace_alternates(self, node_id: str, alternates: List[Alternate]) -> None:
        with self.conn:
            self.conn.execute("DELETE FROM alternate WHERE node_id = ?;", (node_id,))
            for rank, a in enumerate(alternates):
                self.conn.execute(
                    """
                    INSERT INTO alternate (
                        alt_id, node_id, rank,
                        source,
                        manufacturer, manufacturer_part_number, internal_part_number,
                        description,
                        value, package, tolerance, voltage, wattage,
                        stock, unit_cost, supplier,
                        confidence, relationship, matched_mpn,
                        selected, rejected,
                        raw_ref_json, meta_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                    """,
                    (
                        a.id,
                        node_id,
                        int(rank),
                        a.source,
                        a.manufacturer,
                        a.manufacturer_part_number,
                        a.internal_part_number,
                        a.description,
                        a.value,
                        a.package,
                        a.tolerance,
                        a.voltage,
                        a.wattage,
                        int(a.stock or 0),
                        a.unit_cost,
                        a.supplier,
                        float(a.confidence or 0.0),
                        a.relationship,
                        a.matched_mpn,
                        1 if a.selected else 0,
                        1 if a.rejected else 0,
                        json_dumps({}),  # reserved
                        json_dumps(a.meta or {}),
                    ),
                )

    def load_alternates(self, node_id: str) -> List[Alternate]:
        rows = self.conn.execute(
            "SELECT * FROM alternate WHERE node_id = ? ORDER BY rank ASC;",
            (node_id,),
        ).fetchall()

        out: List[Alternate] = []
        for r in rows:
            out.append(
                Alternate(
                    id=r["alt_id"],
                    source=r["source"],
                    manufacturer=r["manufacturer"] or "",
                    manufacturer_part_number=r["manufacturer_part_number"] or "",
                    internal_part_number=r["internal_part_number"] or "",
                    description=r["description"] or "",
                    value=r["value"] or "",
                    package=r["package"] or "",
                    tolerance=r["tolerance"] or "",
                    voltage=r["voltage"] or "",
                    wattage=r["wattage"] or "",
                    stock=int(r["stock"] or 0),
                    unit_cost=r["unit_cost"],
                    supplier=r["supplier"] or "",
                    confidence=float(r["confidence"] or 0.0),
                    relationship=r["relationship"] or "",
                    matched_mpn=r["matched_mpn"] or "",
                    selected=bool(r["selected"]),
                    rejected=bool(r["rejected"]),
                    raw=None,
                    meta=json_loads(r["meta_json"] or "{}"),
                )
            )
        return out
