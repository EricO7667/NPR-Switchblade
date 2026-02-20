# db.py
from __future__ import annotations

import os
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


SCHEMA_VERSION = 1


def default_db_path() -> str:
    base_dir = Path(os.path.dirname(os.path.abspath(__file__)))
    db_dir = base_dir / "data"
    db_dir.mkdir(parents=True, exist_ok=True)
    return str(db_dir / "npr.db")


@dataclass(frozen=True)
class DBConfig:
    path: str = default_db_path()
    timeout_s: float = 30.0


def connect_db(cfg: Optional[DBConfig] = None) -> sqlite3.Connection:
    cfg = cfg or DBConfig()

    # IMPORTANT:
    # - check_same_thread=False lets us use the same connection across worker threads
    # - we will still SERIALIZE access with a lock in repositories.py
    conn = sqlite3.connect(
        cfg.path,
        timeout=cfg.timeout_s,
        check_same_thread=False,
    )
    conn.row_factory = sqlite3.Row

    # Pragmas (safe defaults for a local desktop app)
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA synchronous = NORMAL;")
    conn.execute("PRAGMA temp_store = MEMORY;")

    return conn


def init_db(conn: sqlite3.Connection) -> None:
    """
    Creates meta tables and migrates schema to SCHEMA_VERSION.
    Safe to call on every startup.
    """
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_meta (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            schema_version INTEGER NOT NULL
        );
        """
    )

    row = conn.execute("SELECT schema_version FROM schema_meta WHERE id = 1;").fetchone()
    if row is None:
        conn.execute("INSERT INTO schema_meta (id, schema_version) VALUES (1, 0);")
        current = 0
    else:
        current = int(row["schema_version"])

    if current > SCHEMA_VERSION:
        raise RuntimeError(
            f"DB schema_version={current} is newer than app supports ({SCHEMA_VERSION})."
        )

    # Apply migrations in order
    with conn:
        while current < SCHEMA_VERSION:
            next_v = current + 1
            _apply_migration(conn, next_v)
            conn.execute("UPDATE schema_meta SET schema_version = ? WHERE id = 1;", (next_v,))
            current = next_v


def _apply_migration(conn: sqlite3.Connection, version: int) -> None:
    if version == 1:
        _migration_v1(conn)
        return
    raise RuntimeError(f"Unknown migration version: {version}")


def _migration_v1(conn: sqlite3.Connection) -> None:
    # Workspace table: one “job” that can be reopened
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS workspace (
            workspace_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'ACTIVE', -- ACTIVE | ARCHIVED | EXPORTED
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,

            bom_id TEXT,
            inventory_snapshot_id TEXT,

            -- config hashes/versions to support reproducibility later
            parse_config_hash TEXT DEFAULT '',
            match_config_hash TEXT DEFAULT '',

            FOREIGN KEY (bom_id) REFERENCES bom(bom_id) ON DELETE SET NULL,
            FOREIGN KEY (inventory_snapshot_id) REFERENCES inventory_snapshot(inventory_snapshot_id) ON DELETE SET NULL
        );
        """
    )

    # BOM import artifact
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS bom (
            bom_id TEXT PRIMARY KEY,
            source_path TEXT,
            source_hash TEXT NOT NULL,
            imported_at TEXT NOT NULL,
            row_count INTEGER NOT NULL DEFAULT 0
        );
        """
    )

    # BOM rows (persist your NPRPart-ish row data)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS bom_row (
            bom_row_id TEXT PRIMARY KEY,
            bom_id TEXT NOT NULL,
            row_index INTEGER NOT NULL,

            -- canonical fields used commonly
            partnum TEXT DEFAULT '',
            mfgpn TEXT DEFAULT '',
            mfgname TEXT DEFAULT '',
            supplier TEXT DEFAULT '',
            description TEXT DEFAULT '',

            raw_fields_json TEXT NOT NULL DEFAULT '{}',
            parsed_json TEXT NOT NULL DEFAULT '{}',

            UNIQUE(bom_id, row_index),
            FOREIGN KEY (bom_id) REFERENCES bom(bom_id) ON DELETE CASCADE
        );
        """
    )

    # Inventory snapshot metadata
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS inventory_snapshot (
            inventory_snapshot_id TEXT PRIMARY KEY,
            source_path TEXT,
            source_hash TEXT NOT NULL,
            loaded_at TEXT NOT NULL,
            row_count INTEGER NOT NULL DEFAULT 0,
            notes TEXT DEFAULT ''
        );
        """
    )

    # Only persist “touched” inventory records that decisions reference
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS inventory_resolved (
            inventory_resolved_id TEXT PRIMARY KEY,
            inventory_snapshot_id TEXT NOT NULL,

            -- canonical lookup keys
            itemnum TEXT NOT NULL,
            vendoritem TEXT DEFAULT '',
            mfgpn TEXT DEFAULT '',
            mfgname TEXT DEFAULT '',
            description TEXT DEFAULT '',

            raw_fields_json TEXT NOT NULL DEFAULT '{}',
            parsed_json TEXT NOT NULL DEFAULT '{}',

            UNIQUE(inventory_snapshot_id, itemnum),
            FOREIGN KEY (inventory_snapshot_id) REFERENCES inventory_snapshot(inventory_snapshot_id) ON DELETE CASCADE
        );
        """
    )

    # Decision nodes: your UI unit (DecisionNode)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS decision_node (
            node_id TEXT PRIMARY KEY,
            workspace_id TEXT NOT NULL,

            -- Link back to exact BOM row (hard connection BOM ↔ NPR decisions)
            bom_row_id TEXT,

            base_type TEXT NOT NULL, -- NEW | EXISTS

            bom_uid TEXT DEFAULT '',
            bom_mpn TEXT DEFAULT '',
            description TEXT DEFAULT '',

            internal_part_number TEXT DEFAULT '',
            inventory_mpn TEXT DEFAULT '',

            match_type TEXT DEFAULT '',
            confidence REAL NOT NULL DEFAULT 0.0,

            status TEXT NOT NULL DEFAULT 'NEEDS_DECISION',
            locked INTEGER NOT NULL DEFAULT 0,
            needs_approval INTEGER NOT NULL DEFAULT 0,

            notes TEXT DEFAULT '',
            explain_json TEXT NOT NULL DEFAULT '{}',

            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,

            FOREIGN KEY (workspace_id) REFERENCES workspace(workspace_id) ON DELETE CASCADE,
            FOREIGN KEY (bom_row_id) REFERENCES bom_row(bom_row_id) ON DELETE SET NULL
        );
        """
    )

    # Alternates (Alternate)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS alternate (
            alt_id TEXT PRIMARY KEY,
            node_id TEXT NOT NULL,
            rank INTEGER NOT NULL DEFAULT 0,

            source TEXT NOT NULL, -- inventory|digikey|manual|api

            manufacturer TEXT DEFAULT '',
            manufacturer_part_number TEXT DEFAULT '',
            internal_part_number TEXT DEFAULT '',

            description TEXT DEFAULT '',

            value TEXT DEFAULT '',
            package TEXT DEFAULT '',
            tolerance TEXT DEFAULT '',
            voltage TEXT DEFAULT '',
            wattage TEXT DEFAULT '',

            stock INTEGER NOT NULL DEFAULT 0,
            unit_cost REAL,
            supplier TEXT DEFAULT '',

            confidence REAL NOT NULL DEFAULT 0.0,
            relationship TEXT DEFAULT '',
            matched_mpn TEXT DEFAULT '',

            selected INTEGER NOT NULL DEFAULT 0,
            rejected INTEGER NOT NULL DEFAULT 0,

            raw_ref_json TEXT NOT NULL DEFAULT '{}',
            meta_json TEXT NOT NULL DEFAULT '{}',

            FOREIGN KEY (node_id) REFERENCES decision_node(node_id) ON DELETE CASCADE
        );
        """
    )

    # Indexes that matter immediately
    conn.execute("CREATE INDEX IF NOT EXISTS ix_workspace_status ON workspace(status);")
    conn.execute("CREATE INDEX IF NOT EXISTS ix_bom_row_bom ON bom_row(bom_id);")
    conn.execute("CREATE INDEX IF NOT EXISTS ix_node_workspace ON decision_node(workspace_id);")
    conn.execute("CREATE INDEX IF NOT EXISTS ix_alt_node ON alternate(node_id);")
    conn.execute("CREATE INDEX IF NOT EXISTS ix_inv_resolved_itemnum ON inventory_resolved(itemnum);")
