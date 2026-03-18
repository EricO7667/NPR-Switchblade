# db.py
from __future__ import annotations

import os
import sqlite3
from pathlib import Path
from typing import Optional
from dataclasses import dataclass, field
from platformdirs import user_data_dir

import os
import sys
from dataclasses import dataclass, field
from pathlib import Path

APP_NAME = "NPRTool"


def app_data_dir() -> Path:
    home = Path.home()

    if sys.platform.startswith("win"):
        base = Path(os.environ.get("LOCALAPPDATA", home / "AppData" / "Local"))
    elif sys.platform == "darwin":
        base = home / "Library" / "Application Support"
    else:
        base = Path(os.environ.get("XDG_DATA_HOME", home / ".local" / "share"))

    path = base / APP_NAME
    path.mkdir(parents=True, exist_ok=True)
    return path


def default_db_path() -> str:
    override = os.environ.get("NPR_DB_PATH")
    if override:
        p = Path(override)
        p.parent.mkdir(parents=True, exist_ok=True)
        return str(p)

    return str(app_data_dir() / "npr.db")


@dataclass(frozen=True)
class DBConfig:
    path: str = field(default_factory=default_db_path)
    timeout_s: float = 30.0

def connect_db(cfg: Optional[DBConfig] = None) -> sqlite3.Connection:
    cfg = cfg or DBConfig()

    # check_same_thread=False lets us use the same connection across worker threads.
    # serialize DB access at a higher layer (Store/Repo lock).
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
    """Initialize the database schema (single-pass, no versioning).

    This project is currently in active testing. We intentionally avoid schema
    version tracking/migrations and simply ensure the full schema exists on startup.
    Safe to call on every startup.
    """

    # Core tables
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS workspace (
            workspace_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'ACTIVE',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,

            label TEXT DEFAULT '',
            bom_source_path TEXT DEFAULT '',
            inventory_master_path TEXT DEFAULT '',
            inventory_erp_path TEXT DEFAULT '',
            cns_path TEXT DEFAULT '',
            notes TEXT DEFAULT ''
        );
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS ix_workspace_status ON workspace(status);")

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS bom_line_input (
            workspace_id TEXT NOT NULL,
            input_line_id INTEGER NOT NULL,
            imported_at TEXT NOT NULL,
            raw_json TEXT NOT NULL DEFAULT '{}',

            partnum TEXT DEFAULT '',
            description TEXT DEFAULT '',
            qty REAL NOT NULL DEFAULT 0,
            refdes TEXT DEFAULT '',
            item_type TEXT DEFAULT '',
            mfgname TEXT DEFAULT '',
            mfgpn TEXT DEFAULT '',
            supplier TEXT DEFAULT '',

            PRIMARY KEY (workspace_id, input_line_id),
            FOREIGN KEY (workspace_id) REFERENCES workspace(workspace_id) ON DELETE CASCADE
        );
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS ix_bom_input_ws ON bom_line_input(workspace_id);")

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS bom_line_state (
            workspace_id TEXT NOT NULL,
            line_id INTEGER NOT NULL,
            updated_at TEXT NOT NULL,

            cpn TEXT DEFAULT '',
            selected_mpn TEXT DEFAULT '',
            selected_mfg TEXT DEFAULT '',

            confidence REAL NOT NULL DEFAULT 0.0,
            match_type TEXT DEFAULT '',

            needs_new_cpn INTEGER NOT NULL DEFAULT 0,
            locked INTEGER NOT NULL DEFAULT 0,
            needs_approval INTEGER NOT NULL DEFAULT 0,

            notes TEXT DEFAULT '',
            explain_json TEXT NOT NULL DEFAULT '{}',

            PRIMARY KEY (workspace_id, line_id),
            FOREIGN KEY (workspace_id) REFERENCES workspace(workspace_id) ON DELETE CASCADE
        );
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS ix_bom_state_ws ON bom_line_state(workspace_id);")

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS inventory_company (
            workspace_id TEXT NOT NULL,
            cpn TEXT NOT NULL,

            canonical_desc TEXT DEFAULT '',
            stock_total INTEGER NOT NULL DEFAULT 0,
            alternates_json TEXT NOT NULL DEFAULT '[]',
            imported_at TEXT NOT NULL,

            PRIMARY KEY (workspace_id, cpn),
            FOREIGN KEY (workspace_id) REFERENCES workspace(workspace_id) ON DELETE CASCADE
        );
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS ix_inventory_company_ws ON inventory_company(workspace_id);")
    conn.execute("CREATE INDEX IF NOT EXISTS ix_inventory_company_cpn ON inventory_company(workspace_id, cpn);")

    # Normalized CPN->MPN view for easy UI/querying
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS inventory_company_item (
            workspace_id TEXT NOT NULL,
            cpn TEXT NOT NULL,

            mfgname TEXT DEFAULT '',
            mfgid TEXT DEFAULT '',
            mpn TEXT NOT NULL DEFAULT '',

            unit_price REAL,
            last_unit_price REAL,
            standard_cost REAL,
            average_cost REAL,

            tariff_code TEXT DEFAULT '',
            tariff_rate REAL,
            supplier TEXT DEFAULT '',
            lead_time_days INTEGER,

            meta_json TEXT NOT NULL DEFAULT '{}',
            imported_at TEXT NOT NULL,

            PRIMARY KEY (workspace_id, cpn, mfgname, mpn),
            FOREIGN KEY (workspace_id, cpn) REFERENCES inventory_company(workspace_id, cpn) ON DELETE CASCADE
        );
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS ix_inv_item_cpn ON inventory_company_item(workspace_id, cpn);")
    conn.execute("CREATE INDEX IF NOT EXISTS ix_inv_item_mpn ON inventory_company_item(workspace_id, mpn);")

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS export_log (
            export_id TEXT PRIMARY KEY,
            workspace_id TEXT NOT NULL,
            created_at TEXT NOT NULL,

            export_type TEXT NOT NULL DEFAULT '',
            path TEXT NOT NULL DEFAULT '',
            meta_json TEXT NOT NULL DEFAULT '{}',

            FOREIGN KEY (workspace_id) REFERENCES workspace(workspace_id) ON DELETE CASCADE
        );
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS ix_export_ws ON export_log(workspace_id, created_at DESC);")

    # Match persistence (UI renders from this)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS match_run (
            run_id TEXT PRIMARY KEY,
            workspace_id TEXT NOT NULL,
            created_at TEXT NOT NULL,

            engine_name TEXT NOT NULL DEFAULT 'MatchingEngine',
            engine_version TEXT NOT NULL DEFAULT '',
            config_json TEXT NOT NULL DEFAULT '{}',
            summary_json TEXT NOT NULL DEFAULT '{}',

            FOREIGN KEY (workspace_id) REFERENCES workspace(workspace_id) ON DELETE CASCADE
        );
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS ix_match_run_ws ON match_run(workspace_id, created_at DESC);")

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS match_node (
            workspace_id TEXT NOT NULL,
            run_id TEXT NOT NULL,
            node_id TEXT NOT NULL,

            line_id INTEGER NOT NULL,

            base_type TEXT NOT NULL DEFAULT '',
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

            PRIMARY KEY (workspace_id, run_id, node_id),
            FOREIGN KEY (workspace_id) REFERENCES workspace(workspace_id) ON DELETE CASCADE,
            FOREIGN KEY (run_id) REFERENCES match_run(run_id) ON DELETE CASCADE
        );
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS ix_match_node_ws_run ON match_node(workspace_id, run_id);")
    conn.execute("CREATE INDEX IF NOT EXISTS ix_match_node_line ON match_node(workspace_id, line_id);")

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS match_alt (
            workspace_id TEXT NOT NULL,
            run_id TEXT NOT NULL,
            node_id TEXT NOT NULL,
            alt_id TEXT NOT NULL,

            source TEXT NOT NULL DEFAULT 'inventory',

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

            meta_json TEXT NOT NULL DEFAULT '{}',
            raw_json  TEXT NOT NULL DEFAULT '{}',

            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,

            PRIMARY KEY (workspace_id, run_id, alt_id),
            FOREIGN KEY (workspace_id, run_id, node_id) REFERENCES match_node(workspace_id, run_id, node_id) ON DELETE CASCADE,
            FOREIGN KEY (run_id) REFERENCES match_run(run_id) ON DELETE CASCADE
        );
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS ix_match_alt_node ON match_alt(workspace_id, run_id, node_id);")
    conn.execute("CREATE INDEX IF NOT EXISTS ix_match_alt_selected ON match_alt(workspace_id, run_id, node_id, selected, rejected);")


    # Incremental SPLADE doc cache (row-addressable, model/config scoped)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS splade_doc_cache (
            model_name TEXT NOT NULL,
            preprocess_version INTEGER NOT NULL,
            max_len INTEGER NOT NULL,
            top_terms INTEGER NOT NULL,
            row_key TEXT NOT NULL,
            row_hash TEXT NOT NULL,
            term_ids_blob BLOB NOT NULL,
            term_wts_blob BLOB NOT NULL,
            doc_norm REAL NOT NULL DEFAULT 1.0,
            updated_at TEXT NOT NULL,

            PRIMARY KEY (model_name, preprocess_version, max_len, top_terms, row_key)
        );
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS ix_splade_cache_cfg ON splade_doc_cache(model_name, preprocess_version, max_len, top_terms);")
    conn.execute("CREATE INDEX IF NOT EXISTS ix_splade_cache_hash ON splade_doc_cache(model_name, preprocess_version, max_len, top_terms, row_hash);")


