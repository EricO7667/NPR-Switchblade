# db.py
from __future__ import annotations

import os
import sqlite3
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

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
    conn = sqlite3.connect(
        cfg.path,
        timeout=cfg.timeout_s,
        check_same_thread=False,
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA synchronous = NORMAL;")
    conn.execute("PRAGMA temp_store = MEMORY;")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    """Initialize the active schema for the current testing phase."""

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


    conn.execute("DROP TABLE IF EXISTS inventory_company_item;")
    conn.execute("DROP TABLE IF EXISTS inventory_company;")

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS erp_inventory_raw (
            workspace_id TEXT NOT NULL,
            source_row_key TEXT NOT NULL,
            item_number TEXT NOT NULL,
            description TEXT DEFAULT '',
            primary_vendor_number TEXT DEFAULT '',
            vendor_item TEXT DEFAULT '',
            manufacturer_id TEXT DEFAULT '',
            manufacturer_name TEXT DEFAULT '',
            manufacturer_item_count REAL,
            last_cost REAL,
            standard_cost REAL,
            average_cost REAL,
            revision TEXT DEFAULT '',
            item_lead_time REAL,
            default_whse TEXT DEFAULT '',
            total_qty REAL,
            raw_json TEXT NOT NULL DEFAULT '{}',
            imported_at TEXT NOT NULL,
            PRIMARY KEY (workspace_id, source_row_key),
            FOREIGN KEY (workspace_id) REFERENCES workspace(workspace_id) ON DELETE CASCADE
        );
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS ix_erp_inventory_raw_ws ON erp_inventory_raw(workspace_id);")
    conn.execute("CREATE INDEX IF NOT EXISTS ix_erp_inventory_raw_item ON erp_inventory_raw(workspace_id, item_number);")
    conn.execute("CREATE INDEX IF NOT EXISTS ix_erp_inventory_raw_vendor ON erp_inventory_raw(workspace_id, vendor_item);")

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS alternate_master_raw (
            workspace_id TEXT NOT NULL,
            source_row_key TEXT NOT NULL,
            item_number TEXT NOT NULL,
            description TEXT DEFAULT '',
            active TEXT DEFAULT '',
            manufacturer_id TEXT DEFAULT '',
            manufacturer_name TEXT DEFAULT '',
            manufacturer_part_number TEXT DEFAULT '',
            tariff_code TEXT DEFAULT '',
            tariff_rate REAL,
            last_cost REAL,
            standard_cost REAL,
            average_cost REAL,
            raw_json TEXT NOT NULL DEFAULT '{}',
            imported_at TEXT NOT NULL,
            PRIMARY KEY (workspace_id, source_row_key),
            FOREIGN KEY (workspace_id) REFERENCES workspace(workspace_id) ON DELETE CASCADE
        );
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS ix_alt_master_raw_ws ON alternate_master_raw(workspace_id);")
    conn.execute("CREATE INDEX IF NOT EXISTS ix_alt_master_raw_item ON alternate_master_raw(workspace_id, item_number);")
    conn.execute("CREATE INDEX IF NOT EXISTS ix_alt_master_raw_mpn ON alternate_master_raw(workspace_id, manufacturer_part_number);")

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS company_part (
            company_part_id INTEGER PRIMARY KEY AUTOINCREMENT,
            workspace_id TEXT NOT NULL,
            company_part_number TEXT NOT NULL,
            description TEXT DEFAULT '',
            default_whse TEXT DEFAULT '',
            total_qty REAL,
            revision TEXT DEFAULT '',
            primary_vendor_number TEXT DEFAULT '',
            raw_json TEXT NOT NULL DEFAULT '{}',
            imported_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE (workspace_id, company_part_number),
            FOREIGN KEY (workspace_id) REFERENCES workspace(workspace_id) ON DELETE CASCADE
        );
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS ix_company_part_ws ON company_part(workspace_id);")
    conn.execute("CREATE INDEX IF NOT EXISTS ix_company_part_cpn ON company_part(workspace_id, company_part_number);")

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS manufacturer_part (
            manufacturer_part_id INTEGER PRIMARY KEY AUTOINCREMENT,
            workspace_id TEXT NOT NULL,
            company_part_id INTEGER NOT NULL,
            manufacturer_part_number TEXT DEFAULT '',
            manufacturer_id TEXT DEFAULT '',
            manufacturer_name TEXT DEFAULT '',
            description TEXT DEFAULT '',
            active TEXT DEFAULT '',
            item_lead_time REAL,
            tariff_code TEXT DEFAULT '',
            tariff_rate REAL,
            last_cost REAL,
            standard_cost REAL,
            average_cost REAL,
            is_erp_primary INTEGER NOT NULL DEFAULT 0,
            erp_source_row_key TEXT DEFAULT '',
            master_source_row_key TEXT DEFAULT '',
            raw_json TEXT NOT NULL DEFAULT '{}',
            imported_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE (workspace_id, company_part_id, manufacturer_part_number, manufacturer_id),
            FOREIGN KEY (workspace_id) REFERENCES workspace(workspace_id) ON DELETE CASCADE,
            FOREIGN KEY (company_part_id) REFERENCES company_part(company_part_id) ON DELETE CASCADE
        );
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS ix_manufacturer_part_ws ON manufacturer_part(workspace_id);")
    conn.execute("CREATE INDEX IF NOT EXISTS ix_manufacturer_part_company ON manufacturer_part(workspace_id, company_part_id);")
    conn.execute("CREATE INDEX IF NOT EXISTS ix_manufacturer_part_mpn ON manufacturer_part(workspace_id, manufacturer_part_number);")

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS decision_node (
            workspace_id TEXT NOT NULL,
            node_id TEXT NOT NULL,
            line_id INTEGER NOT NULL,

            base_type TEXT NOT NULL DEFAULT '',
            bom_uid TEXT DEFAULT '',
            bom_mpn TEXT DEFAULT '',
            description TEXT DEFAULT '',

            internal_part_number TEXT DEFAULT '',
            assigned_part_number TEXT DEFAULT '',
            inventory_mpn TEXT DEFAULT '',
            preferred_inventory_mfgpn TEXT DEFAULT '',
            bom_section TEXT DEFAULT 'SURFACE MOUNT',

            match_type TEXT DEFAULT '',
            confidence REAL NOT NULL DEFAULT 0.0,

            status TEXT NOT NULL DEFAULT 'NEEDS_DECISION',
            locked INTEGER NOT NULL DEFAULT 0,
            needs_approval INTEGER NOT NULL DEFAULT 0,

            focused_alt_id TEXT DEFAULT '',
            exclude_customer_part_number_in_npr INTEGER NOT NULL DEFAULT 0,
            notes TEXT DEFAULT '',
            explain_json TEXT NOT NULL DEFAULT '{}',

            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,

            PRIMARY KEY (workspace_id, node_id),
            FOREIGN KEY (workspace_id) REFERENCES workspace(workspace_id) ON DELETE CASCADE
        );
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS ix_decision_node_ws ON decision_node(workspace_id, line_id);")
    conn.execute("CREATE INDEX IF NOT EXISTS ix_decision_node_status ON decision_node(workspace_id, status);")

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS decision_alt (
            workspace_id TEXT NOT NULL,
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
            raw_json TEXT NOT NULL DEFAULT '{}',

            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,

            PRIMARY KEY (workspace_id, node_id, alt_id),
            FOREIGN KEY (workspace_id, node_id) REFERENCES decision_node(workspace_id, node_id) ON DELETE CASCADE
        );
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS ix_decision_alt_node ON decision_alt(workspace_id, node_id);")
    conn.execute("CREATE INDEX IF NOT EXISTS ix_decision_alt_state ON decision_alt(workspace_id, node_id, selected, rejected);")

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
    conn.execute("CREATE INDEX IF NOT EXISTS ix_export_log_ws ON export_log(workspace_id, created_at);")

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
    conn.execute("CREATE INDEX IF NOT EXISTS ix_match_run_ws ON match_run(workspace_id, created_at);")

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS match_node (
            workspace_id TEXT NOT NULL,
            run_id TEXT NOT NULL,
            node_id TEXT NOT NULL,
            line_id INTEGER NOT NULL DEFAULT 0,
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
    conn.execute("CREATE INDEX IF NOT EXISTS ix_match_node_ws_run ON match_node(workspace_id, run_id, line_id);")

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
            raw_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (workspace_id, run_id, node_id, alt_id),
            FOREIGN KEY (workspace_id, run_id, node_id) REFERENCES match_node(workspace_id, run_id, node_id) ON DELETE CASCADE
        );
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS ix_match_alt_node ON match_alt(workspace_id, run_id, node_id);")

