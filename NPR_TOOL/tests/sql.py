import hashlib
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from dataclasses import asdict, dataclass, field
from typing import Iterable, Optional
from pprint import pprint

import pandas as pd


# ============================================================================
# STAGE 0: CONFIGURATION AND SCHEMA CONTRACTS
# - File paths
# - Required Excel headers
# - Stable hash/key definitions
# ============================================================================


ERP_EXCEL_PATH = r"C:\PersonalProjects\archives\erp.xlsx"
ERP_SHEET_NAME: Optional[str] = 0

MASTER_EXCEL_PATH = r"C:\PersonalProjects\archives\MASTERE.xlsx"
MASTER_SHEET_NAME: Optional[str] = 0

SQLITE_DB_PATH = r"C:\PersonalProjects\archives\inventory.db"


ERP_HEADERS = [
    "ItemNumber",
    "Description",
    "PrimaryVendorNumber",
    "VendorItem",
    "MfgId",
    "MfgName",
    "MfgItemCount",
    "LastCost",
    "StdCost",
    "AvgCost",
    "Revision",
    "ItemLeadTime",
    "DefaultWhse",
    "TotalQty",
]

MASTER_HEADERS = [
    "Item Number",
    "Description",
    "Active",
    "Mfg ID",
    "Manufacturer Name",
    "Manufacturer PN",
    "Tariff Code",
    "Tariff Rate",
    "Last Cost",
    "Standard Cost",
    "Average Cost",
]

ERP_HASH_COLUMNS = [
    "ItemNumber",
    "Description",
    "PrimaryVendorNumber",
    "VendorItem",
    "MfgId",
    "MfgName",
    "MfgItemCount",
    "LastCost",
    "StdCost",
    "AvgCost",
    "Revision",
    "ItemLeadTime",
    "DefaultWhse",
    "TotalQty",
]

MASTER_HASH_COLUMNS = [
    "Item Number",
    "Description",
    "Active",
    "Mfg ID",
    "Manufacturer Name",
    "Manufacturer PN",
    "Tariff Code",
    "Tariff Rate",
    "Last Cost",
    "Standard Cost",
    "Average Cost",
]

ERP_KEY_COLUMNS = ["ItemNumber", "VendorItem", "DefaultWhse", "Revision", "MfgId"]
MASTER_KEY_COLUMNS = ["Item Number", "Manufacturer PN", "Mfg ID"]

SEP = "\x1f"


@dataclass(slots=True)
class ManufacturerPartRecord:
    """Hydrated manufacturer part model built from the canonical SQLite tables."""
    manufacturer_part_id: int | None = None
    company_part_id: int | None = None
    company_part_number: str | None = None
    manufacturer_part_number: str | None = None
    manufacturer_id: str | None = None
    manufacturer_name: str | None = None
    description: str | None = None
    active: str | None = None
    item_lead_time: float | None = None
    tariff_code: str | None = None
    tariff_rate: float | None = None
    last_cost: float | None = None
    standard_cost: float | None = None
    average_cost: float | None = None
    is_erp_primary: bool = False
    erp_source_row_key: str | None = None
    master_source_row_key: str | None = None
    updated_at: str | None = None
    last_seen_at: str | None = None


@dataclass(slots=True)
class CompanyPartRecord:
    """Hydrated company part model with optional attached manufacturer part children."""
    company_part_id: int | None = None
    company_part_number: str = ""
    description: str | None = None
    default_whse: str | None = None
    total_qty: float | None = None
    revision: str | None = None
    primary_vendor_number: str | None = None
    updated_at: str | None = None
    last_seen_at: str | None = None
    manufacturer_parts: list[ManufacturerPartRecord] = field(default_factory=list)


# ============================================================================
# STAGE 5: READ MODELS / OBJECT HYDRATION
# - At this point the database is already populated
# - Repository methods convert database rows into Python dataclass objects
# ============================================================================


class CompanyPartRepository:
    """Repository that materializes CompanyPart and ManufacturerPart dataclasses from SQLite rows."""
    def __init__(self, conn: sqlite3.Connection):
        """Bind a SQLite connection for object hydration queries."""
        self.conn = conn
        self.conn.row_factory = sqlite3.Row

    @staticmethod
    def _row_to_company_part(row: sqlite3.Row) -> CompanyPartRecord:
        return CompanyPartRecord(
            company_part_id=row["company_part_id"],
            company_part_number=row["company_part_number"],
            description=row["description"],
            default_whse=row["default_whse"],
            total_qty=row["total_qty"],
            revision=row["revision"],
            primary_vendor_number=row["primary_vendor_number"],
            updated_at=row["updated_at"],
            last_seen_at=row["last_seen_at"],
        )

    @staticmethod
    def _row_to_manufacturer_part(row: sqlite3.Row) -> ManufacturerPartRecord:
        return ManufacturerPartRecord(
            manufacturer_part_id=row["manufacturer_part_id"],
            company_part_id=row["company_part_id"],
            company_part_number=row["company_part_number"] if "company_part_number" in row.keys() else None,
            manufacturer_part_number=row["manufacturer_part_number"],
            manufacturer_id=row["manufacturer_id"],
            manufacturer_name=row["manufacturer_name"],
            description=row["description"],
            active=row["active"],
            item_lead_time=row["item_lead_time"],
            tariff_code=row["tariff_code"],
            tariff_rate=row["tariff_rate"],
            last_cost=row["last_cost"],
            standard_cost=row["standard_cost"],
            average_cost=row["average_cost"],
            is_erp_primary=bool(row["is_erp_primary"]),
            erp_source_row_key=row["erp_source_row_key"],
            master_source_row_key=row["master_source_row_key"],
            updated_at=row["updated_at"],
            last_seen_at=row["last_seen_at"],
        )

    def get_company_part(self, company_part_number: str) -> CompanyPartRecord | None:
        """Return one company part bundle by company part number."""
        cur = self.conn.cursor()
        row = cur.execute(
            """
            SELECT *
            FROM CompanyPart
            WHERE company_part_number = ?
            """,
            (company_part_number,),
        ).fetchone()
        if row is None:
            return None
        company_part = self._row_to_company_part(row)
        company_part.manufacturer_parts = self.get_manufacturer_parts_for_company_part_id(company_part.company_part_id)
        return company_part

    def get_company_part_by_id(self, company_part_id: int) -> CompanyPartRecord | None:
        """Return one company part bundle by primary key."""
        cur = self.conn.cursor()
        row = cur.execute(
            """
            SELECT *
            FROM CompanyPart
            WHERE company_part_id = ?
            """,
            (company_part_id,),
        ).fetchone()
        if row is None:
            return None
        company_part = self._row_to_company_part(row)
        company_part.manufacturer_parts = self.get_manufacturer_parts_for_company_part_id(company_part_id)
        return company_part

    def get_manufacturer_parts_for_company_part_id(self, company_part_id: int | None) -> list[ManufacturerPartRecord]:
        """Return child manufacturer parts for a single company part id."""
        if company_part_id is None:
            return []
        cur = self.conn.cursor()
        rows = cur.execute(
            """
            SELECT mp.*, cp.company_part_number
            FROM ManufacturerPart mp
            JOIN CompanyPart cp ON cp.company_part_id = mp.company_part_id
            WHERE mp.company_part_id = ?
            ORDER BY mp.is_erp_primary DESC, mp.manufacturer_name, mp.manufacturer_part_number
            """,
            (company_part_id,),
        ).fetchall()
        return [self._row_to_manufacturer_part(row) for row in rows]

    def get_all_company_parts(self, limit: int = 200, offset: int = 0) -> list[CompanyPartRecord]:
        """Return a paged list of company part bundles."""
        cur = self.conn.cursor()
        rows = cur.execute(
            """
            SELECT *
            FROM CompanyPart
            ORDER BY company_part_number
            LIMIT ? OFFSET ?
            """,
            (limit, offset),
        ).fetchall()
        company_parts = [self._row_to_company_part(row) for row in rows]
        self.attach_manufacturer_parts(company_parts)
        return company_parts

    def search_company_parts(self, text: str, limit: int = 100) -> list[CompanyPartRecord]:
        """Search company parts and manufacturer part text fields with a LIKE query."""
        cur = self.conn.cursor()
        like_text = f"%{text.strip()}%"
        rows = cur.execute(
            """
            SELECT DISTINCT cp.*
            FROM CompanyPart cp
            LEFT JOIN ManufacturerPart mp ON mp.company_part_id = cp.company_part_id
            WHERE cp.company_part_number LIKE ?
               OR cp.description LIKE ?
               OR cp.primary_vendor_number LIKE ?
               OR mp.manufacturer_part_number LIKE ?
               OR mp.manufacturer_name LIKE ?
               OR mp.manufacturer_id LIKE ?
               OR mp.description LIKE ?
            ORDER BY cp.company_part_number
            LIMIT ?
            """,
            (like_text, like_text, like_text, like_text, like_text, like_text, like_text, limit),
        ).fetchall()
        company_parts = [self._row_to_company_part(row) for row in rows]
        self.attach_manufacturer_parts(company_parts)
        return company_parts

    def attach_manufacturer_parts(self, company_parts: Iterable[CompanyPartRecord]):
        """Batch attach manufacturer part children to already-hydrated company parts."""
        company_parts = list(company_parts)
        if not company_parts:
            return

        company_part_ids = [cp.company_part_id for cp in company_parts if cp.company_part_id is not None]
        if not company_part_ids:
            return

        placeholders = ",".join("?" for _ in company_part_ids)
        cur = self.conn.cursor()
        rows = cur.execute(
            f"""
            SELECT mp.*, cp.company_part_number
            FROM ManufacturerPart mp
            JOIN CompanyPart cp ON cp.company_part_id = mp.company_part_id
            WHERE mp.company_part_id IN ({placeholders})
            ORDER BY mp.company_part_id, mp.is_erp_primary DESC, mp.manufacturer_name, mp.manufacturer_part_number
            """,
            tuple(company_part_ids),
        ).fetchall()

        grouped: dict[int, list[ManufacturerPartRecord]] = {}
        for row in rows:
            mp = self._row_to_manufacturer_part(row)
            grouped.setdefault(mp.company_part_id, []).append(mp)

        for cp in company_parts:
            cp.manufacturer_parts = grouped.get(cp.company_part_id, [])


# ============================================================================
# STAGE 6: OUTPUT / DEBUG DISPLAY HELPERS
# ============================================================================


def print_company_part_bundle(company_part: CompanyPartRecord | None):
    """Pretty-print one hydrated company part bundle for inspection."""
    if company_part is None:
        print("CompanyPart not found.")
        return

    pprint(asdict(company_part), sort_dicts=False)




def print_company_part_bundles(company_parts: Iterable[CompanyPartRecord]):
    """Pretty-print multiple hydrated company part bundles."""
    for company_part in company_parts:
        print_company_part_bundle(company_part)
        print()

# ============================================================================
# STAGE 1: EXTRACTION AND NORMALIZATION HELPERS
# - Read Excel into DataFrames
# - Normalize raw cell values
# - Build deterministic row keys and hashes
# ============================================================================


def utc_now_iso() -> str:
    """Return a UTC timestamp string used for sync metadata columns."""
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def sha256_text(text: str) -> str:
    """Return a SHA-256 hex digest for deterministic row identity and change tracking."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def normalize_cell(value):
    """Normalize one DataFrame cell so Excel values become stable Python/SQLite values."""
    if isinstance(value, pd.Timestamp):
        if pd.isna(value):
            return None
        return value.isoformat()
    if pd.isna(value):
        return None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        f = float(value)
        if f.is_integer():
            return int(f)
        return f
    if isinstance(value, str):
        value = value.strip()
        return value if value else None
    return value


def load_excel(path: str, sheet_name=0) -> pd.DataFrame:
    """Read one Excel sheet into a pandas DataFrame without type coercion beyond object dtype."""
    df = pd.read_excel(path, sheet_name=sheet_name, dtype=object)
    df.columns = [str(col).strip() for col in df.columns]
    return df


def validate_headers(df: pd.DataFrame, expected_headers: list[str], table_name: str):
    """Fail fast if the incoming Excel sheet does not match the expected schema contract."""
    missing = [h for h in expected_headers if h not in df.columns]
    if missing:
        raise ValueError(f"{table_name} is missing required headers: {missing}\nFound headers: {list(df.columns)}")


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Apply cell normalization across every column in the DataFrame."""
    df = df.copy()
    for col in df.columns:
        df[col] = df[col].map(normalize_cell)
    return df


def stable_stringify_series(series: pd.Series) -> pd.Series:
    """Convert a Series into stable strings before composite key/hash generation."""
    return series.map(lambda v: "" if v is None else str(v))


def add_key_and_hash(df: pd.DataFrame, key_columns: list[str], hash_columns: list[str]) -> pd.DataFrame:
    """Add deterministic source_row_key and row_hash columns to an incoming DataFrame."""
    df = df.copy()
    # The key captures source identity. The hash captures row content for change detection.
    key_parts = [stable_stringify_series(df[col]) for col in key_columns]
    hash_parts = [stable_stringify_series(df[col]) for col in hash_columns]

    key_base = key_parts[0]
    for part in key_parts[1:]:
        key_base = key_base + SEP + part

    hash_base = hash_parts[0]
    for part in hash_parts[1:]:
        hash_base = hash_base + SEP + part

    df["source_row_key"] = key_base.map(sha256_text)
    df["row_hash"] = hash_base.map(sha256_text)
    return df


def dedupe_incoming(df: pd.DataFrame) -> pd.DataFrame:
    """Drop duplicate incoming rows by source_row_key, keeping the last copy."""
    return df.drop_duplicates(subset=["source_row_key"], keep="last").reset_index(drop=True)


ERP_STAGE_COLUMNS = [
    "ItemNumber",
    "Description",
    "PrimaryVendorNumber",
    "VendorItem",
    "MfgId",
    "MfgName",
    "MfgItemCount",
    "LastCost",
    "StdCost",
    "AvgCost",
    "Revision",
    "ItemLeadTime",
    "DefaultWhse",
    "TotalQty",
    "source_row_key",
    "row_hash",
]

MASTER_STAGE_COLUMNS = [
    "Item Number",
    "Description",
    "Active",
    "Mfg ID",
    "Manufacturer Name",
    "Manufacturer PN",
    "Tariff Code",
    "Tariff Rate",
    "Last Cost",
    "Standard Cost",
    "Average Cost",
    "source_row_key",
    "row_hash",
]


# ============================================================================
# STAGE 2: DATABASE DDL AND STAGING LOADS
# - Create canonical tables
# - Load cleaned DataFrames into staging tables
# ============================================================================


def create_tables(conn: sqlite3.Connection):
    """Create or ensure the raw, derived, and indexed SQLite tables used by the pipeline."""
    cur = conn.cursor()
    cur.execute("PRAGMA foreign_keys = ON")
    cur.execute("PRAGMA journal_mode = WAL")
    cur.execute("PRAGMA synchronous = NORMAL")
    cur.execute("PRAGMA temp_store = MEMORY")

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS ERPInventory (
            erp_id INTEGER PRIMARY KEY AUTOINCREMENT,
            ItemNumber TEXT NOT NULL,
            Description TEXT,
            PrimaryVendorNumber TEXT,
            VendorItem TEXT,
            MfgId TEXT,
            MfgName TEXT,
            MfgItemCount REAL,
            LastCost REAL,
            StdCost REAL,
            AvgCost REAL,
            Revision TEXT,
            ItemLeadTime REAL,
            DefaultWhse TEXT,
            TotalQty REAL,
            source_row_key TEXT NOT NULL UNIQUE,
            row_hash TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            last_seen_at TEXT NOT NULL
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS AlternateMasterSheet (
            master_id INTEGER PRIMARY KEY AUTOINCREMENT,
            ItemNumber TEXT NOT NULL,
            Description TEXT,
            Active TEXT,
            MfgID TEXT,
            ManufacturerName TEXT,
            ManufacturerPN TEXT,
            TariffCode TEXT,
            TariffRate REAL,
            LastCost REAL,
            StandardCost REAL,
            AverageCost REAL,
            source_row_key TEXT NOT NULL UNIQUE,
            row_hash TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            last_seen_at TEXT NOT NULL
        )
        """
    )

    cur.execute("DROP TABLE IF EXISTS ITEM")

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS CompanyPart (
            company_part_id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_part_number TEXT NOT NULL UNIQUE,
            description TEXT,
            default_whse TEXT,
            total_qty REAL,
            revision TEXT,
            primary_vendor_number TEXT,
            updated_at TEXT NOT NULL,
            last_seen_at TEXT NOT NULL
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS ManufacturerPart (
            manufacturer_part_id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_part_id INTEGER NOT NULL,
            manufacturer_part_number TEXT,
            manufacturer_id TEXT,
            manufacturer_name TEXT,
            description TEXT,
            active TEXT,
            item_lead_time REAL,
            tariff_code TEXT,
            tariff_rate REAL,
            last_cost REAL,
            standard_cost REAL,
            average_cost REAL,
            is_erp_primary INTEGER NOT NULL DEFAULT 0,
            erp_source_row_key TEXT,
            master_source_row_key TEXT,
            updated_at TEXT NOT NULL,
            last_seen_at TEXT NOT NULL,
            FOREIGN KEY (company_part_id) REFERENCES CompanyPart(company_part_id) ON DELETE CASCADE
        )
        """
    )

    cur.execute("CREATE INDEX IF NOT EXISTS idx_erp_itemnumber ON ERPInventory(ItemNumber)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_master_itemnumber ON AlternateMasterSheet(ItemNumber)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_cp_number ON CompanyPart(company_part_number)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_mp_company ON ManufacturerPart(company_part_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_mp_mpn ON ManufacturerPart(manufacturer_part_number)")
    cur.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_mp_company_mpn_mfgid
        ON ManufacturerPart(company_part_id, manufacturer_part_number, manufacturer_id)
        """
    )
    conn.commit()


def load_staging_tables(conn: sqlite3.Connection, erp_df: pd.DataFrame, master_df: pd.DataFrame):
    """Replace the temporary staging tables with the current cleaned ERP and Master snapshots."""
    erp_stage = erp_df[ERP_STAGE_COLUMNS].copy()
    master_stage = master_df[MASTER_STAGE_COLUMNS].copy()
    erp_stage.columns = [
        "ItemNumber", "Description", "PrimaryVendorNumber", "VendorItem", "MfgId", "MfgName",
        "MfgItemCount", "LastCost", "StdCost", "AvgCost", "Revision", "ItemLeadTime",
        "DefaultWhse", "TotalQty", "source_row_key", "row_hash",
    ]
    master_stage.columns = [
        "ItemNumber", "Description", "Active", "MfgID", "ManufacturerName", "ManufacturerPN",
        "TariffCode", "TariffRate", "LastCost", "StandardCost", "AverageCost", "source_row_key", "row_hash",
    ]

    erp_stage.to_sql("stg_ERPInventory", conn, if_exists="replace", index=False)
    master_stage.to_sql("stg_AlternateMasterSheet", conn, if_exists="replace", index=False)

    cur = conn.cursor()
    cur.execute("CREATE INDEX IF NOT EXISTS idx_stg_erp_key ON stg_ERPInventory(source_row_key)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_stg_erp_item ON stg_ERPInventory(ItemNumber)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_stg_master_key ON stg_AlternateMasterSheet(source_row_key)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_stg_master_item ON stg_AlternateMasterSheet(ItemNumber)")
    conn.commit()


# ============================================================================
# STAGE 3: RAW TABLE SYNC
# - Compare staging tables to canonical raw tables
# - Insert new rows, update changed rows, delete pruned rows
# ============================================================================


def fetch_stats(conn: sqlite3.Connection, query: str) -> int:
    """Execute a COUNT-style query and return its integer result."""
    cur = conn.cursor()
    return int(cur.execute(query).fetchone()[0])


def collect_dirty_cpns(conn: sqlite3.Connection) -> set[str]:
    """Collect company part numbers whose raw source rows changed between snapshots."""
    cur = conn.cursor()
    rows = cur.execute(
        """
        SELECT DISTINCT cpn FROM (
            SELECT s.ItemNumber AS cpn
            FROM stg_ERPInventory s
            LEFT JOIN ERPInventory t ON t.source_row_key = s.source_row_key
            WHERE t.source_row_key IS NULL OR t.row_hash <> s.row_hash

            UNION

            SELECT t.ItemNumber AS cpn
            FROM ERPInventory t
            LEFT JOIN stg_ERPInventory s ON s.source_row_key = t.source_row_key
            WHERE s.source_row_key IS NULL

            UNION

            SELECT s.ItemNumber AS cpn
            FROM stg_AlternateMasterSheet s
            LEFT JOIN AlternateMasterSheet t ON t.source_row_key = s.source_row_key
            WHERE t.source_row_key IS NULL OR t.row_hash <> s.row_hash

            UNION

            SELECT t.ItemNumber AS cpn
            FROM AlternateMasterSheet t
            LEFT JOIN stg_AlternateMasterSheet s ON s.source_row_key = t.source_row_key
            WHERE s.source_row_key IS NULL
        )
        WHERE cpn IS NOT NULL AND TRIM(cpn) <> ''
        """
    ).fetchall()
    return {str(row[0]) for row in rows}


def sync_erp_raw(conn: sqlite3.Connection, now: str) -> dict[str, int]:
    """Synchronize the canonical ERP raw table from the ERP staging snapshot."""
    stats = {
        "new": fetch_stats(conn, "SELECT COUNT(*) FROM stg_ERPInventory s LEFT JOIN ERPInventory t ON t.source_row_key=s.source_row_key WHERE t.source_row_key IS NULL"),
        "changed": fetch_stats(conn, "SELECT COUNT(*) FROM stg_ERPInventory s JOIN ERPInventory t ON t.source_row_key=s.source_row_key WHERE t.row_hash<>s.row_hash"),
        "unchanged": fetch_stats(conn, "SELECT COUNT(*) FROM stg_ERPInventory s JOIN ERPInventory t ON t.source_row_key=s.source_row_key WHERE t.row_hash=s.row_hash"),
        "pruned": fetch_stats(conn, "SELECT COUNT(*) FROM ERPInventory t LEFT JOIN stg_ERPInventory s ON s.source_row_key=t.source_row_key WHERE s.source_row_key IS NULL"),
    }

    cur = conn.cursor()
    cur.execute(
        """
        UPDATE ERPInventory
        SET
            ItemNumber = (SELECT s.ItemNumber FROM stg_ERPInventory s WHERE s.source_row_key = ERPInventory.source_row_key),
            Description = (SELECT s.Description FROM stg_ERPInventory s WHERE s.source_row_key = ERPInventory.source_row_key),
            PrimaryVendorNumber = (SELECT s.PrimaryVendorNumber FROM stg_ERPInventory s WHERE s.source_row_key = ERPInventory.source_row_key),
            VendorItem = (SELECT s.VendorItem FROM stg_ERPInventory s WHERE s.source_row_key = ERPInventory.source_row_key),
            MfgId = (SELECT s.MfgId FROM stg_ERPInventory s WHERE s.source_row_key = ERPInventory.source_row_key),
            MfgName = (SELECT s.MfgName FROM stg_ERPInventory s WHERE s.source_row_key = ERPInventory.source_row_key),
            MfgItemCount = (SELECT s.MfgItemCount FROM stg_ERPInventory s WHERE s.source_row_key = ERPInventory.source_row_key),
            LastCost = (SELECT s.LastCost FROM stg_ERPInventory s WHERE s.source_row_key = ERPInventory.source_row_key),
            StdCost = (SELECT s.StdCost FROM stg_ERPInventory s WHERE s.source_row_key = ERPInventory.source_row_key),
            AvgCost = (SELECT s.AvgCost FROM stg_ERPInventory s WHERE s.source_row_key = ERPInventory.source_row_key),
            Revision = (SELECT s.Revision FROM stg_ERPInventory s WHERE s.source_row_key = ERPInventory.source_row_key),
            ItemLeadTime = (SELECT s.ItemLeadTime FROM stg_ERPInventory s WHERE s.source_row_key = ERPInventory.source_row_key),
            DefaultWhse = (SELECT s.DefaultWhse FROM stg_ERPInventory s WHERE s.source_row_key = ERPInventory.source_row_key),
            TotalQty = (SELECT s.TotalQty FROM stg_ERPInventory s WHERE s.source_row_key = ERPInventory.source_row_key),
            row_hash = (SELECT s.row_hash FROM stg_ERPInventory s WHERE s.source_row_key = ERPInventory.source_row_key),
            updated_at = CASE
                WHEN row_hash <> (SELECT s.row_hash FROM stg_ERPInventory s WHERE s.source_row_key = ERPInventory.source_row_key)
                THEN ? ELSE updated_at END,
            last_seen_at = ?
        WHERE EXISTS (
            SELECT 1 FROM stg_ERPInventory s WHERE s.source_row_key = ERPInventory.source_row_key
        )
        """,
        (now, now),
    )

    cur.execute(
        """
        INSERT INTO ERPInventory (
            ItemNumber, Description, PrimaryVendorNumber, VendorItem, MfgId, MfgName,
            MfgItemCount, LastCost, StdCost, AvgCost, Revision, ItemLeadTime,
            DefaultWhse, TotalQty, source_row_key, row_hash, created_at, updated_at, last_seen_at
        )
        SELECT
            s.ItemNumber, s.Description, s.PrimaryVendorNumber, s.VendorItem, s.MfgId, s.MfgName,
            s.MfgItemCount, s.LastCost, s.StdCost, s.AvgCost, s.Revision, s.ItemLeadTime,
            s.DefaultWhse, s.TotalQty, s.source_row_key, s.row_hash, ?, ?, ?
        FROM stg_ERPInventory s
        LEFT JOIN ERPInventory t ON t.source_row_key = s.source_row_key
        WHERE t.source_row_key IS NULL
        """,
        (now, now, now),
    )

    cur.execute("DELETE FROM ERPInventory WHERE source_row_key NOT IN (SELECT source_row_key FROM stg_ERPInventory)")
    conn.commit()
    return stats


def sync_master_raw(conn: sqlite3.Connection, now: str) -> dict[str, int]:
    """Synchronize the canonical Master raw table from the Master staging snapshot."""
    stats = {
        "new": fetch_stats(conn, "SELECT COUNT(*) FROM stg_AlternateMasterSheet s LEFT JOIN AlternateMasterSheet t ON t.source_row_key=s.source_row_key WHERE t.source_row_key IS NULL"),
        "changed": fetch_stats(conn, "SELECT COUNT(*) FROM stg_AlternateMasterSheet s JOIN AlternateMasterSheet t ON t.source_row_key=s.source_row_key WHERE t.row_hash<>s.row_hash"),
        "unchanged": fetch_stats(conn, "SELECT COUNT(*) FROM stg_AlternateMasterSheet s JOIN AlternateMasterSheet t ON t.source_row_key=s.source_row_key WHERE t.row_hash=s.row_hash"),
        "pruned": fetch_stats(conn, "SELECT COUNT(*) FROM AlternateMasterSheet t LEFT JOIN stg_AlternateMasterSheet s ON s.source_row_key=t.source_row_key WHERE s.source_row_key IS NULL"),
    }

    cur = conn.cursor()
    cur.execute(
        """
        UPDATE AlternateMasterSheet
        SET
            ItemNumber = (SELECT s.ItemNumber FROM stg_AlternateMasterSheet s WHERE s.source_row_key = AlternateMasterSheet.source_row_key),
            Description = (SELECT s.Description FROM stg_AlternateMasterSheet s WHERE s.source_row_key = AlternateMasterSheet.source_row_key),
            Active = (SELECT s.Active FROM stg_AlternateMasterSheet s WHERE s.source_row_key = AlternateMasterSheet.source_row_key),
            MfgID = (SELECT s.MfgID FROM stg_AlternateMasterSheet s WHERE s.source_row_key = AlternateMasterSheet.source_row_key),
            ManufacturerName = (SELECT s.ManufacturerName FROM stg_AlternateMasterSheet s WHERE s.source_row_key = AlternateMasterSheet.source_row_key),
            ManufacturerPN = (SELECT s.ManufacturerPN FROM stg_AlternateMasterSheet s WHERE s.source_row_key = AlternateMasterSheet.source_row_key),
            TariffCode = (SELECT s.TariffCode FROM stg_AlternateMasterSheet s WHERE s.source_row_key = AlternateMasterSheet.source_row_key),
            TariffRate = (SELECT s.TariffRate FROM stg_AlternateMasterSheet s WHERE s.source_row_key = AlternateMasterSheet.source_row_key),
            LastCost = (SELECT s.LastCost FROM stg_AlternateMasterSheet s WHERE s.source_row_key = AlternateMasterSheet.source_row_key),
            StandardCost = (SELECT s.StandardCost FROM stg_AlternateMasterSheet s WHERE s.source_row_key = AlternateMasterSheet.source_row_key),
            AverageCost = (SELECT s.AverageCost FROM stg_AlternateMasterSheet s WHERE s.source_row_key = AlternateMasterSheet.source_row_key),
            row_hash = (SELECT s.row_hash FROM stg_AlternateMasterSheet s WHERE s.source_row_key = AlternateMasterSheet.source_row_key),
            updated_at = CASE
                WHEN row_hash <> (SELECT s.row_hash FROM stg_AlternateMasterSheet s WHERE s.source_row_key = AlternateMasterSheet.source_row_key)
                THEN ? ELSE updated_at END,
            last_seen_at = ?
        WHERE EXISTS (
            SELECT 1 FROM stg_AlternateMasterSheet s WHERE s.source_row_key = AlternateMasterSheet.source_row_key
        )
        """,
        (now, now),
    )

    cur.execute(
        """
        INSERT INTO AlternateMasterSheet (
            ItemNumber, Description, Active, MfgID, ManufacturerName, ManufacturerPN,
            TariffCode, TariffRate, LastCost, StandardCost, AverageCost,
            source_row_key, row_hash, created_at, updated_at, last_seen_at
        )
        SELECT
            s.ItemNumber, s.Description, s.Active, s.MfgID, s.ManufacturerName, s.ManufacturerPN,
            s.TariffCode, s.TariffRate, s.LastCost, s.StandardCost, s.AverageCost,
            s.source_row_key, s.row_hash, ?, ?, ?
        FROM stg_AlternateMasterSheet s
        LEFT JOIN AlternateMasterSheet t ON t.source_row_key = s.source_row_key
        WHERE t.source_row_key IS NULL
        """,
        (now, now, now),
    )

    cur.execute("DELETE FROM AlternateMasterSheet WHERE source_row_key NOT IN (SELECT source_row_key FROM stg_AlternateMasterSheet)")
    conn.commit()
    return stats


# ============================================================================
# STAGE 4: DERIVED TABLE REBUILD
# - Rebuild CompanyPart and ManufacturerPart only for dirty company part numbers
# - Preserve set-based SQL behavior for speed and consistency
# ============================================================================


def rebuild_dirty_company_parts(conn: sqlite3.Connection, dirty_cpns: set[str], now: str) -> dict[str, int]:
    """Rebuild derived CompanyPart and ManufacturerPart rows for the dirty CPN subset."""
    if not dirty_cpns:
        return {
            "cpns_processed": 0,
            "company_parts_deleted": 0,
            "company_parts_inserted": 0,
            "manufacturer_parts_deleted": 0,
            "manufacturer_parts_inserted": 0,
        }

    cur = conn.cursor()

    # Use a temporary table so the rebuild stays set-based inside SQLite.
    cur.execute("DROP TABLE IF EXISTS tmp_dirty_cpns")
    cur.execute("CREATE TEMP TABLE tmp_dirty_cpns (CompanyPartNumber TEXT PRIMARY KEY)")
    cur.executemany(
        "INSERT INTO tmp_dirty_cpns (CompanyPartNumber) VALUES (?)",
        [(cpn,) for cpn in sorted(dirty_cpns)],
    )

    manufacturer_parts_deleted = fetch_stats(
        conn,
        """
        SELECT COUNT(*)
        FROM ManufacturerPart mp
        JOIN CompanyPart cp ON cp.company_part_id = mp.company_part_id
        WHERE cp.company_part_number IN (SELECT CompanyPartNumber FROM tmp_dirty_cpns)
        """,
    )
    company_parts_deleted = fetch_stats(
        conn,
        "SELECT COUNT(*) FROM CompanyPart WHERE company_part_number IN (SELECT CompanyPartNumber FROM tmp_dirty_cpns)",
    )

    cur.execute(
        "DELETE FROM ManufacturerPart WHERE company_part_id IN (SELECT company_part_id FROM CompanyPart WHERE company_part_number IN (SELECT CompanyPartNumber FROM tmp_dirty_cpns))"
    )
    cur.execute(
        "DELETE FROM CompanyPart WHERE company_part_number IN (SELECT CompanyPartNumber FROM tmp_dirty_cpns)"
    )

    # First rebuild the parent CompanyPart rows for the dirty CPN set.
    cur.execute(
        """
        INSERT INTO CompanyPart (
            company_part_number,
            description,
            default_whse,
            total_qty,
            revision,
            primary_vendor_number,
            updated_at,
            last_seen_at
        )
        WITH erp_rollup AS (
            SELECT
                e.ItemNumber AS company_part_number,
                SUM(COALESCE(e.TotalQty, 0)) AS total_qty_sum,
                MIN(e.erp_id) AS min_erp_id
            FROM ERPInventory e
            WHERE e.ItemNumber IN (SELECT CompanyPartNumber FROM tmp_dirty_cpns)
            GROUP BY e.ItemNumber
        )
        SELECT
            r.company_part_number,
            e.Description,
            e.DefaultWhse,
            r.total_qty_sum,
            e.Revision,
            e.PrimaryVendorNumber,
            ?,
            ?
        FROM erp_rollup r
        JOIN ERPInventory e ON e.erp_id = r.min_erp_id
        """,
        (now, now),
    )
    company_parts_inserted = cur.rowcount if cur.rowcount != -1 else fetch_stats(
        conn,
        "SELECT COUNT(*) FROM CompanyPart WHERE company_part_number IN (SELECT CompanyPartNumber FROM tmp_dirty_cpns)",
    )

    # Then rebuild the child ManufacturerPart rows from both ERP and Master sources.
    cur.execute(
        """
        INSERT INTO ManufacturerPart (
            company_part_id,
            manufacturer_part_number,
            manufacturer_id,
            manufacturer_name,
            description,
            active,
            item_lead_time,
            tariff_code,
            tariff_rate,
            last_cost,
            standard_cost,
            average_cost,
            is_erp_primary,
            erp_source_row_key,
            master_source_row_key,
            updated_at,
            last_seen_at
        )
        WITH manufacturer_candidates AS (
            SELECT
                e.ItemNumber AS company_part_number,
                e.VendorItem AS manufacturer_part_number,
                e.MfgId AS manufacturer_id,
                e.MfgName AS manufacturer_name,
                e.Description AS description,
                NULL AS active,
                e.ItemLeadTime AS item_lead_time,
                NULL AS tariff_code,
                NULL AS tariff_rate,
                e.LastCost AS last_cost,
                e.StdCost AS standard_cost,
                e.AvgCost AS average_cost,
                1 AS is_erp_primary,
                e.source_row_key AS erp_source_row_key,
                NULL AS master_source_row_key,
                1 AS source_priority
            FROM ERPInventory e
            WHERE e.ItemNumber IN (SELECT CompanyPartNumber FROM tmp_dirty_cpns)

            UNION ALL

            SELECT
                m.ItemNumber AS company_part_number,
                m.ManufacturerPN AS manufacturer_part_number,
                m.MfgID AS manufacturer_id,
                m.ManufacturerName AS manufacturer_name,
                m.Description AS description,
                m.Active AS active,
                NULL AS item_lead_time,
                m.TariffCode AS tariff_code,
                m.TariffRate AS tariff_rate,
                m.LastCost AS last_cost,
                m.StandardCost AS standard_cost,
                m.AverageCost AS average_cost,
                0 AS is_erp_primary,
                NULL AS erp_source_row_key,
                m.source_row_key AS master_source_row_key,
                2 AS source_priority
            FROM AlternateMasterSheet m
            WHERE m.ItemNumber IN (SELECT CompanyPartNumber FROM tmp_dirty_cpns)
        ),
        grouped AS (
            SELECT
                company_part_number,
                manufacturer_part_number,
                manufacturer_id,
                COALESCE(
                    MAX(CASE WHEN source_priority = 2 AND manufacturer_name IS NOT NULL THEN manufacturer_name END),
                    MAX(CASE WHEN source_priority = 1 AND manufacturer_name IS NOT NULL THEN manufacturer_name END)
                ) AS manufacturer_name,
                COALESCE(
                    MAX(CASE WHEN source_priority = 2 AND description IS NOT NULL THEN description END),
                    MAX(CASE WHEN source_priority = 1 AND description IS NOT NULL THEN description END)
                ) AS description,
                MAX(active) AS active,
                MAX(item_lead_time) AS item_lead_time,
                MAX(tariff_code) AS tariff_code,
                MAX(tariff_rate) AS tariff_rate,
                COALESCE(
                    MAX(CASE WHEN source_priority = 2 AND last_cost IS NOT NULL THEN last_cost END),
                    MAX(CASE WHEN source_priority = 1 AND last_cost IS NOT NULL THEN last_cost END)
                ) AS last_cost,
                COALESCE(
                    MAX(CASE WHEN source_priority = 2 AND standard_cost IS NOT NULL THEN standard_cost END),
                    MAX(CASE WHEN source_priority = 1 AND standard_cost IS NOT NULL THEN standard_cost END)
                ) AS standard_cost,
                COALESCE(
                    MAX(CASE WHEN source_priority = 2 AND average_cost IS NOT NULL THEN average_cost END),
                    MAX(CASE WHEN source_priority = 1 AND average_cost IS NOT NULL THEN average_cost END)
                ) AS average_cost,
                MAX(is_erp_primary) AS is_erp_primary,
                MAX(erp_source_row_key) AS erp_source_row_key,
                MAX(master_source_row_key) AS master_source_row_key
            FROM manufacturer_candidates
            WHERE company_part_number IS NOT NULL
              AND TRIM(company_part_number) <> ''
              AND (
                    COALESCE(TRIM(manufacturer_part_number), '') <> ''
                 OR COALESCE(TRIM(manufacturer_id), '') <> ''
              )
            GROUP BY company_part_number, manufacturer_part_number, manufacturer_id
        )
        SELECT
            cp.company_part_id,
            g.manufacturer_part_number,
            g.manufacturer_id,
            g.manufacturer_name,
            g.description,
            g.active,
            g.item_lead_time,
            g.tariff_code,
            g.tariff_rate,
            g.last_cost,
            g.standard_cost,
            g.average_cost,
            g.is_erp_primary,
            g.erp_source_row_key,
            g.master_source_row_key,
            ?,
            ?
        FROM grouped g
        JOIN CompanyPart cp ON cp.company_part_number = g.company_part_number
        """,
        (now, now),
    )

    manufacturer_parts_inserted = cur.rowcount if cur.rowcount != -1 else fetch_stats(
        conn,
        """
        SELECT COUNT(*)
        FROM ManufacturerPart mp
        JOIN CompanyPart cp ON cp.company_part_id = mp.company_part_id
        WHERE cp.company_part_number IN (SELECT CompanyPartNumber FROM tmp_dirty_cpns)
        """,
    )

    cur.execute("DROP TABLE tmp_dirty_cpns")
    conn.commit()
    return {
        "cpns_processed": len(dirty_cpns),
        "company_parts_deleted": company_parts_deleted,
        "company_parts_inserted": company_parts_inserted,
        "manufacturer_parts_deleted": manufacturer_parts_deleted,
        "manufacturer_parts_inserted": manufacturer_parts_inserted,
    }


def print_summary(conn: sqlite3.Connection, erp_stats: dict[str, int], master_stats: dict[str, int], rebuild_stats: dict[str, int]):
    """Print final row counts and change statistics for the completed pipeline run."""
    cur = conn.cursor()
    erp_count = cur.execute("SELECT COUNT(*) FROM ERPInventory").fetchone()[0]
    master_count = cur.execute("SELECT COUNT(*) FROM AlternateMasterSheet").fetchone()[0]
    company_part_count = cur.execute("SELECT COUNT(*) FROM CompanyPart").fetchone()[0]
    manufacturer_part_count = cur.execute("SELECT COUNT(*) FROM ManufacturerPart").fetchone()[0]

    print(f"ERPInventory rows:         {erp_count}")
    print(f"AlternateMasterSheet rows: {master_count}")
    print(f"CompanyPart rows:          {company_part_count}")
    print(f"ManufacturerPart rows:     {manufacturer_part_count}")
    print()
    print(f"[ERP] new={erp_stats['new']} changed={erp_stats['changed']} unchanged={erp_stats['unchanged']} pruned={erp_stats['pruned']}")
    print(f"[MASTER] new={master_stats['new']} changed={master_stats['changed']} unchanged={master_stats['unchanged']} pruned={master_stats['pruned']}")
    print(
        "[DERIVED] "
        f"cpns_processed={rebuild_stats['cpns_processed']} "
        f"company_parts_deleted={rebuild_stats['company_parts_deleted']} "
        f"company_parts_inserted={rebuild_stats['company_parts_inserted']} "
        f"manufacturer_parts_deleted={rebuild_stats['manufacturer_parts_deleted']} "
        f"manufacturer_parts_inserted={rebuild_stats['manufacturer_parts_inserted']}"
    )


# ============================================================================
# PIPELINE STAGE WRAPPERS
# - These wrappers make the run order explicit at the bottom of the file
# - They do not change the underlying ETL behavior
# ============================================================================


def stage_1_extract_excel_inputs() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Stage 1. Read the ERP and Master Excel files into raw DataFrames."""
    if not Path(ERP_EXCEL_PATH).exists():
        raise FileNotFoundError(f"ERP file not found: {ERP_EXCEL_PATH}")
    if not Path(MASTER_EXCEL_PATH).exists():
        raise FileNotFoundError(f"Master file not found: {MASTER_EXCEL_PATH}")

    # Extraction only happens here. No validation, hashing, or database work yet.
    erp_df = load_excel(ERP_EXCEL_PATH, ERP_SHEET_NAME)
    master_df = load_excel(MASTER_EXCEL_PATH, MASTER_SHEET_NAME)
    return erp_df, master_df


def stage_2_validate_and_prepare_frames(erp_df: pd.DataFrame, master_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Stage 2. Validate headers, normalize values, generate keys/hashes, and dedupe incoming rows."""
    # Fail fast on schema drift before any database mutation occurs.
    validate_headers(erp_df, ERP_HEADERS, "ERPInventory Excel")
    validate_headers(master_df, MASTER_HEADERS, "AlternateMasterSheet Excel")

    # Normalize raw Excel cell values into stable Python values.
    erp_df = clean_dataframe(erp_df)
    master_df = clean_dataframe(master_df)

    # Add deterministic identity and change-tracking columns.
    erp_df = add_key_and_hash(erp_df, ERP_KEY_COLUMNS, ERP_HASH_COLUMNS)
    master_df = add_key_and_hash(master_df, MASTER_KEY_COLUMNS, MASTER_HASH_COLUMNS)

    # Collapse duplicates before loading the staging tables.
    erp_df = dedupe_incoming(erp_df)
    master_df = dedupe_incoming(master_df)
    return erp_df, master_df


def stage_3_sync_database(erp_df: pd.DataFrame, master_df: pd.DataFrame) -> sqlite3.Connection:
    """Stage 3. Load staging tables, sync raw tables, and rebuild the dirty derived rows."""
    conn = sqlite3.connect(SQLITE_DB_PATH)

    # Schema creation is idempotent, so it is safe to run on each execution.
    create_tables(conn)

    # Snapshot the cleaned inputs into staging tables first.
    load_staging_tables(conn, erp_df, master_df)

    # Dirty CPNs are discovered before the raw sync so the rebuild can target only changed areas.
    dirty_cpns = collect_dirty_cpns(conn)
    now = utc_now_iso()

    # Sync the raw source-backed tables.
    erp_stats = sync_erp_raw(conn, now)
    master_stats = sync_master_raw(conn, now)

    # Rebuild only the affected derived rows.
    rebuild_stats = rebuild_dirty_company_parts(conn, dirty_cpns, now)
    print_summary(conn, erp_stats, master_stats, rebuild_stats)
    return conn


def stage_4_preview_object_models(conn: sqlite3.Connection, limit: int = 3) -> None:
    """Stage 4. Hydrate sample dataclass objects from the database for inspection."""
    repo = CompanyPartRepository(conn)
    sample = repo.get_all_company_parts(limit=limit)
    if sample:
        print()
        print("Sample object bundles:")
        print_company_part_bundles(sample)


def main():
    """Run the end-to-end inventory pipeline in explicit, readable stages."""
    # ---------------------------------------------------------------------
    # Stage 1: extract raw Excel sheets into DataFrames.
    # ---------------------------------------------------------------------
    erp_df, master_df = stage_1_extract_excel_inputs()

    # ---------------------------------------------------------------------
    # Stage 2: validate and normalize the incoming frames before persistence.
    # ---------------------------------------------------------------------
    erp_df, master_df = stage_2_validate_and_prepare_frames(erp_df, master_df)

    conn: sqlite3.Connection | None = None
    try:
        # -----------------------------------------------------------------
        # Stage 3: stage, sync, and rebuild the database.
        # -----------------------------------------------------------------
        conn = stage_3_sync_database(erp_df, master_df)

        # -----------------------------------------------------------------
        # Stage 4: hydrate sample object models from the synced database.
        # -----------------------------------------------------------------
        stage_4_preview_object_models(conn, limit=3)
    finally:
        if conn is not None:
            conn.close()


if __name__ == "__main__":
    main()
