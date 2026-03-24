import hashlib
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd


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


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def normalize_cell(value):
    if pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        # Collapse 5.0 -> 5 for stable hashing and storage readability.
        f = float(value)
        if f.is_integer():
            return int(f)
        return f
    if isinstance(value, str):
        value = value.strip()
        return value if value else None
    return value


def load_excel(path: str, sheet_name=0) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name=sheet_name, dtype=object)
    df.columns = [str(col).strip() for col in df.columns]
    return df


def validate_headers(df: pd.DataFrame, expected_headers: list[str], table_name: str):
    missing = [h for h in expected_headers if h not in df.columns]
    if missing:
        raise ValueError(f"{table_name} is missing required headers: {missing}\nFound headers: {list(df.columns)}")


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col in df.columns:
        df[col] = df[col].map(normalize_cell)
    return df


def stable_stringify_series(series: pd.Series) -> pd.Series:
    out = series.copy()
    out = out.map(lambda v: "" if v is None else str(v))
    return out


def add_key_and_hash(df: pd.DataFrame, key_columns: list[str], hash_columns: list[str]) -> pd.DataFrame:
    df = df.copy()
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
    # Keep the last occurrence for any repeated natural key inside the same file.
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


def create_tables(conn: sqlite3.Connection):
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

    # ITEM is a derived table now. Recreate it so schema changes never linger.
    cur.execute("DROP TABLE IF EXISTS ITEM")
    cur.execute(
        """
        CREATE TABLE ITEM (
            item_id INTEGER PRIMARY KEY AUTOINCREMENT,
            CompanyPartNumber TEXT NOT NULL,

            ERPSourceRowKey TEXT NOT NULL,
            MasterSourceRowKey TEXT,

            ERPDescription TEXT,
            ERPPrimaryVendorNumber TEXT,
            ERPVendorItem TEXT,
            ERPMfgId TEXT,
            ERPMfgName TEXT,
            ERPMfgItemCount REAL,
            ERPLastCost REAL,
            ERPStdCost REAL,
            ERPAvgCost REAL,
            ERPRevision TEXT,
            ERPItemLeadTime REAL,
            ERPDefaultWhse TEXT,
            ERPTotalQty REAL,

            MasterDescription TEXT,
            MasterActive TEXT,
            MasterMfgID TEXT,
            MasterManufacturerName TEXT,
            MasterManufacturerPN TEXT,
            MasterTariffCode TEXT,
            MasterTariffRate REAL,
            MasterLastCost REAL,
            MasterStandardCost REAL,
            MasterAverageCost REAL,

            HasMasterRow INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL,
            last_seen_at TEXT NOT NULL,

            UNIQUE(ERPSourceRowKey, MasterSourceRowKey)
        )
        """
    )

    cur.execute("CREATE INDEX IF NOT EXISTS idx_erp_itemnumber ON ERPInventory(ItemNumber)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_master_itemnumber ON AlternateMasterSheet(ItemNumber)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_item_cpn ON ITEM(CompanyPartNumber)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_item_erp_key ON ITEM(ERPSourceRowKey)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_item_master_key ON ITEM(MasterSourceRowKey)")
    conn.commit()


def load_staging_tables(conn: sqlite3.Connection, erp_df: pd.DataFrame, master_df: pd.DataFrame):
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


def fetch_stats(conn: sqlite3.Connection, query: str) -> int:
    cur = conn.cursor()
    return int(cur.execute(query).fetchone()[0])


def collect_dirty_cpns(conn: sqlite3.Connection) -> set[str]:
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
    stats = {
        "new": fetch_stats(conn, "SELECT COUNT(*) FROM stg_ERPInventory s LEFT JOIN ERPInventory t ON t.source_row_key=s.source_row_key WHERE t.source_row_key IS NULL"),
        "changed": fetch_stats(conn, "SELECT COUNT(*) FROM stg_ERPInventory s JOIN ERPInventory t ON t.source_row_key=s.source_row_key WHERE t.row_hash<>s.row_hash"),
        "unchanged": fetch_stats(conn, "SELECT COUNT(*) FROM stg_ERPInventory s JOIN ERPInventory t ON t.source_row_key=s.source_row_key WHERE t.row_hash=s.row_hash"),
        "pruned": fetch_stats(conn, "SELECT COUNT(*) FROM ERPInventory t LEFT JOIN stg_ERPInventory s ON s.source_row_key=t.source_row_key WHERE s.source_row_key IS NULL"),
    }

    cur = conn.cursor()

    # Update existing rows first. This avoids relying on SQLite UPSERT syntax variants
    # that are inconsistent across installed versions.
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

    # Insert brand new rows.
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


def rebuild_dirty_items(conn: sqlite3.Connection, dirty_cpns: set[str], now: str) -> dict[str, int]:
    if not dirty_cpns:
        return {"cpns_processed": 0, "items_deleted": 0, "items_inserted": 0}

    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS tmp_dirty_cpns")
    cur.execute("CREATE TEMP TABLE tmp_dirty_cpns (CompanyPartNumber TEXT PRIMARY KEY)")
    cur.executemany(
        "INSERT INTO tmp_dirty_cpns (CompanyPartNumber) VALUES (?)",
        [(cpn,) for cpn in sorted(dirty_cpns)],
    )

    items_deleted = fetch_stats(
        conn,
        "SELECT COUNT(*) FROM ITEM WHERE CompanyPartNumber IN (SELECT CompanyPartNumber FROM tmp_dirty_cpns)"
    )

    cur.execute(
        "DELETE FROM ITEM WHERE CompanyPartNumber IN (SELECT CompanyPartNumber FROM tmp_dirty_cpns)"
    )

    cur.execute(
        """
        INSERT INTO ITEM (
            CompanyPartNumber,
            ERPSourceRowKey,
            MasterSourceRowKey,
            ERPDescription,
            ERPPrimaryVendorNumber,
            ERPVendorItem,
            ERPMfgId,
            ERPMfgName,
            ERPMfgItemCount,
            ERPLastCost,
            ERPStdCost,
            ERPAvgCost,
            ERPRevision,
            ERPItemLeadTime,
            ERPDefaultWhse,
            ERPTotalQty,
            MasterDescription,
            MasterActive,
            MasterMfgID,
            MasterManufacturerName,
            MasterManufacturerPN,
            MasterTariffCode,
            MasterTariffRate,
            MasterLastCost,
            MasterStandardCost,
            MasterAverageCost,
            HasMasterRow,
            updated_at,
            last_seen_at
        )
        SELECT
            e.ItemNumber,
            e.source_row_key,
            m.source_row_key,
            e.Description,
            e.PrimaryVendorNumber,
            e.VendorItem,
            e.MfgId,
            e.MfgName,
            e.MfgItemCount,
            e.LastCost,
            e.StdCost,
            e.AvgCost,
            e.Revision,
            e.ItemLeadTime,
            e.DefaultWhse,
            e.TotalQty,
            m.Description,
            m.Active,
            m.MfgID,
            m.ManufacturerName,
            m.ManufacturerPN,
            m.TariffCode,
            m.TariffRate,
            m.LastCost,
            m.StandardCost,
            m.AverageCost,
            CASE WHEN m.source_row_key IS NULL THEN 0 ELSE 1 END,
            ?,
            ?
        FROM ERPInventory e
        LEFT JOIN AlternateMasterSheet m
            ON m.ItemNumber = e.ItemNumber
        WHERE e.ItemNumber IN (SELECT CompanyPartNumber FROM tmp_dirty_cpns)
        """,
        (now, now),
    )

    items_inserted = cur.rowcount if cur.rowcount != -1 else fetch_stats(
        conn,
        "SELECT COUNT(*) FROM ITEM WHERE CompanyPartNumber IN (SELECT CompanyPartNumber FROM tmp_dirty_cpns)"
    )

    cur.execute("DROP TABLE tmp_dirty_cpns")
    conn.commit()
    return {"cpns_processed": len(dirty_cpns), "items_deleted": items_deleted, "items_inserted": items_inserted}


def print_summary(conn: sqlite3.Connection, erp_stats: dict[str, int], master_stats: dict[str, int], dirty_stats: dict[str, int]):
    cur = conn.cursor()
    erp_count = cur.execute("SELECT COUNT(*) FROM ERPInventory").fetchone()[0]
    master_count = cur.execute("SELECT COUNT(*) FROM AlternateMasterSheet").fetchone()[0]
    item_count = cur.execute("SELECT COUNT(*) FROM ITEM").fetchone()[0]

    print(f"ERPInventory rows:         {erp_count}")
    print(f"AlternateMasterSheet rows: {master_count}")
    print(f"ITEM rows:                 {item_count}")
    print()
    print(f"[ERP] new={erp_stats['new']} changed={erp_stats['changed']} unchanged={erp_stats['unchanged']} pruned={erp_stats['pruned']}")
    print(f"[MASTER] new={master_stats['new']} changed={master_stats['changed']} unchanged={master_stats['unchanged']} pruned={master_stats['pruned']}")
    print(f"[ITEM] cpns_processed={dirty_stats['cpns_processed']} items_deleted={dirty_stats['items_deleted']} items_inserted={dirty_stats['items_inserted']}")


def main():
    if not Path(ERP_EXCEL_PATH).exists():
        raise FileNotFoundError(f"ERP file not found: {ERP_EXCEL_PATH}")
    if not Path(MASTER_EXCEL_PATH).exists():
        raise FileNotFoundError(f"Master file not found: {MASTER_EXCEL_PATH}")

    erp_df = load_excel(ERP_EXCEL_PATH, ERP_SHEET_NAME)
    master_df = load_excel(MASTER_EXCEL_PATH, MASTER_SHEET_NAME)

    validate_headers(erp_df, ERP_HEADERS, "ERPInventory Excel")
    validate_headers(master_df, MASTER_HEADERS, "AlternateMasterSheet Excel")

    erp_df = clean_dataframe(erp_df)
    master_df = clean_dataframe(master_df)

    erp_df = add_key_and_hash(erp_df, ERP_KEY_COLUMNS, ERP_HASH_COLUMNS)
    master_df = add_key_and_hash(master_df, MASTER_KEY_COLUMNS, MASTER_HASH_COLUMNS)

    erp_df = dedupe_incoming(erp_df)
    master_df = dedupe_incoming(master_df)

    conn = sqlite3.connect(SQLITE_DB_PATH)
    try:
        create_tables(conn)
        load_staging_tables(conn, erp_df, master_df)
        dirty_cpns = collect_dirty_cpns(conn)
        now = utc_now_iso()
        erp_stats = sync_erp_raw(conn, now)
        master_stats = sync_master_raw(conn, now)
        dirty_stats = rebuild_dirty_items(conn, dirty_cpns, now)
        print_summary(conn, erp_stats, master_stats, dirty_stats)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
