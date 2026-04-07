from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from .data_models import (
    AlternateMasterRow,
    CNSRecord,
    CompanyPartRecord,
    ERPInventoryRow,
    ManufacturerPartRecord,
    NPRPart,
)

try:
    from .parsing_engine import parse_description  # type: ignore
except Exception:  # pragma: no cover
    parse_description = None

try:
    from .config_loader import load_config  # type: ignore
except Exception:  # pragma: no cover
    load_config = None


# =====================================================
# SAFE SHARED FILE READS
# =====================================================
# 1 = always read a copied file (cached first), 0 = try direct then fallback
SAFE_SHARED_FILE_READS = 1


# =====================================================
# REGEX HELPERS
# =====================================================
PB_RE = re.compile(r"\b(\d{2})\s*-\s*(\d{5})\b")
PFX_RE = re.compile(r"^\s*(\d{2})\s*$")
BODY_RE = re.compile(r"^\s*(\d{5})\s*$")


# =====================================================
# PARSER CONFIG CACHE
# =====================================================
_PARSER_CFG: Optional[Dict[str, Any]] = None


def _get_parser_cfg(config_path: Optional[str] = None) -> Dict[str, Any]:
    global _PARSER_CFG
    if _PARSER_CFG is None:
        try:
            if config_path and load_config is not None:
                _PARSER_CFG = load_config(config_path) or {}
            else:
                _PARSER_CFG = {}
        except Exception:
            _PARSER_CFG = {}
    return _PARSER_CFG


def _parse_desc_fields(desc: str) -> Dict[str, object]:
    desc = safe_str(desc)
    if not desc or parse_description is None:
        return {}
    try:
        parsed = parse_description(desc, _get_parser_cfg()) or {}
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}


# =====================================================
# SMALL HELPERS
# =====================================================
def safe_str(x: Any) -> str:
    """Convert pandas cell values into clean strings and collapse duplicate-header Series."""
    if isinstance(x, pd.Series):
        for v in x.tolist():
            s = safe_str(v)
            if s:
                return s
        return ""

    if isinstance(x, (list, tuple)):
        for v in x:
            s = safe_str(v)
            if s:
                return s
        return ""

    try:
        if pd.isna(x):
            return ""
    except Exception:
        pass

    return str(x).strip()


def norm_header(h: str) -> str:
    return str(h).strip().lower().replace("\n", " ").replace("\r", " ")


def norm_header_snake(h: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", norm_header(h)).strip("_")


def _excel_cache_root() -> str:
    root = os.path.join(tempfile.gettempdir(), "nprtool_excel_cache")
    os.makedirs(root, exist_ok=True)
    return root


def _file_fingerprint(path: str) -> str:
    p = Path(path)
    st = p.stat()
    raw = f"{p.name}|{st.st_size}|{st.st_mtime_ns}".encode("utf-8", errors="ignore")
    return hashlib.sha1(raw).hexdigest()[:16]


def _cached_copy_path(path: str) -> str:
    src = Path(path)
    fp = _file_fingerprint(path)
    return os.path.join(_excel_cache_root(), f"{src.stem}__{fp}{src.suffix}")


def _ensure_cached_copy(path: str) -> str:
    cached = _cached_copy_path(path)
    if not os.path.exists(cached):
        shutil.copy2(path, cached)
    return cached


def read_excel_with_temp_copy(path: str, **read_excel_kwargs) -> pd.DataFrame:
    """
    Excel reader that avoids touching shared files.

    Behavior:
      - If SAFE_SHARED_FILE_READS=1, always read the cached copy.
      - If SAFE_SHARED_FILE_READS=0, try original path first, then fall back to temp copy.
    """
    if SAFE_SHARED_FILE_READS:
        safe_path = _ensure_cached_copy(path)
        return pd.read_excel(safe_path, **read_excel_kwargs)

    try:
        return pd.read_excel(path, **read_excel_kwargs)
    except PermissionError:
        pass
    except OSError as e:
        if "Permission denied" not in str(e):
            raise

    safe_path = _ensure_cached_copy(path)
    return pd.read_excel(safe_path, **read_excel_kwargs)


@dataclass(frozen=True)
class HeaderAliases:
    aliases: Dict[str, str]


# =====================================================
# HEADER ALIASES
# =====================================================
DEFAULT_NPR_ALIASES = HeaderAliases(
    aliases={
        "mfg_p_n": "manufacturer_part_",
        "mfgpn": "manufacturer_part_",
        "mfg_part_number": "manufacturer_part_",
        "manufacturer_part_number": "manufacturer_part_",
        "manufacturer_part_no": "manufacturer_part_",
        "manufacturer_p_n": "manufacturer_part_",
        "manufacturer_part": "manufacturer_part_",
        "mpn": "manufacturer_part_",
        "mfrpn": "manufacturer_part_",
        "mfr_pn": "manufacturer_part_",
        "mfrp_n": "manufacturer_part_",
        "mfr_p_n": "manufacturer_part_",
        "manufacturer_part_number_1": "manufacturer_part_",
        "manufacturer_part_number1": "manufacturer_part_",
        "mfrpn_1": "manufacturer_part_",
        "mpn_1": "manufacturer_part_",
        "manufacturer_part_number_2": "manufacturer_part_2",
        "manufacturer_part_number2": "manufacturer_part_2",
        "manufacturer_2_part_number": "manufacturer_part_2",
        "manufacturer_part_2": "manufacturer_part_2",
        "mpn_2": "manufacturer_part_2",
        "mfrpn_2": "manufacturer_part_2",
        "manufacturer_part_number_3": "manufacturer_part_3",
        "manufacturer_part_number3": "manufacturer_part_3",
        "mpn_3": "manufacturer_part_3",
        "mfrpn_3": "manufacturer_part_3",
        "elan_part_number": "part_number",
        "part_number_": "part_number",
        "part_number": "part_number",
        "p_n": "part_number",
        "pn": "part_number",
        "p_n_": "part_number",
        "part_no": "part_number",
        "part": "part_number",
        "pno": "part_number",
        "reference": "designator",
        "refdes": "designator",
        "designator": "designator",
        "footprint": "footprint",
        "subs_allowed": "subs_allowed",
        "name": "name",
        "description": "item_description",
        "item_description": "item_description",
        "desc": "item_description",
        "title": "item_description",
        "manufacturer": "manufacturer_name",
        "manufacturer_name": "manufacturer_name",
        "mfr_name": "manufacturer_name",
        "mfg_name": "manufacturer_name",
        "mfr": "manufacturer_name",
        "manufacturer_1": "manufacturer_name",
        "manufacturer1": "manufacturer_name",
        "supplier_name": "supplier",
        "vendor": "supplier",
        "supplier": "supplier",
        "distributor": "supplier",
        "digikey_pn": "supplier",
        "digikey_part_number": "supplier",
        "digikey_part_no": "supplier",
        "quantity_per": "quantity",
        "qty": "quantity",
        "quantity": "quantity",
        "qty4one": "quantity",
    }
)

DEFAULT_ALT_ALIASES = HeaderAliases(
    aliases={
        "item_number": "itemnum",
        "itemnumber": "itemnum",
        "item_num": "itemnum",
        "description": "desc",
        "desc": "desc",
        "active": "active",
        "mfg_id": "mfgid",
        "mfgid": "mfgid",
        "manufacturer_id": "mfgid",
        "manufacturer_name": "mfgname",
        "manufacturer": "mfgname",
        "mfg_name": "mfgname",
        "manufacturer_pn": "mfgpn",
        "manufacturer_part_no": "mfgpn",
        "manufacturer_part_number": "mfgpn",
        "mpn": "mfgpn",
        "vendoritem": "mfgpn",
        "vendor_item": "mfgpn",
        "vendor_part_number": "mfgpn",
        "tariff_code": "tariff_code",
        "tariff_rate": "tariff_rate",
        "last_cost": "last_cost",
        "standard_cost": "standard_cost",
        "average_cost": "average_cost",
    }
)

DEFAULT_INV_ALIASES = HeaderAliases(
    aliases={
        "itemnumber": "itemnum",
        "item_number": "itemnum",
        "item_no": "itemnum",
        "item_num": "itemnum",
        "internal_part_number": "itemnum",
        "part_number": "itemnum",
        "pn": "itemnum",
        "description": "desc",
        "desc": "desc",
        "item_description": "desc",
        "mfgid": "mfgid",
        "mfg_id": "mfgid",
        "manufacturer_id": "mfgid",
        "mfgname": "mfgname",
        "mfg_name": "mfgname",
        "manufacturer": "mfgname",
        "manufacturer_name": "mfgname",
        "vendoritem": "vendoritem",
        "vendor_item": "vendoritem",
        "manufacturer_part_number": "vendoritem",
        "manufacturer_part_no": "vendoritem",
        "mpn": "vendoritem",
        "mfgpn": "vendoritem",
        "mfg_pn": "vendoritem",
        "primaryvendornumber": "primaryvendornumber",
        "primary_vendor_number": "primaryvendornumber",
        "supplier": "primaryvendornumber",
        "mfgitemcount": "mfgitemcount",
        "mfg_item_count": "mfgitemcount",
        "manufacturer_item_count": "mfgitemcount",
        "lastcost": "lastcost",
        "last_cost": "lastcost",
        "standardcost": "standard_cost",
        "standard_cost": "standard_cost",
        "stdcost": "standard_cost",
        "std_cost": "standard_cost",
        "avgcost": "average_cost",
        "avg_cost": "average_cost",
        "average_cost": "average_cost",
        "revision": "revision",
        "rev": "revision",
        "itemleadtime": "itemleadtime",
        "item_lead_time": "itemleadtime",
        "lead_time": "itemleadtime",
        "defaultwhse": "defaultwhse",
        "default_whse": "defaultwhse",
        "default_warehouse": "defaultwhse",
        "totalqty": "totalqty",
        "total_qty": "totalqty",
        "qty": "totalqty",
    }
)

DEFAULT_CNS_ALIASES = HeaderAliases(
    aliases={
        "prefix": "prefix",
        "pfx": "prefix",
        "pref": "prefix",
        "body": "body",
        "base": "body",
        "series": "body",
        "number": "body",
        "suffix": "suffix",
        "suf": "suffix",
        "rev": "suffix",
        "variant": "suffix",
        "description": "description",
        "desc": "description",
        "item_description": "description",
        "title": "description",
        "comment": "description",
        "date": "date",
        "created": "date",
        "created_date": "date",
        "initials": "initials",
        "owner": "initials",
        "author": "initials",
    }
)

ERP_EXPECTED_HEADERS = [
    "ItemNumber",
    "Description",
    "PrimaryVendorNumber",
    "VendorItem",
    "ManufacturerId",
    "ManufacturerName",
    "ManufacturerItemCount",
    "LastCost",
    "StandardCost",
    "AverageCost",
    "Revision",
    "ItemLeadTime",
    "DefaultWhse",
    "TotalQty",
]

MASTER_EXPECTED_HEADERS = [
    "ItemNumber",
    "Description",
    "Active",
    "ManufacturerId",
    "ManufacturerName",
    "ManufacturerPartNumber",
    "TariffCode",
    "TariffRate",
    "LastCost",
    "StandardCost",
    "AverageCost",
]


class DataLoader:
    """
    Data loader with legacy-safe Excel handling restored.

    This file owns:
      - safe workbook reading through cached copies
      - header row detection for offset Excel exports
      - alias normalization from real workbook headers into canonical names
      - conversion into the current canonical row/object models

    This file does not own database persistence.
    """

    # =====================================================
    # GENERIC HELPERS
    # =====================================================
    @staticmethod
    def load_excel(path: str, sheet_name: int | str = 0, **kwargs: Any) -> pd.DataFrame:
        kwargs.setdefault("dtype", object)
        return read_excel_with_temp_copy(path, sheet_name=sheet_name, **kwargs)

    @staticmethod
    def validate_headers(df: pd.DataFrame, expected_headers: List[str], table_name: str) -> None:
        actual = [str(col).strip() for col in df.columns]
        missing = [h for h in expected_headers if h not in actual]
        if missing:
            raise ValueError(f"{table_name} missing required headers: {missing}\nFound headers: {actual}")

    @staticmethod
    def normalize_cell(value: Any) -> Any:
        try:
            if pd.isna(value):
                return None
        except Exception:
            pass
        if hasattr(value, "item"):
            try:
                return value.item()
            except Exception:
                pass
        return value

    @staticmethod
    def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        out.columns = [str(col).strip() for col in out.columns]
        out = out.map(DataLoader.normalize_cell)
        return out

    @staticmethod
    def stable_stringify_series(series: pd.Series) -> pd.Series:
        return series.map(lambda x: "" if x is None else str(x).strip())

    @staticmethod
    def sha256_text(text: str) -> str:
        return hashlib.sha256(text.encode("utf-8")).hexdigest()

    @staticmethod
    def add_key_and_hash(df: pd.DataFrame, key_columns: List[str], hash_columns: List[str]) -> pd.DataFrame:
        out = df.copy()
        key_source = out[key_columns].apply(DataLoader.stable_stringify_series, axis=0)
        out["source_row_key"] = key_source.apply(lambda row: "|".join(row.values.tolist()), axis=1)
        hash_source = out[hash_columns].apply(DataLoader.stable_stringify_series, axis=0)
        out["row_hash"] = hash_source.apply(
            lambda row: DataLoader.sha256_text(json.dumps(row.values.tolist(), ensure_ascii=False)),
            axis=1,
        )
        return out

    @staticmethod
    def dedupe_incoming(df: pd.DataFrame) -> pd.DataFrame:
        if "source_row_key" not in df.columns:
            return df
        return df.drop_duplicates(subset=["source_row_key"], keep="first").reset_index(drop=True)

    @staticmethod
    def find_duplicate_source_row_keys(df: pd.DataFrame) -> pd.DataFrame:
        """Return only rows whose source_row_key appears more than once."""
        if "source_row_key" not in df.columns or df.empty:
            return df.iloc[0:0].copy()
        mask = df["source_row_key"].astype(str).duplicated(keep=False)
        return df.loc[mask].copy()

    @staticmethod
    def _report_output_base(path: str) -> Path:
        src = Path(path)
        return src.with_name(f"{src.stem}_master_duplicate_source_row_keys")

    @staticmethod
    def write_duplicate_source_row_key_report(df: pd.DataFrame, path: str) -> None:
        """
        Write duplicate Master raw-row identity reports grouped by source_row_key.

        This is a debug/reporting helper only. It does not dedupe or mutate the incoming rows.
        """
        dupes = DataLoader.find_duplicate_source_row_keys(df)
        if dupes.empty:
            print("[MASTER][DUPLICATE SOURCE_ROW_KEYS] none detected")
            return

        dupes = dupes.copy()
        dupes["_row_number"] = range(1, len(dupes) + 1)
        dupes["_group_size"] = dupes.groupby("source_row_key")["source_row_key"].transform("size")

        sort_cols = [c for c in [
            "source_row_key",
            "ItemNumber",
            "ManufacturerPartNumber",
            "ManufacturerId",
            "ManufacturerName",
            "Description",
            "_row_number",
        ] if c in dupes.columns]
        if sort_cols:
            dupes = dupes.sort_values(sort_cols, kind="stable")

        base = DataLoader._report_output_base(path)
        csv_path = base.with_suffix(".csv")
        txt_path = base.with_suffix(".txt")

        dupes.to_csv(csv_path, index=False)

        grouped = dupes.groupby("source_row_key", sort=False)
        with open(txt_path, "w", encoding="utf-8") as f:
            f.write("MASTER duplicate source_row_key report\n")
            f.write(f"Source file: {path}\n")
            f.write(f"Duplicate groups: {grouped.ngroups}\n")
            f.write(f"Duplicate rows: {len(dupes)}\n\n")
            for i, (key, grp) in enumerate(grouped, start=1):
                f.write(f"[{i}] source_row_key={key} count={len(grp)}\n")
                for _, row in grp.iterrows():
                    payload = {col: safe_str(row[col]) for col in grp.columns if not str(col).startswith("_")}
                    f.write(json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n")
                f.write("\n")

        print(f"[MASTER][DUPLICATE SOURCE_ROW_KEYS] groups={grouped.ngroups} rows={len(dupes)}")
        print(f"[MASTER][DUPLICATE SOURCE_ROW_KEYS] csv={csv_path}")
        print(f"[MASTER][DUPLICATE SOURCE_ROW_KEYS] txt={txt_path}")

    @staticmethod
    def _safe_float(value: Any) -> Optional[float]:
        text = safe_str(value)
        if not text:
            return None
        try:
            return float(text)
        except Exception:
            return None

    @staticmethod
    def _safe_int(value: Any) -> Optional[int]:
        text = safe_str(value)
        if not text:
            return None
        try:
            return int(float(text))
        except Exception:
            return None

    @staticmethod
    def _source_row_key(*parts: Any) -> str:
        joined = "|".join([safe_str(p).strip() for p in parts])
        return hashlib.sha256(joined.encode("utf-8")).hexdigest()

    @staticmethod
    def clean_mpn(s: str) -> str:
        if not s:
            return ""
        s = str(s)
        s = re.sub(r"_x[0-9a-fA-F]{4}_", "", s)
        return s.replace("\n", "").replace("\r", "").strip()

    @staticmethod
    def norm_mpn_key(s: str) -> str:
        s = DataLoader.clean_mpn(s or "")
        return re.sub(r"\s+", "", s).upper()

    # =====================================================
    # HEADER DETECTION / NORMALIZATION
    # =====================================================
    @staticmethod
    def _find_header_row_by_keywords(df: pd.DataFrame, required_keywords: List[str], scan_rows: int = 50) -> int:
        required = [str(k).strip().lower() for k in required_keywords]
        best_row: Optional[int] = None
        best_hits = -1
        for i in range(min(scan_rows, len(df))):
            row = df.iloc[i].astype(str).str.lower().str.strip()
            joined = " ".join(row.tolist())
            hits = sum(1 for k in required if k in joined)
            if hits > best_hits:
                best_hits = hits
                best_row = i
        if best_row is None or best_hits <= 0:
            raise ValueError(f"Could not locate a header row using keywords: {required_keywords}")
        return best_row

    @staticmethod
    def _find_npr_header_row(raw: pd.DataFrame, scan_rows: int = 40) -> int:
        header_keywords = ["mfg", "part", "desc", "manufacturer", "item", "mpn", "qty"]
        best_row: Optional[int] = None
        best_hits = 0
        for i in range(min(scan_rows, len(raw))):
            row = raw.iloc[i].astype(str).str.lower().str.strip()
            joined = " ".join(row.tolist())
            hits = sum(k in joined for k in header_keywords)
            if hits > best_hits:
                best_hits = hits
                best_row = i
        if best_row is None or best_hits == 0:
            raise ValueError("No header row found with recognizable keywords.")
        return best_row

    @staticmethod
    def _make_unique_columns(df: pd.DataFrame) -> pd.DataFrame:
        cols = list(df.columns)
        counts: Dict[str, int] = {}
        new_cols: List[str] = []
        for c in cols:
            base = str(c)
            n = counts.get(base, 0) + 1
            counts[base] = n
            new_cols.append(base if n == 1 else f"{base}_{n}")
        out = df.copy()
        out.columns = new_cols
        return out

    @staticmethod
    def _normalize_and_alias_columns(df: pd.DataFrame, aliases: HeaderAliases) -> pd.DataFrame:
        out = df.copy()
        out.columns = [norm_header_snake(c) for c in out.columns]
        out.columns = [aliases.aliases.get(c, c) for c in out.columns]
        return DataLoader._make_unique_columns(out)

    @staticmethod
    def _open_excel_file_with_temp_copy(path: str) -> tuple[pd.ExcelFile, str | None]:
        if SAFE_SHARED_FILE_READS:
            safe_path = _ensure_cached_copy(path)
            xf = pd.ExcelFile(safe_path)
            setattr(xf, "_nprtool_safe_path", safe_path)
            return xf, None

        try:
            xf = pd.ExcelFile(path)
            setattr(xf, "_nprtool_safe_path", path)
            return xf, None
        except PermissionError:
            pass
        except OSError as e:
            if "Permission denied" not in str(e):
                raise

        tmp_dir = tempfile.mkdtemp(prefix="nprtool_")
        tmp_path = os.path.join(tmp_dir, os.path.basename(path))
        shutil.copy2(path, tmp_path)
        xf = pd.ExcelFile(tmp_path)
        setattr(xf, "_nprtool_safe_path", tmp_path)
        return xf, tmp_dir

    # =====================================================
    # HEADER RENAMING INTO CANONICAL SCHEMA
    # =====================================================
    @staticmethod
    def _to_canonical_erp_dataframe(df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        rename = {
            "itemnum": "ItemNumber",
            "desc": "Description",
            "primaryvendornumber": "PrimaryVendorNumber",
            "vendoritem": "VendorItem",
            "mfgid": "ManufacturerId",
            "mfgname": "ManufacturerName",
            "mfgitemcount": "ManufacturerItemCount",
            "lastcost": "LastCost",
            "standard_cost": "StandardCost",
            "average_cost": "AverageCost",
            "revision": "Revision",
            "itemleadtime": "ItemLeadTime",
            "defaultwhse": "DefaultWhse",
            "totalqty": "TotalQty",
        }
        out = out.rename(columns={k: v for k, v in rename.items() if k in out.columns})
        for col in ERP_EXPECTED_HEADERS:
            if col not in out.columns:
                out[col] = ""
        return out

    @staticmethod
    def _to_canonical_master_dataframe(df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        rename = {
            "itemnum": "ItemNumber",
            "desc": "Description",
            "active": "Active",
            "mfgid": "ManufacturerId",
            "mfgname": "ManufacturerName",
            "mfgpn": "ManufacturerPartNumber",
            "tariff_code": "TariffCode",
            "tariff_rate": "TariffRate",
            "last_cost": "LastCost",
            "standard_cost": "StandardCost",
            "average_cost": "AverageCost",
        }
        out = out.rename(columns={k: v for k, v in rename.items() if k in out.columns})
        for col in MASTER_EXPECTED_HEADERS:
            if col not in out.columns:
                out[col] = ""
        return out

    # =====================================================
    # INVENTORY LOADERS
    # =====================================================
    @staticmethod
    def load_erp_rows(path: str, sheet_name: int | str = 0) -> List[ERPInventoryRow]:
        raw = read_excel_with_temp_copy(path, sheet_name=sheet_name, header=None, dtype=str)
        header_row = DataLoader._find_header_row_by_keywords(
            raw, required_keywords=["item", "description", "vendor", "qty"], scan_rows=80
        )
        df = read_excel_with_temp_copy(path, sheet_name=sheet_name, header=header_row, dtype=str).fillna("")
        df = DataLoader._normalize_and_alias_columns(df, DEFAULT_INV_ALIASES)
        df = DataLoader._to_canonical_erp_dataframe(df)
        df = DataLoader.clean_dataframe(df)
        DataLoader.validate_headers(df, ERP_EXPECTED_HEADERS, "ERP")
        df = DataLoader.add_key_and_hash(
            df,
            key_columns=["ItemNumber", "VendorItem", "ManufacturerId", "ManufacturerName"],
            hash_columns=ERP_EXPECTED_HEADERS,
        )
        df = DataLoader.dedupe_incoming(df)

        rows: List[ERPInventoryRow] = []
        for record in df.to_dict(orient="records"):
            item_number = safe_str(record.get("ItemNumber"))
            description = safe_str(record.get("Description"))
            vendor_item = DataLoader.clean_mpn(safe_str(record.get("VendorItem")))
            if not item_number and not vendor_item and not description:
                continue

            record = dict(record)
            rows.append(
                ERPInventoryRow(
                    item_number=item_number,
                    description=description,
                    primary_vendor_number=safe_str(record.get("PrimaryVendorNumber")),
                    vendor_item=vendor_item,
                    manufacturer_id=safe_str(record.get("ManufacturerId")),
                    manufacturer_name=safe_str(record.get("ManufacturerName")),
                    manufacturer_item_count=DataLoader._safe_float(record.get("ManufacturerItemCount")),
                    last_cost=DataLoader._safe_float(record.get("LastCost")),
                    standard_cost=DataLoader._safe_float(record.get("StandardCost")),
                    average_cost=DataLoader._safe_float(record.get("AverageCost")),
                    revision=safe_str(record.get("Revision")),
                    item_lead_time=DataLoader._safe_float(record.get("ItemLeadTime")),
                    default_whse=safe_str(record.get("DefaultWhse")),
                    total_qty=DataLoader._safe_float(record.get("TotalQty")),
                    source_row_key=safe_str(record.get("source_row_key")),
                    row_hash=safe_str(record.get("row_hash")),
                    raw_fields=record,
                )
            )
        return rows

    @staticmethod
    def load_alternate_master_rows(path: str, sheet_name: int | str = 0) -> List[AlternateMasterRow]:
        raw = read_excel_with_temp_copy(path, sheet_name=sheet_name, header=None, dtype=str)
        header_row = DataLoader._find_header_row_by_keywords(
            raw, required_keywords=["item", "description", "manufacturer", "pn"], scan_rows=80
        )
        df = read_excel_with_temp_copy(path, sheet_name=sheet_name, header=header_row, dtype=str).fillna("")
        df = DataLoader._normalize_and_alias_columns(df, DEFAULT_ALT_ALIASES)
        df = DataLoader._to_canonical_master_dataframe(df)
        df = DataLoader.clean_dataframe(df)
        DataLoader.validate_headers(df, MASTER_EXPECTED_HEADERS, "MASTER")
        df = DataLoader.add_key_and_hash(
            df,
            key_columns=["ItemNumber", "ManufacturerPartNumber", "ManufacturerId", "ManufacturerName"],
            hash_columns=MASTER_EXPECTED_HEADERS,
        )
        DataLoader.write_duplicate_source_row_key_report(df, path)

        rows: List[AlternateMasterRow] = []
        for record in df.to_dict(orient="records"):
            item_number = safe_str(record.get("ItemNumber"))
            description = safe_str(record.get("Description"))
            mfgpn = DataLoader.clean_mpn(safe_str(record.get("ManufacturerPartNumber")))
            if not item_number and not mfgpn and not description:
                continue

            record = dict(record)
            rows.append(
                AlternateMasterRow(
                    item_number=item_number,
                    description=description,
                    active=safe_str(record.get("Active")),
                    manufacturer_id=safe_str(record.get("ManufacturerId")),
                    manufacturer_name=safe_str(record.get("ManufacturerName")),
                    manufacturer_part_number=mfgpn,
                    tariff_code=safe_str(record.get("TariffCode")),
                    tariff_rate=DataLoader._safe_float(record.get("TariffRate")),
                    last_cost=DataLoader._safe_float(record.get("LastCost")),
                    standard_cost=DataLoader._safe_float(record.get("StandardCost")),
                    average_cost=DataLoader._safe_float(record.get("AverageCost")),
                    source_row_key=safe_str(record.get("source_row_key")),
                    row_hash=safe_str(record.get("row_hash")),
                    raw_fields=record,
                )
            )
        return rows

    # Compatibility names retained for older callers.
    load_erp_inventory_rows = load_erp_rows
    load_master_inventory_rows = load_alternate_master_rows

    # =====================================================
    # COMPANY PART BUNDLING
    # =====================================================
    @staticmethod
    def build_company_part_records(
        erp_rows: List[ERPInventoryRow],
        master_rows: List[AlternateMasterRow],
    ) -> List[CompanyPartRecord]:
        grouped: Dict[str, CompanyPartRecord] = {}
        manufacturer_index: Dict[tuple[str, str, str], ManufacturerPartRecord] = {}

        def ensure_company(item_number: str) -> CompanyPartRecord:
            key = safe_str(item_number)
            rec = grouped.get(key)
            if rec is None:
                rec = CompanyPartRecord(company_part_number=key)
                grouped[key] = rec
            return rec

        for erp in erp_rows:
            cpn = safe_str(erp.item_number)
            if not cpn:
                continue

            company = ensure_company(cpn)
            if erp.description and not company.description:
                company.description = erp.description
            if erp.default_whse and not company.default_whse:
                company.default_whse = erp.default_whse
            if erp.revision and not company.revision:
                company.revision = erp.revision
            if erp.primary_vendor_number and not company.primary_vendor_number:
                company.primary_vendor_number = erp.primary_vendor_number
            company.total_qty = float(company.total_qty or 0) + float(erp.total_qty or 0)
            company.raw_fields.setdefault("erp", []).append(dict(erp.raw_fields or {}))

            if erp.vendor_item or erp.manufacturer_name or erp.manufacturer_id:
                key = (
                    cpn,
                    safe_str(erp.vendor_item),
                    safe_str(erp.manufacturer_id),
                )
                mp = manufacturer_index.get(key)
                if mp is None:
                    mp = ManufacturerPartRecord(
                        company_part_number=cpn,
                        manufacturer_part_number=safe_str(erp.vendor_item) or cpn,
                        manufacturer_id=safe_str(erp.manufacturer_id),
                        manufacturer_name=safe_str(erp.manufacturer_name),
                        description=erp.description,
                        item_lead_time=erp.item_lead_time,
                        last_cost=erp.last_cost,
                        standard_cost=erp.standard_cost,
                        average_cost=erp.average_cost,
                        is_erp_primary=True,
                        erp_source_row_key=safe_str((erp.raw_fields or {}).get("source_row_key")),
                        raw_fields=dict(erp.raw_fields or {}),
                    )
                    manufacturer_index[key] = mp
                    company.manufacturer_parts.append(mp)

        seen = set()
        dupes = []
        for row in master_rows:
            key = safe_str(getattr(row, "source_row_key", ""))
            if not key:
                continue
            if key in seen:
                dupes.append(key)
            seen.add(key)

        if dupes:
            print(f"[MASTER][DUPLICATE SOURCE_ROW_KEYS] duplicate rows preserved for reporting/resolution: {len(dupes)}")

        for master in master_rows:
            cpn = safe_str(master.item_number)
            if not cpn:
                continue

            company = ensure_company(cpn)
            if master.description and not company.description:
                company.description = master.description
            company.raw_fields.setdefault("master", []).append(dict(master.raw_fields or {}))

            mpn = safe_str(master.manufacturer_part_number)
            if not mpn and not master.manufacturer_name and not master.manufacturer_id:
                continue

            key = (cpn, mpn, safe_str(master.manufacturer_id))
            mp = manufacturer_index.get(key)
            if mp is None:
                mp = ManufacturerPartRecord(
                    company_part_number=cpn,
                    manufacturer_part_number=mpn or cpn,
                    manufacturer_id=safe_str(master.manufacturer_id),
                    manufacturer_name=safe_str(master.manufacturer_name),
                    description=master.description,
                    active=safe_str(master.active),
                    tariff_code=safe_str(master.tariff_code),
                    tariff_rate=master.tariff_rate,
                    last_cost=master.last_cost,
                    standard_cost=master.standard_cost,
                    average_cost=master.average_cost,
                    master_source_row_key=safe_str(master.source_row_key) or safe_str((master.raw_fields or {}).get("source_row_key")),
                    raw_fields=dict(master.raw_fields or {}),
                )
                manufacturer_index[key] = mp
                company.manufacturer_parts.append(mp)

        return list(grouped.values())

    # =====================================================
    # BOM / NPR LOADING
    # =====================================================
    @staticmethod
    def load_bom_any(path: str, sheet_name: int | str = 0, aliases: HeaderAliases = DEFAULT_NPR_ALIASES) -> List[NPRPart]:
        raw = read_excel_with_temp_copy(path, sheet_name=sheet_name, header=None, dtype=str)
        header_row = DataLoader._find_npr_header_row(raw, scan_rows=60)
        df = read_excel_with_temp_copy(path, sheet_name=sheet_name, header=header_row, dtype=str).fillna("")
        df = DataLoader._normalize_and_alias_columns(df, aliases)

        parts: List[NPRPart] = []
        for _, row in df.iterrows():
            partnum = safe_str(row.get("part_number", ""))
            desc = safe_str(row.get("item_description", ""))
            mfgname = safe_str(row.get("manufacturer_name", ""))
            mfgpn = DataLoader.clean_mpn(safe_str(row.get("manufacturer_part_", "")))
            supplier = safe_str(row.get("supplier", ""))
            quantity_raw = safe_str(row.get("quantity", ""))
            refdes = safe_str(row.get("designator", ""))

            qty: Optional[float] = None
            if quantity_raw:
                try:
                    qty = float(quantity_raw)
                except Exception:
                    qty = None

            if not any([partnum, desc, mfgname, mfgpn, supplier, quantity_raw, refdes]):
                continue

            raw_fields = {str(c): safe_str(row.get(c, "")) for c in df.columns}
            parsed = _parse_desc_fields(desc)
            item_type = safe_str((parsed or {}).get("type", "")) if isinstance(parsed, dict) else ""
            parts.append(
                NPRPart(
                    partnum=partnum,
                    desc=desc,
                    qty=qty,
                    refdes=refdes,
                    item_type=item_type,
                    mfgname=mfgname,
                    mfgpn=mfgpn,
                    supplier=supplier,
                    raw_fields=raw_fields,
                    parsed=parsed if isinstance(parsed, dict) else {},
                )
            )
        return parts

    # =====================================================
    # CNS WORKBOOK
    # =====================================================
    @staticmethod
    def _sheet_category(sheet_name: str) -> str:
        m = re.match(r"^\s*(\d{2})\b", str(sheet_name))
        return m.group(1) if m else ""

    @staticmethod
    def load_cns_workbook(path: str, aliases: HeaderAliases = DEFAULT_CNS_ALIASES) -> List[CNSRecord]:
        xf, tmp_dir = DataLoader._open_excel_file_with_temp_copy(path)
        safe_path = getattr(xf, "_nprtool_safe_path", path)

        try:
            records: List[CNSRecord] = []
            for sheet in xf.sheet_names:
                raw = read_excel_with_temp_copy(safe_path, sheet_name=sheet, header=None, dtype=str)
                header_row: Optional[int] = None
                try:
                    header_row = DataLoader._find_header_row_by_keywords(
                        raw, required_keywords=["prefix", "body"], scan_rows=120
                    )
                except Exception:
                    header_row = None

                cat = DataLoader._sheet_category(sheet)

                if header_row is not None:
                    df = read_excel_with_temp_copy(safe_path, sheet_name=sheet, header=header_row, dtype=str).fillna("")
                    df = DataLoader._normalize_and_alias_columns(df, aliases)
                    for _, row in df.iterrows():
                        prefix = safe_str(row.get("prefix", ""))
                        body = safe_str(row.get("body", ""))
                        if not prefix or not body:
                            continue
                        records.append(
                            CNSRecord(
                                prefix=prefix,
                                body=body,
                                suffix=safe_str(row.get("suffix", "")),
                                description=safe_str(row.get("description", "")),
                                sheet_name=str(sheet),
                                category=cat,
                                date=safe_str(row.get("date", "")),
                                initials=safe_str(row.get("initials", "")),
                                raw_fields={str(c): safe_str(row.get(c, "")) for c in df.columns},
                                parsed={},
                            )
                        )
                    continue

                seen_pb: set[tuple[str, str]] = set()
                max_rows = min(500, len(raw))
                max_cols = min(40, raw.shape[1] if raw is not None else 0)
                for i in range(max_rows):
                    row = raw.iloc[i, :max_cols].tolist()

                    for cell in row:
                        s = safe_str(cell)
                        if not s:
                            continue
                        m = PB_RE.search(s)
                        if m:
                            pair = (m.group(1), m.group(2))
                            if pair not in seen_pb:
                                seen_pb.add(pair)

                    for j in range(len(row) - 1):
                        left = safe_str(row[j])
                        right = safe_str(row[j + 1])
                        if PFX_RE.match(left) and BODY_RE.match(right):
                            pair = (left.strip(), right.strip())
                            if pair not in seen_pb:
                                seen_pb.add(pair)

                for prefix, body in sorted(seen_pb):
                    records.append(
                        CNSRecord(
                            prefix=prefix,
                            body=body,
                            suffix="",
                            description="",
                            sheet_name=str(sheet),
                            category=cat,
                            date="",
                            initials="",
                            raw_fields={},
                            parsed={},
                        )
                    )

            return records
        finally:
            try:
                xf.close()
            except Exception:
                pass
            if tmp_dir:
                shutil.rmtree(tmp_dir, ignore_errors=True)
