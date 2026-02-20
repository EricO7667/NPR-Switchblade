from __future__ import annotations

import os
import re
import shutil
import tempfile
import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

from .data_models import InventoryPart, NPRPart, CNSRecord, SubstitutePart


PB_RE = re.compile(r"\b(\d{2})\s*-\s*(\d{5})\b")
PFX_RE = re.compile(r"^\s*(\d{2})\s*$")
BODY_RE = re.compile(r"^\s*(\d{5})\s*$")

# 1 = always read a copied file (cached first), 0 = try direct then fallback
SAFE_SHARED_FILE_READS = 1

# =====================================================
# Small helpers
# =====================================================
def safe_str(x) -> str:
    """Convert pandas cell values into clean strings (never NaN)."""
    # If duplicate headers exist, pandas may return a Series here.
    if isinstance(x, pd.Series):
        # pick the first non-empty cell
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

    if pd.isna(x):
        return ""
    return str(x).strip()


def norm_header(h: str) -> str:
    """Normalize a header to snake_case-ish."""
    return str(h).strip().lower().replace("\n", " ").replace("\r", " ")


def norm_header_snake(h: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", norm_header(h)).strip("_")


# =====================================================
# SAFE EXCEL READS (copy-first, cache-first)
# =====================================================
def _excel_cache_root() -> str:
    """
    Where we store persistent safe copies.
    Default: %TEMP%/nprtool_excel_cache
    """
    root = os.path.join(tempfile.gettempdir(), "nprtool_excel_cache")
    os.makedirs(root, exist_ok=True)
    return root


def _file_fingerprint(path: str) -> str:
    """
    Fast fingerprint for cache key: basename + size + mtime_ns.
    Good enough to detect edits without hashing entire file.
    """
    p = Path(path)
    st = p.stat()
    raw = f"{p.name}|{st.st_size}|{st.st_mtime_ns}".encode("utf-8", errors="ignore")
    return hashlib.sha1(raw).hexdigest()[:16]


def _cached_copy_path(path: str) -> str:
    src = Path(path)
    fp = _file_fingerprint(path)
    # keep extension (.xlsx, .xlsm, etc.)
    return os.path.join(_excel_cache_root(), f"{src.stem}__{fp}{src.suffix}")


def _ensure_cached_copy(path: str) -> str:
    """
    Ensure a cached copy exists and return its path.
    Never opens the original in pandas/openpyxl; only copy2 touches it.
    """
    cached = _cached_copy_path(path)
    if not os.path.exists(cached):
        shutil.copy2(path, cached)
    return cached


def read_excel_with_temp_copy(path: str, **read_excel_kwargs) -> pd.DataFrame:
    """
    Excel reader that avoids touching shared files.

    Behavior:
      - If SAFE_SHARED_FILE_READS == 1:
          * Use a cached copy (persistent) if possible; create it if missing.
          * Read ONLY from the cached copy.
      - Else:
          * Try direct read, and if locked, fall back to a temp copy.
    """
    if SAFE_SHARED_FILE_READS:
        safe_path = _ensure_cached_copy(path)
        return pd.read_excel(safe_path, **read_excel_kwargs)

    # Legacy behavior (direct first, then temp fallback)
    try:
        return pd.read_excel(path, **read_excel_kwargs)
    except PermissionError:
        pass
    except OSError as e:
        # Windows sometimes throws OSError for locked files
        if "Permission denied" not in str(e):
            raise

    tmp_dir = tempfile.mkdtemp(prefix="nprtool_")
    tmp_path = os.path.join(tmp_dir, os.path.basename(path))
    shutil.copy2(path, tmp_path)
    return pd.read_excel(tmp_path, **read_excel_kwargs)


@dataclass(frozen=True)
class HeaderAliases:
    """
    Central alias map (scalable).
    Later: load this from config/yaml/user input.
    """
    aliases: Dict[str, str]


# =====================================================
# NPR/BOM Aliases TODO: figure out how to accomlish this is a way that inst just hardcoded exprected values
#   for now we simply have to add these in manually
# =====================================================
DEFAULT_NPR_ALIASES = HeaderAliases(
    aliases={
        # --- MPN variants ---
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
        # Manufacturer numbered columns
        "manufacturer_1": "manufacturer_name",
        "manufacturer1": "manufacturer_name",

        # MPN numbered columns (treat "1" as the primary MPN)
        "manufacturer_part_number_1": "manufacturer_part_",
        "manufacturer_part_number1": "manufacturer_part_",
        "mfrpn_1": "manufacturer_part_",
        "mpn_1": "manufacturer_part_",

        # Pre-found alternates (map explicit “2” columns to unique names so we can ingest them all)
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

        # --- Part number / row identity variants ---
        "elan_part_number": "part_number",
        "part_number_": "part_number",
        "part_number": "part_number",
        "p_n": "part_number",
        "pn": "part_number",
        "p_n_": "part_number",
        "part_no": "part_number",
        "part": "part_number",
        "pno": "part_number",

        # Often a stable BOM row ID
        "reference": "part_number",
        "refdes": "part_number",

        "designator": "designator",
        "footprint": "footprint",
        "subs_allowed": "subs_allowed",
        "name": "name",
        
        # --- Description variants ---
        "description": "item_description",
        "item_description": "item_description",
        "desc": "item_description",
        "title": "item_description",

        # --- Manufacturer / supplier variants ---
        "manufacturer": "manufacturer_name",
        "manufacturer_name": "manufacturer_name",
        "mfr_name": "manufacturer_name",
        "mfg_name": "manufacturer_name",

        "supplier_name": "supplier",
        "vendor": "supplier",
        "supplier": "supplier",
        "distributor": "supplier",

        # --- Quantity variants (kept as raw field, but normalize name anyway) ---
        "qty": "quantity",
        "quantity": "quantity",
        "qty4one": "quantity",

        "comment": "comment",
    }
)

# =====================================================
# ALTERNATES BASE SHEET (internal base item + many MFGPN rows)
# =====================================================
DEFAULT_ALT_ALIASES = HeaderAliases(
    aliases={
        # Your exact headers (normalized to snake by norm_header_snake)
        "item_number": "itemnum",
        "itemnumber": "itemnum",
        "item_num": "itemnum",

        "description": "desc",
        "desc": "desc",

        "active": "active",

        "mfg_id": "mfgid",
        "mfgid": "mfgid",

        "manufacturer_name": "mfgname",
        "manufacturer": "mfgname",
        "mfg_name": "mfgname",

        "manufacturer_pn": "mfgpn",
        "manufacturer_part_no": "mfgpn",
        "manufacturer_part_number": "mfgpn",
        "mpn": "mfgpn",
        # Common master inventory exports use VendorItem / Vendor Item for manufacturer PN
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




# =====================================================
# Inventory Aliases (NEW)
# =====================================================
DEFAULT_INV_ALIASES = HeaderAliases(
    aliases={
        # Internal PN
        "itemnumber": "itemnum",
        "item_number": "itemnum",
        "item_no": "itemnum",
        "item_num": "itemnum",
        "internal_part_number": "itemnum",
        "part_number": "itemnum",
        "pn": "itemnum",

        # Description
        "description": "desc",
        "desc": "desc",
        "item_description": "desc",

        # Manufacturer fields
        "mfgid": "mfgid",
        "mfg_id": "mfgid",
        "manufacturer_id": "mfgid",

        "mfgname": "mfgname",
        "mfg_name": "mfgname",
        "manufacturer": "mfgname",
        "manufacturer_name": "mfgname",

        # Manufacturer part number / vendor item
        "vendoritem": "vendoritem",
        "vendor_item": "vendoritem",
        "manufacturer_part_number": "vendoritem",
        "manufacturer_part_no": "vendoritem",
        "mpn": "vendoritem",
        "mfgpn": "vendoritem",
        "mfg_pn": "vendoritem",
    }
)

DEFAULT_CNS_ALIASES = HeaderAliases(
    aliases={
        # Prefix
        "prefix": "prefix",
        "pfx": "prefix",
        "pref": "prefix",

        # Body
        "body": "body",
        "base": "body",
        "series": "body",
        "number": "body",

        # Suffix
        "suffix": "suffix",
        "suf": "suffix",
        "rev": "suffix",
        "variant": "suffix",

        # Description
        "description": "description",
        "desc": "description",
        "item_description": "description",
        "title": "description",
        "comment": "description",

        # Date / Initials
        "date": "date",
        "created": "date",
        "created_date": "date",
        "initials": "initials",
        "init": "initials",
        "author": "initials",
    }
)


class DataLoader:
    """
    Loads Inventory and NPR/BOM Excel files and normalizes them into Part objects.

    The loader is intentionally *not* the place for matching logic.
    It should:
      - read files robustly
      - resolve headers
      - normalize strings
      - preserve raw_fields
      - attach parsed engineering data (optional but useful for UI)
    """

    # =====================================================
    # MPN CLEANER (handles Excel _x garbage)
    # =====================================================
    @staticmethod
    def clean_mpn(s: str) -> str:
        if not s:
            return ""
        s = str(s)
        s = re.sub(r"_x[0-9a-fA-F]{4}_", "", s)  # remove Excel encoding
        s = s.replace("\n", "").replace("\r", "").strip()
        return s

    # =====================================================
    # MPN KEY NORMALIZER (for alternates lookups)
    # =====================================================
    @staticmethod
    def norm_mpn_key(s: str) -> str:
        """
        Normalization for MFGPN lookups.
        - Uses existing clean_mpn to strip Excel _x####_ junk.
        - Uppercases.
        - Removes whitespace so 'ERJ 3EKF...' matches 'ERJ-3EKF...'
        """
        s = DataLoader.clean_mpn(s or "")
        s = re.sub(r"\s+", "", s).upper()
        return s

    # =====================================================
    # HEADER FINDERS
    # =====================================================
    @staticmethod
    def _find_header_row_by_keywords(df: pd.DataFrame, required_keywords: List[str], scan_rows: int = 20) -> int:
        """
        Find a header row by scanning early rows for keywords.
        Works when header is offset by merged cells / title blocks.
        """
        required = [k.lower() for k in required_keywords]
        for i in range(min(scan_rows, len(df))):
            row = df.iloc[i].astype(str).str.lower()
            row_str = " ".join(row)
            if any(k in row_str for k in required):
                return i
        raise ValueError("Could not locate a header row.")

    @staticmethod
    def _find_npr_header_row(raw: pd.DataFrame, scan_rows: int = 40) -> int:
        """
        Detect a likely header row by counting keyword hits.
        """
        header_keywords = ["mfg", "part", "desc", "manufacturer", "item", "mpn", "qty"]
        best_row = None
        best_hits = 0
        for i in range(min(scan_rows, len(raw))):
            row = raw.iloc[i].astype(str).str.lower().str.strip()
            joined = " ".join(row)
            hits = sum(k in joined for k in header_keywords)
            if hits > best_hits:
                best_hits = hits
                best_row = i
        if best_row is None or best_hits == 0:
            raise ValueError("No header row found with recognizable keywords.")
        return best_row

    # =====================================================
    # CNS WORKBOOK (multi-sheet)
    # =====================================================
    @staticmethod
    def _open_excel_file_with_temp_copy(path: str) -> tuple[pd.ExcelFile, str | None]:
        """
        For opening the workbook so we can list sheet names.
        Returns (ExcelFile, tmp_dir_to_cleanup_or_None).

        IMPORTANT:
          - When SAFE_SHARED_FILE_READS=1, we open the CACHED COPY and attach
            the path we opened on xf._nprtool_safe_path, so callers can read sheets
            from the same safe file (never the original).
        """
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

    @staticmethod
    def _sheet_category(sheet_name: str) -> str:
        """
        Best-effort parse of a leading category number like '00', '01', ... '99' from sheet name.
        Examples: '00 - Resistors' -> '00', '12Capacitors' -> '12'
        """
        m = re.match(r"^\s*(\d{2})\b", str(sheet_name))
        return m.group(1) if m else ""

    @staticmethod
    def load_cns_workbook(path: str, aliases: HeaderAliases = DEFAULT_CNS_ALIASES) -> List[CNSRecord]:
        """
        Load CNS workbook across all sheets.
        We primarily care about Prefix + Body; suffix/description may often be blank or absent.

        Works even if:
          - header row is offset
          - columns out of order
          - some sheets have no clean header: we fallback to scanning for prefix/body patterns
        """
        xf, tmp_dir = DataLoader._open_excel_file_with_temp_copy(path)
        safe_path = getattr(xf, "_nprtool_safe_path", path)

        try:
            records: List[CNSRecord] = []

            for sheet in xf.sheet_names:
                raw = read_excel_with_temp_copy(safe_path, sheet_name=sheet, header=None, dtype=str)

                header_row = None
                try:
                    # only require Prefix + Body now
                    header_row = DataLoader._find_header_row_by_keywords(
                        raw,
                        required_keywords=["prefix", "body"],
                        scan_rows=120,
                    )
                except Exception:
                    header_row = None

                # -------------------------
                # Path A: header-based read
                # -------------------------
                if header_row is not None:
                    df = read_excel_with_temp_copy(safe_path, sheet_name=sheet, header=header_row, dtype=str).fillna("")
                    df = DataLoader._normalize_and_alias_columns(df, aliases)

                    cat = DataLoader._sheet_category(sheet)

                    for _, row in df.iterrows():
                        def get(name: str) -> str:
                            return safe_str(row.get(name, ""))

                        prefix = get("prefix")
                        body = get("body")

                        # If we don't have prefix/body, skip
                        if not prefix or not body:
                            continue

                        # We don't care if suffix/desc are missing; keep them if present
                        suffix = get("suffix")
                        desc = get("description")

                        records.append(
                            CNSRecord(
                                prefix=prefix,
                                body=body,
                                suffix=suffix,
                                description=desc,
                                sheet_name=str(sheet),
                                category=cat,
                                date=get("date"),
                                initials=get("initials"),
                                raw_fields={c: safe_str(row.get(c, "")) for c in df.columns},
                                parsed={},
                            )
                        )

                    continue  # done with this sheet

                # -------------------------
                # Path B: fallback scan (no header)
                # -------------------------
                cat = DataLoader._sheet_category(sheet)

                # We'll collect unique prefix-body pairs from the sheet
                seen_pb = set()

                # scan first N rows/cols (fast, sufficient)
                max_rows = min(500, len(raw))
                max_cols = min(40, raw.shape[1] if raw is not None else 0)

                for i in range(max_rows):
                    row = raw.iloc[i, :max_cols].tolist()

                    # 1) Look for combined "NN-NNNNN" in any cell
                    for cell in row:
                        s = safe_str(cell)
                        if not s:
                            continue
                        m = PB_RE.search(s)
                        if m:
                            pb = (m.group(1), m.group(2))
                            if pb not in seen_pb:
                                seen_pb.add(pb)

                    # 2) Look for separate prefix + body in the same row
                    pfx = None
                    body = None
                    for cell in row:
                        s = safe_str(cell)
                        if not s:
                            continue
                        if pfx is None:
                            mp = PFX_RE.match(s)
                            if mp:
                                pfx = mp.group(1)
                                continue
                        if body is None:
                            mb = BODY_RE.match(s)
                            if mb:
                                body = mb.group(1)
                                continue
                    if pfx and body:
                        pb = (pfx, body)
                        if pb not in seen_pb:
                            seen_pb.add(pb)

                # Emit records with empty suffix/description (by design)
                for pfx, body in seen_pb:
                    records.append(
                        CNSRecord(
                            prefix=pfx,
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

            print(f" Loaded {len(records)} CNS records from {path}")
            return records

        finally:
            if tmp_dir and os.path.isdir(tmp_dir):
                try:
                    shutil.rmtree(tmp_dir, ignore_errors=True)
                except Exception:
                    pass

    # ===========================================================
    # Make columns unique after aliasing (prevents this everywhere)
    # ===========================================================
    @staticmethod
    def _make_unique_columns(df: pd.DataFrame) -> pd.DataFrame:
        """
        If aliasing creates duplicate column names (common), make them unique by suffixing.
        Example: item_description, item_description -> item_description, item_description_2
        """
        cols = list(df.columns)
        counts = {}
        new_cols = []
        for c in cols:
            base = str(c)
            n = counts.get(base, 0) + 1
            counts[base] = n
            new_cols.append(base if n == 1 else f"{base}_{n}")
        df = df.copy()
        df.columns = new_cols
        return df

    # =====================================================
    # INTERNAL: normalize + alias dataframe columns
    # =====================================================
    @staticmethod
    def _normalize_and_alias_columns(df: pd.DataFrame, aliases: HeaderAliases) -> pd.DataFrame:
        df = df.copy()
        df.columns = [norm_header_snake(c) for c in df.columns]
        df.columns = [aliases.aliases.get(c, c) for c in df.columns]
        df = DataLoader._make_unique_columns(df)
        return df

    # =====================================================
    # INVENTORY SHEET
    # =====================================================
    @staticmethod
    def load_inventory(path: str, aliases: HeaderAliases = DEFAULT_INV_ALIASES) -> List[InventoryPart]:
        """
        Load inventory file and return a list of InventoryPart objects.
        Robust against header naming changes via normalization + aliasing.
        """
        raw = read_excel_with_temp_copy(path, header=None, dtype=str)

        header_row = DataLoader._find_header_row_by_keywords(
            raw,
            required_keywords=["item", "desc", "vendor", "mfg", "manufacturer"],
            scan_rows=30,
        )

        df = read_excel_with_temp_copy(path, header=header_row, dtype=str).fillna("")
        df = DataLoader._normalize_and_alias_columns(df, aliases)

        # Minimal sanity: must have an internal PN or vendor item or description
        has_any_id = any(c in df.columns for c in ["itemnum", "vendoritem", "desc"])
        if not has_any_id:
            raise ValueError(f" Inventory sheet missing core columns. Detected: {list(df.columns)}")

        inventory_parts: List[InventoryPart] = []
        for _, row in df.iterrows():
            raw_fields = {col: safe_str(row.get(col, "")) for col in df.columns}

            itemnum = safe_str(row.get("itemnum", ""))
            desc = safe_str(row.get("desc", ""))
            vendoritem = DataLoader.clean_mpn(safe_str(row.get("vendoritem", "")))

            # Skip blank-enough inventory rows (prevents garbage candidates)
            if not itemnum and not vendoritem and not desc:
                continue

            inv = InventoryPart(
                itemnum=itemnum,
                desc=desc,
                mfgid=safe_str(row.get("mfgid", "")),
                mfgname=safe_str(row.get("mfgname", "")),
                vendoritem=vendoritem,
                raw_fields=raw_fields,
                parsed={},
            )
            inventory_parts.append(inv)

        return inventory_parts



    @staticmethod
    def load_master_inventory(
        path: str,
        *,
        inv_aliases: HeaderAliases = DEFAULT_INV_ALIASES,
        alt_aliases: HeaderAliases = DEFAULT_ALT_ALIASES,
    ) -> tuple[List[InventoryPart], Dict[str, List[SubstitutePart]], Dict[str, List[str]]]:
        """
        Load the **master inventory** sheet (previously 'alternates' sheet) and convert it into:

          1) A *deduplicated* inventory list keyed by **company/internal item number** (itemnum)
             - exactly one InventoryPart per itemnum (the first row becomes the "representative")
          2) A substitutes mapping (base_itemnum -> [SubstitutePart...]) built from additional rows
             for the same itemnum, each with a different manufacturer part number (mfgpn)
          3) An MPN index (normalized_mfgpn -> [base_itemnum...]) for deterministic resolution and
             conflict detection (same MPN appears under multiple base items).

        This makes alternates tied to **company part number (itemnum)**, NOT grouped under a single MPN.
        """
        # Read raw to find headers robustly (the master sheet may not have row-0 headers)
        raw = read_excel_with_temp_copy(path, header=None, dtype=str)

        # Prefer the alternates-style header search (item/description/manufacturer/pn)
        header_row = DataLoader._find_header_row_by_keywords(
            raw,
            required_keywords=["item", "description", "manufacturer", "pn"],
            scan_rows=50,
        )

        df = read_excel_with_temp_copy(path, header=header_row, dtype=str).fillna("")
        df = DataLoader._normalize_and_alias_columns(df, alt_aliases)

        if "itemnum" not in df.columns or "mfgpn" not in df.columns:
            raise ValueError(f"Master inventory missing required columns. Detected: {list(df.columns)}")

        inv_by_item: Dict[str, InventoryPart] = {}
        subs_by_base: Dict[str, List[SubstitutePart]] = {}
        mpn_to_base: Dict[str, List[str]] = {}

        for _, row in df.iterrows():
            base_item = safe_str(row.get("itemnum", "")).strip()
            desc = safe_str(row.get("desc", "")).strip()
            mfgpn = DataLoader.clean_mpn(safe_str(row.get("mfgpn", "")).strip())
            mfgname = safe_str(row.get("mfgname", "")).strip()
            mfgid = safe_str(row.get("mfgid", "")).strip()

            if not base_item:
                continue

            if base_item not in inv_by_item:
                rep_vendoritem = mfgpn or ""
                raw_fields = {str(k): safe_str(v) for k, v in dict(row).items()}
                inv_by_item[base_item] = InventoryPart(
                    itemnum=base_item,
                    desc=desc,
                    mfgid=mfgid,
                    mfgname=mfgname,
                    vendoritem=rep_vendoritem,
                    raw_fields=raw_fields,
                    parsed={},
                )
                subs_by_base.setdefault(base_item, [])

            # Treat additional MPN rows as substitutes (but not the representative MPN)
            if mfgpn:
                rep = (inv_by_item[base_item].vendoritem or "").strip()
                if not rep:
                    inv_by_item[base_item].vendoritem = mfgpn
                    rep = mfgpn

                if DataLoader.norm_mpn_key(mfgpn) != DataLoader.norm_mpn_key(rep):
                    sub = SubstitutePart(
                        base_itemnum=base_item,
                        sub_itemnum="",
                        description=desc,
                        mfgpn=mfgpn,
                        notes=(mfgname or "").strip(),
                    )
                    subs_by_base.setdefault(base_item, []).append(sub)
                    try:
                        inv_by_item[base_item].add_substitute(sub)
                    except Exception:
                        inv_by_item[base_item].substitutes.append(sub)

                key = DataLoader.norm_mpn_key(mfgpn)
                if key:
                    mpn_to_base.setdefault(key, [])
                    if base_item not in mpn_to_base[key]:
                        mpn_to_base[key].append(base_item)

        # Also index the representative MPN for each base item
        for base_item, inv in inv_by_item.items():
            rep = (getattr(inv, "vendoritem", "") or "").strip()
            key = DataLoader.norm_mpn_key(rep)
            if key:
                mpn_to_base.setdefault(key, [])
                if base_item not in mpn_to_base[key]:
                    mpn_to_base[key].append(base_item)

        return list(inv_by_item.values()), subs_by_base, mpn_to_base

    @staticmethod
    def load_alternates_db(
        path: str,
        aliases: HeaderAliases = DEFAULT_ALT_ALIASES
    ) -> tuple[Dict[str, List[SubstitutePart]], Dict[str, List[str]]]:
        """
        Load the alternates base sheet.

        Returns:
          subs_by_base: base_itemnum -> [SubstitutePart...]
          mpn_to_base:  normalized_mfgpn -> [base_itemnum,...]  (list to detect conflicts)
        """
        raw = read_excel_with_temp_copy(path, header=None, dtype=str)

        header_row = DataLoader._find_header_row_by_keywords(
            raw,
            required_keywords=["item", "description", "manufacturer", "pn"],
            scan_rows=50,
        )

        df = read_excel_with_temp_copy(path, header=header_row, dtype=str).fillna("")
        df = DataLoader._normalize_and_alias_columns(df, aliases)

        # sanity
        if "itemnum" not in df.columns or "mfgpn" not in df.columns:
            raise ValueError(f"Alternates DB missing required columns. Detected: {list(df.columns)}")

        subs_by_base: Dict[str, List[SubstitutePart]] = {}
        mpn_to_base: Dict[str, List[str]] = {}

        for _, row in df.iterrows():
            base_item = safe_str(row.get("itemnum", "")).strip()
            desc = safe_str(row.get("desc", "")).strip()
            mfgpn = safe_str(row.get("mfgpn", "")).strip()
            mfgname = safe_str(row.get("mfgname", "")).strip()

            if not base_item or not mfgpn:
                continue
            
            # Create a SubstitutePart entry (sub_itemnum is unknown in this sheet, so keep "")
            sub = SubstitutePart(
                base_itemnum=base_item,
                sub_itemnum="",
                description=desc,
                mfgpn=mfgpn,
                notes=(mfgname or "").strip(),
            )
            subs_by_base.setdefault(base_item, []).append(sub)

            key = DataLoader.norm_mpn_key(mfgpn)
            if key:
                mpn_to_base.setdefault(key, [])
                if base_item not in mpn_to_base[key]:
                    mpn_to_base[key].append(base_item)

        return subs_by_base, mpn_to_base


    # =====================================================
    # NEW (v2 schema): Inventory Company Snapshot Builders
    # =====================================================
    @staticmethod
    def load_erp_stock_totals(path: str) -> Dict[str, int]:
        """Load ERP inventory sheet and return {itemnum/cpn -> stock_total}.

        This function is intentionally defensive: ERP extracts vary a lot.
        We detect the header row by keywords and then look for a likely
        stock/on-hand column.
        """
        raw = read_excel_with_temp_copy(path, header=None, dtype=str)

        header_row = DataLoader._find_header_row_by_keywords(
            raw,
            required_keywords=["item"],
            scan_rows=50,
        )

        df = read_excel_with_temp_copy(path, header=header_row, dtype=str).fillna("")
        df.columns = [norm_header_snake(c) for c in df.columns]
        df = DataLoader._make_unique_columns(df)

        # Identify the CPN/itemnum column
        item_col = None
        for c in df.columns:
            if c in ("itemnum","ItemNumber", "item_number", "itemnumber"):
                item_col = c
                break
        if item_col is None:
            # fallback: first column containing 'item'
            for c in df.columns:
                if "item" in str(c):
                    item_col = c
                    break
        if item_col is None:
            raise ValueError(f"ERP inventory: could not identify item number column. Columns={list(df.columns)}")

        # Identify a stock/on-hand column
        stock_col = None
        candidates = [
            "TotalQty", "qty", "Total_Qty"
        ]
        for c in candidates:
            if c in df.columns:
                stock_col = c
                break
        if stock_col is None:
            # fuzzy: any column containing both qty/quantity and hand/avail/stock
            for c in df.columns:
                lc = str(c).lower()
                if ("qty" in lc or "quantity" in lc):
                    stock_col = c
                    break
        if stock_col is None:
            raise ValueError(f"ERP inventory: could not identify stock/on-hand column. Columns={list(df.columns)}")

        out: Dict[str, int] = {}
        for _, row in df.iterrows():
            itemnum = safe_str(row.get(item_col, "")).strip()
            if not itemnum:
                continue
            raw_qty = safe_str(row.get(stock_col, ""))
            try:
                qty = int(float(raw_qty)) if raw_qty else 0
            except Exception:
                qty = 0
            out[itemnum] = out.get(itemnum, 0) + qty
        return out

    @staticmethod
    def build_inventory_company_parts(
        master_inventory_path: str,
        *,
        erp_inventory_path: Optional[str] = None,
        alt_aliases: HeaderAliases = DEFAULT_ALT_ALIASES,
    ) -> tuple[List[dict], List[InventoryPart]]:
        """Build inventory_company rows for DB persistence + a flat InventoryPart list for matching.

        Returns:
          company_parts_rows: list[dict] matching InventoryCompanyRepo.upsert_company_parts() contract
          flat_inventory:     list[InventoryPart] (one per alternate) for MatchingEngine compatibility
        """
        raw = read_excel_with_temp_copy(master_inventory_path, header=None, dtype=str)

        header_row = DataLoader._find_header_row_by_keywords(
            raw,
            # include vendor/item wording because many master exports use VendorItem instead of Manufacturer PN
            required_keywords=["item", "description", "manufacturer", "pn", "vendor"],
            scan_rows=50,
        )

        df = read_excel_with_temp_copy(master_inventory_path, header=header_row, dtype=str).fillna("")
        df = DataLoader._normalize_and_alias_columns(df, alt_aliases)

        # Fallback normalization: some master inventory extracts label the MPN column as VendorItem.
        # If aliasing did not create mfgpn, map vendoritem/vendor_item style columns into mfgpn.
        if "mfgpn" not in df.columns:
            for c in list(df.columns):
                lc = str(c).strip().lower()
                if lc in ("vendoritem", "vendor_item", "vendor_part_number"):
                    df = df.copy()
                    df["mfgpn"] = df[c]
                    break

        if "itemnum" not in df.columns:
            raise ValueError(f"Master inventory missing itemnum column. Detected: {list(df.columns)}")

        # Optional ERP stock totals
        stock_totals: Dict[str, int] = {}
        if erp_inventory_path:
            try:
                stock_totals = DataLoader.load_erp_stock_totals(erp_inventory_path)
            except Exception as e:
                print(f"[WARN] ERP stock load failed ({erp_inventory_path}): {e}")

        grouped: Dict[str, dict] = {}
        flat: List[InventoryPart] = []

        def _as_float(x: str) -> Optional[float]:
            x = (x or "").strip()
            if not x:
                return None
            try:
                return float(x)
            except Exception:
                return None

        for _, row in df.iterrows():
            cpn = safe_str(row.get("itemnum", "")).strip()
            if not cpn:
                continue

            desc = safe_str(row.get("desc", "")).strip()
            mfgname = safe_str(row.get("mfgname", "")).strip()
            mfgid = safe_str(row.get("mfgid", "")).strip()
            mpn = DataLoader.clean_mpn(safe_str(row.get("mfgpn", "") or row.get("vendoritem", "")))

            tariff_code = safe_str(row.get("tariff_code", "")).strip()
            tariff_rate = _as_float(safe_str(row.get("tariff_rate", "")))
            last_cost = _as_float(safe_str(row.get("last_cost", "")))
            standard_cost = _as_float(safe_str(row.get("standard_cost", "")))
            average_cost = _as_float(safe_str(row.get("average_cost", "")))

            if cpn not in grouped:
                grouped[cpn] = {
                    "cpn": cpn,
                    "canonical_desc": desc,
                    "stock_total": int(stock_totals.get(cpn, 0) or 0),
                    "alternates": [],
                }
            else:
                if desc and not grouped[cpn].get("canonical_desc"):
                    grouped[cpn]["canonical_desc"] = desc

            if mpn or mfgname or mfgid:
                grouped[cpn]["alternates"].append(
                    {
                        "mfgname": mfgname,
                        "mfgid": mfgid,
                        "mpn": mpn,
                        # keep both “current” and “last” price fields if available
                        "unit_price": average_cost if average_cost is not None else last_cost,
                        "last_unit_price": last_cost,
                        "standard_cost": standard_cost,
                        "average_cost": average_cost,
                        "tariff_code": tariff_code,
                        "tariff_rate": tariff_rate,
                        "meta": {k: safe_str(row.get(k, "")) for k in df.columns},
                    }
                )

                # Flat inventory record for matching (legacy-compatible)
                flat.append(
                    InventoryPart(
                        itemnum=cpn,
                        desc=desc,
                        mfgid=mfgid,
                        mfgname=mfgname,
                        vendoritem=mpn,
                        raw_fields={k: safe_str(row.get(k, "")) for k in df.columns},
                        parsed={},
                    )
                )

        # If ERP stock was provided, ensure all known stock-only parts exist too
        for cpn, qty in (stock_totals or {}).items():
            if cpn not in grouped:
                grouped[cpn] = {
                    "cpn": cpn,
                    "canonical_desc": "",
                    "stock_total": int(qty or 0),
                    "alternates": [],
                }
            else:
                grouped[cpn]["stock_total"] = int(qty or 0)

        return list(grouped.values()), flat


    # =====================================================
    # NPR / BOM SHEET
    # =====================================================
    @staticmethod
    def load_npr(path: str, aliases: HeaderAliases = DEFAULT_NPR_ALIASES) -> List[NPRPart]:
        """
        Robust loader for BOM/NPR sheets:
        - detects offset headers
        - normalizes headers to snake_case
        - applies alias mapping
        - preserves raw_fields
        - ingests multiple MPN columns (pre-found alternates)
           deterministic: primary is manufacturer_part_, alternates are manufacturer_part_2+
        - guarantees unique partnum fallback (ROW-#)
        - drops blank-enough rows
        """
        raw = read_excel_with_temp_copy(path, header=None, dtype=str)

        try:
            header_row = DataLoader._find_npr_header_row(raw)
        except Exception as e:
            print(f" Header not found: {e}. Defaulting to row 0.")
            header_row = 0

        df = read_excel_with_temp_copy(path, header=header_row, dtype=str).fillna("")

        # Retry if mostly unnamed columns
        unnamed_ratio = sum(str(c).lower().startswith("unnamed") for c in df.columns) / max(1, len(df.columns))
        if unnamed_ratio > 0.5 and header_row < len(raw) - 1:
            print(f" Mostly unnamed columns at row {header_row}, retrying with next row...")
            header_row += 1
            df = read_excel_with_temp_copy(path, header=header_row, dtype=str).fillna("")

        # Normalize + alias headers (+ make unique in your _normalize_and_alias_columns)
        df = DataLoader._normalize_and_alias_columns(df, aliases)

        print(f" Normalized Columns: {list(df.columns)}")

        # Validate minimal required fields
        has_desc = any("item_description" == c for c in df.columns)
        has_mpn = any(str(c).startswith("manufacturer_part_") for c in df.columns)
        if not has_desc and not has_mpn:
            raise ValueError(
                " Unable to find Description or Manufacturer Part columns in sheet.\n"
                f"Detected columns: {list(df.columns)}"
            )

        # --- Patch B v2: deterministic primary + alternates ---
        primary_col = "manufacturer_part_" if "manufacturer_part_" in df.columns else None

        alt_cols = [
            c for c in df.columns
            if str(c).startswith("manufacturer_part_") and c != "manufacturer_part_"
        ]

        def _mpn_suffix_key(col: str) -> int:
            # manufacturer_part_2 < manufacturer_part_10
            m = re.search(r"manufacturer_part_(\d+)$", str(col))
            return int(m.group(1)) if m else 999999

        alt_cols = sorted(alt_cols, key=_mpn_suffix_key)

        npr_parts: List[NPRPart] = []
        for i, (_, row) in enumerate(df.iterrows(), start=1):

            def get(name: str) -> str:
                return safe_str(row.get(name, ""))

            # Prefer Description; fall back to Comment if you keep that column separate
            desc = get("item_description") or get("comment")

            primary_mpn = DataLoader.clean_mpn(get(primary_col)) if primary_col else ""
            alt_mpns = [DataLoader.clean_mpn(get(c)) for c in alt_cols]
            alt_mpns = [m for m in alt_mpns if m]

            # guarantee a unique identifier for node creation downstream
            partnum = get("part_number") or f"ROW-{i}"

            # drop blank-enough BOM rows
            if not desc and not primary_mpn and not alt_mpns:
                continue

            parsed = {}
            if alt_mpns:
                parsed["mpn_alts"] = alt_mpns

            npr = NPRPart(
                partnum=partnum,
                desc=desc,
                mfgname=get("manufacturer_name"),
                mfgpn=primary_mpn,
                supplier=get("supplier"),
                raw_fields={c: safe_str(row.get(c, "")) for c in df.columns},
                parsed=parsed,
            )
            npr_parts.append(npr)

        print(f" Loaded {len(npr_parts)} NPR/BOM parts successfully from {path}")
        return npr_parts

    # =====================================================
    # wrapper that falls back automatically
    # =====================================================
    @staticmethod
    def load_bom_any(path: str, aliases: HeaderAliases = DEFAULT_NPR_ALIASES) -> List[NPRPart]:
        """
        Try the full NPR/BOM loader; if it can't parse core columns,
        fallback to the simple parts-list loader.
        """
        try:
            return DataLoader.load_npr(path, aliases=aliases)
        except Exception as e:
            print(f"load_npr failed ({e}). Falling back to load_simple_parts_list...")
            return DataLoader.load_simple_parts_list(path)

    # =====================================================
    # SIMPLE 2-COLUMN PARTS LIST 
    # =====================================================
    @staticmethod
    def load_simple_parts_list(path: str) -> List[NPRPart]:
        """
        Fallback for minimal Excel lists with no consistent header structure.
        """
        df = read_excel_with_temp_copy(path, dtype=str).fillna("")
        normalized = [norm_header_snake(c) for c in df.columns]

        desc_col: Optional[str] = None
        mpn_col: Optional[str] = None
        for orig, norm in zip(df.columns, normalized):
            if ("desc" in norm or "title" in norm or "description" in norm) and desc_col is None:
                desc_col = orig
            if any(k in norm for k in ["part", "mpn", "mfg", "pn", "p_n"]) and mpn_col is None:
                mpn_col = orig

        if not desc_col and not mpn_col:
            raise ValueError(f"Could not find description or MPN columns. Detected: {list(df.columns)}")

        parts: List[NPRPart] = []
        for i, (_, row) in enumerate(df.iterrows(), start=1):
            desc_val = safe_str(row.get(desc_col or "", ""))
            mpn_raw = safe_str(row.get(mpn_col or "", ""))
            mpn_val = DataLoader.clean_mpn(mpn_raw)

            # drop blank-enough rows
            if not desc_val and not mpn_val:
                continue

            parts.append(
                NPRPart(
                    partnum=f"ROW-{i}",  #  unique id
                    desc=desc_val,
                    mfgname="",
                    mfgpn=mpn_val,
                    supplier="",
                    raw_fields={str(k): safe_str(v) for k, v in dict(row).items()},
                    parsed={},
                )
            )
        return parts
