from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, Iterable
import shutil
import tempfile
import os
import pandas as pd

from .data_models import InventoryPart, NPRPart, CNSRecord

from collections import defaultdict

PB_RE = re.compile(r"\b(\d{2})\s*-\s*(\d{5})\b")
PFX_RE = re.compile(r"^\s*(\d{2})\s*$")
BODY_RE = re.compile(r"^\s*(\d{5})\s*$")


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
    return (
        str(h)
        .strip()
        .lower()
        .replace("\n", " ")
        .replace("\r", " ")
    )


def norm_header_snake(h: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", norm_header(h)).strip("_")




def read_excel_with_temp_copy(path: str, **read_excel_kwargs) -> pd.DataFrame:
    """
    Read an Excel file even if it's open in Excel (common file-lock issue).
    Strategy:
      1) Try normal pandas read.
      2) If PermissionError / lock, copy to a temp file and read the copy.
    Note:
      - If Excel holds a hard lock that blocks copying too, this will still fail.
      - In that case, you’d need a COM-based reader (Windows/pywin32).
    """
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
    # Copy to temp and read the temp file
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

        # Pre-found alternates (map explicit “2” columns to unique names so we can ingest them all)
        "manufacturer_part_number_2": "manufacturer_part_2",
        "manufacturer_part_number2": "manufacturer_part_2",
        "manufacturer_2_part_number": "manufacturer_part_2",
        "manufacturer_part_2": "manufacturer_part_2",
        "mpn_2": "manufacturer_part_2",
        "mfrpn_2": "manufacturer_part_2",

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
        "designator": "part_number",
        "reference": "part_number",
        "refdes": "part_number",

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
    


    #==================================
    # Create the CNS reader/loader
    #==================================
    # =====================================================
    # CNS WORKBOOK (multi-sheet)
    # =====================================================

    @staticmethod
    def _open_excel_file_with_temp_copy(path: str) -> tuple[pd.ExcelFile, str | None]:
        """
        Like read_excel_with_temp_copy, but for opening the workbook so we can list sheet names.
        Returns (ExcelFile, tmp_dir_to_cleanup_or_None).
        """
        try:
            return pd.ExcelFile(path), None
        except PermissionError:
            pass
        except OSError as e:
            if "Permission denied" not in str(e):
                raise

        tmp_dir = tempfile.mkdtemp(prefix="nprtool_")
        tmp_path = os.path.join(tmp_dir, os.path.basename(path))
        shutil.copy2(path, tmp_path)
        return pd.ExcelFile(tmp_path), tmp_dir

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
        try:
            records: List[CNSRecord] = []

            for sheet in xf.sheet_names:
                raw = read_excel_with_temp_copy(path, sheet_name=sheet, header=None, dtype=str)

                header_row = None
                try:
                    #  only require Prefix + Body now
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
                    df = read_excel_with_temp_copy(path, sheet_name=sheet, header=header_row, dtype=str).fillna("")
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



    #===========================================================
    # Make columns unique after aliasing (prevents this everywhere)
    #===========================================================
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
        df = DataLoader._make_unique_columns(df)  # <-- add this line
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
    

    # =====================================================
    # NPR / BOM SHEET (patched A/B/C)
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

            # Patch C: guarantee a unique identifier for node creation downstream
            partnum = get("part_number") or f"ROW-{i}"

            # Patch A: drop blank-enough BOM rows
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
            print(f"⚠️ load_npr failed ({e}). Falling back to load_simple_parts_list...")
            return DataLoader.load_simple_parts_list(path)


    # =====================================================
    # SIMPLE 2-COLUMN PARTS LIST (patched C)
    # =====================================================
    @staticmethod
    def load_simple_parts_list(path: str) -> List[NPRPart]:
        """
        Fallback for minimal Excel lists with no consistent header structure.
        """
        df = pd.read_excel(path, dtype=str).fillna("")
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

            # Patch A: drop blank-enough rows
            if not desc_val and not mpn_val:
                continue

            parts.append(
                NPRPart(
                    partnum=f"ROW-{i}",  # Patch C: unique id
                    desc=desc_val,
                    mfgname="",
                    mfgpn=mpn_val,
                    supplier="",
                    raw_fields={str(k): safe_str(v) for k, v in dict(row).items()},
                    parsed={},
                )
            )
        return parts
