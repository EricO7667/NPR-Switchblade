from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Dict, List, Optional

import pandas as pd

from .data_models import InventoryPart, NPRPart


# =====================================================
# Small helpers
# =====================================================
def safe_str(x) -> str:
    """Convert pandas cell values into clean strings (never NaN)."""
    if pd.isna(x):
        return ""
    return str(x).strip()


@dataclass(frozen=True)
class HeaderAliases:
    """
    Central alias map (scalable).
    Later: load this from config/yaml/user input.
    """
    aliases: Dict[str, str]


DEFAULT_NPR_ALIASES = HeaderAliases(
    aliases={
        "mfg_p_n": "manufacturer_part_",
        "mfgpn": "manufacturer_part_",
        "manufacturer_part_number": "manufacturer_part_",
        "mfg_part_number": "manufacturer_part_",
        "elan_part_number": "part_number",
        "part_number_": "part_number",
        "part_number": "part_number",
        "description": "item_description",
        "item_description": "item_description",
        "manufacturer": "manufacturer_name",
        "manufacturer_name": "manufacturer_name",
        "mfr_name": "manufacturer_name",
        "supplier_name": "supplier",
        "vendor": "supplier",
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
        This keeps your original behavior but isolates it as a single function.
        """
        header_keywords = ["mfg", "part", "desc", "manufacturer", "item"]
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
    # INVENTORY SHEET
    # =====================================================
    @staticmethod
    def load_inventory(path: str) -> List[InventoryPart]:
        """
        Load inventory file and return a list of InventoryPart objects.
        """
        raw = pd.read_excel(path, header=None, dtype=str)
        header_row = DataLoader._find_header_row_by_keywords(
            raw,
            required_keywords=["item", "desc", "vendor", "mfg"],
            scan_rows=20,
        )

        df = pd.read_excel(path, header=header_row, dtype=str)
        df.columns = [str(c).strip() for c in df.columns]

        inventory_parts: List[InventoryPart] = []
        for _, row in df.iterrows():
            raw_fields = {col: safe_str(row.get(col, "")) for col in df.columns}

            desc = safe_str(row.get("Description", ""))
            inv = InventoryPart(
                itemnum=safe_str(row.get("ItemNumber", "")),
                desc=desc,
                mfgid=safe_str(row.get("MfgId", "")),
                mfgname=safe_str(row.get("MfgName", "")),
                vendoritem=DataLoader.clean_mpn(safe_str(row.get("VendorItem", ""))),
                raw_fields=raw_fields,
                parsed={},
            )
            inventory_parts.append(inv)

        return inventory_parts

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
        """
        raw = pd.read_excel(path, header=None, dtype=str)

        try:
            header_row = DataLoader._find_npr_header_row(raw)
        except Exception as e:
            print(f"⚠️ Header not found: {e}. Defaulting to row 0.")
            header_row = 0

        df = pd.read_excel(path, header=header_row, dtype=str).fillna("")

        # Retry if mostly unnamed columns
        unnamed_ratio = sum(str(c).lower().startswith("unnamed") for c in df.columns) / max(1, len(df.columns))
        if unnamed_ratio > 0.5 and header_row < len(raw) - 1:
            print(f"⚠️ Mostly unnamed columns at row {header_row}, retrying with next row...")
            header_row += 1
            df = pd.read_excel(path, header=header_row, dtype=str).fillna("")

        # Normalize + alias headers
        df.columns = (
            df.columns.astype(str)
            .str.strip()
            .str.lower()
            .str.replace(r"[^a-z0-9]+", "_", regex=True)
        )
        df.columns = [aliases.aliases.get(c, c) for c in df.columns]

        print(f"🧠 Normalized Columns: {list(df.columns)}")

        # Validate minimal required fields
        has_desc = any("item_description" == c for c in df.columns)
        has_mpn = any(c.startswith("manufacturer_part_") for c in df.columns)
        if not has_desc and not has_mpn:
            raise ValueError(
                "❌ Unable to find Description or Manufacturer Part columns in sheet.\n"
                f"Detected columns: {list(df.columns)}"
            )

        npr_parts: List[NPRPart] = []
        for _, row in df.iterrows():
            def get(name: str) -> str:
                return safe_str(row.get(name, ""))

            desc = get("item_description")
            npr = NPRPart(
                partnum=get("part_number"),
                desc=desc,
                mfgname=get("manufacturer_name"),
                mfgpn=DataLoader.clean_mpn(get("manufacturer_part_")),
                supplier=get("supplier"),
                raw_fields={c: safe_str(row.get(c, "")) for c in df.columns},
                parsed={},
            )
            npr_parts.append(npr)

        print(f" Loaded {len(npr_parts)} NPR/BOM parts successfully from {path}")
        return npr_parts

    # =====================================================
    # SIMPLE 2-COLUMN PARTS LIST
    # =====================================================
    @staticmethod
    def load_simple_parts_list(path: str) -> List[NPRPart]:
        """
        Fallback for minimal Excel lists with no consistent header structure.
        """
        df = pd.read_excel(path, dtype=str).fillna("")
        normalized = (
            df.columns.astype(str)
            .str.strip()
            .str.lower()
            .str.replace(r"[^a-z0-9]+", "_", regex=True)
        )

        desc_col: Optional[str] = None
        mpn_col: Optional[str] = None
        for orig, norm in zip(df.columns, normalized):
            if "desc" in norm and desc_col is None:
                desc_col = orig
            if any(k in norm for k in ["part", "mpn", "mfg", "pn"]) and mpn_col is None:
                mpn_col = orig

        if not desc_col and not mpn_col:
            raise ValueError(f"❌ Could not find description or MPN columns. Detected: {list(df.columns)}")

        parts: List[NPRPart] = []
        for _, row in df.iterrows():
            desc_val = safe_str(row.get(desc_col or "", ""))
            mpn_raw = safe_str(row.get(mpn_col or "", ""))
            parts.append(
                NPRPart(
                    partnum="",
                    desc=desc_val,
                    mfgname="",
                    mfgpn=DataLoader.clean_mpn(mpn_raw),
                    supplier="",
                    raw_fields={str(k): safe_str(v) for k, v in dict(row).items()},
                    parsed={},
                )
            )
        return parts
