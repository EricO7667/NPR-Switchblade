import re
import pandas as pd
from .data_models import NPRPart, InventoryPart
from .parsing_engine import parse_description


def safe_str(x):
    if pd.isna(x):
        return ""
    return str(x).strip()


class DataLoader:
    # =====================================================
    # MPN CLEANER (handles Excel _x garbage)
    # =====================================================
    @staticmethod
    def clean_mpn(s: str):
        if not s:
            return ""
        s = str(s)
        s = re.sub(r"_x[0-9a-fA-F]{4}_", "", s)  # remove Excel encoding
        s = s.replace("\n", "").replace("\r", "").strip()
        return s

    # =====================================================
    # INVENTORY SHEET
    # =====================================================
    @staticmethod
    def find_header_row(df, required_keywords):
        for i in range(min(20, len(df))):
            row = df.iloc[i].astype(str).str.lower()
            row_str = " ".join(row)
            if any(k in row_str for k in required_keywords):
                return i
        raise ValueError("Could not locate a header row.")

    @staticmethod
    def load_inventory(path):
        raw = pd.read_excel(path, header=None, dtype=str)
        header_row = DataLoader.find_header_row(
            raw, required_keywords=["item", "desc", "vendor", "mfg"]
        )
        df = pd.read_excel(path, header=header_row, dtype=str)
        df.columns = [c.strip() for c in df.columns]

        inventory_parts = []
        for _, row in df.iterrows():
            raw_fields = {col: safe_str(row[col]) for col in df.columns}
            part = InventoryPart(
                itemnum=safe_str(row.get("ItemNumber", "")),
                desc=safe_str(row.get("Description", "")),
                mfgid=safe_str(row.get("MfgId", "")),
                mfgname=safe_str(row.get("MfgName", "")),
                vendoritem=DataLoader.clean_mpn(safe_str(row.get("VendorItem", ""))),
                raw_fields=raw_fields,
                parsed=parse_description(safe_str(row.get("Description", ""))),
            )
            inventory_parts.append(part)
        return inventory_parts

    # =====================================================
    # NPR / BOM SHEET
    # =====================================================
    @staticmethod
    def find_npr_header_row(df):
        """Detect likely header row by scanning top 40 rows."""
        header_keywords = ["mfg", "part", "desc", "manufacturer", "item"]
        best_row = None
        best_hits = 0
        for i in range(min(40, len(df))):
            row = df.iloc[i].astype(str).str.lower().str.strip()
            joined = " ".join(row)
            hits = sum(k in joined for k in header_keywords)
            if hits > best_hits:
                best_hits = hits
                best_row = i
        if best_row is None or best_hits == 0:
            raise ValueError("No header row found with recognizable keywords.")
        return best_row

    @staticmethod
    def load_npr(path):
        """Robust loader for BOM/NPR sheets (handles offset headers, merged cells, aliases)."""
        raw = pd.read_excel(path, header=None, dtype=str)

        # Step 1. Detect header row
        try:
            header_row = DataLoader.find_npr_header_row(raw)
        except Exception as e:
            print(f"⚠️ Header not found: {e}. Defaulting to row 0.")
            header_row = 0

        # Step 2. Load from detected header
        df = pd.read_excel(path, header=header_row, dtype=str).fillna("")

        # Step 3. Retry if mostly unnamed
        unnamed_ratio = sum(c.lower().startswith("unnamed") for c in df.columns) / len(df.columns)
        if unnamed_ratio > 0.5 and header_row < len(raw) - 1:
            print(f"⚠️ Mostly unnamed columns at row {header_row}, retrying with next row...")
            header_row += 1
            df = pd.read_excel(path, header=header_row, dtype=str).fillna("")

        # Step 4. Normalize + alias headers
        df.columns = (
            df.columns.astype(str)
            .str.strip()
            .str.lower()
            .str.replace(r"[^a-z0-9]+", "_", regex=True)
        )

        aliases = {
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
        df.columns = [aliases.get(c, c) for c in df.columns]

        print(f"🧠 Normalized Columns: {list(df.columns)}")

        # Step 5. Validate minimally required fields
        has_desc = any("item_description" in c for c in df.columns)
        has_mpn = any(c.startswith("manufacturer_part_") for c in df.columns)
        if not has_desc and not has_mpn:
            raise ValueError(
                f"❌ Unable to find Description or Manufacturer Part columns in sheet.\n"
                f"Detected columns: {list(df.columns)}"
            )

        # Step 6. Build NPRPart list
        npr_parts = []
        for _, row in df.iterrows():
            def get(name):
                return safe_str(row.get(name, ""))

            npr_parts.append(
                NPRPart(
                    partnum=get("part_number"),
                    desc=get("item_description"),
                    mfgname=get("manufacturer_name"),
                    mfgpn=DataLoader.clean_mpn(get("manufacturer_part_")),
                    supplier=get("supplier"),
                    raw_fields={c: safe_str(row[c]) for c in df.columns},
                    parsed=parse_description(get("item_description")),
                )
            )

        print(f"✅ Loaded {len(npr_parts)} NPR/BOM parts successfully from {path}")
        return npr_parts

    # =====================================================
    # SIMPLE 2-COLUMN NPR-LIKE PARTS LIST
    # =====================================================
    @staticmethod
    def load_simple_parts_list(path):
        """Fallback for minimal Excel lists (no header structure)."""
        df = pd.read_excel(path, dtype=str).fillna("")
        normalized = df.columns.str.strip().str.lower().str.replace(r"[^a-z0-9]+", "_", regex=True)

        desc_col = None
        mpn_col = None
        for orig, norm in zip(df.columns, normalized):
            if "desc" in norm:
                desc_col = orig
            if any(k in norm for k in ["part", "mpn", "mfg", "pn"]):
                mpn_col = orig

        if not desc_col and not mpn_col:
            raise ValueError(
                f"❌ Could not find description or MPN columns. Detected: {list(df.columns)}"
            )

        parts = []
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
                    raw_fields=dict(row),
                    parsed=parse_description(desc_val),
                )
            )
        return parts
