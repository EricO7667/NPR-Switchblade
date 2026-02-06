import re
import pandas as pd
from .data_models import NPRPart, InventoryPart
from .parsing_engine import parse_description


# -------------------------------
# Helper safe conversion
# -------------------------------
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
        """
        Normalize manufacturer part numbers:
        - Strip whitespace / newlines
        - Remove Excel's _x / _x000 style suffixes
        """
        if not s:
            return ""
        s = str(s)
        s = s.replace("\n", "").replace("\r", "").strip()

        # Exact "_x" suffix
        if s.endswith("_x"):
            s = s[:-2]

        # Handle cases like "_x000", "_x00123"
        s = re.sub(r"_x\d+$", "", s)

        return s

    # =====================================================
    # FIND HEADER ROW IN FIRST N ROWS
    # =====================================================
    @staticmethod
    def find_header_row(df, required_keywords):
        """
        Find the header row by looking for a row that contains ANY
        of the required keywords inside the first N rows.
        """
        for i in range(min(20, len(df))):
            row = df.iloc[i].astype(str).str.lower()
            row_str = " ".join(row)
            if any(k in row_str for k in required_keywords):
                return i
        raise ValueError("Could not locate a header row.")

    # =====================================================
    # LOAD INVENTORY SHEET
    # =====================================================
    @staticmethod
    def load_inventory(path):
        raw = pd.read_excel(path, header=None, dtype=str)

        # detect header row
        header_row = DataLoader.find_header_row(
            raw,
            required_keywords=["item", "desc", "vendor", "mfg"]
        )

        df = pd.read_excel(path, header=header_row, dtype=str)
        df.columns = [c.strip() for c in df.columns]

        inventory_parts = []

        for _, row in df.iterrows():
            # Convert entire row into raw_fields dictionary
            raw_fields = {col: safe_str(row[col]) for col in df.columns}

            # Extract common fields
            itemnum = safe_str(row.get("ItemNumber", ""))
            desc = safe_str(row.get("Description", ""))
            mfgid = safe_str(row.get("MfgId", ""))
            mfgname = safe_str(row.get("MfgName", ""))
            vendoritem_raw = safe_str(row.get("VendorItem", ""))
            vendoritem = DataLoader.clean_mpn(vendoritem_raw)

            # Parse the engineering description
            parsed = parse_description(desc, part_type_hint=None)

            part = InventoryPart(
                itemnum=itemnum,
                desc=desc,
                mfgid=mfgid,
                mfgname=mfgname,
                vendoritem=vendoritem,
                raw_fields=raw_fields,
                parsed=parsed
            )

            inventory_parts.append(part)

        return inventory_parts
    

    @staticmethod
    def find_npr_header_row(df):
        required = [
            "part number",
            "item description",
            "manufacturer name",
            "manufacturer part #",
            "supplier"
        ]

        # search top 25 rows because user said header is always in there
        for i in range(min(25, len(df))):
            # normalize row text
            row = df.iloc[i].astype(str).str.lower().str.strip()

            # count how many required header labels appear in this row
            hits = sum(any(req in cell for cell in row) for req in required)

            # if enough required labels appear, assume this is header row
            if hits >= 3:  # threshold — at least 3 must match
                return i

        raise ValueError("Could not detect NPR header row in first 25 rows.")

    # =====================================================
    # LOAD NPR SHEET (Bulletproof Version)
    # =====================================================
    @staticmethod
    def load_npr(path):
        # ---- Load RAW sheet to find header row ----
        raw = pd.read_excel(path, header=None, dtype=str)

        try:
            header_row = DataLoader.find_npr_header_row(raw)
        except Exception:
            # Try simple 2-column sheet instead
            print("Falling back to simple parts list mode.")
            return DataLoader.load_simple_parts_list(path)

        # ---- Load again using detected header ----
        df = pd.read_excel(path, header=header_row, dtype=str).fillna("")

        print("NPR Header row detected at:", header_row)
        print("NPR Columns detected:", df.columns.tolist())

        # ---- Normalize column names safely ----
        df.columns = (
            df.columns
            .str.strip()
            .str.lower()
            .str.replace(r"[^a-z0-9]+", "_", regex=True)  # Example: "Part Number" -> "part_number"
        )

        # Expected columns after normalization
        required_cols = [
            "part_number",
            "item_description",
            "manufacturer_name",
            "manufacturer_part_",
            "supplier"
        ]

        # ---- Validate that NPR sheet contains all required columns ----
        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            raise ValueError(
                f"❌ ERROR: This file does not look like an NPR sheet.\n"
                f"Missing required columns: {missing}\n"
                f"Detected columns were: {df.columns.tolist()}"
            )

        # ---- Helper for column lookup ----
        def get(row, name):
            return safe_str(row.get(name, ""))

        npr_parts = []

        for _, row in df.iterrows():
            mfgpn_raw = get(row, "manufacturer_part_")
            mfgpn = DataLoader.clean_mpn(mfgpn_raw)

            part = NPRPart(
                partnum  = get(row, "part_number"),
                desc     = get(row, "item_description"),
                mfgname  = get(row, "manufacturer_name"),
                mfgpn    = mfgpn,
                supplier = get(row, "supplier"),
                raw_fields = {c: safe_str(row[c]) for c in df.columns},
                parsed = parse_description(get(row, "item_description"))
            )

            npr_parts.append(part)

        return npr_parts

    # =====================================================
    # LOAD SIMPLE 2-COLUMN NPR-LIKE PARTS LIST
    # =====================================================
    @staticmethod
    def load_simple_parts_list(path):
        df = pd.read_excel(path, dtype=str).fillna("")

        # Normalize columns for detection only
        normalized = (
            df.columns.str.strip().str.lower().str.replace(r"[^a-z0-9]+", "_", regex=True)
        )

        desc_col = None
        mpn_col = None

        for orig_name, norm in zip(df.columns, normalized):
            if "desc" in norm:
                desc_col = orig_name
            if "part" in norm or "mpn" in norm or "mfg" in norm:
                mpn_col = orig_name

        if not desc_col or not mpn_col:
            raise ValueError("Simple sheet must contain Description and Manufacturer Part Number.")

        parts = []

        for _, row in df.iterrows():
            desc_val = safe_str(row.get(desc_col, ""))
            mpn_raw  = safe_str(row.get(mpn_col, ""))
            mpn_val  = DataLoader.clean_mpn(mpn_raw)

            parts.append(NPRPart(
                partnum   = "",
                desc      = desc_val,
                mfgname   = "",
                mfgpn     = mpn_val,
                supplier  = "",
                raw_fields = dict(row),
                parsed    = parse_description(desc_val)
            ))

        return parts
    
    @staticmethod
    def clean_mpn(s: str):
        if not s:
            return ""

        s = str(s)

        # Remove Excel's encoded control characters like _x000d_, _x000a_, _x0009_
        s = re.sub(r"_x[0-9a-fA-F]{4}_", "", s)

        # Also strip whitespace and newlines
        s = s.replace("\n", "").replace("\r", "").strip()

        return s
