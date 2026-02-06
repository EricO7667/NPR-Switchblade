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
            vendoritem = safe_str(row.get("VendorItem", ""))

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

            part = NPRPart(
                partnum  = get(row, "part_number"),
                desc     = get(row, "item_description"),
                mfgname  = get(row, "manufacturer_name"),
                mfgpn    = get(row, "manufacturer_part_"),
                supplier = get(row, "supplier"),
                raw_fields = {c: safe_str(row[c]) for c in df.columns},
                parsed = parse_description(get(row, "item_description"))
            )

            npr_parts.append(part)

        return npr_parts

    @staticmethod
    def load_simple_parts_list(path):
        """
        Loads a file that contains *only*:
            - Description
            - Manufacturer Part Number (or similar)
        Returns a list of NPRPart objects with minimal structure.
        """
        df = pd.read_excel(path, dtype=str).fillna("")
    
        # Normalize columns
        cols = (
            df.columns.str.strip().str.lower().str.replace(r"[^a-z0-9]+", "_", regex=True)
        )
    
        df.columns = cols
    
        # Find possible column names
        desc_col = None
        mpn_col = None
    
        for c in cols:
            if "desc" in c:
                desc_col = c
            if "part" in c or "mpn" in c or "manufacturer" in c:
                mpn_col = c
    
        if not desc_col or not mpn_col:
            raise ValueError(
                "This simple parts list must contain at least a Description and a Manufacturer Part Number column."
            )
    
        parts = []
    
        for _, row in df.iterrows():
            part = NPRPart(
                partnum="",                     # No internal part #
                desc=row.get(desc_col, ""),
                mfgname="",                     # Not provided
                mfgpn=row.get(mpn_col, ""),     # Manufacturer PN
                supplier="",                    # Not provided
                raw_fields=dict(row),
                parsed=parse_description(row.get(desc_col, "")),
            )
    
            parts.append(part)
    
        return parts
    


