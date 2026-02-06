from enum import Enum


# ---------------------------------------------------------
# MATCH TYPES
# ---------------------------------------------------------
class MatchType(Enum):
    EXACT_MFG_PN = "Exact MFG Part #"
    EXACT_ITEMNUM = "Exact Item Number"
    PARSED_MATCH = "Parsed Engineering Match"
    NO_MATCH = "No Match"
    PREFIX_FAMILY = "MPN Family Prefix Match"   # <-- NEW


# ---------------------------------------------------------
# MATCH RESULT OBJECT
# ---------------------------------------------------------
class MatchResult:
    def __init__(self, match_type, confidence, inventory_part, notes=""):
        self.match_type = match_type
        self.confidence = confidence
        self.inventory_part = inventory_part
        self.notes = notes


# ---------------------------------------------------------
# NPR PART MODEL
# ---------------------------------------------------------
class NPRPart:
    def __init__(self, partnum, desc, mfgname, mfgpn, supplier, raw_fields, parsed):
        """
        raw_fields: dict of every NPR column and value
        parsed: dict of parsed engineering fields
        """
        self.partnum = partnum
        self.description = desc
        self.mfgname = mfgname
        self.mfgpn = mfgpn
        self.supplier = supplier

        # full NPR row stored here
        self.raw_fields = raw_fields    

        # parsed description results
        self.parsed = parsed   

        # Convenience shortcuts
        self.part_type = parsed.get("type")
        self.parsed_value = parsed.get("value")
        self.tolerance = parsed.get("tolerance")
        self.wattage = parsed.get("wattage")
        self.parsed_voltage = parsed.get("voltage")
        self.dielectric = parsed.get("dielectric")
        self.parsed_package = parsed.get("package")
        self.mount_type = parsed.get("mount")


# ---------------------------------------------------------
# INVENTORY PART MODEL
# ---------------------------------------------------------
class InventoryPart:
    def __init__(self, itemnum, desc, mfgid, mfgname, vendoritem, raw_fields, parsed):
        """
        raw_fields: dict of every Inventory column and value
        parsed: dict of parsed engineering fields
        """
        self.itemnum = itemnum
        self.description = desc
        self.mfgid = mfgid
        self.mfgname = mfgname
        self.vendoritem = vendoritem

        # full inventory row
        self.raw_fields = raw_fields

        # parsed engineering description
        self.parsed = parsed

        # Shortcuts
        self.part_type = parsed.get("type")
        self.parsed_value = parsed.get("value")
        self.tolerance = parsed.get("tolerance")
        self.wattage = parsed.get("wattage")
        self.parsed_voltage = parsed.get("voltage")
        self.dielectric = parsed.get("dielectric")
        self.parsed_package = parsed.get("package")
        self.mount_type = parsed.get("mount")
