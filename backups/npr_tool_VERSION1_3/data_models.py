from enum import Enum
from typing import List, Dict, Optional, Any


# =========================================================
# MATCH TYPES ENUM
# =========================================================
class MatchType(Enum):
    EXACT_MFG_PN = "Exact MFG Part #"
    EXACT_ITEMNUM = "Exact Item Number"
    PARSED_MATCH = "Parsed Engineering Match"
    NO_MATCH = "No Match"
    PREFIX_FAMILY = "MPN Family Prefix Match"


# =========================================================
# MATCH RESULT
# =========================================================
class MatchResult:
    def __init__(self, match_type: MatchType, confidence: float, inventory_part: "InventoryPart", notes: str = ""):
        self.match_type = match_type
        self.confidence = confidence
        self.inventory_part = inventory_part
        self.notes = notes

    def __repr__(self):
        return f"<MatchResult {self.match_type.value} ({self.confidence:.2f})>"


# =========================================================
# SUBSTITUTES & API DATA
# =========================================================
class SubstitutePart:
    """
    Represents an alternate or equivalent part for an Inventory item.
    Example: base resistor has multiple supplier-specific alternates.
    """
    def __init__(self, base_itemnum: str, sub_itemnum: str, description: str, mfgpn: str, notes: str = ""):
        self.base_itemnum = base_itemnum
        self.sub_itemnum = sub_itemnum
        self.description = description
        self.mfgpn = mfgpn
        self.notes = notes

    def to_dict(self) -> Dict[str, Any]:
        return {
            "base_itemnum": self.base_itemnum,
            "sub_itemnum": self.sub_itemnum,
            "description": self.description,
            "mfgpn": self.mfgpn,
            "notes": self.notes
        }

    def __repr__(self):
        return f"<Substitute {self.sub_itemnum or 'N/A'} for {self.base_itemnum}>"


class DigiKeyData:
    """
    Represents manufacturer API data for a given part number.
    """
    def __init__(self, mfgpn: str, url: str, specs: Dict[str, str], availability: str, price: str):
        self.mfgpn = mfgpn
        self.url = url
        self.specs = specs or {}
        self.availability = availability
        self.price = price

    def to_dict(self) -> Dict[str, Any]:
        return {
            "mfgpn": self.mfgpn,
            "url": self.url,
            "specs": self.specs,
            "availability": self.availability,
            "price": self.price
        }

    def __repr__(self):
        return f"<DigiKeyData {self.mfgpn}: {self.availability}, {self.price}>"


# =========================================================
# NPR PART MODEL
# =========================================================
class NPRPart:
    def __init__(self, partnum: str, desc: str, mfgname: str, mfgpn: str,
                 supplier: str, raw_fields: Dict[str, str], parsed: Dict[str, Any]):
        """
        Represents a new part request (NPR) entry from Excel.
        """
        self.partnum = partnum
        self.description = desc
        self.mfgname = mfgname
        self.mfgpn = mfgpn
        self.supplier = supplier
        self.raw_fields = raw_fields
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

    def to_dict(self) -> Dict[str, Any]:
        return {
            "partnum": self.partnum,
            "description": self.description,
            "mfgname": self.mfgname,
            "mfgpn": self.mfgpn,
            "supplier": self.supplier,
            "parsed": self.parsed,
            "raw_fields": self.raw_fields
        }

    def __repr__(self):
        return f"<NPRPart {self.partnum or self.mfgpn or 'unknown'}>"


# =========================================================
# INVENTORY PART MODEL
# =========================================================
class InventoryPart:
    def __init__(self, itemnum: str, desc: str, mfgid: str, mfgname: str,
                 vendoritem: str, raw_fields: Dict[str, str], parsed: Dict[str, Any],
                 substitutes: Optional[List[SubstitutePart]] = None,
                 api_data: Optional[DigiKeyData] = None):
        """
        Represents an inventory record from internal ERP or stock list.
        """
        self.itemnum = itemnum
        self.description = desc
        self.mfgid = mfgid
        self.mfgname = mfgname
        self.vendoritem = vendoritem
        self.raw_fields = raw_fields
        self.parsed = parsed

        # New fields for scalability
        self.substitutes: List[SubstitutePart] = substitutes or []
        self.api_data: Optional[DigiKeyData] = api_data

        # Shortcuts
        self.part_type = parsed.get("type")
        self.parsed_value = parsed.get("value")
        self.tolerance = parsed.get("tolerance")
        self.wattage = parsed.get("wattage")
        self.parsed_voltage = parsed.get("voltage")
        self.dielectric = parsed.get("dielectric")
        self.parsed_package = parsed.get("package")
        self.mount_type = parsed.get("mount")

    # --------------------------
    # Utility
    # --------------------------
    def add_substitute(self, sub: SubstitutePart):
        self.substitutes.append(sub)

    def set_api_data(self, data: DigiKeyData):
        self.api_data = data

    def to_dict(self) -> Dict[str, Any]:
        return {
            "itemnum": self.itemnum,
            "description": self.description,
            "mfgid": self.mfgid,
            "mfgname": self.mfgname,
            "vendoritem": self.vendoritem,
            "parsed": self.parsed,
            "raw_fields": self.raw_fields,
            "substitutes": [s.to_dict() for s in self.substitutes],
            "api_data": self.api_data.to_dict() if self.api_data else None
        }

    def __repr__(self):
        sub_count = len(self.substitutes)
        api = "API✓" if self.api_data else "API✗"
        return f"<InventoryPart {self.itemnum} [{sub_count} subs, {api}]>"
"""

| Category          | Old                | New                                                                   |
| ----------------- | ------------------ | --------------------------------------------------------------------- |
| **Extensibility** | NPR/Inventory only | Now supports Substitutes and API data                                 |
| **Serialization** | None               | Added `.to_dict()` methods                                            |
| **Logging**       | Minimal            | Helpful `__repr__()` summaries                                        |
| **Relationships** | Flat               | `InventoryPart` can now hold sub-items and API info                   |
| **Type safety**   | Implicit           | Added type hints                                                      |
| **Future API**    | None               | `.set_api_data()` and `.add_substitute()` ready for Digikey/Alt parts |

"""