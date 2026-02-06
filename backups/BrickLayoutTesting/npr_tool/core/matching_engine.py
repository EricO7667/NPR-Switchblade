from typing import List, Optional
from .data_models import MatchType, MatchResult, InventoryPart, NPRPart


# ---------------------------------------------------------
# Helper: Prefix Extractor
# ---------------------------------------------------------
def mpn_prefix(s: str, remove_last: int = 5) -> str:
    if not s:
        return ""
    s = s.strip()
    return s[:-remove_last] if len(s) > remove_last else ""


# ---------------------------------------------------------
# Configurable confidence weights
# ---------------------------------------------------------
CONFIDENCE_WEIGHTS = {
    "exact_mfgpn": 1.00,
    "exact_itemnum": 0.85,
    "prefix_family": 0.60,
    "parsed_engineering": 0.75,
    "substitute_match": 0.65,
    "api_match": 0.55,
}

# Optional: per-type confidence scaling
TYPE_CONFIDENCE = {
    "RES": 1.0,
    "CAP": 1.0,
    "LED": 0.9,
    "DIODE": 0.85,
    "MOSFET": 0.9,
    "TRANSISTOR": 0.85,
    "TRIAC": 0.8,
}


# ---------------------------------------------------------
# MATCHING ENGINE
# ---------------------------------------------------------
class MatchingEngine:
    def __init__(self, inventory_parts: List[InventoryPart]):
        self.inventory = inventory_parts

    # =====================================================
    # MAIN ENTRY POINT
    # =====================================================
    def match_npr_list(self, npr_list: List[NPRPart]) -> List[tuple]:
        results = []
        for npr in npr_list:
            result = self.match_single_part(npr)
            results.append((npr, result))
        return results

    # =====================================================
    # MATCH A SINGLE NPR PART
    # =====================================================
    def match_single_part(self, npr: NPRPart) -> MatchResult:
        """
        Apply tiered matching logic.
        """

        # Tier 1 — Exact Manufacturer PN
        if npr.mfgpn:
            match = self._match_by_mfgpn(npr)
            if match:
                return match

        # Tier 1.5 — Family Prefix
        if npr.mfgpn:
            match = self._match_by_prefix(npr)
            if match:
                return match

        # Tier 2 — Exact Item Number
        if npr.partnum:
            match = self._match_by_itemnum(npr)
            if match:
                return match

        # Tier 2.5 — Substitute
        match = self._match_by_substitute(npr)
        if match:
            return match

        # Tier 3 — Parsed Engineering
        match = self.engineering_match(npr)
        if match:
            return match

        # Tier 4 — API Assisted (future)
        match = self._match_by_api_data(npr)
        if match:
            return match

        # Fallback
        return MatchResult(
            match_type=MatchType.NO_MATCH,
            confidence=0.0,
            inventory_part=None,
            notes="No match found across all tiers."
        )

    # =====================================================
    # Tier 1 — Exact Manufacturer PN
    # =====================================================
    def _match_by_mfgpn(self, npr: NPRPart) -> Optional[MatchResult]:
        for inv in self.inventory:
            if inv.vendoritem and inv.vendoritem.upper() == npr.mfgpn.upper():
                return MatchResult(
                    match_type=MatchType.EXACT_MFG_PN,
                    confidence=CONFIDENCE_WEIGHTS["exact_mfgpn"],
                    inventory_part=inv,
                    notes="Matched by exact Manufacturer Part #"
                )
        return None

    # =====================================================
    # Tier 1.5 — Family Prefix
    # =====================================================
    def _match_by_prefix(self, npr: NPRPart) -> Optional[MatchResult]:
        prefix = mpn_prefix(npr.mfgpn)
        if not prefix:
            return None

        for inv in self.inventory:
            if inv.vendoritem and inv.vendoritem.startswith(prefix):
                return MatchResult(
                    match_type=MatchType.PREFIX_FAMILY,
                    confidence=CONFIDENCE_WEIGHTS["prefix_family"],
                    inventory_part=inv,
                    notes=f"Matched by Manufacturer Family Prefix '{prefix}'"
                )
        return None

    # =====================================================
    # Tier 2 — Exact Item Number
    # =====================================================
    def _match_by_itemnum(self, npr: NPRPart) -> Optional[MatchResult]:
        for inv in self.inventory:
            if inv.itemnum and inv.itemnum.upper() == npr.partnum.upper():
                return MatchResult(
                    match_type=MatchType.EXACT_ITEMNUM,
                    confidence=CONFIDENCE_WEIGHTS["exact_itemnum"],
                    inventory_part=inv,
                    notes="Matched by exact Item Number"
                )
        return None

    # =====================================================
    # Tier 2.5 — Substitute Match
    # =====================================================
    def _match_by_substitute(self, npr: NPRPart) -> Optional[MatchResult]:
        if not npr.mfgpn:
            return None

        for inv in self.inventory:
            for sub in inv.substitutes:
                if sub.mfgpn and sub.mfgpn.upper() == npr.mfgpn.upper():
                    return MatchResult(
                        match_type=MatchType.PARSED_MATCH,
                        confidence=CONFIDENCE_WEIGHTS["substitute_match"],
                        inventory_part=inv,
                        notes=f"Matched via substitute part: {sub.sub_itemnum}"
                    )
        return None

    # =====================================================
    # Tier 3 — Parsed Engineering Match
    # =====================================================
    def engineering_match(self, npr: NPRPart) -> Optional[MatchResult]:
        p_npr = npr.parsed or {}
        if not p_npr:
            return None

        for inv in self.inventory:
            p_inv = inv.parsed or {}
            if not p_inv or p_npr.get("type") != p_inv.get("type"):
                continue

            ptype = p_npr.get("type")
            matchers = {
                "RES": self.match_resistor,
                "CAP": self.match_capacitor,
                "LED": self.match_led,
                "DIODE": self.match_diode,
                "MOSFET": self.match_mosfet,
                "TRANSISTOR": self.match_transistor,
                "TRIAC": self.match_triac,
            }

            if ptype in matchers and matchers[ptype](p_npr, p_inv):
                scale = TYPE_CONFIDENCE.get(ptype, 1.0)
                conf = CONFIDENCE_WEIGHTS["parsed_engineering"] * scale
                return MatchResult(
                    match_type=MatchType.PARSED_MATCH,
                    confidence=conf,
                    inventory_part=inv,
                    notes=f"Matched {ptype} via parsed engineering fields."
                )

        return None

    # =====================================================
    # Tier 4 — API Match (future)
    # =====================================================
    def _match_by_api_data(self, npr: NPRPart) -> Optional[MatchResult]:
        if not npr.mfgpn:
            return None

        for inv in self.inventory:
            api = getattr(inv, "api_data", None)
            if not api or not getattr(api, "specs", None):
                continue

            matches = 0
            total = 0
            for key in ("package", "voltage", "dielectric"):
                n_val = npr.parsed.get(key)
                a_val = api.specs.get(key)
                if n_val and a_val:
                    total += 1
                    if str(n_val).upper() == str(a_val).upper():
                        matches += 1

            if total > 0 and (matches / total) >= 0.6:
                return MatchResult(
                    match_type=MatchType.PARSED_MATCH,
                    confidence=CONFIDENCE_WEIGHTS["api_match"],
                    inventory_part=inv,
                    notes=f"Matched via API data ({matches}/{total} attribute match)"
                )

        return None

    # =====================================================
    # Engineering Rule Sets
    # =====================================================
    def match_resistor(self, npr, inv):
        if npr.get("value") != inv.get("value"):
            return False
        if npr.get("wattage") != inv.get("wattage"):
            return False
        if npr.get("package") != inv.get("package"):
            return False
        if npr.get("mount") != inv.get("mount"):
            return False

        tol_npr, tol_inv = npr.get("tolerance"), inv.get("tolerance")
        if tol_npr and tol_inv and tol_inv > tol_npr:
            return False
        return True

    def match_capacitor(self, npr, inv):
        if npr.get("value") != inv.get("value"):
            return False
        if npr.get("dielectric") != inv.get("dielectric"):
            return False

        v_npr, v_inv = npr.get("voltage"), inv.get("voltage")
        if v_npr and v_inv and v_inv < v_npr:
            return False

        tol_npr, tol_inv = npr.get("tolerance"), inv.get("tolerance")
        if tol_npr and tol_inv and tol_inv > tol_npr:
            return False

        if npr.get("package") != inv.get("package"):
            return False
        if npr.get("mount") != inv.get("mount"):
            return False
        return True

    def match_led(self, npr, inv):
        if npr.get("color") != inv.get("color"):
            return False
        if npr.get("package") != inv.get("package"):
            return False
        if npr.get("mount") != inv.get("mount"):
            return False

        b_npr, b_inv = npr.get("brightness_mcd"), inv.get("brightness_mcd")
        if b_npr and b_inv:
            if not (0.8 * b_npr <= b_inv <= 1.2 * b_npr):
                return False
        return True

    def match_diode(self, npr, inv):
        if npr.get("subtype") != inv.get("subtype"):
            return False
        if npr.get("package") != inv.get("package"):
            return False
        if npr.get("mount") != inv.get("mount"):
            return False

        v_npr, v_inv = npr.get("voltage"), inv.get("voltage")
        if v_npr and v_inv and abs(v_inv - v_npr) > 10:
            return False
        return True

    def match_mosfet(self, npr, inv):
        if npr.get("channel") != inv.get("channel"):
            return False
        if npr.get("package") != inv.get("package"):
            return False
        if npr.get("mount") != inv.get("mount"):
            return False

        v_npr, v_inv = npr.get("voltage"), inv.get("voltage")
        if v_npr and v_inv and abs(v_inv - v_npr) > 10:
            return False

        c_npr, c_inv = npr.get("current"), inv.get("current")
        if c_npr and c_inv and abs(c_inv - c_npr) > 1:
            return False
        return True

    def match_transistor(self, npr, inv):
        if npr.get("polarity") != inv.get("polarity"):
            return False
        if npr.get("package") != inv.get("package"):
            return False
        if npr.get("mount") != inv.get("mount"):
            return False
        return True

    def match_triac(self, npr, inv):
        if npr.get("package") != inv.get("package"):
            return False
        if npr.get("mount") != inv.get("mount"):
            return False

        v_npr, v_inv = npr.get("voltage"), inv.get("voltage")
        if v_npr and v_inv and abs(v_inv - v_npr) > 50:
            return False

        c_npr, c_inv = npr.get("current"), inv.get("current")
        if c_npr and c_inv and abs(c_inv - c_npr) > 2:
            return False
        return True
