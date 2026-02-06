from .data_models import MatchType, MatchResult
def mpn_prefix(s: str, remove_last: int = 5) -> str:
    if not s:
        return ""
    s = s.strip()
    if len(s) <= remove_last:
        return ""
    return s[:-remove_last]

class MatchingEngine:

    def __init__(self, inventory_parts):
        self.inventory = inventory_parts

    # ====================================================================
    # MAIN ENTRY POINT
    # ====================================================================
    def match_npr_list(self, npr_list):
        return [(npr, self.match_single_part(npr)) for npr in npr_list]
    

    # ====================================================================
    # MATCH A SINGLE NPR PART
    # ====================================================================
    def match_single_part(self, npr):

        # -----------------------------
        # TIER 1 — Exact MFG PN match
        # -----------------------------
        if npr.mfgpn:
            for inv in self.inventory:
                if inv.vendoritem and inv.vendoritem.upper() == npr.mfgpn.upper():
                    return MatchResult(
                        match_type=MatchType.EXACT_MFG_PN,
                        confidence=1.0,
                        inventory_part=inv,
                        notes="Matched Manufacturer Part # to Inventory VendorItem"
                    )


        # -----------------------------
        # TIER 1.5 — MFG Part Family Prefix Match
        # (match MPN except last 5 characters)
        # -----------------------------
        if npr.mfgpn:
            base_prefix = mpn_prefix(npr.mfgpn, 5)

            if base_prefix:
                for inv in self.inventory:
                    if inv.vendoritem and inv.vendoritem.startswith(base_prefix):
                        return MatchResult(
                            match_type=MatchType.PREFIX_FAMILY,
                            confidence=0.60,
                            inventory_part=inv,
                            notes=f"Matched by MFG Part Family Prefix: '{base_prefix}'"
                        )
                    
        # -----------------------------
        # TIER 2 — Exact ItemNumber match
        # -----------------------------
        if npr.partnum:
            for inv in self.inventory:
                if inv.itemnum and inv.itemnum.upper() == npr.partnum.upper():
                    return MatchResult(
                        match_type=MatchType.EXACT_ITEMNUM,
                        confidence=0.85,
                        inventory_part=inv,
                        notes="Matched NPR Part Number to Inventory ItemNumber"
                    )

        # -----------------------------
        # TIER 3 — Parsed Engineering Match
        # -----------------------------
        parsed_match = self.engineering_match(npr)
        if parsed_match:
            return parsed_match

        # -----------------------------
        # NO MATCH FOUND
        # -----------------------------
        return MatchResult(
            match_type=MatchType.NO_MATCH,
            confidence=0.0,
            inventory_part=None,
            notes="No matching VendorItem, ItemNumber, or parsed engineering match."
        )

    # ====================================================================
    # ENGINEERING / PARSED MATCH LOGIC
    # ====================================================================
    def engineering_match(self, npr):
        p_npr = npr.parsed
        if not p_npr:  # cannot match without parsed fields
            return None

        best_match = None

        for inv in self.inventory:
            p_inv = inv.parsed
            if not p_inv:
                continue

            # Part types must match
            if p_npr.get("type") != p_inv.get("type"):
                continue

            # ---------------------------------------------------------------
            # RESISTOR ENGINEERING MATCH
            # ---------------------------------------------------------------
            if p_npr.get("type") == "RES":
                if self.match_resistor(p_npr, p_inv):
                    return MatchResult(
                        match_type=MatchType.PARSED_MATCH,
                        confidence=0.70,
                        inventory_part=inv,
                        notes="Matched resistor by parsed engineering fields."
                    )

            # ---------------------------------------------------------------
            # CAPACITOR ENGINEERING MATCH
            # ---------------------------------------------------------------
            if p_npr.get("type") == "CAP":
                if self.match_capacitor(p_npr, p_inv):
                    return MatchResult(
                        match_type=MatchType.PARSED_MATCH,
                        confidence=0.70,
                        inventory_part=inv,
                        notes="Matched capacitor by parsed engineering fields."
                    )

        return best_match

    # ====================================================================
    # RESISTOR MATCH RULES
    # ====================================================================
    def match_resistor(self, npr, inv):

        # Value must match exactly
        if npr.get("value") != inv.get("value"):
            return False

        # Wattage must match exactly
        if npr.get("wattage") != inv.get("wattage"):
            return False

        # Package must match exactly
        if npr.get("package") != inv.get("package"):
            return False

        # Mount type must match
        if npr.get("mount") != inv.get("mount"):
            return False

        # Tolerance must be equal or BETTER
        tol_npr = npr.get("tolerance")
        tol_inv = inv.get("tolerance")

        if tol_npr is not None and tol_inv is not None:
            if tol_inv > tol_npr:
                return False

        return True

    # ====================================================================
    # CAPACITOR MATCH RULES
    # ====================================================================
    def match_capacitor(self, npr, inv):

        # Capacitance must match exactly
        if npr.get("value") != inv.get("value"):
            return False

        # Dielectric must be EXACT match (strict)
        if npr.get("dielectric") != inv.get("dielectric"):
            return False

        # Voltage: inventory ≥ NPR
        v_npr = npr.get("voltage")
        v_inv = inv.get("voltage")
        if v_npr is not None and v_inv is not None:
            if v_inv < v_npr:
                return False

        # Tolerance: inventory ≤ NPR
        tol_npr = npr.get("tolerance")
        tol_inv = inv.get("tolerance")
        if tol_npr is not None and tol_inv is not None:
            if tol_inv > tol_npr:
                return False

        # Package must match
        if npr.get("package") != inv.get("package"):
            return False

        # Mount must match
        if npr.get("mount") != inv.get("mount"):
            return False

        return True
