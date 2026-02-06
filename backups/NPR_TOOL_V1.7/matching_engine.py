from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from .data_models import MatchResult, MatchType, InventoryPart, NPRPart
from .config_loader import NPRConfig



# ---------------------------------------------------------
# Helper: Prefix Extractor
# ---------------------------------------------------------
def mpn_prefix(s: str, remove_last: int = 5) -> str:
    """
    Family prefix heuristic.

    WARNING:
    - This is not guaranteed safe for all MPNs.
    - It is intentionally treated as a lower-confidence tier.
    """
    if not s:
        return ""
    s = s.strip()
    return s[:-remove_last] if len(s) > remove_last else ""


# ---------------------------------------------------------
# Configurable confidence weights (scalable)
# ---------------------------------------------------------
DEFAULT_CONFIDENCE_WEIGHTS = {
    MatchType.EXACT_MFG_PN: 1.00,
    MatchType.EXACT_ITEMNUM: 0.85,
    MatchType.PREFIX_FAMILY: 0.60,
    MatchType.SUBSTITUTE: 1.00,
    MatchType.PARSED_MATCH: 0.75,
    MatchType.API_ASSISTED: 0.55,
    MatchType.NO_MATCH: 0.00,
}


# Optional: per-type confidence scaling
DEFAULT_TYPE_CONFIDENCE = {
    "RES": 1.0,
    "CAP": 1.0,
    "LED": 0.9,
    "DIODE": 0.85,
    "MOSFET": 0.9,
    "TRANSISTOR": 0.85,
    "TRIAC": 0.8,
    "OTHER": 0.7,
}


@dataclass(frozen=True)
class FieldRule:
    """
    A single field comparison rule.
    - mode="eq": strict equality
    - mode="min": inventory >= npr
    - mode="max": inventory <= npr
    - mode="range": inventory within a percentage range of npr (for brightness, etc.)
    """
    field: str
    mode: str = "eq"
    tolerance: float = 0.0  # used for range comparisons


class MatchingEngine:
    """
    Tiered matching engine.

    Scalability rules:
    - Tier logic is stable (you can add new tiers without breaking old ones).
    - Per-type engineering comparisons are data-driven by TypeMatchSpec.
    - Confidence is explainable (UI can display why something matched).
    """

    def __init__(self,inventory_parts,config: Optional[NPRConfig] = None,):
        self.inventory = inventory_parts
        self.config = config  
    
        if config:
            self.confidence_weights = {
                MatchType[k]: v for k, v in config.tier_confidence.items()
            }
            self.type_confidence = {
                name: comp.confidence_scale
                for name, comp in config.components.items()
            }
        else:
            self.confidence_weights = DEFAULT_CONFIDENCE_WEIGHTS
            self.type_confidence = DEFAULT_TYPE_CONFIDENCE
            


    # =====================================================
    # MAIN ENTRY POINT
    # =====================================================
    def match_npr_list(self, npr_list: List[NPRPart]) -> List[tuple]:
        return [(npr, self.match_single_part(npr)) for npr in npr_list]

    # =====================================================
    # MATCH A SINGLE NPR PART
    # =====================================================
    def match_single_part(self, npr: NPRPart) -> MatchResult:
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
        match = self._engineering_match(npr)
        if match:
            return match

        # Tier 4 — API Assisted (future)
        match = self._match_by_api_data(npr)
        if match:
            return match

        # Fallback
        return MatchResult(
            match_type=MatchType.NO_MATCH,
            confidence=self.confidence_weights[MatchType.NO_MATCH],
            inventory_part=None,
            notes="No match found across all tiers.",
            explain={"tier": "fallback"},
        )

    # =====================================================
    # Tier 1 — Exact Manufacturer PN
    # =====================================================
    def _match_by_mfgpn(self, npr: NPRPart) -> Optional[MatchResult]:
        needle = npr.mfgpn.upper().strip()
        for inv in self.inventory:
            if inv.vendoritem and inv.vendoritem.upper().strip() == needle:
                return MatchResult(
                    match_type=MatchType.EXACT_MFG_PN,
                    confidence=self.confidence_weights[MatchType.EXACT_MFG_PN],
                    inventory_part=inv,
                    notes="Matched by exact Manufacturer Part #",
                    explain={"tier": "exact_mfgpn"},
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
                    confidence=self.confidence_weights[MatchType.PREFIX_FAMILY],
                    inventory_part=inv,
                    notes=f"Matched by Manufacturer Family Prefix '{prefix}'",
                    explain={"tier": "prefix_family", "prefix": prefix},
                )
        return None

    # =====================================================
    # Tier 2 — Exact Item Number
    # =====================================================
    def _match_by_itemnum(self, npr: NPRPart) -> Optional[MatchResult]:
        needle = npr.partnum.upper().strip()
        for inv in self.inventory:
            if inv.itemnum and inv.itemnum.upper().strip() == needle:
                return MatchResult(
                    match_type=MatchType.EXACT_ITEMNUM,
                    confidence=self.confidence_weights[MatchType.EXACT_ITEMNUM],
                    inventory_part=inv,
                    notes="Matched by exact Item Number",
                    explain={"tier": "exact_itemnum"},
                )
        return None

    # =====================================================
    # Tier 2.5 — Substitute Match
    # =====================================================
    def _match_by_substitute(self, npr: NPRPart) -> Optional[MatchResult]:
        if not npr.mfgpn:
            return None
        needle = npr.mfgpn.upper().strip()

        for inv in self.inventory:
            for sub in inv.substitutes:
                if sub.mfgpn and sub.mfgpn.upper().strip() == needle:
                    return MatchResult(
                        match_type=MatchType.SUBSTITUTE,
                        confidence=self.confidence_weights[MatchType.SUBSTITUTE],
                        inventory_part=inv,
                        notes=f"Matched via substitute part: {sub.sub_itemnum}",
                        explain={"tier": "substitute", "sub_itemnum": sub.sub_itemnum},
                    )
        return None

    # =====================================================
    # Tier 3 — Parsed Engineering Match (data-driven)
    # =====================================================
    def _engineering_match(self, npr):
        p_npr = npr.parsed
        ptype = p_npr.get("type")

        comp = self.config.components.get(ptype)
        if not comp:
            return None

        for inv in self.inventory:
            p_inv = inv.parsed
            if p_inv.get("type") != ptype:
                continue

            ok, explain = self._apply_field_rules(p_npr,p_inv,comp.matching_rules)
            if ok:
                base = self.confidence_weights[MatchType.PARSED_MATCH]
                scale = comp.confidence_scale
                return MatchResult(
                    match_type=MatchType.PARSED_MATCH,
                    confidence=base * scale,
                    inventory_part=inv,
                    explain=explain,
                )

        return None

    def _apply_field_rules(self,npr_p: Dict[str, Any],inv_p: Dict[str, Any],rules: list,) -> tuple[bool, Dict[str, Any]]:

        """
        Apply field-by-field rules and return (pass/fail, explain dict).

        This is the heart of "scalable matching":
        - Add new rules by editing TypeMatchSpec (later: config)
        - UI can show what failed
        """
        explain: Dict[str, Any] = {"fields": {}}

        for rule in rules:
            f = rule.field
            n = npr_p.get(f)
            i = inv_p.get(f)

            # If both missing, treat as neutral (do not fail hard).
            if n in (None, "") and i in (None, ""):
                explain["fields"][f] = {"npr": n, "inv": i, "mode": rule.mode, "result": "skip_both_missing"}
                continue

            # If one side missing, fail (conservative).
            if n in (None, "") or i in (None, ""):
                explain["fields"][f] = {"npr": n, "inv": i, "mode": rule.mode, "result": "fail_missing_one_side"}
                return False, explain

            if rule.mode == "eq":
                if str(n) != str(i):
                    explain["fields"][f] = {"npr": n, "inv": i, "mode": "eq", "result": "fail_not_equal"}
                    return False, explain
                explain["fields"][f] = {"npr": n, "inv": i, "mode": "eq", "result": "pass"}

            elif rule.mode == "min":
                # inventory >= npr (e.g., voltage rating)
                try:
                    if float(i) < float(n):
                        explain["fields"][f] = {"npr": n, "inv": i, "mode": "min", "result": "fail_inv_lt_npr"}
                        return False, explain
                    explain["fields"][f] = {"npr": n, "inv": i, "mode": "min", "result": "pass"}
                except Exception:
                    explain["fields"][f] = {"npr": n, "inv": i, "mode": "min", "result": "fail_non_numeric"}
                    return False, explain

            elif rule.mode == "max":
                # inventory <= npr (e.g., tolerance % where lower is "better")
                try:
                    if float(i) > float(n):
                        explain["fields"][f] = {"npr": n, "inv": i, "mode": "max", "result": "fail_inv_gt_npr"}
                        return False, explain
                    explain["fields"][f] = {"npr": n, "inv": i, "mode": "max", "result": "pass"}
                except Exception:
                    explain["fields"][f] = {"npr": n, "inv": i, "mode": "max", "result": "fail_non_numeric"}
                    return False, explain

            elif rule.mode == "range":
                # inventory within ±tolerance of npr (percentage)
                try:
                    tol = float(rule.tolerance)
                    n_f = float(n)
                    i_f = float(i)
                    lo = (1.0 - tol) * n_f
                    hi = (1.0 + tol) * n_f
                    if not (lo <= i_f <= hi):
                        explain["fields"][f] = {"npr": n, "inv": i, "mode": "range", "tol": tol, "result": "fail_outside_range"}
                        return False, explain
                    explain["fields"][f] = {"npr": n, "inv": i, "mode": "range", "tol": tol, "result": "pass"}
                except Exception:
                    explain["fields"][f] = {"npr": n, "inv": i, "mode": "range", "result": "fail_non_numeric"}
                    return False, explain

            else:
                explain["fields"][f] = {"npr": n, "inv": i, "mode": rule.mode, "result": "fail_unknown_mode"}
                return False, explain

        return True, explain

    # =====================================================
    # Tier 4 — API Match (future)
    # =====================================================
    def _match_by_api_data(self, npr: NPRPart) -> Optional[MatchResult]:
        # This tier exists because your issues.txt calls out a real-world need:
        # substitutes may not parse-match cleanly; API specs are required. (future)
        if not npr.mfgpn:
            return None

        for inv in self.inventory:
            api = inv.api_data
            if not api or not api.specs:
                continue

            # Example attribute cross-check:
            matches = 0
            total = 0
            for key in ("package", "voltage", "dielectric"):
                n_val = (npr.parsed or {}).get(key)
                a_val = api.specs.get(key)
                if n_val and a_val:
                    total += 1
                    if str(n_val).upper() == str(a_val).upper():
                        matches += 1

            if total > 0 and (matches / total) >= 0.6:
                return MatchResult(
                    match_type=MatchType.API_ASSISTED,
                    confidence=self.confidence_weights[MatchType.API_ASSISTED],
                    inventory_part=inv,
                    notes=f"Matched via API data ({matches}/{total} attribute match).",
                    explain={"tier": "api_assisted", "matches": matches, "total": total},
                )

        return None
