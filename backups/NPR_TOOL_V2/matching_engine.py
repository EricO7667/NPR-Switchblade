from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple
from rapidfuzz import fuzz
import re
import os, json
import hashlib
from pathlib import Path
import numpy as np
from sentence_transformers import SentenceTransformer
import threading
from .data_models import MatchResult, MatchType, InventoryPart, NPRPart
from .config_loader import NPRConfig
import tkinter
# ---------------------------------------------------------
# Helper: Prefix Extractor
# ---------------------------------------------------------
def mpn_prefix(s: str, remove_last: int = 5) -> str:
    if not s:
        return ""
    s = s.strip()
    return s[:-remove_last] if len(s) > remove_last else ""


# --- PATCH: synonym normalization for fuzzy logic ---
def _normalize_description_synonyms(text: str) -> str:
    text = text.lower()
    replacements = {
        "monolithic": "ceramic",
        "mono": "ceramic",
        "cer": "ceramic",
        "cer.": "ceramic",
        "cap ": "capacitor ",
        "cap.": "capacitor ",
    }
    for k, v in replacements.items():
        text = text.replace(k, v)
    return text


# ---------------------------------------------------------
# Embedding disk cache helpers
# ---------------------------------------------------------
_PREPROCESS_VERSION = 1

def _norm_for_embed_cache(s: str) -> str:
    s = (s or "").strip().upper()
    s = " ".join(s.split())
    return s

def _hash_desc(s: str) -> str:
    b = _norm_for_embed_cache(s).encode("utf-8")
    return hashlib.sha1(b).hexdigest()

def _safe_model_tag(model_name: str) -> str:
    # filenames safe-ish
    tag = (model_name or "model").replace("/", "__").replace(":", "_")
    return tag


def clamp01(x: float) -> float:
    try:
        x = float(x)
    except Exception:
        return 0.0
    if x < 0.0:
        return 0.0
    if x > 1.0:
        return 1.0
    return x


# ---------------------------------------------------------
# Configurable confidence weights
# ---------------------------------------------------------
DEFAULT_CONFIDENCE_WEIGHTS = {
    MatchType.EXACT_MFG_PN: 1.00,
    MatchType.PARTIAL_ITEMNUM: 0.90,
    MatchType.PREFIX_FAMILY: 0.60,
    MatchType.SUBSTITUTE: 1.00,
    MatchType.PARSED_MATCH: 0.55,
    #MatchType.SEMANTIC_DESC: 0.55,  
    MatchType.API_ASSISTED: 0.50,
    MatchType.NO_MATCH: 0.00,
}


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


class MatchingEngine:
    """
    Tiered matching engine — final refactor version.
    All functionality preserved, PLM removed, Semantic tier added.
    """

    def __init__(self, inventory_parts, config: Optional[NPRConfig] = None, *, ui_root=None, cache_dir: Optional[str] = None):
        self.inventory = inventory_parts
        self.config = config
        self.ui_root = ui_root  # Tk root for UI progress callbacks (avoid tkinter._default_root import bugs)
        # Disk cache directory (relative to cwd by default)
        self.cache_dir = Path(cache_dir or ".npr_semantic_cache")
        self.cache_dir.mkdir(parents=True, exist_ok=True)

        #self.progress_cb = progress_cb  

        if config:
            self.confidence_weights = {
                MatchType[k]: v for k, v in config.tier_confidence.items()
            }
            self.type_confidence = {
                name: comp.confidence_scale for name, comp in config.components.items()
            }
        else:
            self.confidence_weights = DEFAULT_CONFIDENCE_WEIGHTS
            self.type_confidence = DEFAULT_TYPE_CONFIDENCE

        # REPLACE this in __init__
        self._embedder = SentenceTransformer("intfloat/e5-base-v2")
        self.model_name = "intfloat/e5-base-v2"
        self._inventory_vecs = None
        self._inventory_texts = None
        self._embedder.max_seq_length = 128
        import threading
        self._embeddings_lock = threading.Lock()
        self._embeddings_ready = False


    def _get_ui_root(self):
        # Prefer explicitly passed root; fall back to tkinter's global default root if available.
        try:
            if self.ui_root is not None:
                return self.ui_root
        except Exception:
            pass
        try:
            return getattr(tkinter, "_default_root", None)
        except Exception:
            return None


    def ensure_embeddings_cache(self, *, force: bool = False) -> None:
        """Ensure inventory embeddings are computed exactly once (thread-safe).

        Uses a disk cache keyed by a stable hash of the (normalized) inventory description.
        """
        if (not force) and self._embeddings_ready and (self._inventory_vecs is not None):
            return

        with self._embeddings_lock:
            if (not force) and self._embeddings_ready and (self._inventory_vecs is not None):
                return
            self._init_embeddings_cache(force=force)
            self._embeddings_ready = True


    def async_init_embeddings(self):
        """Run embedding initialization in a background thread."""
        import threading
        def _run():
            try:
                self.ensure_embeddings_cache()
            except Exception as e:
                import traceback
                traceback.print_exc()
                print("[MatchingEngine] Async embedding initialization failed:", e)
        threading.Thread(target=_run, daemon=True).start()



    def _init_embeddings_cache(self, *, force: bool = False):
        """
        Compute and store semantic embeddings for inventory parts in memory,
        emitting live progress updates to the UI via Tk callbacks.

        Uses a disk cache: only embeds descriptions whose hash is missing.
        """
        root = self._get_ui_root()
        progress_cb = getattr(root, "loading_progress_callback", None) if root else None

        descs = [self._inv_desc(inv) for inv in self.inventory]
        total = len(descs)
        if total == 0:
            self._inventory_vecs = np.zeros((0, 1), dtype=np.float32)
            self._inventory_texts = []
            return

        # Ensure model is ready
        if self._embedder is None:
            print(f"[SEMANTIC] Loading model: {self.model_name}")
            self._embedder = SentenceTransformer(self.model_name)
            self._embedder.max_seq_length = 128

        # ---- Disk cache load / validate ----
        model_tag = _safe_model_tag(self.model_name)
        vec_path = self.cache_dir / f"embeddings_{model_tag}.npz"
        meta_path = self.cache_dir / f"embeddings_{model_tag}_meta.json"

        expected_meta = {
            "model_name": self.model_name,
            "max_seq_length": int(getattr(self._embedder, "max_seq_length", 128)),
            "preprocess_version": _PREPROCESS_VERSION,
        }

        cache = {}  # hash -> vector (np.ndarray)
        if (not force) and vec_path.exists() and meta_path.exists():
            try:
                meta = json.loads(meta_path.read_text(encoding="utf-8"))
                if all(meta.get(k) == v for k, v in expected_meta.items()):
                    data = np.load(vec_path, allow_pickle=False)
                    keys = data["keys"]
                    vecs = data["vecs"]
                    # keys: (N,), vecs: (N, dim)
                    cache = {str(keys[i]): vecs[i] for i in range(len(keys))}
            except Exception as e:
                print("[SEMANTIC] Cache load failed, rebuilding:", e)
                cache = {}

        # Hash current inventory descriptions
        hashes = [_hash_desc(d) for d in descs]

        # Inform UI that some percent may already be cached
        cached_now = sum(1 for h in hashes if h in cache)
        if progress_cb and root:
            ratio0 = cached_now / total
            root.after(0, lambda r=ratio0: progress_cb(r))

        # Determine missing items
        missing = [(h, d) for h, d in zip(hashes, descs) if h not in cache]

        if missing:
            print(f"[SEMANTIC] Embedding {len(missing)} / {total} inventory descriptions (missing from cache)...")

            # Encode missing in batches (fast)
            batch_size = 64
            processed = cached_now

            # sentence-transformers encode options
            for i in range(0, len(missing), batch_size):
                batch = missing[i:i + batch_size]
                batch_descs = [d for _, d in batch]
                batch_vecs = self._embedder.encode(
                    batch_descs,
                    batch_size=batch_size,
                    convert_to_numpy=True,
                    show_progress_bar=False,
                    normalize_embeddings=True,
                )
                for (h, _), v in zip(batch, batch_vecs):
                    cache[h] = v
                processed += len(batch)

                if progress_cb and root:
                    ratio = processed / total
                    root.after(0, lambda r=ratio: progress_cb(r))

            # Save updated cache
            try:
                keys = np.array(list(cache.keys()), dtype="<U40")
                vecs = np.vstack([cache[k] for k in keys])
                np.savez_compressed(vec_path, keys=keys, vecs=vecs)
                meta_path.write_text(json.dumps(expected_meta, indent=2), encoding="utf-8")
                print(f"[SEMANTIC] Cache saved: {vec_path} ({len(keys)} vectors)")
            except Exception as e:
                print("[SEMANTIC] Cache save failed:", e)
        else:
            print(f"[SEMANTIC] All {total} inventory descriptions loaded from cache.")

        # Assemble inventory vectors in current inventory order
        try:
            self._inventory_vecs = np.vstack([cache[h] for h in hashes])
        except Exception:
            # fallback: re-embed everything if something went wrong (should be rare)
            print("[SEMANTIC] Cache assembly failed, rebuilding full cache once.")
            vecs = self._embedder.encode(
                descs,
                batch_size=64,
                convert_to_numpy=True,
                show_progress_bar=False,
                normalize_embeddings=True,
            )
            self._inventory_vecs = vecs
            self._inventory_texts = descs
            return

        self._inventory_texts = descs
        print(f"[SEMANTIC] Semantic cache ready for {total} items.")


    # =====================================================
    # Helper methods
    # =====================================================
    def _inv_item(self, inv: Any) -> str:
        return (getattr(inv, "itemnum", None) or "").strip()

    def _inv_desc(self, inv: Any) -> str:
        return (
            getattr(inv, "desc", None)
            or getattr(inv, "description", None)
            or ""
        ).strip()

    def _summarize_inv(self, inv: Any, *, seed=None, score=None, ratio=None) -> Dict[str, Any]:
        return {
            "inv_item": self._inv_item(inv),
            "inv_desc": self._inv_desc(inv),
            "vendor_mpn": (getattr(inv, "vendoritem", None) or "").strip(),
            "mfg": (getattr(inv, "manufacturer", None) or getattr(inv, "mfgname", None) or "").strip(),
            "stock": int(getattr(inv, "stock", 0) or 0),
            "seed": float(seed) if seed is not None else None,
            "score": float(score) if score is not None else None,
            "ratio": float(ratio) if ratio is not None else None,
        }

    def _attach_top_lists(
        self,
        match: "MatchResult",
        *,
        candidates: List[Any],
        winner: Optional[Any],
        limit: int = 10,
    ) -> None:
        if not hasattr(match, "explain") or not isinstance(match.explain, dict):
            match.explain = {}

        winner_item = self._inv_item(winner) if winner else ""

        seen = set()
        top_objs: List[Any] = []
        for inv in (candidates or [])[: max(1, int(limit)) * 3]:
            key = self._inv_item(inv) or repr(inv)
            if key in seen:
                continue
            seen.add(key)
            top_objs.append(inv)
            if len(top_objs) >= limit:
                break

        top_list = []
        for inv in top_objs:
            seed = getattr(inv, "_pc_seed", None)
            score = getattr(inv, "_pc_score", None)
            top_list.append(self._summarize_inv(inv, seed=seed, score=score))

        match.explain["top"] = top_list

    # =====================================================
    # Match tiers
    # =====================================================
    def match_single_part(self, npr: NPRPart) -> MatchResult:
        # Tier 1 — Exact MFG PN
        if npr.mfgpn:
            match = self._match_by_mfgpn(npr)
            if match:
                return match

        # Tier 1.5 — Family Prefix
        if npr.mfgpn:
            match = self._match_by_prefix(npr)
            if match:
                return match

        # Tier 2 — Item Number
        if npr.partnum:
            match = self._match_by_itemnum(npr)
            if match:
                return match

        # Tier 2.5 — Substitute
        match = self._match_by_substitute(npr)
        if match:
            return match

        # Tier 3 — Semantic (replaces engineering)
        match = self._engineering_match(npr)
        if match:
            return match

        # Tier 4 — API Assisted (future)
        match = self._match_by_api_data(npr)
        if match:
            return match

        return MatchResult(
            match_type=MatchType.NO_MATCH,
            confidence=self.confidence_weights[MatchType.NO_MATCH],
            inventory_part=None,
            notes="No match found across all tiers.",
            explain={"tier": "fallback", "top": []},
        )

    # =====================================================
    # Other tiers 
    # =====================================================
    def _match_by_mfgpn(self, npr: NPRPart) -> Optional[MatchResult]:
        needle = npr.mfgpn.upper().strip()
        hits = [
            inv for inv in self.inventory
            if inv.vendoritem and inv.vendoritem.upper().strip() == needle
        ]
        if not hits:
            return None

        best = max(hits, key=lambda inv: getattr(inv, "stock", 0) or 0)
        return MatchResult(
            match_type=MatchType.EXACT_MFG_PN,
            confidence=self.confidence_weights[MatchType.EXACT_MFG_PN],
            inventory_part=best,
            candidates=hits,
            notes=f"Matched by exact Manufacturer Part # ({len(hits)} candidates)",
            explain={"tier": "exact_mfgpn", "candidate_count": len(hits)},
        )

    def _match_by_prefix(self, npr: NPRPart) -> Optional[MatchResult]:
        prefix = mpn_prefix(npr.mfgpn)
        if not prefix:
            return None

        prefix_u = prefix.strip().upper()
        hits = [
            inv for inv in self.inventory
            if inv.vendoritem and inv.vendoritem.strip().upper().startswith(prefix_u)
        ]
        if not hits:
            return None

        best = max(hits, key=lambda inv: getattr(inv, "stock", 0) or 0)
        return MatchResult(
            match_type=MatchType.PREFIX_FAMILY,
            confidence=self.confidence_weights[MatchType.PREFIX_FAMILY],
            inventory_part=best,
            candidates=hits,
            notes=f"Matched by Manufacturer Family Prefix '{prefix_u}'",
            explain={"tier": "prefix_family", "prefix": prefix_u, "candidate_count": len(hits)},
        )

    def _match_by_itemnum(self, npr: NPRPart) -> Optional[MatchResult]:
        needle = npr.partnum.upper().strip()
        for inv in self.inventory:
            if inv.itemnum and inv.itemnum.upper().strip() == needle:
                return MatchResult(
                    match_type=MatchType.PARTIAL_ITEMNUM,
                    confidence=self.confidence_weights[MatchType.PARTIAL_ITEMNUM],
                    inventory_part=inv,
                    candidates=[inv],
                    notes="Matched by PARTIAL Item Number",
                    explain={"tier": "partial_itemnum", "candidate_count": 1},
                )
        return None

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
                        candidates=[inv],
                        notes=f"Matched via substitute part: {sub.sub_itemnum}",
                        explain={"tier": "substitute", "candidate_count": 1},
                    )
        return None

    # =====================================================
    # TIER 3 — SEMANTIC EMBEDDING 
    # =====================================================
    def _engineering_match(self, npr: NPRPart) -> Optional[MatchResult]:
        """Optimized semantic similarity matching (replaces parsing tier)."""
        desc = (npr.description or npr.desc or "").strip()
        if not desc:
            return None

        if self._inventory_vecs is None:
            self.ensure_embeddings_cache()

        bom_vec = self._embedder.encode([desc], normalize_embeddings=True)[0]
        sims = np.dot(self._inventory_vecs, bom_vec)

        # Pick top-k
        top_k = 50
        top_idx = np.argsort(-sims)[:top_k]
        top_inv = [self.inventory[i] for i in top_idx]
        top_scores = [float(sims[i]) for i in top_idx]

        best_score = top_scores[0]
        best_inv = top_inv[0]

        # Confidence threshold
        if best_score < 0.52:
            return None

        explain = {
            "tier": "semantic_match",
            "candidate_count": len(top_inv),
            "top_candidates": [
                {
                    "inv_item": getattr(inv, "itemnum", ""),
                    "inv_desc": getattr(inv, "description", getattr(inv, "desc", "")),
                    "score": float(score),
                }
                for inv, score in zip(top_inv[:10], top_scores[:10])
            ],
        }

        return MatchResult(
            match_type=MatchType.PARSED_MATCH,
            confidence=round(best_score, 3),
            inventory_part=best_inv,
            candidates=top_inv[:10],
            notes=f"Semantic similarity match (cos={best_score:.3f})",
            explain=explain,
        )


    def _apply_field_rules(
        self,
        npr_p: Dict[str, Any],
        inv_p: Dict[str, Any],
        rules: list,
    ) -> tuple[bool, Dict[str, Any], float]:
        from rapidfuzz import fuzz
        import re

        _SI = {"P": 1e-12, "N": 1e-9, "U": 1e-6, "Μ": 1e-6, "M": 1e-3, "K": 1e3, "R": 1.0, "G": 1e9}

        def _norm(s: Any) -> str:
            return str(s or "").strip().upper()

        def _parse_eng_number(s: Any) -> Optional[float]:
            t = _norm(s).replace("OHM", "").replace("Ω", "").replace(" ", "")
            if not t:
                return None

            # 4K7 / 2R2 style
            m = re.match(r"^(\d+)([RKMUNP]|Μ)(\d+)$", t)
            if m:
                a, sym, b = m.groups()
                return float(f"{a}.{b}") * _SI.get(sym, 1.0)

            # 10K / 10000 / 0.1UF / 100NF etc (unit char optional)
            m = re.match(r"^([0-9]*\.?[0-9]+)([PNUMKGR]?)(?:F|H|V|A)?$", t)
            if m:
                val, sym = m.groups()
                return float(val) * _SI.get(sym, 1.0)

            return None

        def _rel_close(a: float, b: float, tol: float) -> bool:
            if a == 0:
                return b == 0
            return abs(a - b) / abs(a) <= tol

        # Treat "STANDARD" as package too since your YAML uses standard->package extractor.
        PACKAGE_FIELDS = {"PACKAGE", "FOOTPRINT", "CASE", "SIZE", "STANDARD"}
        VALUE_FIELDS = {"RESISTANCE", "CAPACITANCE"}

        explain: Dict[str, Any] = {"fields": {}}
        total_w = 0.0
        score_w = 0.0

        for rule in rules:
            f = rule.field
            n = npr_p.get(f)
            i = inv_p.get(f)

            # If both missing, ignore (do NOT count in ratio)
            if n in (None, "") and i in (None, ""):
                explain["fields"][f] = {"npr": n, "inv": i, "mode": rule.mode, "result": "skip_both_missing"}
                continue

            # If BOM/NPR is missing but inventory has value:
            # For min/max/range constraints (like tolerance "max"), absence in BOM means "no constraint".
            if n in (None, "") and i not in (None, ""):
                if rule.mode in ("min", "max", "range"):
                    explain["fields"][f] = {"npr": n, "inv": i, "mode": rule.mode, "result": "skip_npr_missing_optional"}
                    continue

                total_w += 1.0
                explain["fields"][f] = {"npr": n, "inv": i, "mode": rule.mode, "result": "fail_npr_missing"}
                continue

            # If inventory is missing but NPR has a required value, count as scoring fail.
            if i in (None, "") and n not in (None, ""):
                total_w += 1.0
                explain["fields"][f] = {"npr": n, "inv": i, "mode": rule.mode, "result": "fail_inv_missing"}
                continue

            n_u = _norm(n)
            i_u = _norm(i)

            # HARD GATE: PACKAGE
            if f.strip().upper() in PACKAGE_FIELDS:
                if n_u != i_u:
                    explain["fields"][f] = {"npr": n_u, "inv": i_u, "mode": "HARD_EQ", "result": "HARD_FAIL_package_mismatch"}
                    return False, explain, 0.0
                total_w += 1.0
                score_w += 1.0
                explain["fields"][f] = {"npr": n_u, "inv": i_u, "mode": "HARD_EQ", "result": "pass"}
                continue

            # HARD GATE: VALUE (engineering numeric)
            if f.strip().upper() in VALUE_FIELDS:
                n_val = _parse_eng_number(n)
                i_val = _parse_eng_number(i)
                if n_val is None or i_val is None:
                    explain["fields"][f] = {"npr": n_u, "inv": i_u, "mode": "HARD_NUM", "result": "HARD_FAIL_unparseable_value"}
                    return False, explain, 0.0

                if not _rel_close(n_val, i_val, tol=0.01):  # 1%
                    explain["fields"][f] = {"npr": n_u, "inv": i_u, "mode": "HARD_NUM", "result": "HARD_FAIL_value_mismatch"}
                    return False, explain, 0.0

                total_w += 1.0
                score_w += 1.0
                explain["fields"][f] = {"npr": n_u, "inv": i_u, "mode": "HARD_NUM", "result": "pass"}
                continue

            # SOFT/SCORING RULES
            total_w += 1.0
            result = "fail"
            add = 0.0

            if rule.mode == "eq":
                if n_u == i_u:
                    result = "pass"
                    add = 1.0

            elif rule.mode in ("min", "max", "range"):
                try:
                    n_f, i_f = float(n), float(i)
                    if rule.mode == "min":
                        ok = i_f >= n_f
                    elif rule.mode == "max":
                        ok = i_f <= n_f
                    else:
                        tol = float(getattr(rule, "tolerance", 0.05))
                        ok = (1.0 - tol) * n_f <= i_f <= (1.0 + tol) * n_f

                    if ok:
                        result = "pass"
                        add = 1.0
                    else:
                        result = "fail_numeric_gate"
                except Exception:
                    result = "fail_non_numeric"

            elif rule.mode == "hybrid":
                # --- PATCH: normalize common capacitor synonyms before fuzzy scoring ---
                n_u_norm = _normalize_description_synonyms(n_u)
                i_u_norm = _normalize_description_synonyms(i_u)
                ratio = fuzz.token_set_ratio(n_u_norm, i_u_norm) / 100.0

                #ratio = fuzz.token_set_ratio(n_u, i_u) / 100.0
                threshold = float(getattr(rule, "threshold", 0.85))
                if ratio >= threshold:
                    result = "pass" if ratio >= 0.92 else "soft_pass"
                    add = 1.0 if result == "pass" else 0.6
                explain["fields"][f] = {"npr": n_u, "inv": i_u, "mode": "hybrid", "ratio": ratio, "threshold": threshold, "result": result}
                score_w += add
                continue
            else:
                result = "fail_unknown_mode"

            explain["fields"][f] = {"npr": n_u, "inv": i_u, "mode": rule.mode, "result": result}
            score_w += add

        match_ratio = (score_w / total_w) if total_w > 0 else 0.0
        passed_threshold = match_ratio >= 0.85
        return passed_threshold, explain, match_ratio

    #def _apply_field_rules(self,npr_p: Dict[str, Any],inv_p: Dict[str, Any],rules: list,) -> tuple[bool, Dict[str, Any]]:
#
    #    """
    #    Apply field-by-field rules and return (pass/fail, explain dict).
#
    #    This is the heart of "scalable matching":
    #    - Add new rules by editing TypeMatchSpec (later: config)
    #    - UI can show what failed
    #    """
    #    explain: Dict[str, Any] = {"fields": {}}
#
    #    for rule in rules:
    #        f = rule.field
    #        n = npr_p.get(f)
    #        i = inv_p.get(f)
#
    #        # If both missing, treat as neutral (do not fail hard).
    #        if n in (None, "") and i in (None, ""):
    #            explain["fields"][f] = {"npr": n, "inv": i, "mode": rule.mode, "result": "skip_both_missing"}
    #            continue
#
    #        # If one side missing, fail (conservative).
    #        if n in (None, "") or i in (None, ""):
    #            explain["fields"][f] = {"npr": n, "inv": i, "mode": rule.mode, "result": "fail_missing_one_side"}
    #            return False, explain
#
    #        if rule.mode == "eq":
    #            if str(n) != str(i):
    #                explain["fields"][f] = {"npr": n, "inv": i, "mode": "eq", "result": "fail_not_equal"}
    #                return False, explain
    #            explain["fields"][f] = {"npr": n, "inv": i, "mode": "eq", "result": "pass"}
#
    #        elif rule.mode == "min":
    #            # inventory >= npr (e.g., voltage rating)
    #            try:
    #                if float(i) < float(n):
    #                    explain["fields"][f] = {"npr": n, "inv": i, "mode": "min", "result": "fail_inv_lt_npr"}
    #                    return False, explain
    #                explain["fields"][f] = {"npr": n, "inv": i, "mode": "min", "result": "pass"}
    #            except Exception:
    #                explain["fields"][f] = {"npr": n, "inv": i, "mode": "min", "result": "fail_non_numeric"}
    #                return False, explain
#
    #        elif rule.mode == "max":
    #            # inventory <= npr (e.g., tolerance % where lower is "better")
    #            try:
    #                if float(i) > float(n):
    #                    explain["fields"][f] = {"npr": n, "inv": i, "mode": "max", "result": "fail_inv_gt_npr"}
    #                    return False, explain
    #                explain["fields"][f] = {"npr": n, "inv": i, "mode": "max", "result": "pass"}
    #            except Exception:
    #                explain["fields"][f] = {"npr": n, "inv": i, "mode": "max", "result": "fail_non_numeric"}
    #                return False, explain
#
    #        elif rule.mode == "range":
    #            # inventory within ±tolerance of npr (percentage)
    #            try:
    #                tol = float(rule.tolerance)
    #                n_f = float(n)
    #                i_f = float(i)
    #                lo = (1.0 - tol) * n_f
    #                hi = (1.0 + tol) * n_f
    #                if not (lo <= i_f <= hi):
    #                    explain["fields"][f] = {"npr": n, "inv": i, "mode": "range", "tol": tol, "result": "fail_outside_range"}
    #                    return False, explain
    #                explain["fields"][f] = {"npr": n, "inv": i, "mode": "range", "tol": tol, "result": "pass"}
    #            except Exception:
    #                explain["fields"][f] = {"npr": n, "inv": i, "mode": "range", "result": "fail_non_numeric"}
    #                return False, explain
    #            
    #        elif rule.mode == "hybrid":
    #            # Hybrid fuzzy + numeric-token guard
    #            n_str = str(n)
    #            i_str = str(i)
    #            ratio = fuzz.token_set_ratio(n_str, i_str) / 100.0
    #            threshold = float(getattr(rule, "threshold", 0.75))
    #            nums_n = set(re.findall(r"\d+", n_str))
    #            nums_i = set(re.findall(r"\d+", i_str))
    #            conflict = bool(nums_n and nums_i and nums_n != nums_i)
#
    #            explain["fields"][f] = {
    #                "npr": n_str,
    #                "inv": i_str,
    #                "mode": "hybrid",
    #                "ratio": ratio,
    #                "threshold": threshold,
    #                "numeric_conflict": conflict,
    #            }
#
    #            if conflict or ratio < threshold:
    #                explain["fields"][f]["result"] = "fail"
    #                return False, explain
    #            else:
    #                explain["fields"][f]["result"] = "pass"
#
#
    #        else:
    #            explain["fields"][f] = {"npr": n, "inv": i, "mode": rule.mode, "result": "fail_unknown_mode"}
    #            return False, explain
#
    #    return True, explain

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
    

    def match_async(self, npr_list: List[NPRPart], callback):
        """Runs full match list in a background thread to keep UI responsive."""
        def task():
            results = [(npr, self.match_single_part(npr)) for npr in npr_list]
            callback(results)
        threading.Thread(target=task, daemon=True).start()