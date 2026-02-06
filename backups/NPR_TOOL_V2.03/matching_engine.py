from __future__ import annotations

from typing import Any, Dict, List, Optional
import json
import hashlib
from pathlib import Path
import numpy as np
from sentence_transformers import SentenceTransformer
import threading
from .data_models import MatchResult, MatchType, NPRPart
from .config_loader import NPRConfig
import tkinter
from rapidfuzz import fuzz
import re
import os
import threading
from dataclasses import dataclass
from collections import defaultdict
from typing import Tuple
import time

try:
    import torch
    from transformers import AutoTokenizer, AutoModelForMaskedLM
except Exception:
    torch = None
    AutoTokenizer = None
    AutoModelForMaskedLM = None

try:
    from sentence_transformers import CrossEncoder
except Exception:
    CrossEncoder = None

import errno


# =====================================================
# Engineering Match v2.02 - global debug/behavior flags
# =====================================================

# Master toggles
ENG_DEBUG = 1
ENG_USE_DENSE = 1
ENG_USE_SPARSE = 1
ENG_USE_RERANK = 1
ENG_USE_FUZZY_FALLBACK = 1

# Primary retrieval order: "dense" or "sparse"
ENG_PRIMARY_RETRIEVER = "dense"  # set to "sparse" to swap order

# Retrieval sizes
ENG_TOPK_PRIMARY = 250          # candidates after primary retrieval
ENG_TOPK_SECONDARY = 250        # secondary rescoring pool (typically same as primary)
ENG_RERANK_K = 80               # rerank budget
ENG_RETURN_K = 25               # final candidates shown

# Score mixing
ENG_W_DENSE = 0.60              # weight for dense stage score
ENG_W_SPARSE = 0.40             # weight for sparse stage score
ENG_W_RERANK = 0.70             # how much reranker dominates final score (0..1)

# SPLADE settings + caching
ENG_SPLADE_MAX_LEN = 128
ENG_SPLADE_TOP_TERMS = 256
ENG_SPARSE_CACHE_ENABLE = 1     # cache doc sparse vectors to disk
ENG_SPARSE_CACHE_BUILD_INV_INDEX = 1  # build inverted index for full sparse retrieval

# Fuzzy fallback
ENG_FUZZY_TOPK = 250            # how many best fuzzy candidates to keep


# =====================================================
# Engineering trace dump (JSONL)
# =====================================================
ENG_TRACE_ENABLE = 1
ENG_TRACE_PATH = "eng_trace.jsonl"   # relative to cache_dir
ENG_TRACE_TOPN = 250                 # how many candidates to store per stage (save space or look and gander at everything set to size of ENG_TOPK_PRIMARY)

# ---------------------------------------------------------
# Helpers for the MATCHING ENIGINE: Prefix Extractor
# ---------------------------------------------------------
def mpn_prefix(s: str, remove_last: int = 5) -> str:
    if not s:
        return ""
    s = s.strip()
    return s[:-remove_last] if len(s) > remove_last else ""

# --- Depreciated synonym normalization for fuzzy logic ---
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
def _os_replace_retry(src: str, dst: str, tries: int = 10, delay: float = 0.08) -> bool:
    last = None
    for _ in range(tries):
        try:
            os.replace(src, dst)
            return True
        except PermissionError as e:
            last = e
            time.sleep(delay)
        except OSError as e:
            last = e
            time.sleep(delay)
    return False
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

    def __init__(self, inventory_parts, config: Optional[NPRConfig] = None, *, ui_root=None,
             cache_dir: Optional[str] = None, stop_event: Optional[threading.Event] = None):
        self.inventory = inventory_parts
        self.stop_event = stop_event
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

        self._embedder = SentenceTransformer("intfloat/e5-base-v2")
        self.model_name = "intfloat/e5-base-v2"
        self._inventory_vecs = None
        self._inventory_texts = None
        self._embedder.max_seq_length = 128
        
        self._embeddings_lock = threading.Lock()
        self._embeddings_ready = False

        self._sub_mpn_index: Dict[str, Any] = {}   # normalized mfgpn -> InventoryPart (base)
        self._sub_mpn_conflicts: Dict[str, List[str]] = {}  # normalized mfgpn -> [base itemnums...]
        self._sub_index_ready = False

        # ---------------------------------------------------------
        # Sparse (SPLADE) + Rerank (CrossEncoder)
        # ---------------------------------------------------------
        self.sparse_model_name = getattr(config, "sparse_model_name", None) if config else None
        self.reranker_model_name = getattr(config, "reranker_model_name", None) if config else None

        # Defaults (easy swaps)
        self.sparse_model_name = self.sparse_model_name or "naver/splade-cocondenser-ensembledistil"
        self.reranker_model_name = self.reranker_model_name or "cross-encoder/ms-marco-MiniLM-L-6-v2"

        self._splade_tokenizer = None
        self._splade_model = None
        self._splade_device = None

        # Inverted index: token_id -> list[(doc_index, weight)]
        self._splade_inv_index = defaultdict(list)
        self._splade_doc_norms = []
        self._splade_docs = []
        self._splade_ready = False

        self._reranker = None

        self._sparse_lock = threading.Lock()

        # Optional disk cache for SPLADE doc vectors
        self._splade_doc_term_ids = None   # List[np.ndarray] (token ids)
        self._splade_doc_term_wts = None   # List[np.ndarray] (weights)
        self._splade_doc_hash = None       # inventory hash used for cache validation

    #===================================================
    # HELPERS
    #===================================================
    def _trace_path(self):
        # keep trace next to your caches
        return (self.cache_dir / ENG_TRACE_PATH)

    def _trace_write(self, rec: dict) -> None:
        if not ENG_TRACE_ENABLE:
            return
        try:
            p = self._trace_path()
            p.parent.mkdir(parents=True, exist_ok=True)

            # thread-safe append
            if not hasattr(self, "_trace_lock"):
                self._trace_lock = threading.Lock()

            line = json.dumps(rec, ensure_ascii=False)
            with self._trace_lock:
                with open(p, "a", encoding="utf-8") as f:
                    f.write(line + "\n")
        except Exception as e:
            if ENG_DEBUG:
                print(f"[TRACE] failed to write: {e}")

    def trace_reset(self) -> None:
        """Call this before a run if you want a clean file."""
        try:
            p = self._trace_path()
            if p.exists():
                p.unlink()
        except Exception:
            pass


    def _inv_fingerprint(self) -> str:
        """
        Stable-ish fingerprint for inventory descriptions used to validate caches.
        """
        h = hashlib.sha1()
        for inv in self.inventory:
            d = (self._inv_desc(inv) or "").strip().upper().encode("utf-8", errors="ignore")
            h.update(d)
            h.update(b"\n")
        return h.hexdigest()

    def _get_or_parse_inv_fields(self, inv: Any) -> dict:
        """
        Try to get parsed fields from the inventory object.
        If missing, do a light parse of inv description and cache it on the object.
        """
        ip = self._safe_get_parsed(inv)
        if ip:
            return ip

        cached = getattr(inv, "_pc_parsed_cache", None)
        if isinstance(cached, dict):
            return cached

        # Light parse (cheap): just try to infer type/value/package from text
        desc = str(self._inv_desc(inv) or "")
        d = desc.upper()

        out = {}

        # Type buckets (minimal)
        if "RES" in d:
            out["type"] = "RES"
        elif "CAP" in d or "MLCC" in d:
            out["type"] = "CAP"
        elif "IND" in d:
            out["type"] = "IND"

        # Package (common passives)
        for pkg in ("01005", "0201", "0402", "0603", "0805", "1206", "1210", "1812", "2010", "2512"):
            if pkg in d:
                out["package"] = pkg
                break

        # Value: resistors like 20K / 20.0K / 20K0 / 20KOHM, etc.
        # (kept simple because _norm_res_value will normalize it)
        import re
        m = re.search(r"\b(\d+(?:\.\d+)?)\s*([RKMGT])\b", d)
        if m:
            out["value"] = m.group(1) + m.group(2)
        else:
            # also catch raw ohms like "20000" or "20000OHM"
            m2 = re.search(r"\b(\d+(?:\.\d+)?)\s*(OHM|Ω)\b", d)
            if m2:
                out["value"] = m2.group(1)

        inv._pc_parsed_cache = out
        return out

    def _splade_cache_paths(self) -> tuple[Path, Path]:
        model_tag = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(self.sparse_model_name))
        vec_path = self.cache_dir / f"splade_{model_tag}_docs.npz"
        meta_path = self.cache_dir / f"splade_{model_tag}_meta.json"
        return vec_path, meta_path

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

    def _should_stop(self) -> bool:
        try:
            return bool(self.stop_event and self.stop_event.is_set())
        except Exception:
            return False

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

    # =====================================================
    # SPLADE internals 
    # =====================================================

    def _try_load_splade_doc_cache(self) -> bool:
        if not ENG_SPARSE_CACHE_ENABLE:
            return False

        vec_path, meta_path = self._splade_cache_paths()
        if not vec_path.exists() or not meta_path.exists():
            return False

        expected = {
            "model_name": self.sparse_model_name,
            "preprocess_version": _PREPROCESS_VERSION,
            "max_len": int(ENG_SPLADE_MAX_LEN),
            "top_terms": int(ENG_SPLADE_TOP_TERMS),
            "inventory_hash": self._inv_fingerprint(),
        }

        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
            if any(meta.get(k) != v for k, v in expected.items()):
                return False

            with np.load(vec_path, allow_pickle=False) as data:
                ids_flat = data["ids_flat"]
                wts_flat = data["wts_flat"]
                offsets = data["offsets"]
                doc_norms = data["doc_norms"].tolist()

            term_ids = []
            term_wts = []
            for i in range(len(offsets) - 1):
                a = int(offsets[i])
                b = int(offsets[i + 1])
                term_ids.append(ids_flat[a:b].copy())
                term_wts.append(wts_flat[a:b].copy())

            self._splade_doc_term_ids = term_ids
            self._splade_doc_term_wts = term_wts
            self._splade_doc_norms = doc_norms
            self._splade_doc_hash = expected["inventory_hash"]

            if ENG_DEBUG:
                print(f"[SPLADE] loaded doc cache: {vec_path.name}")
            return True

        except Exception as e:
            if ENG_DEBUG:
                print(f"[SPLADE] failed to load doc cache: {e}")
            return False

    
    
    def _save_splade_doc_cache(self) -> None:
        if not ENG_SPARSE_CACHE_ENABLE:
            return
        if self._splade_doc_term_ids is None or self._splade_doc_term_wts is None or not self._splade_doc_norms:
            return

        inv_hash = self._inv_fingerprint()
        vec_path, meta_path = self._splade_cache_paths()

        meta = {
            "model_name": self.sparse_model_name,
            "preprocess_version": _PREPROCESS_VERSION,
            "max_len": int(ENG_SPLADE_MAX_LEN),
            "top_terms": int(ENG_SPLADE_TOP_TERMS),
            "inventory_hash": inv_hash,
            "format": "flat+offsets",
        }

        try:
            # Flatten ragged (ids,wts) into one big array + offsets
            offsets = [0]
            ids_all = []
            wts_all = []
            for ids, wts in zip(self._splade_doc_term_ids, self._splade_doc_term_wts):
                ids = np.asarray(ids, dtype=np.int32)
                wts = np.asarray(wts, dtype=np.float32)
                ids_all.append(ids)
                wts_all.append(wts)
                offsets.append(offsets[-1] + len(ids))

            ids_flat = np.concatenate(ids_all, axis=0) if ids_all else np.array([], dtype=np.int32)
            wts_flat = np.concatenate(wts_all, axis=0) if wts_all else np.array([], dtype=np.float32)
            offsets = np.asarray(offsets, dtype=np.int64)
            doc_norms = np.asarray(self._splade_doc_norms, dtype=np.float32)

            np.savez_compressed(
                str(vec_path),
                ids_flat=ids_flat,
                wts_flat=wts_flat,
                offsets=offsets,
                doc_norms=doc_norms,
            )
            meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")

            if ENG_DEBUG:
                print(f"[SPLADE] saved doc cache: {vec_path.name}")
        except Exception as e:
            if ENG_DEBUG:
                print(f"[SPLADE] failed to save doc cache: {e}")

    def _splade_ensure_model(self) -> None:
        if self._splade_model is not None:
            return
        if torch is None or AutoTokenizer is None or AutoModelForMaskedLM is None:
            raise RuntimeError("SPLADE requires torch + transformers installed.")

        self._splade_tokenizer = AutoTokenizer.from_pretrained(self.sparse_model_name)
        self._splade_model = AutoModelForMaskedLM.from_pretrained(self.sparse_model_name)

        self._splade_device = "cuda" if torch.cuda.is_available() else "cpu"
        self._splade_model.to(self._splade_device)
        self._splade_model.eval()

    def _splade_encode_sparse(self, text: str, *, max_length: int = 128, top_terms: int = 256) -> Dict[int, float]:
        """
        SPLADE-ish sparse expansion.
        w_j = max_i log(1 + relu(logit_{i,j}))
        Returns dict[token_id] = weight
        """
        self._splade_ensure_model()
        assert torch is not None

        with torch.no_grad():
            toks = self._splade_tokenizer(
                text or "",
                truncation=True,
                max_length=max_length,
                padding=False,
                return_tensors="pt",
            )
            toks = {k: v.to(self._splade_device) for k, v in toks.items()}
            out = self._splade_model(**toks)
            logits = out.logits  # [B, T, V]

            w = torch.log1p(torch.relu(logits))
            w = torch.max(w, dim=1).values[0]  # [V]
            w = w.detach().cpu()

            if 0 < top_terms < w.numel():
                topv, topi = torch.topk(w, k=top_terms)
                return {int(i): float(v) for i, v in zip(topi.tolist(), topv.tolist()) if v > 0}
            else:
                nz = torch.nonzero(w > 0).squeeze(-1)
                return {int(i): float(w[i]) for i in nz.tolist()}

    def ensure_splade_index(self, *, force: bool = False) -> None:
        """
        Ensure SPLADE doc vectors exist (and optionally an inverted index for full retrieval).
        Uses optional disk cache to avoid rebuild cost.
        """
        if self._splade_ready and (not force):
            return

        with self._sparse_lock:
            if self._splade_ready and (not force):
                return

            # Try disk cache first
            if (not force) and self._try_load_splade_doc_cache():
                # Optionally rebuild inverted index from cached doc vectors
                if ENG_SPARSE_CACHE_BUILD_INV_INDEX:
                    self._splade_inv_index.clear()
                    for di, (ids, wts) in enumerate(zip(self._splade_doc_term_ids, self._splade_doc_term_wts)):
                        for tid, wt in zip(ids.tolist(), wts.tolist()):
                            self._splade_inv_index[int(tid)].append((di, float(wt)))
                self._splade_ready = True
                return

            # Build fresh
            self._splade_ensure_model()

            self._splade_inv_index.clear()
            self._splade_docs = [self._inv_desc(inv) for inv in self.inventory]
            self._splade_doc_norms = []
            self._splade_doc_term_ids = []
            self._splade_doc_term_wts = []

            for di, doc in enumerate(self._splade_docs):
                sparse = self._splade_encode_sparse(doc, max_length=ENG_SPLADE_MAX_LEN, top_terms=ENG_SPLADE_TOP_TERMS)
                # store compact doc vector
                ids = np.array(list(sparse.keys()), dtype=np.int32)
                wts = np.array(list(sparse.values()), dtype=np.float32)
                self._splade_doc_term_ids.append(ids)
                self._splade_doc_term_wts.append(wts)

                norm2 = float(np.dot(wts, wts)) if len(wts) else 0.0
                self._splade_doc_norms.append((norm2 ** 0.5) if norm2 > 0 else 1.0)

                if ENG_SPARSE_CACHE_BUILD_INV_INDEX:
                    for tid, wt in sparse.items():
                        self._splade_inv_index[int(tid)].append((di, float(wt)))

            # Save cache
            self._save_splade_doc_cache()
            self._splade_ready = True

    def _splade_score_shortlist(self, query: str, cand_indices: List[int]) -> List[float]:
        """
        Score query vs a shortlist of inventory docs using cached SPLADE doc vectors.
        Returns cosine-like scores aligned with cand_indices.
        """
        self.ensure_splade_index()
        q_sparse = self._splade_encode_sparse(query, max_length=ENG_SPLADE_MAX_LEN, top_terms=ENG_SPLADE_TOP_TERMS)
        if not q_sparse:
            return [0.0 for _ in cand_indices]

        q_ids = list(q_sparse.keys())
        q_wts = list(q_sparse.values())
        qnorm2 = sum(w * w for w in q_wts)
        qnorm = (qnorm2 ** 0.5) if qnorm2 > 0 else 1.0

        # Build dict for fast lookups (query is small)
        q_map = q_sparse

        out = []
        for di in cand_indices:
            ids = self._splade_doc_term_ids[di]
            wts = self._splade_doc_term_wts[di]
            dot = 0.0
            # doc vector is compact; iterate doc terms
            for tid, dw in zip(ids.tolist(), wts.tolist()):
                qw = q_map.get(int(tid))
                if qw:
                    dot += float(qw) * float(dw)

            denom = qnorm * float(self._splade_doc_norms[di] or 1.0)
            out.append(dot / denom if denom > 0 else 0.0)
        return out

    def _splade_search(self, query: str, *, top_k: int = 200) -> List[Tuple[int, float]]:
        """
        Returns [(doc_index, score)] sorted desc
        """
        if not self._splade_ready:
            self.ensure_splade_index()

        q_sparse = self._splade_encode_sparse(query, max_length=128, top_terms=256)
        acc = defaultdict(float)
        qnorm2 = 0.0

        for tid, qw in q_sparse.items():
            qnorm2 += qw * qw
            postings = self._splade_inv_index.get(tid)
            if not postings:
                continue
            for di, dw in postings:
                acc[di] += qw * dw

        qnorm = (qnorm2 ** 0.5) if qnorm2 > 0 else 1.0

        scored = []
        for di, dot in acc.items():
            s = dot / (qnorm * self._splade_doc_norms[di])
            scored.append((di, float(s)))

        scored.sort(key=lambda x: x[1], reverse=True)
        return scored[: max(1, int(top_k))]

    # =====================================================
    # Reranker internals 
    # =====================================================
    def ensure_reranker(self) -> None:
        if self._reranker is not None:
            return
        if CrossEncoder is None:
            raise RuntimeError("Reranker requires sentence-transformers installed.")
        self._reranker = CrossEncoder(self.reranker_model_name)

    def _rerank(self, query: str, candidates: List[Any]) -> List[float]:
        self.ensure_reranker()
        pairs = [(query, self._inv_desc(inv)) for inv in candidates]
        if not pairs:
            return []
        scores = self._reranker.predict(pairs)
        return [float(s) for s in scores]

    # =====================================================
    # Small utilities for scores + parsed gating
    # =====================================================
    def _minmax_norm(self, xs: List[float]) -> List[float]:
        if not xs:
            return []
        lo, hi = min(xs), max(xs)
        if hi <= lo:
            return [0.0 for _ in xs]
        return [(x - lo) / (hi - lo) for x in xs]

    def _safe_get_parsed(self, obj: Any) -> Dict[str, Any]:
        if obj is None:
            return {}
        for attr in ("parsed", "parsed_fields", "parse", "fields"):
            d = getattr(obj, attr, None)
            if isinstance(d, dict):
                return d
        ex = getattr(obj, "explain", None)
        if isinstance(ex, dict) and isinstance(ex.get("parsed"), dict):
            return ex["parsed"]
        return {}

    # =====================================================
    # PTYPE SIGNAL (engine-owned)
    # =====================================================
    def _ptype_signal(self, obj: Any) -> Dict[str, Any]:
        """
        Returns a dict:
          {
            "ptype": "RESISTOR"|"CAPACITOR"|"INDUCTOR"|"DIODE"|"OTHER",
            "confidence": 0..1,
            "source": "cns"|"parsed"|"unknown"
          }

        Notes:
        - We treat OTHER as "unknown-ish": do NOT hard-gate on it.
        - If CNS (or other authoritative upstream) provides a type, it wins.
        """
        p = self._safe_get_parsed(obj)

        # 1) Authoritative hint (optional): if you ever attach CNS-derived type to parsed
        #    e.g. p["cns_type"] = "CAP", treat as highest confidence.
        raw = (p.get("cns_type") or p.get("type") or p.get("component") or p.get("kind") or "")
        raw_u = str(raw).strip().upper()

        # Map to buckets
        if raw_u in ("RES", "RESISTOR"):
            ptype = "RESISTOR"
            src = "parsed" if "cns_type" not in p else "cns"
        elif raw_u in ("CAP", "CAPACITOR"):
            ptype = "CAPACITOR"
            src = "parsed" if "cns_type" not in p else "cns"
        elif raw_u in ("IND", "INDUCTOR"):
            ptype = "INDUCTOR"
            src = "parsed" if "cns_type" not in p else "cns"
        elif raw_u in ("DIODE", "TVS", "ZENER"):
            ptype = "DIODE"
            src = "parsed" if "cns_type" not in p else "cns"
        else:
            ptype = "OTHER"
            src = "unknown"

        # Confidence: prefer config-driven scales if present
        #  DEFAULT_TYPE_CONFIDENCE uses keys like "RES","CAP","DIODE","OTHER" :contentReference[oaicite:2]{index=2}
        key = {
            "RESISTOR": "RES",
            "CAPACITOR": "CAP",
            "INDUCTOR": "IND",
            "DIODE": "DIODE",
            "OTHER": "OTHER",
        }.get(ptype, "OTHER")

        conf = float(self.type_confidence.get(key, 0.7))
        if src == "cns":
            conf = max(conf, 0.95)

        # Attach to object for controller/UI (engine-owned signal)
        try:
            setattr(obj, "_pc_ptype", ptype)
            setattr(obj, "_pc_ptype_conf", conf)
            setattr(obj, "_pc_ptype_src", src)
        except Exception:
            pass

        return {"ptype": ptype, "confidence": conf, "source": src}
    
    def _ptype(self, parsed: Dict[str, Any]) -> str:
        # Backward-compatible wrapper: "ptype bucket only"
        t = (parsed.get("type") or parsed.get("component") or parsed.get("kind") or "").strip().upper()
        if t in ("RES", "RESISTOR"):
            return "RESISTOR"
        if t in ("CAP", "CAPACITOR"):
            return "CAPACITOR"
        if t in ("IND", "INDUCTOR"):
            return "INDUCTOR"
        if t in ("DIODE", "TVS", "ZENER"):
            return "DIODE"
        return "OTHER"

    def _norm_pkg(self, x: str) -> str:
        s = (x or "").strip().upper()
        return s.replace(" ", "").replace("-", "").replace("_", "")

    def _norm_res_value(self, v: str) -> str:
        s = (v or "").strip().upper().replace("OHM", "").replace("Ω", "").strip().replace(" ", "")
        if not s:
            return ""
        if "R" in s and s.replace("R", "").replace(".", "").isdigit():
            s = s.replace("R", ".")
        mult = 1.0
        if s.endswith("K"):
            mult, s = 1e3, s[:-1]
        elif s.endswith("M"):
            mult, s = 1e6, s[:-1]
        elif s.endswith("G"):
            mult, s = 1e9, s[:-1]
        try:
            val = float(s) * mult
            if abs(val - round(val)) < 1e-6:
                return str(int(round(val)))
            return str(val)
        except Exception:
            return ""

    def _norm_cap_value(self, v: str) -> str:
        s = (v or "").strip().upper().replace(" ", "")
        if not s:
            return ""
        if s.endswith("PF"):
            mult, s = 1.0, s[:-2]
        elif s.endswith("NF"):
            mult, s = 1e3, s[:-2]
        elif s.endswith("UF"):
            mult, s = 1e6, s[:-2]
        else:
            return ""
        try:
            return str(int(round(float(s) * mult)))
        except Exception:
            return ""

    def _apply_parsed_gates(self, npr: NPRPart, candidates: List[Any]) -> List[Any]:
        """
        Deterministic safety gates.

        Rules:
        - NEVER use confidence for logic.
        - Only gate when ptype is a known family where gates are meaningful.
        - Only gate on a field if BOTH sides parse/normalize that field cleanly.
        - If ptype is OTHER/unknown -> DO NOT gate (avoid coding out matches).
        """
        npr_p = self._safe_get_parsed(npr)
        n_type = self._ptype(npr_p)  # bucket only: RESISTOR/CAPACITOR/INDUCTOR/DIODE/OTHER
        

        # If we don't know the family, don't gate at all.
        if n_type == "OTHER":
            return list(candidates or [])

        # Extract/normalize NPR fields
        n_pkg = self._norm_pkg(str(npr_p.get("package") or getattr(npr, "package", "") or ""))
        n_val = str(npr_p.get("value") or getattr(npr, "value", "") or "").strip()

        n_res = self._norm_res_value(n_val) if n_type == "RESISTOR" else ""
        n_cap = self._norm_cap_value(n_val) if n_type == "CAPACITOR" else ""

        out = []
        for inv in (candidates or []):
            ip = self._get_or_parse_inv_fields(inv)
            i_type = self._ptype(ip)

            # Type gate ONLY if BOTH are known (not OTHER)
            if i_type != "OTHER" and i_type != n_type:
                continue

            # Package gate for passives ONLY if both packages exist
            if n_type in {"RESISTOR", "CAPACITOR", "INDUCTOR"}:
                i_pkg = self._norm_pkg(str(ip.get("package") or getattr(inv, "package", "") or ""))
                if n_pkg and i_pkg and n_pkg != i_pkg:
                    continue

            # Value gate only when both normalize cleanly
            if n_type == "RESISTOR":
                i_val = str(ip.get("value") or getattr(inv, "value", "") or "").strip()
                i_res = self._norm_res_value(i_val)
                if n_res and i_res and n_res != i_res:
                    continue

            if n_type == "CAPACITOR":
                i_val = str(ip.get("value") or getattr(inv, "value", "") or "").strip()
                i_cap = self._norm_cap_value(i_val)
                if n_cap and i_cap and n_cap != i_cap:
                    continue

            # Diodes: add gates later (polarity/package/voltage) if want

            out.append(inv)

        return out

    def _fuzzy_fallback_candidates(self, query_desc: str, top_k: int) -> List[tuple[int, float]]:
        q = _normalize_description_synonyms(query_desc or "")
        scored = []
        for i, inv in enumerate(self.inventory):
            d = _normalize_description_synonyms(self._inv_desc(inv) or "")
            s = fuzz.token_set_ratio(q, d) / 100.0
            scored.append((i, float(s)))
        scored.sort(key=lambda x: x[1], reverse=True)
        return scored[:max(1, int(top_k))]


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
        
        if self._should_stop():
            return

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
                    with np.load(vec_path, allow_pickle=False) as data:
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
            if root and root.winfo_exists():
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
                if self._should_stop():
                    print("[SEMANTIC] Cancelled embedding build.")
                    return
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
                    try:
                        if root.winfo_exists():
                            ratio = processed / total
                            root.after(0, lambda r=ratio: progress_cb(r))
                    except Exception:
                        pass

            # Save updated cache
            # Save updated cache
            try:
                keys = np.array(list(cache.keys()), dtype="<U40")
                vecs = np.vstack([cache[k] for k in keys])

                model_tag = _safe_model_tag(self.model_name)
                vec_path = self.cache_dir / f"embeddings_{model_tag}.npz"
                meta_path = self.cache_dir / f"embeddings_{model_tag}_meta.json"

                # ---- Always write temp that ENDS WITH .npz ----
                tmp_vec = self.cache_dir / f"embeddings_{model_tag}.tmp.{os.getpid()}.npz"
                tmp_meta = self.cache_dir / f"embeddings_{model_tag}.tmp.{os.getpid()}.json"

                # Write temp files
                np.savez_compressed(str(tmp_vec), keys=keys, vecs=vecs)
                tmp_meta.write_text(json.dumps(expected_meta, indent=2), encoding="utf-8")

                # Try atomic replace with retries
                ok = _os_replace_retry(str(tmp_vec), str(vec_path))
                if not ok:
                    # destination locked; write a side-by-side versioned cache and keep going
                    alt_path = str(vec_path).replace(".npz", f".alt.{os.getpid()}.npz")
                    os.replace(str(tmp_vec), alt_path)
                    print(f"[SEMANTIC] WARNING: primary cache locked; wrote alternate: {alt_path}")
                else:
                    print(f"[SEMANTIC] Cache saved: {vec_path} ({len(keys)} vectors)")
                _os_replace_retry(str(tmp_meta), str(meta_path))

                print(f"[SEMANTIC] Cache saved: {vec_path} ({len(keys)} vectors)")
            except Exception as e:
                print("[SEMANTIC] Cache save failed:", e)
                # Best-effort cleanup of temp files (avoid littering)
                try:
                    if 'tmp_vec' in locals() and os.path.exists(str(tmp_vec)):
                        os.remove(str(tmp_vec))
                except Exception:
                    pass
                try:
                    if 'tmp_meta' in locals() and os.path.exists(str(tmp_meta)):
                        os.remove(str(tmp_meta))
                except Exception:
                    pass
                
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
    
    def _norm_mpn(self, s: str) -> str:
        # normalize like your loader: uppercase + remove whitespace
        s = (s or "").strip().upper()
        s = re.sub(r"\s+", "", s)
        return s

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
        # Tier 1.5 — Family Prefix (DO NOT early-return; use as seed only)
        prefix_hits = None
        if npr.mfgpn:
            prefix = mpn_prefix(npr.mfgpn)
            if prefix:
                prefix_u = prefix.strip().upper()
                prefix_hits = [
                    inv for inv in self.inventory
                    if inv.vendoritem and inv.vendoritem.strip().upper().startswith(prefix_u)
                ]
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
        match = self._engineering_match(npr, seed_candidates=prefix_hits)
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
    
        self._ensure_sub_index()
    
        key = self._norm_mpn(npr.mfgpn)
        if not key:
            return None
    
        # conflict guard: don’t silently pick
        if key in self._sub_mpn_conflicts:
            return MatchResult(
                match_type=MatchType.NO_MATCH,
                confidence=0.0,
                inventory_part=None,
                candidates=[],
                notes="Alternates DB conflict: same MFGPN maps to multiple base parts",
                explain={
                    "tier": "substitute",
                    "conflict": True,
                    "mfgpn": npr.mfgpn,
                    "bases": self._sub_mpn_conflicts.get(key, []),
                },
            )
    
        inv = self._sub_mpn_index.get(key)
        if inv is None:
            return None
    
        # stamp perfect deterministic match so UI never shows 0%
        try:
            setattr(inv, "_pc_seed", 1.0)
            setattr(inv, "_pc_score", 1.0)
        except Exception:
            pass
        
        return MatchResult(
            match_type=MatchType.SUBSTITUTE,
            confidence=self.confidence_weights[MatchType.SUBSTITUTE],
            inventory_part=inv,
            candidates=[inv],
            notes=f"Matched via alternates DB (MFGPN alias)",
            explain={
                "tier": "substitute",
                "candidate_count": 1,
                "substitute_mpn": npr.mfgpn,
                "base_itemnum": self._inv_item(inv),
            },
        )
    
    
    def _ensure_sub_index(self, *, force: bool = False) -> None:
        if self._sub_index_ready and not force:
            return
    
        idx: Dict[str, Any] = {}
        conflicts: Dict[str, List[str]] = {}
    
        for inv in (self.inventory or []):
            base_item = self._inv_item(inv)
            subs = getattr(inv, "substitutes", None) or []
            for sub in subs:
                mpn = (getattr(sub, "mfgpn", "") or "").strip()
                if not mpn:
                    continue
                key = self._norm_mpn(mpn)
                if not key:
                    continue
                
                if key not in idx:
                    idx[key] = inv
                else:
                    # conflict: same MPN points to multiple base items
                    a = self._inv_item(idx[key])
                    b = base_item
                    if a != b:
                        conflicts.setdefault(key, [])
                        if a not in conflicts[key]:
                            conflicts[key].append(a)
                        if b not in conflicts[key]:
                            conflicts[key].append(b)
    
        self._sub_mpn_index = idx
        self._sub_mpn_conflicts = conflicts
        self._sub_index_ready = True
    

    # ===========================================================================================================
    # TIER 3 — DENSE SEMANTIC EMBEDDING → DENSE SPARCE EMBEDDING → RERANKIG → 
    # =============================================================================================================

    def _engineering_match(self, npr: NPRPart, seed_candidates: Optional[List[Any]] = None) -> Optional[MatchResult]:

        """
        Engineering tier v3 (Option A, single-class):

        Default pipeline (configurable):
          - Primary retrieval: DENSE or SPARSE
          - Secondary rescoring: the other signal (SPARSE or DENSE)
          - Hybrid seed score (_pc_seed)
          - Rerank on top-K with CrossEncoder -> final score (_pc_score)
          - Deterministic parsed-field gates (no confidence logic)
          - Fuzzy fallback if everything fails
        """
        desc = (npr.description or getattr(npr, "desc", "") or "").strip()
        if not desc:
            return None

        trace = {
            "tier": "eng_v3",
            "npr": {
                "partnum": getattr(npr, "partnum", "") or "",
                "mfgpn": getattr(npr, "mfgpn", "") or "",
                "desc": desc,
                "parsed": self._safe_get_parsed(npr),
            },
            "config": {
                "primary": ENG_PRIMARY_RETRIEVER,
                "topk_primary": ENG_TOPK_PRIMARY,
                "topk_secondary": ENG_TOPK_SECONDARY,
                "rerank_k": ENG_RERANK_K,
                "return_k": ENG_RETURN_K,
                "w_dense": ENG_W_DENSE,
                "w_sparse": ENG_W_SPARSE,
                "w_rerank": ENG_W_RERANK,
                "dense_model": getattr(self, "model_name", None),
                "sparse_model": getattr(self, "sparse_model_name", None),
                "reranker_model": getattr(self, "reranker_model_name", None),
            },
            "stages": {}
        }

        # -----------------------------
        # Stage 0: ensure caches/models
        # -----------------------------
        dense_ok = False
        sparse_ok = False

        if ENG_USE_DENSE:
            try:
                self.ensure_embeddings_cache()
                dense_ok = (self._inventory_vecs is not None)
            except Exception as e:
                if ENG_DEBUG:
                    print(f"[ENG] dense not available: {e}")

        if ENG_USE_SPARSE:
            try:
                self.ensure_splade_index()
                sparse_ok = True
            except Exception as e:
                if ENG_DEBUG:
                    print(f"[ENG] sparse not available: {e}")

        # -----------------------------
        # Stage 1: Primary retrieval
        # -----------------------------
        primary = ENG_PRIMARY_RETRIEVER.lower().strip()
        candidates_idx: List[int] = []
        primary_scores: List[float] = []
        primary_label = None

        try:
            if primary == "dense" and dense_ok:
                bom_vec = self._embedder.encode([desc], normalize_embeddings=True)[0]
                sims = np.dot(self._inventory_vecs, bom_vec)
                top_idx = np.argsort(-sims)[:int(ENG_TOPK_PRIMARY)]
                candidates_idx = top_idx.tolist()
                primary_scores = [float(sims[i]) for i in candidates_idx]
                primary_label = "dense"

            elif primary == "sparse" and sparse_ok:
                hits = self._splade_search(desc, top_k=int(ENG_TOPK_PRIMARY))
                candidates_idx = [i for (i, _) in hits]
                primary_scores = [s for (_, s) in hits]
                primary_label = "sparse"

            else:
                # fall back to whichever is available
                if dense_ok:
                    bom_vec = self._embedder.encode([desc], normalize_embeddings=True)[0]
                    sims = np.dot(self._inventory_vecs, bom_vec)
                    top_idx = np.argsort(-sims)[:int(ENG_TOPK_PRIMARY)]
                    candidates_idx = top_idx.tolist()
                    primary_scores = [float(sims[i]) for i in candidates_idx]
                    primary_label = "dense"
                elif sparse_ok:
                    hits = self._splade_search(desc, top_k=int(ENG_TOPK_PRIMARY))
                    candidates_idx = [i for (i, _) in hits]
                    primary_scores = [s for (_, s) in hits]
                    primary_label = "sparse"
        except Exception as e:
            if ENG_DEBUG:
                print(f"[ENG] primary retrieval failed: {e}")
            candidates_idx = []
            primary_scores = []
            primary_label = None

        # If still empty, fuzzy fallback (last resort)
        if not candidates_idx and ENG_USE_FUZZY_FALLBACK:
            if ENG_DEBUG:
                print("[ENG] primary empty -> fuzzy fallback")
            hits = self._fuzzy_fallback_candidates(desc, top_k=int(ENG_FUZZY_TOPK))
            candidates_idx = [i for (i, _) in hits]
            primary_scores = [s for (_, s) in hits]
            primary_label = "fuzzy"

        if not candidates_idx:
            return None
        
        # ---- TRACE: primary (sorted by primary score) ----
        stage_primary_name = primary_label if primary_label in ("dense", "sparse", "fuzzy") else "primary"
        
        pairs = list(zip(candidates_idx, primary_scores))
        pairs.sort(key=lambda x: x[1], reverse=True)  # ensure rank #1 is best by primary
        
        top_pairs = pairs[:int(ENG_TRACE_TOPN)]
        trace["stages"][stage_primary_name] = {
            "label": primary_label,
            "count": len(candidates_idx),
            "top": [
                {
                    "inv_index": int(i),
                    "score": float(s),
                    "itemnum": self._inv_item(self.inventory[i]),
                    "desc": self._inv_desc(self.inventory[i]),
                }
                for (i, s) in top_pairs
            ],
        }
        # -----------------------------
        # Stage 2: Secondary rescoring
        # -----------------------------
        secondary_scores = [0.0 for _ in candidates_idx]
        secondary_label = None

        try:
            if primary_label == "dense" and sparse_ok:
                # Sparse score only on the dense shortlist
                secondary_scores = self._splade_score_shortlist(desc, candidates_idx)
                secondary_label = "sparse"
            elif primary_label == "sparse" and dense_ok:
                # Dense score only on the sparse shortlist
                bom_vec = self._embedder.encode([desc], normalize_embeddings=True)[0]
                secondary_scores = [float(np.dot(self._inventory_vecs[i], bom_vec)) for i in candidates_idx]
                secondary_label = "dense"
            else:
                # no secondary available (or primary was fuzzy)
                secondary_scores = [0.0 for _ in candidates_idx]
                secondary_label = None
        except Exception as e:
            if ENG_DEBUG:
                print(f"[ENG] secondary rescoring failed: {e}")
            secondary_scores = [0.0 for _ in candidates_idx]
            secondary_label = None

        # ---- TRACE FIX #2: store secondary under explicit stage name ("dense" or "sparse") ----
        # Important: primary already wrote one of dense/sparse/fuzzy; secondary writes the OTHER signal.
        # If it happens to match the same name (shouldn't in normal dense<->sparse), we suffix it to avoid overwriting.
        if secondary_label in ("dense", "sparse"):
            stage_secondary_name = secondary_label
            if stage_secondary_name in trace["stages"]:
                stage_secondary_name = f"{secondary_label}_secondary"
        else:
            stage_secondary_name = "secondary"

        pairs = list(zip(candidates_idx, secondary_scores))
        pairs.sort(key=lambda x: x[1], reverse=True)
        
        top_pairs = pairs[:ENG_TRACE_TOPN]
        trace["stages"][stage_secondary_name] = {
            "label": secondary_label,
            "count": len(candidates_idx),
            "top": [
                {
                    "inv_index": int(i),
                    "score": float(s),
                    "itemnum": self._inv_item(self.inventory[i]),
                    "desc": self._inv_desc(self.inventory[i]),
                }
                for (i, s) in top_pairs
            ],
        }

        # Normalize and hybrid-combine
        p_norm = self._minmax_norm(primary_scores)
        s_norm = self._minmax_norm(secondary_scores) if secondary_label else [0.0 for _ in candidates_idx]

        # weight assignment depends on which label is dense vs sparse
        def _is_dense(lbl): return lbl == "dense"
        def _is_sparse(lbl): return lbl == "sparse"

        w_dense = float(ENG_W_DENSE)
        w_sparse = float(ENG_W_SPARSE)

        hybrid = []
        for a, b in zip(p_norm, s_norm):
            if _is_dense(primary_label) and _is_sparse(secondary_label):
                hybrid.append(w_dense * a + w_sparse * b)
            elif _is_sparse(primary_label) and _is_dense(secondary_label):
                hybrid.append(w_sparse * a + w_dense * b)
            else:
                # fuzzy-only or one-signal-only
                hybrid.append(float(a))

        # Build candidate objects
        candidates = [self.inventory[i] for i in candidates_idx]
        for inv, seed in zip(candidates, hybrid):
            inv._pc_seed = float(seed)

        # candidates_idx aligns with hybrid before you sort candidates
        topn = min(int(ENG_TRACE_TOPN), len(candidates_idx))
        trace["stages"]["hybrid_seed"] = {
            "count": len(candidates_idx),
            "top": [
                {
                    "inv_index": int(candidates_idx[i]),
                    "seed": float(hybrid[i]),
                    "itemnum": self._inv_item(self.inventory[candidates_idx[i]]),
                    "desc": self._inv_desc(self.inventory[candidates_idx[i]]),
                }
                for i in range(topn)
            ],
        }

        # Sort by seed and cap pool before rerank
        candidates = sorted(candidates, key=lambda x: float(getattr(x, "_pc_seed", 0.0) or 0.0), reverse=True)
        candidates = candidates[:int(ENG_TOPK_SECONDARY)]

        # -----------------------------
        # Stage 3: Rerank
        # -----------------------------
        shortlist = candidates[:int(ENG_RERANK_K)]
        rerank_norm = []

        if ENG_USE_RERANK and shortlist:
            try:
                rr_scores = self._rerank(desc, shortlist)
                rerank_norm = self._minmax_norm(rr_scores)
            except Exception as e:
                if ENG_DEBUG:
                    print(f"[ENG] rerank failed -> use seed: {e}")
                rerank_norm = [float(getattr(inv, "_pc_seed", 0.0) or 0.0) for inv in shortlist]
        else:
            rerank_norm = [float(getattr(inv, "_pc_seed", 0.0) or 0.0) for inv in shortlist]

        # Final score: combine seed + rerank
        for inv, rr in zip(shortlist, rerank_norm):
            seed = float(getattr(inv, "_pc_seed", 0.0) or 0.0)
            final = (1.0 - float(ENG_W_RERANK)) * seed + float(ENG_W_RERANK) * float(rr)
            inv._pc_score = float(final)

        # Fill _pc_score for non-reranked tail with seed (so UI has something)
        for inv in candidates[int(ENG_RERANK_K):]:
            inv._pc_score = float(getattr(inv, "_pc_seed", 0.0) or 0.0)

        # ---- TRACE FIX #3: rerank stage uses a consistent schema (count + top list) ----
        # ---- TRACE: rerank (sorted by final score) ----
        rr_pairs = []
        for inv, rr in zip(shortlist, rerank_norm):
            rr_pairs.append((
                float(getattr(inv, "_pc_score", 0.0) or 0.0),  # final score
                inv,
                float(rr),
            ))

        rr_pairs.sort(key=lambda x: x[0], reverse=True)

        top_rr = rr_pairs[:int(ENG_TRACE_TOPN)]

        trace["stages"]["rerank"] = {
            "label": "rerank",
            "count": len(shortlist),
            "top": [
                {
                    "itemnum": self._inv_item(inv),
                    "desc": self._inv_desc(inv),
                    "seed": float(getattr(inv, "_pc_seed", 0.0) or 0.0),
                    "rerank_norm": float(rr),
                    "score": float(getattr(inv, "_pc_score", 0.0) or 0.0),
                }
                for (_score, inv, rr) in top_rr
            ],
        }


        # -----------------------------
        # Stage 4: deterministic gates (no confidence logic)
        # -----------------------------
        gated = self._apply_parsed_gates(npr, candidates)
        final_pool = gated if gated else candidates

        final_pool = sorted(final_pool, key=lambda x: float(getattr(x, "_pc_score", 0.0) or 0.0), reverse=True)
        final_pool = final_pool[:int(ENG_RETURN_K)]

        if not final_pool:
            return None

        best = final_pool[0]
        best_score = float(getattr(best, "_pc_score", 0.0) or 0.0)

        explain = {
            "tier": "eng_v3_dense_sparse_rerank",
            "primary": primary_label,
            "secondary": secondary_label,
            "dense_model": getattr(self, "model_name", None),
            "sparse_model": getattr(self, "sparse_model_name", None),
            "reranker_model": getattr(self, "reranker_model_name", None),
            "counts": {
                "primary_k": len(candidates_idx),
                "secondary_k": len(candidates),
                "rerank_k": len(shortlist),
                "gated_k": len(gated),
                "final_k": len(final_pool),
            },
            "top": [
                self._summarize_inv(inv, seed=getattr(inv, "_pc_seed", None), score=getattr(inv, "_pc_score", None))
                for inv in final_pool
            ],
        }

        trace["stages"]["gates"] = {
            "gated_count": len(gated),
            "final_count": len(final_pool),
            "final_top": [
                {
                    "itemnum": self._inv_item(inv),
                    "score": float(getattr(inv, "_pc_score", 0.0) or 0.0),
                    "seed": float(getattr(inv, "_pc_seed", 0.0) or 0.0),
                    "desc": self._inv_desc(inv),
                }
                for inv in final_pool[:min(int(ENG_TRACE_TOPN), len(final_pool))]
            ],
        }

        # Winner summary
        trace["winner"] = {
            "itemnum": self._inv_item(best),
            "score": float(best_score),
            "desc": self._inv_desc(best),
        }
        self._trace_write(trace)

        return MatchResult(
            match_type=MatchType.PARSED_MATCH,
            confidence=round(min(0.95, clamp01(best_score)), 3),
            inventory_part=best,
            candidates=final_pool,
            notes=f"ENGv3 {primary_label}->{secondary_label}->rerank score={best_score:.3f}",
            explain=explain,
        )

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