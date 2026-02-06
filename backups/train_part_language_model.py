"""
train_part_language_model.py
============================

A+B Trainer for Part Language Model (PLM)

A) Hard-negative mining from your JSONL:
   - Positives are ONLY: explain.tier == "exact_mfgpn"
   - Negatives come from:
       - explain["hard_negatives_top"] (preferred; from PLM_TRAIN_MODE mining)
       - explain["top"] (fallback)
       - random same-family inventory/seen descriptions

B) Inventory self-augmentation:
   - Create multiple "same-part" description variants from inventory descriptions
   - Add (variant_i, variant_j) as POS pairs
   - Add (variant_i, other_part_desc_same_family) as NEG pairs

Outputs:
   - Trained model directory (SentenceTransformer format)
   - TSV snapshot of generated pairs (for sanity checks)

Install:
   pip install torch sentence-transformers tqdm pandas

Run examples:
   python train_part_language_model.py --jsonl explain_all.jsonl --epochs 3 --batch-size 32 --hard-neg-cap 10 --neg-per-pos 12
   python train_part_language_model.py --jsonl explain_all.jsonl --inventory inventory.csv --epochs 3 --aug-per-inv 4

Notes:
- This uses explicit 1/0 labels (CosineSimilarityLoss) so hard negatives are actually learned.
- Works even with small gold count, because each gold row generates many negatives.
"""

from __future__ import annotations

import argparse
import csv
import json
import random
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple

try:
    from tqdm import tqdm  # type: ignore
except Exception:
    tqdm = None

import pandas as pd
import torch
from sentence_transformers import SentenceTransformer, InputExample, losses
from sentence_transformers.evaluation import EmbeddingSimilarityEvaluator
from torch.utils.data import DataLoader


# =========================================================
# Normalization helpers
# =========================================================

_WS_RE = re.compile(r"\s+")
TOKEN_RE = re.compile(r"[A-Z0-9]+(?:\.[A-Z0-9]+)?")

DEFAULT_SYNONYMS = {
    "MONO": "CER",
    "MONOLITHIC": "CER",
    "CERAMIC": "CER",
    "CAPACITOR": "CAP",
    "RESISTOR": "RES",
    "OHMS": "OHM",
    "Ω": "OHM",
}

FILLER_TOKENS = {
    "ROHS", "ROHS2", "LEADFREE", "LEAD-FREE", "PB-FREE",
    "SMD", "SMT", "CHIP", "SOLDER", "TAPE", "REEL", "TR", "T/R",
    "AECQ200", "AEC-Q200", "GENERIC", "ELECTRONIC"
}

FAMILY_TOKENS = {"CAP", "RES", "IC", "DIODE", "IND", "CONN", "SW", "LED", "TRANSISTOR", "MOSFET", "FUSE", "RELAY"}


def clean_desc(s: str) -> str:
    s = (s or "").strip().upper()
    s = s.replace(",", " ").replace(";", " ").replace("/", " ").replace("\\", " ")
    s = s.replace("\n", " ").replace("\r", " ").replace("\t", " ")
    s = _WS_RE.sub(" ", s).strip()
    return s


def apply_synonyms(s: str, synonyms: Dict[str, str]) -> str:
    if not s:
        return s
    toks = s.split()
    return " ".join(synonyms.get(t, t) for t in toks)


def tokenize(s: str) -> List[str]:
    s = clean_desc(s)
    if not s:
        return []
    return s.split()


def coarse_family(s: str) -> str:
    """
    Coarse family bucket used for negative sampling.
    Searches first few tokens for CAP/RES/etc.
    """
    toks = tokenize(s)
    for t in toks[:4]:
        if t in FAMILY_TOKENS:
            return t
    return "UNK"


# =========================================================
# JSONL schema + gold selection
# =========================================================
@dataclass
class LogRow:
    npr_desc: str
    inv_desc: str
    inv_item: str
    match_type: str
    explain: object
    raw_obj: dict  # full JSON row so we can mine payload-level candidates/winner



def iter_jsonl_rows(path: Path, max_lines: Optional[int]) -> Iterable[LogRow]:
    """
    Supports BOTH schemas:

    Old schema:
      { "npr_desc":..., "inv_desc":..., "inv_item":..., "match_type":..., "explain": {...} }

    New schema (recommended):
      {
        "npr_desc": ...,
        "match_type": ...,
        "winner": {"inv_item":..., "inv_desc":...},
        "candidates": [{"inv_item":..., "inv_desc":..., ...}, ...],
        "explain": {...}
      }

    We always yield (npr_desc, winner_inv_desc, winner_inv_item, match_type, explain, raw_obj).
    """
    n = 0
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            if max_lines is not None and n >= max_lines:
                break

            line = line.strip()
            if not line:
                continue

            try:
                obj = json.loads(line)
            except Exception:
                continue

            # Required-ish
            npr_desc = str(obj.get("npr_desc") or "").strip()
            match_type = str(obj.get("match_type") or "").strip()
            explain = obj.get("explain")

            # Prefer new schema winner block
            inv_desc = ""
            inv_item = ""
            winner = obj.get("winner")
            if isinstance(winner, dict):
                inv_desc = str(winner.get("inv_desc") or "").strip()
                inv_item = str(winner.get("inv_item") or "").strip()

            # Fallback old schema
            if not inv_desc:
                inv_desc = str(obj.get("inv_desc") or "").strip()
            if not inv_item:
                inv_item = str(obj.get("inv_item") or "").strip()

            # Skip unusable rows
            if not npr_desc or not inv_desc:
                continue

            yield LogRow(
                npr_desc=npr_desc,
                inv_desc=inv_desc,
                inv_item=inv_item,
                match_type=match_type,
                explain=explain,
                raw_obj=obj if isinstance(obj, dict) else {},
            )
            n += 1



def is_gold_exact_mfgpn(match_type: str, explain: object) -> bool:
    """
    Decide whether a row is a trusted positive seed.

    Accept if:
      - match_type indicates EXACT MFG match (controller-side truth), OR
      - explain["tier"] indicates exact match (legacy expectation)

    This is intentionally conservative: you can widen later to API_ASSISTED
    or (best) UI-selected rows once you log them.
    """
    mt = (match_type or "").strip().upper()

    # Handle forms like:
    # "MatchType.EXACT_MFG_PN", "EXACT_MFG_PN", "exact_mfgpn", etc.
    if "EXACT" in mt and ("MFG" in mt or "MPN" in mt):
        return True

    if not isinstance(explain, dict):
        return False

    tier = str(explain.get("tier") or "").strip().lower()
    return tier in ("exact_mfgpn", "exact_mfg_pn", "exact_mfgpn_match")


def collect_hard_neg_descs(raw_obj: dict, winner_inv_item: str) -> List[str]:
    """
    Hard negative mining priority:

    1) explain["hard_negatives_top"] = [{"inv_item":..., "inv_desc":...}, ...]
    2) explain["top"]               = [{"inv_item":..., "inv_desc":...}, ...]
    3) payload["candidates"]        = [{"inv_item":..., "inv_desc":...}, ...]   <-- NEW, most valuable

    Returns list of inv_desc strings, excluding the winner item.
    """
    if not isinstance(raw_obj, dict):
        return []

    winner = (winner_inv_item or "").strip()
    out: List[str] = []

    # 1/2) explain-driven lists
    exp = raw_obj.get("explain")
    if isinstance(exp, dict):
        top = exp.get("hard_negatives_top")
        if not isinstance(top, list):
            top = exp.get("top")

        if isinstance(top, list):
            for entry in top:
                if not isinstance(entry, dict):
                    continue
                inv_item = str(entry.get("inv_item") or "").strip()
                inv_desc = str(entry.get("inv_desc") or "").strip()
                if not inv_desc:
                    continue
                if winner and inv_item == winner:
                    continue
                out.append(inv_desc)

            if out:
                return out  # prefer explain-mined negatives

    # 3) payload candidates
    cands = raw_obj.get("candidates")
    if isinstance(cands, list):
        for c in cands:
            if not isinstance(c, dict):
                continue
            inv_item = str(c.get("inv_item") or "").strip()
            inv_desc = str(c.get("inv_desc") or "").strip()
            if not inv_desc:
                continue
            if winner and inv_item == winner:
                continue
            out.append(inv_desc)

    return out



# =========================================================
# Inventory augmentation (B)
# =========================================================

def _drop_fillers(tokens: List[str]) -> List[str]:
    return [t for t in tokens if t not in FILLER_TOKENS]


def _maybe_shuffle(tokens: List[str], rng: random.Random) -> List[str]:
    if len(tokens) < 4:
        return tokens
    # keep first family token stable if present
    head = []
    rest = tokens[:]
    if rest and rest[0] in FAMILY_TOKENS:
        head = [rest.pop(0)]
    rng.shuffle(rest)
    return head + rest


def augment_desc_variants(desc: str, rng: random.Random, max_variants: int) -> List[str]:
    """
    Create multiple "same-part" variants:
    - remove filler tokens
    - synonym normalization already handled upstream
    - shuffle token order (light)
    - drop one random non-critical token sometimes
    """
    base = clean_desc(desc)
    toks = tokenize(base)
    if not toks:
        return []

    variants: Set[str] = set()

    # v0: base
    variants.add(" ".join(toks))

    # v1: drop fillers
    v = _drop_fillers(toks)
    if v:
        variants.add(" ".join(v))

    # v2: shuffle (light)
    variants.add(" ".join(_maybe_shuffle(v if v else toks, rng)))

    # v3..: random drops + shuffles
    attempts = 0
    while len(variants) < max_variants and attempts < 50:
        attempts += 1
        cur = v[:] if v else toks[:]

        # drop one token sometimes (avoid dropping family token)
        if len(cur) > 4 and rng.random() < 0.6:
            idxs = [i for i in range(len(cur)) if not (i == 0 and cur[i] in FAMILY_TOKENS)]
            if idxs:
                cur.pop(rng.choice(idxs))

        if rng.random() < 0.7:
            cur = _maybe_shuffle(cur, rng)

        out = " ".join(cur).strip()
        if out:
            variants.add(out)

    return list(variants)[:max_variants]


def load_inventory_descs(inventory_path: Path, desc_col: str) -> List[str]:
    df = pd.read_csv(inventory_path)
    if desc_col not in df.columns:
        raise ValueError(f"Inventory CSV missing column '{desc_col}'. Columns: {list(df.columns)}")
    descs = [str(x or "").strip() for x in df[desc_col].tolist()]
    descs = [d for d in descs if d]
    return descs


# =========================================================
# Pair generation (A+B)
# =========================================================

@dataclass
class PairStats:
    gold_rows: int = 0
    pos_pairs: int = 0
    neg_pairs: int = 0
    aug_pos_pairs: int = 0
    aug_neg_pairs: int = 0


def build_pairs_A_B(
    jsonl_path: Path,
    inventory_path: Optional[Path],
    inventory_desc_col: str,
    max_lines: Optional[int],
    max_pairs: int,
    neg_per_pos: int,
    hard_neg_cap: int,
    aug_per_inv: int,
    aug_neg_per_pos: int,
    seed: int,
    export_tsv: Optional[Path],
) -> Tuple[List[InputExample], PairStats]:
    """
    Patched to support controller-built payload JSONL:

    - Gold selection uses match_type and/or explain["tier"]
    - Hard negatives mined from explain["hard_negatives_top"/"top"] OR payload["candidates"]
    - Still supports inventory augmentation (B)
    """
    rng = random.Random(seed)
    synonyms = dict(DEFAULT_SYNONYMS)

    pool_by_family: Dict[str, List[str]] = {}

    gold_rows: List[Tuple[str, str, str, dict]] = []  # (npr_desc, inv_desc, inv_item, raw_obj)
    stats = PairStats()

    rows = iter_jsonl_rows(jsonl_path, max_lines=max_lines)
    if tqdm is not None:
        rows = tqdm(rows, desc="Reading JSONL", unit="row")

    for row in rows:
        a = apply_synonyms(clean_desc(row.npr_desc), synonyms)
        b = apply_synonyms(clean_desc(row.inv_desc), synonyms)

        fam = coarse_family(b)
        pool_by_family.setdefault(fam, []).append(b)

        if is_gold_exact_mfgpn(row.match_type, row.explain):
            gold_rows.append((a, b, row.inv_item, row.raw_obj))
            stats.gold_rows += 1

    if stats.gold_rows == 0:
        raise RuntimeError(
            "No gold rows found.\n"
            "Your trainer currently seeds positives only from exact-mfgpn matches.\n"
            "Fix by ensuring either:\n"
            "  - match_type contains EXACT_MFG_PN, OR\n"
            "  - explain['tier'] is set to 'exact_mfgpn'\n"
            "in your JSONL payload rows."
        )

    # optionally load inventory descs for augmentation and negative pool
    inv_descs: List[str] = []
    if inventory_path is not None:
        inv_descs = load_inventory_descs(inventory_path, inventory_desc_col)
        for d in inv_descs:
            d2 = apply_synonyms(clean_desc(d), synonyms)
            fam = coarse_family(d2)
            pool_by_family.setdefault(fam, []).append(d2)

    # TSV export
    tsv_writer = None
    tsv_fh = None
    if export_tsv is not None:
        export_tsv.parent.mkdir(parents=True, exist_ok=True)
        tsv_fh = export_tsv.open("w", encoding="utf-8", newline="")
        tsv_writer = csv.writer(tsv_fh, delimiter="\t")
        tsv_writer.writerow(["label", "text_a", "text_b", "source"])

    examples: List[InputExample] = []
    seen: Set[Tuple[str, str, float]] = set()

    def add(a: str, b: str, label: float, source: str) -> None:
        key = (a, b, float(label))
        if key in seen:
            return
        seen.add(key)
        examples.append(InputExample(texts=[a, b], label=float(label)))
        if tsv_writer is not None:
            tsv_writer.writerow([label, a, b, source])

    # ---------- A: gold rows + hard negatives ----------
    rng.shuffle(gold_rows)
    gold_iter = gold_rows
    if tqdm is not None:
        gold_iter = tqdm(gold_rows, desc="Building A pairs (gold + hard negs)", unit="gold")

    for (a, b_pos, inv_item, raw_obj) in gold_iter:
        if len(examples) >= max_pairs:
            break

        # Positive
        add(a, b_pos, 1.0, "A_pos_exact_mfgpn")
        stats.pos_pairs += 1

        # Hard negatives (top confusables)
        hard_negs_raw = collect_hard_neg_descs(raw_obj, inv_item)
        hard_negs = [apply_synonyms(clean_desc(x), synonyms) for x in hard_negs_raw if x]
        hard_negs = list(dict.fromkeys(hard_negs))  # dedup keep order
        hard_negs = hard_negs[:hard_neg_cap]

        neg_added = 0
        for hn in hard_negs:
            if neg_added >= neg_per_pos:
                break
            if not hn or hn == b_pos:
                continue
            add(a, hn, 0.0, "A_neg_hard_top")
            stats.neg_pairs += 1
            neg_added += 1

        # Fill remaining negatives with random same-family
        fam = coarse_family(b_pos)
        pool = pool_by_family.get(fam, []) or pool_by_family.get("UNK", [])
        tries = 0
        while neg_added < neg_per_pos and tries < 80 and pool:
            tries += 1
            cand = rng.choice(pool)
            if not cand or cand == b_pos:
                continue
            add(a, cand, 0.0, f"A_neg_rand_{fam}")
            stats.neg_pairs += 1
            neg_added += 1

    # ---------- B: inventory augmentation ----------
    if inv_descs and len(examples) < max_pairs:
        inv_iter = inv_descs
        if tqdm is not None:
            inv_iter = tqdm(inv_descs, desc="Building B pairs (inventory augmentation)", unit="inv")

        for d in inv_iter:
            if len(examples) >= max_pairs:
                break

            base = apply_synonyms(clean_desc(d), synonyms)
            fam = coarse_family(base)
            variants = augment_desc_variants(base, rng=rng, max_variants=max(2, aug_per_inv))

            if len(variants) < 2:
                continue

            rng.shuffle(variants)
            v0 = variants[0]
            for v1 in variants[1:]:
                if len(examples) >= max_pairs:
                    break
                add(v0, v1, 1.0, "B_pos_inv_aug")
                stats.aug_pos_pairs += 1

                pool = pool_by_family.get(fam, []) or []
                if pool:
                    negs_done = 0
                    tries = 0
                    while negs_done < aug_neg_per_pos and tries < 50 and len(examples) < max_pairs:
                        tries += 1
                        other = rng.choice(pool)
                        if not other or other == base or other in variants:
                            continue
                        add(v0, other, 0.0, f"B_neg_inv_other_{fam}")
                        stats.aug_neg_pairs += 1
                        negs_done += 1

    if tsv_fh is not None:
        tsv_fh.close()

    rng.shuffle(examples)
    return examples, stats



# =========================================================
# Training
# =========================================================

def train_model(
    base_model: str,
    out_dir: Path,
    examples: List[InputExample],
    epochs: int,
    batch_size: int,
    val_split: float,
    seed: int,
) -> None:
    rng = random.Random(seed)
    rng.shuffle(examples)

    n_total = len(examples)
    n_val = int(n_total * val_split)
    val = examples[:n_val] if n_val > 0 else []
    train_ex = examples[n_val:] if n_val > 0 else examples

    print(f"[PLM] Examples total: {n_total} | train: {len(train_ex)} | val: {len(val)}")
    print(f"[PLM] Loading base model: {base_model}")

    model = SentenceTransformer(base_model)
    train_dl = DataLoader(train_ex, shuffle=True, batch_size=batch_size)
    train_loss = losses.CosineSimilarityLoss(model)

    evaluator = None
    if val:
        evaluator = EmbeddingSimilarityEvaluator(
            sentences1=[ex.texts[0] for ex in val],
            sentences2=[ex.texts[1] for ex in val],
            scores=[float(ex.label) for ex in val],
            name="val",
        )

    out_dir.mkdir(parents=True, exist_ok=True)

    model.fit(
        train_objectives=[(train_dl, train_loss)],
        epochs=epochs,
        evaluator=evaluator,
        evaluation_steps=max(100, len(train_dl) // 2) if evaluator is not None else 0,
        show_progress_bar=True,
        output_path=str(out_dir),
    )

    print(f"[PLM] ✅ Saved model to: {out_dir}")


# =========================================================
# CLI
# =========================================================

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Train PLM with A(hard negatives) + B(inventory augmentation).")

    p.add_argument("--jsonl", type=str, required=True, help="Path to explain_all.jsonl")
    p.add_argument("--out", type=str, default="models/part_language_model", help="Output directory for model")
    p.add_argument("--base-model", type=str, default="sentence-transformers/all-MiniLM-L6-v2")

    p.add_argument("--inventory", type=str, default="", help="Optional path to inventory.csv for augmentation")
    p.add_argument("--inventory-desc-col", type=str, default="inv_desc", help="Which column holds inventory description")

    p.add_argument("--epochs", type=int, default=3)
    p.add_argument("--batch-size", type=int, default=32)
    p.add_argument("--val-split", type=float, default=0.05)

    p.add_argument("--max-lines", type=int, default=None)
    p.add_argument("--max-pairs", type=int, default=150000)

    # A controls
    p.add_argument("--neg-per-pos", type=int, default=12, help="Total negatives per gold positive")
    p.add_argument("--hard-neg-cap", type=int, default=10, help="How many to take from hard_negatives_top/top")

    # B controls
    p.add_argument("--aug-per-inv", type=int, default=4, help="Max variants per inventory description")
    p.add_argument("--aug-neg-per-pos", type=int, default=2, help="Negatives per augmented positive pair")

    p.add_argument("--seed", type=int, default=1337)
    p.add_argument("--export-tsv", type=str, default="debug_match/plm_pairs.tsv", help="Set empty to disable")

    return p.parse_args()


def main() -> None:
    args = parse_args()

    jsonl_path = Path(args.jsonl)
    if not jsonl_path.exists():
        raise FileNotFoundError(f"JSONL not found: {jsonl_path}")

    inv_path = Path(args.inventory) if args.inventory.strip() else None
    if inv_path is not None and not inv_path.exists():
        raise FileNotFoundError(f"Inventory CSV not found: {inv_path}")

    out_dir = Path(args.out)

    export_tsv = None
    if args.export_tsv and args.export_tsv.strip():
        export_tsv = Path(args.export_tsv)

    print(f"[PLM] JSONL: {jsonl_path}")
    print(f"[PLM] OUT:  {out_dir}")
    print(f"[PLM] MAX_LINES={args.max_lines}  MAX_PAIRS={args.max_pairs}")
    print(f"[PLM] A: neg_per_pos={args.neg_per_pos} hard_neg_cap={args.hard_neg_cap}")
    if inv_path:
        print(f"[PLM] B: inventory={inv_path} desc_col={args.inventory_desc_col} aug_per_inv={args.aug_per_inv} aug_neg_per_pos={args.aug_neg_per_pos}")
    else:
        print(f"[PLM] B: inventory augmentation disabled (no --inventory provided)")
    print(f"[PLM] Export TSV: {export_tsv if export_tsv else '(disabled)'}")

    examples, stats = build_pairs_A_B(
        jsonl_path=jsonl_path,
        inventory_path=inv_path,
        inventory_desc_col=args.inventory_desc_col,
        max_lines=args.max_lines,
        max_pairs=args.max_pairs,
        neg_per_pos=args.neg_per_pos,
        hard_neg_cap=args.hard_neg_cap,
        aug_per_inv=args.aug_per_inv,
        aug_neg_per_pos=args.aug_neg_per_pos,
        seed=args.seed,
        export_tsv=export_tsv,
    )

    print("[PLM] Pair stats:")
    print(f"  gold_rows:     {stats.gold_rows}")
    print(f"  A positives:   {stats.pos_pairs}")
    print(f"  A negatives:   {stats.neg_pairs}")
    print(f"  B pos aug:     {stats.aug_pos_pairs}")
    print(f"  B neg aug:     {stats.aug_neg_pairs}")
    print(f"  total pairs:   {len(examples)}")

    train_model(
        base_model=args.base_model,
        out_dir=out_dir,
        examples=examples,
        epochs=args.epochs,
        batch_size=args.batch_size,
        val_split=args.val_split,
        seed=args.seed,
    )


if __name__ == "__main__":
    main()
