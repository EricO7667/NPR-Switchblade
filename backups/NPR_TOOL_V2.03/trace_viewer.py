#!/usr/bin/env python3
r"""
trace_viewer.py

JSONL "table viewer" for MatchingEngine traces.

Defaults (no args):
  - Assumes this file lives at: <repo_root>/NPR_TOOL/trace_viewer.py
  - Computes repo_root = parent directory of THIS script's folder
  - Reads trace at: <repo_root>/.npr_semantic_cache/eng_trace.jsonl
  - Writes outputs into: folder containing this script (NPR_TOOL)

Outputs:
  - trace_summary.csv
  - trace_candidates.csv
  - trace_viewer.html  (sortable/searchable tables)

Usage:
  python trace_viewer.py
  python trace_viewer.py --trace "C:/PersonalProjects/PartChecker/.npr_semantic_cache/eng_trace.jsonl"
  python trace_viewer.py --outdir "C:/PersonalProjects/PartChecker/NPR_TOOL/trace_outputs"
"""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any, List, Tuple


def _default_repo_root() -> Path:
    # If script is .../PartChecker/NPR_TOOL/trace_viewer.py, repo root is .../PartChecker
    return Path(__file__).resolve().parent.parent


def _default_trace_path() -> Path:
    return _default_repo_root() / ".npr_semantic_cache" / "eng_trace.jsonl"


def _read_jsonl(path: Path) -> List[dict]:
    """
    Robust reader:
    - Standard JSONL (1 object per line) works.
    - Also recovers when objects are pretty-printed across multiple lines by buffering
      until json.loads succeeds.
    """
    rows: List[dict] = []
    bad_lines = 0

    buf = ""
    buf_start_ln = 1

    with path.open("r", encoding="utf-8") as f:
        for ln, line in enumerate(f, start=1):
            s = line.rstrip("\n")
            if not s.strip() and not buf:
                continue

            # Start of a buffered object
            if not buf:
                buf_start_ln = ln
                buf = s
            else:
                buf += "\n" + s

            try:
                obj = json.loads(buf)
                if isinstance(obj, dict):
                    rows.append(obj)
                else:
                    # If someone wrote a list or something weird, still keep it but count it.
                    rows.append({"_non_dict": obj})
                buf = ""
            except Exception:
                # not done yet; keep buffering
                continue

    # leftover buffer that never became valid JSON
    if buf.strip():
        bad_lines += 1
        print(f"[trace_viewer] WARNING: trailing incomplete JSON object starting at line {buf_start_ln} (ignored).")

    if bad_lines:
        print(f"[trace_viewer] WARNING: encountered {bad_lines} malformed/incomplete JSON blocks.")
    return rows


def _safe_get(d: dict, keys: List[str], default=None):
    cur: Any = d
    for k in keys:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(k)
    return default if cur is None else cur


def build_tables(traces: List[dict]) -> Tuple[List[dict], List[dict]]:
    summary: List[dict] = []
    cands: List[dict] = []

    for rec in traces:
        ts = rec.get("ts") or rec.get("timestamp") or ""
        npr_item = rec.get("npr_item") or rec.get("bom_uid") or rec.get("uid") or ""
        bom_mpn = rec.get("bom_mpn") or rec.get("mfgpn") or ""
        desc = rec.get("desc") or rec.get("npr_desc") or rec.get("description") or ""

        cfg = rec.get("config") or {}
        stages = rec.get("stages") or rec.get("trace", {}).get("stages") or {}

        winner = rec.get("winner") or _safe_get(rec, ["result", "winner"], None) or {}
        win_item = winner.get("inv_item") or winner.get("itemnum") or ""
        win_desc = winner.get("inv_desc") or winner.get("desc") or ""
        win_seed = winner.get("_pc_seed") or winner.get("seed") or ""
        win_score = winner.get("_pc_score") or winner.get("score") or ""
        win_conf = winner.get("confidence") or rec.get("confidence") or ""
        win_notes = winner.get("notes") or rec.get("notes") or ""

        primary = stages.get("primary", {}) if isinstance(stages, dict) else {}
        secondary = stages.get("secondary", {}) if isinstance(stages, dict) else {}
        rerank = stages.get("rerank", {}) if isinstance(stages, dict) else {}

        summary.append({
            "ts": ts,
            "npr_item": npr_item,
            "bom_mpn": bom_mpn,
            "npr_desc": desc,
            "primary_label": primary.get("label", ""),
            "primary_count": primary.get("count", ""),
            "secondary_label": secondary.get("label", ""),
            "secondary_count": secondary.get("count", ""),
            "rerank_label": rerank.get("label", ""),
            "rerank_count": rerank.get("count", ""),
            "winner_itemnum": win_item,
            "winner_desc": win_desc,
            "winner_seed": win_seed,
            "winner_score": win_score,
            "winner_confidence": win_conf,
            "winner_notes": win_notes,
            "config_primary": cfg.get("primary_retriever", ""),
            "config_topk_primary": cfg.get("topk_primary", ""),
            "config_rerank_k": cfg.get("rerank_k", ""),
            "config_return_k": cfg.get("return_k", ""),
            "dense_model": cfg.get("dense_model", ""),
            "sparse_model": cfg.get("sparse_model", ""),
            "reranker_model": cfg.get("reranker_model", ""),
        })

        if isinstance(stages, dict):
            for stage_name, stage_obj in stages.items():
                if not isinstance(stage_obj, dict):
                    continue
                top = stage_obj.get("top") or []
                if not isinstance(top, list):
                    continue
                for rank, t in enumerate(top, start=1):
                    if not isinstance(t, dict):
                        continue
                    cands.append({
                        "ts": ts,
                        "npr_item": npr_item,
                        "bom_mpn": bom_mpn,
                        "stage": stage_name,
                        "stage_label": stage_obj.get("label", ""),
                        "stage_count": stage_obj.get("count", ""),
                        "rank": rank,
                        "inv_index": t.get("inv_index", ""),
                        "inv_itemnum": t.get("itemnum", ""),
                        "inv_desc": t.get("desc", ""),
                        "score": t.get("score", ""),
                        "seed": t.get("seed", t.get("_pc_seed", "")),
                    })

    return summary, cands


def write_csv(rows: List[dict], path: Path) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    cols = list(rows[0].keys())
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in cols})


def write_html_from_traces(traces: list[dict], out_path: Path) -> None:
    import html

    def esc(x) -> str:
        return html.escape("" if x is None else str(x))

    def pick_stage(stages: dict, want: str) -> dict:
        # want in {"dense","sparse","rerank"}
        # prefer exact key; else look for suffixed variants
        if want in stages and isinstance(stages[want], dict):
            return stages[want]
        for k in (f"{want}_secondary", f"{want}_primary"):
            if k in stages and isinstance(stages[k], dict):
                return stages[k]
        # fallback: try legacy keys
        if want == "dense" and "primary" in stages and stages["primary"].get("label") == "dense":
            return stages["primary"]
        if want == "sparse" and "secondary" in stages and stages["secondary"].get("label") == "sparse":
            return stages["secondary"]
        return {}

    def render_stage(stage_obj: dict, title: str) -> str:
        top = stage_obj.get("top") or []
        count = stage_obj.get("count")
        if count is None:
            # some of your older traces used rerank_count
            count = stage_obj.get("rerank_count", len(top))

        rows_html = []
        for i, r in enumerate(top, start=1):
            itemnum = r.get("itemnum") or r.get("inv_item") or ""
            desc = r.get("desc") or r.get("inv_desc") or ""
            score = r.get("score")
            seed = r.get("seed")
            rr = r.get("rerank_norm")
            score_txt = ""
            if score is not None:
                score_txt = f"score={score:.4f}" if isinstance(score, (int,float)) else f"score={esc(score)}"
            elif seed is not None:
                score_txt = f"seed={seed:.4f}" if isinstance(seed, (int,float)) else f"seed={esc(seed)}"
            elif rr is not None:
                score_txt = f"rerank_norm={rr:.4f}" if isinstance(rr, (int,float)) else f"rerank_norm={esc(rr)}"

            rows_html.append(f"""
              <div class="candRow">
                <div class="candLeft">
                  <div class="candPN"><span class="rank">#{i}</span> <span class="pn">{esc(itemnum)}</span></div>
                  <div class="candDesc">{esc(desc)}</div>
                </div>
                <div class="candRight">{esc(score_txt)}</div>
              </div>
            """)

        if not top:
            rows_html.append('<div class="empty">No candidates recorded for this stage.</div>')

        return f"""
        <details class="stage" open>
          <summary>{esc(title)} ({esc(count)})</summary>
          <div class="stageBody">{''.join(rows_html)}</div>
        </details>
        """

    cards = []
    for rec in traces:
        npr = rec.get("npr") or {}
        stages = rec.get("stages") or {}

        partnum = npr.get("partnum", "")
        mfgpn = npr.get("mfgpn", "")
        desc = npr.get("desc", "")
        ts = rec.get("ts", rec.get("timestamp", ""))

        dense = pick_stage(stages, "dense")
        sparse = pick_stage(stages, "sparse")
        rerank = pick_stage(stages, "rerank")

        winner = rec.get("winner") or {}
        win_item = winner.get("itemnum", "")
        win_score = winner.get("score", "")

        header = f"""
        <div class="partTop">
          <div class="partId">
            <div class="npr">{esc(partnum)}</div>
            <div class="mpn">{esc(mfgpn)}</div>
          </div>
          <div class="meta">{esc(ts)}</div>
        </div>
        <div class="partDesc">{esc(desc)}</div>
        <div class="winner">Winner: <b>{esc(win_item)}</b> <span class="muted">score={esc(win_score)}</span></div>
        """

        cards.append(f"""
        <details class="partCard">
          <summary class="partHead">{header}</summary>
          <div class="partBody">
            {render_stage(dense, "Dense")}
            {render_stage(sparse, "Sparse")}
            {render_stage(rerank, "Rerank")}
          </div>
        </details>
        """)

    doc = f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>NPR Trace Viewer (Per Part)</title>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 18px; background:#fafafa; }}
    h2 {{ margin: 0 0 10px 0; }}
    #q {{ width:min(900px,100%); padding:10px 12px; border:1px solid #ccc; border-radius:10px; }}
    .hint {{ color:#666; font-size:13px; margin:10px 0 14px; }}
    .partCard {{ border:1px solid #ddd; background:#fff; border-radius:14px; padding:10px 12px; margin:12px 0; box-shadow:0 1px 3px rgba(0,0,0,.05); }}
    .partCard > summary {{ list-style:none; cursor:pointer; }}
    .partCard > summary::-webkit-details-marker {{ display:none; }}
    .partTop {{ display:flex; justify-content:space-between; gap:12px; }}
    .npr {{ font-weight:700; font-size:16px; }}
    .mpn {{ font-family: ui-monospace, Menlo, Consolas, monospace; font-size:13px; color:#333; }}
    .meta {{ color:#777; font-size:12px; }}
    .partDesc {{ margin-top:6px; font-size:13px; }}
    .winner {{ margin-top:6px; font-size:13px; }}
    .muted {{ color:#777; }}
    .partBody {{ margin-top:10px; display:flex; flex-direction:column; gap:10px; }}
    .stage {{ border:1px solid #e6e6e6; border-radius:12px; padding:8px 10px; background:#fcfcfc; }}
    .stage > summary {{ cursor:pointer; font-weight:700; }}
    .stage > summary::-webkit-details-marker {{ display:none; }}
    .stageBody {{ margin-top:8px; display:flex; flex-direction:column; gap:8px; }}
    .candRow {{ display:flex; justify-content:space-between; gap:12px; border:1px solid #eee; border-radius:12px; padding:8px 10px; background:#fff; }}
    .candLeft {{ min-width:0; }}
    .candPN {{ display:flex; gap:10px; align-items:baseline; }}
    .rank {{ color:#666; font-size:12px; }}
    .pn {{ font-family: ui-monospace, Menlo, Consolas, monospace; font-weight:700; font-size:13px; }}
    .candDesc {{ margin-top:4px; font-size:13px; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; max-width:950px; }}
    .candRight {{ min-width:180px; text-align:right; font-family: ui-monospace, Menlo, Consolas, monospace; color:#333; }}
    .empty {{ color:#777; font-size:13px; padding:6px 0; }}
    .hidden {{ display:none !important; }}
  </style>
</head>
<body>
  <h2>NPR Trace Viewer (Per Part)</h2>
  <input id="q" placeholder="Filter by partnum, mfgpn, description, winner..." />
  <div class="hint">Each JSONL record is one part. Inside: Dense → Sparse → Rerank candidates for that part.</div>
  <div id="cards">{''.join(cards)}</div>
  <script>
    const q = document.getElementById('q');
    const cards = Array.from(document.querySelectorAll('.partCard'));
    function norm(s){{ return (s||'').toLowerCase(); }}
    function apply(){{
      const needle = norm(q.value.trim());
      if(!needle){{ cards.forEach(c=>c.classList.remove('hidden')); return; }}
      cards.forEach(card => {{
        const hay = norm(card.innerText);
        if(hay.includes(needle)) card.classList.remove('hidden');
        else card.classList.add('hidden');
      }});
    }}
    q.addEventListener('input', apply);
  </script>
</body>
</html>
"""
    out_path.write_text(doc, encoding="utf-8")




def main():
    repo_root = _default_repo_root()
    default_trace = _default_trace_path()
    default_outdir = Path(__file__).resolve().parent  # NPR_TOOL

    ap = argparse.ArgumentParser()
    ap.add_argument("--trace", type=str, default=str(default_trace), help=f"Path to trace (default: {default_trace})")
    ap.add_argument("--outdir", type=str, default=str(default_outdir), help=f"Output dir (default: {default_outdir})")
    args = ap.parse_args()

    trace_path = Path(args.trace).expanduser().resolve()
    outdir = Path(args.outdir).expanduser().resolve()
    outdir.mkdir(parents=True, exist_ok=True)

    print(f"[trace_viewer] repo_root = {repo_root}")
    print(f"[trace_viewer] trace_path = {trace_path}")
    print(f"[trace_viewer] outdir     = {outdir}")

    if not trace_path.exists():
        raise SystemExit(f"Trace file not found: {trace_path}")

    traces = _read_jsonl(trace_path)
    print(f"[trace_viewer] Loaded {len(traces)} trace records")

    if not traces:
        raise SystemExit(f"No trace records found in: {trace_path}")

    summary_rows, cand_rows = build_tables(traces)

    sum_csv = outdir / "trace_summary.csv"
    cand_csv = outdir / "trace_candidates.csv"
    html_path = outdir / "trace_viewer.html"

    write_csv(summary_rows, sum_csv)
    write_csv(cand_rows, cand_csv)
    traces = _read_jsonl(trace_path)
    write_html_from_traces(traces, outdir / "trace_viewer.html")

    print("[trace_viewer] Wrote:")
    print(f"  {sum_csv}")
    print(f"  {cand_csv}")
    print(f"  {html_path}")
    print("Open trace_viewer.html in your browser.")


if __name__ == "__main__":
    main()
