"""
===============================================================
Part Matching Tool — UI Module (v1.3-dev)
===============================================================

This revision focuses on the NEW “DecisionNode / Task Manager” flow and fixes
ALL the bad attribute references we kept tripping over (mpn vs mfgpn, internal PN
fields, alternates fields, etc.).

Key goals:
- Main table is now “decision-centric” (one row = one DecisionNode)
- Double-click opens a Decision Inspector
- Robust field access using safe getters so mismatched datamodel names don’t crash the UI

Run: python -m npr_tool.ui_main
"""

from __future__ import annotations

import threading
import tkinter as tk
from tkinter import ttk, filedialog, messagebox, simpledialog
from pathlib import Path
from datetime import datetime
from typing import Any, Optional

from .data_loader import DataLoader
from .matching_engine import MatchingEngine
from .parsing_engine import parse_description
from .config_loader import load_config

# NOTE: MatchType is still used for tagging / coloring (and any future display)
from .data_models import MatchType

# Existing NPR workspace/export (already in your repo)
from .npr_workspace import NPRWorkspace, NPRPrimaryNewItem, NPRSecondaryRow, NPRRowKind
from .npr_export import NPRExportMetadata, export_npr_from_rows, get_template_headers

# Decision-node layer (in YOUR local revision)
# (UI is written to be resilient even if field names vary slightly.)
from .npr_workspace import DecisionNode, DecisionStatus, build_decision_node


class NPRToolUI:
    """Decision-node centered UI (task-manager style)."""

    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("Part Matching Tool — v1.3-dev (Decision Nodes)")
        self.root.geometry("1650x950")

        # Core data
        self.inventory = []
        self.npr_list = []
        # match_pairs is always: List[tuple[NPRPart, MatchResult]]
        self.match_pairs: list[tuple[Any, Any]] = []

        # Decision layer
        self.decision_nodes: list[DecisionNode] = []

        # UI State
        self.filter_text = tk.StringVar()
        self.status_var = tk.StringVar(value="Ready.")
        self.confidence_threshold = tk.DoubleVar(value=0.0)

        self.tooltip: Optional[tk.Toplevel] = None
        self.hover_after_id = None
        self.hover_row = None
        self.hover_col = None

        self.last_clicked_item = None
        self.last_clicked_col = None

        # NPR Workspace tab state
        self.npr_workspace: NPRWorkspace | None = None
        base_dir = Path(__file__).parent
        self.npr_template_path = base_dir / "NPR_Master2023_v4 Form(pulled on 12-16-2025).xlsx"

        self.settings_window = None

        self._build_ui()
        self._init_npr_workspace_tab()

    # =====================================================
    # SAFE FIELD GETTERS (prevents the “mpn vs mfgpn” chaos)
    # =====================================================
    def _get(self, obj: Any, *names: str, default: Any = "") -> Any:
        for n in names:
            if obj is None:
                return default
            if hasattr(obj, n):
                v = getattr(obj, n)
                if v is not None:
                    return v
        return default

    def _node_base_type(self, node: DecisionNode) -> str:
        return str(self._get(node, "base_type", default="")).upper() or "UNKNOWN"

    def _node_status_label(self, node: DecisionNode) -> str:
        st = self._get(node, "status", default="")
        # DecisionStatus or string
        if hasattr(st, "value"):
            return str(st.value)
        return str(st) if st else "—"

    def _node_locked(self, node: DecisionNode) -> bool:
        return bool(self._get(node, "locked", default=False))

    def _node_bom_uid(self, node: DecisionNode) -> str:
        # Prefer explicit bom_uid if present
        uid = self._get(node, "bom_uid", default="")
        if uid:
            return str(uid)

        # Else derive from base_bom_part
        bom = self._get(node, "base_bom_part", default=None)
        return str(self._get(bom, "bom_uid", "partnum", "itemnum", default=""))

    def _node_base_part_number_display(self, node: DecisionNode) -> str:
        bt = self._node_base_type(node)

        if bt == "NEW":
            bom = self._get(node, "base_bom_part", default=None)
            # In your current models: NPRPart has partnum + mfgpn (NOT mpn)
            # We display “BOM UID / MFGPN” style if available.
            uid = str(self._get(bom, "bom_uid", "partnum", "itemnum", default=""))
            mfgpn = str(self._get(bom, "bom_mpn", "mfgpn", "mpn", default=""))
            if uid and mfgpn:
                return f"{uid} | {mfgpn}"
            return uid or mfgpn or "—"

        if bt == "EXISTS":
            inv = self._get(node, "base_inventory_part", default=None)
            # InventoryPart uses itemnum and vendoritem in your current datamodels.py
            itemnum = str(self._get(inv, "itemnum", "internal_part_number", default=""))
            vendoritem = str(self._get(inv, "vendoritem", "manufacturer_part_number", default=""))
            if itemnum and vendoritem:
                return f"{itemnum} | {vendoritem}"
            return itemnum or vendoritem or "—"

        return "—"

    def _node_description(self, node: DecisionNode) -> str:
        d = self._get(node, "description", default="")
        if d:
            return str(d)
        # fallback: derive from bom/inv objects
        bom = self._get(node, "base_bom_part", default=None)
        inv = self._get(node, "base_inventory_part", default=None)
        return str(self._get(bom, "description", "desc", default="")) or str(self._get(inv, "description", "desc", default=""))

    def _alt_label(self, alt: Any) -> str:
        # Works with either NPRSecondaryRow-style alternates OR InventoryPart-style alternates
        mfgpn = str(self._get(alt, "manufacturer_part_number", "vendoritem", "mfgpn", "mpn", default=""))
        itemnum = str(self._get(alt, "internal_part_number", "itemnum", default=""))
        desc = str(self._get(alt, "description", "desc", default=""))
        bits = [b for b in [mfgpn, itemnum, desc] if b]
        return " | ".join(bits) if bits else "—"

    def _count_selected_alts(self, node: DecisionNode) -> int:
        sel = self._get(node, "selected_alternates", default=[])
        try:
            return len(sel)
        except Exception:
            return 0

    def _count_candidate_alts(self, node: DecisionNode) -> int:
        cand = self._get(node, "alternate_candidates", default=[])
        try:
            return len(cand)
        except Exception:
            return 0

    def _node_needs_approval(self, node: DecisionNode) -> bool:
        # If you have a flag in DecisionNode, use it. Otherwise infer:
        # - EXISTS nodes: approval is relevant if any alternates selected
        # - NEW nodes: approval is relevant because you’re proposing new / alternates
        v = self._get(node, "needs_approval", default=None)
        if v is not None:
            return bool(v)
        bt = self._node_base_type(node)
        if bt == "EXISTS":
            return self._count_selected_alts(node) > 0
        if bt == "NEW":
            return True
        return False

    # =====================================================
    # UI BUILD
    # =====================================================
    def _build_ui(self):
        tk.Label(self.root, text="NPR Tool — Decision Workspace", font=("Segoe UI", 22, "bold")).pack(pady=10)

        toolbar = tk.Frame(self.root)
        toolbar.pack(pady=5)

        tk.Button(toolbar, text="Load Inventory", command=self.load_inventory).grid(row=0, column=0, padx=5)
        tk.Button(toolbar, text="Load Parts", command=self.load_npr).grid(row=0, column=1, padx=5)
        tk.Button(toolbar, text="Run Matching", command=self.run_matching_async).grid(row=0, column=2, padx=5)

        tk.Button(toolbar, text="Settings ⚙️", command=self.open_settings).grid(row=0, column=5, padx=5)

        search_frame = tk.Frame(self.root)
        search_frame.pack(fill="x", pady=4)
        tk.Label(search_frame, text="Search / Filter:").pack(side="left", padx=5)
        entry = tk.Entry(search_frame, textvariable=self.filter_text, width=40)
        entry.pack(side="left", padx=5)
        entry.bind("<KeyRelease>", lambda e: self.filter_table())

        self.notebook = ttk.Notebook(self.root)
        self.notebook.pack(fill="both", expand=True)

        self.tab_matches = ttk.Frame(self.notebook)
        self.tab_substitutes = ttk.Frame(self.notebook)
        self.tab_api = ttk.Frame(self.notebook)

        self.notebook.add(self.tab_matches, text="Decisions")
        self.notebook.add(self.tab_substitutes, text="Substitutes")
        self.notebook.add(self.tab_api, text="API Data")

        self._build_decision_tab()
        self._build_placeholder_tab(self.tab_substitutes, "Substitute data not yet connected.")
        self._build_placeholder_tab(self.tab_api, "API data not yet connected.")

        tk.Label(self.root, textvariable=self.status_var, anchor="w", relief="sunken").pack(fill="x", side="bottom")

    def _build_placeholder_tab(self, parent, text):
        frame = tk.Frame(parent)
        frame.pack(expand=True)
        tk.Label(frame, text=text, font=("Segoe UI", 11, "italic"), fg="#555").pack(pady=50)

    # =====================================================
    # MAIN DECISION TABLE
    # =====================================================
    def _build_decision_tab(self):
        frame = tk.Frame(self.tab_matches)
        frame.pack(fill="both", expand=True)

        self.tree = ttk.Treeview(
            frame,
            columns=("Base Type", "Base Part #", "Description", "Status", "Alternates", "Approval"),
            show="headings",
        )

        for col in self.tree["columns"]:
            self.tree.heading(col, text=col)
            self.tree.column(col, anchor="w", stretch=True)

        scrollbar = ttk.Scrollbar(frame, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=scrollbar.set)
        self.tree.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")

        self._configure_tree_colors()

        self.tree.bind("<Double-1>", self._on_double_click)
        self.tree.bind("<Motion>", self._on_hover)
        self.tree.bind("<Leave>", lambda e: self._hide_tooltip())
        self.tree.bind("<Button-1>", self.on_tree_click, add="+")
        self.tree.bind("<Control-c>", self.copy_selected_cell)

    def _configure_tree_colors(self):
        # Decision status tags (keep simple + readable)
        self.tree.tag_configure("needs_alt", background="#ffe5e5")
        self.tree.tag_configure("needs_decision", background="#fff5cc")
        self.tree.tag_configure("auto", background="#e8f5e9")
        self.tree.tag_configure("ready", background="#e3f2fd")
        self.tree.tag_configure("locked", background="#f2f2f2")

    def _tag_for_node(self, node: DecisionNode) -> str:
        if self._node_locked(node):
            return "locked"

        status = self._get(node, "status", default=None)
        # If your DecisionStatus enum exists, match by name/value safely.
        status_name = ""
        if status is None:
            status_name = ""
        elif hasattr(status, "name"):
            status_name = str(status.name)
        else:
            status_name = str(status)

        # Heuristics:
        bt = self._node_base_type(node)
        selected = self._count_selected_alts(node)
        candidates = self._count_candidate_alts(node)

        if "READY" in status_name:
            return "ready"
        if bt == "NEW" and selected == 0:
            return "needs_alt"
        if bt == "EXISTS" and candidates > 0 and selected == 0:
            return "needs_decision"
        return "auto"

    def render_table(self, nodes: Optional[list[DecisionNode]] = None):
        nodes = nodes if nodes is not None else self.decision_nodes

        for iid in self.tree.get_children():
            self.tree.delete(iid)

        filter_txt = (self.filter_text.get() or "").strip().lower()

        for idx, node in enumerate(nodes):
            base_type = self._node_base_type(node)
            base_pn = self._node_base_part_number_display(node)
            desc = self._node_description(node)
            status = self._node_status_label(node)

            selected = self._count_selected_alts(node)
            candidates = self._count_candidate_alts(node)
            alt_text = f"{selected} selected / {candidates} candidates"

            approval = "YES" if self._node_needs_approval(node) else "NO"

            row_blob = f"{base_type} {base_pn} {desc} {status} {alt_text} {approval}".lower()
            if filter_txt and filter_txt not in row_blob:
                continue

            iid = f"node_{idx}"
            tag = self._tag_for_node(node)
            self.tree.insert(
                "",
                "end",
                iid=iid,
                values=(base_type, base_pn, desc, status, alt_text, approval),
                tags=(tag,),
            )

            # Store node reference on the row for quick retrieval
            # (Treeview doesn't let us attach objects directly; we keep an index map.)
        self.status_var.set(f"Rendered {len(self.tree.get_children())} decisions.")

    # =====================================================
    # LOAD / MATCH
    # =====================================================
    def load_inventory(self):
        path = filedialog.askopenfilename(filetypes=[("Excel Files", "*.xlsx")])
        if not path:
            return
        try:
            self.inventory = DataLoader.load_inventory(path)
            self.status_var.set(f"Loaded inventory ({len(self.inventory)} parts).")
        except Exception as e:
            messagebox.showerror("Error", str(e))

    def load_npr(self):
        path = filedialog.askopenfilename(filetypes=[("Excel Files", "*.xlsx")])
        if not path:
            return
        try:
            self.npr_list = DataLoader.load_npr(path)
            self.status_var.set(f"Loaded NPR list ({len(self.npr_list)} parts).")
        except Exception as e:
            messagebox.showerror("Error", str(e))

    def run_matching_async(self):
        threading.Thread(target=self.run_matching, daemon=True).start()

    def run_matching(self):
        if not self.inventory or not self.npr_list:
            messagebox.showerror("Error", "Load inventory and parts first.")
            return

        # Load config (keep local for now)
        cfg = load_config("./config/components.yaml")

        # Parse both sides explicitly
        for inv in self.inventory:
            inv.parsed = parse_description(getattr(inv, "description", ""), cfg)
        for npr in self.npr_list:
            npr.parsed = parse_description(getattr(npr, "description", ""), cfg)

        engine = MatchingEngine(self.inventory, config=cfg)

        # IMPORTANT:
        # match_npr_list returns list of tuples: [(npr, MatchResult), ...]
        self.match_pairs = engine.match_npr_list(self.npr_list)

        # Build DecisionNodes from PAIRS (prevents the “tuple has no attribute inventory_part” crash)
        nodes: list[DecisionNode] = []
        for npr_part, match in self.match_pairs:
            try:
                node = build_decision_node(npr_part, match)
                nodes.append(node)
            except Exception as e:
                # Don’t crash the entire run; surface the first meaningful error
                messagebox.showerror("Decision Build Error", f"{e}")
                return

        self.decision_nodes = nodes

        self.status_var.set(f"Matched {len(self.match_pairs)} parts. Built {len(self.decision_nodes)} decisions.")
        self.render_table(self.decision_nodes)

        # Also refresh NPR workspace tab if it exists (optional)
        # (You can keep this or remove it depending on how “pages” evolve)
        # self.build_npr_workspace()

    # =====================================================
    # DOUBLE CLICK -> INSPECTOR
    # =====================================================
    def _on_double_click(self, event):
        row = self.tree.identify_row(event.y)
        if not row:
            return
        try:
            idx = int(row.split("_", 1)[1])
        except Exception:
            return
        if idx < 0 or idx >= len(self.decision_nodes):
            return
        self.open_decision_inspector(self.decision_nodes[idx])

    def open_decision_inspector(self, node: DecisionNode):
        win = tk.Toplevel(self.root)
        win.title(f"NPR Decision — {self._node_bom_uid(node)}")
        win.geometry("900x600")
        win.transient(self.root)
        win.grab_set()

        self._build_inspector_ui(win, node)

    def _build_inspector_ui(self, parent, node: DecisionNode):
        outer = ttk.Frame(parent)
        outer.pack(fill="both", expand=True)

        self._build_context_section(outer, node)
        self._build_alternate_candidates(outer, node)
        self._build_selected_alternates(outer, node)
        self._build_decision_controls(outer, node, parent)

    def _build_context_section(self, parent, node: DecisionNode):
        frame = ttk.LabelFrame(parent, text="Base Context")
        frame.pack(fill="x", padx=8, pady=6)

        bt = self._node_base_type(node)
        ttk.Label(frame, text=f"Base Type: {bt}").pack(anchor="w")

        if bt == "NEW":
            bom = self._get(node, "base_bom_part", default=None)
            uid = str(self._get(bom, "bom_uid", "partnum", "itemnum", default=""))
            mfgpn = str(self._get(bom, "bom_mpn", "mfgpn", "mpn", default=""))
            ttk.Label(frame, text=f"BOM UID: {uid or '—'}").pack(anchor="w")
            ttk.Label(frame, text=f"BOM MFG PN: {mfgpn or '—'}").pack(anchor="w")

        elif bt == "EXISTS":
            inv = self._get(node, "base_inventory_part", default=None)
            # FIX: internal_part_number DOES NOT EXIST on InventoryPart in your current datamodel;
            # it is itemnum.
            itemnum = str(self._get(inv, "itemnum", "internal_part_number", default=""))
            vendoritem = str(self._get(inv, "vendoritem", "manufacturer_part_number", default=""))
            ttk.Label(frame, text=f"Internal PN: {itemnum or '—'}").pack(anchor="w")
            ttk.Label(frame, text=f"MFG PN: {vendoritem or '—'}").pack(anchor="w")

        ttk.Label(frame, text=f"Description: {self._node_description(node)}").pack(anchor="w")

        mt = self._get(node, "match_type", default="")
        if hasattr(mt, "value"):
            mt = mt.value
        ttk.Label(frame, text=f"Match Type: {mt or '—'}").pack(anchor="w")

        ttk.Label(frame, text=f"Status: {self._node_status_label(node)}").pack(anchor="w")

    def _build_alternate_candidates(self, parent, node: DecisionNode):
        frame = ttk.LabelFrame(parent, text="Alternate Candidates")
        frame.pack(fill="both", expand=True, padx=8, pady=6)

        self.alt_list = tk.Listbox(frame, height=10)
        self.alt_list.pack(fill="both", expand=True, padx=4, pady=4)

        candidates = self._get(node, "alternate_candidates", default=[])
        for alt in candidates:
            self.alt_list.insert("end", self._alt_label(alt))

        btns = ttk.Frame(frame)
        btns.pack(fill="x", padx=4, pady=(0, 4))

        ttk.Button(btns, text="Add Manual", command=lambda: self._add_manual_alt(node)).pack(side="left")
        ttk.Button(btns, text="Remove", command=lambda: self._remove_alt(node)).pack(side="left")
        ttk.Button(btns, text="Select", command=lambda: self._select_alt(node)).pack(side="right")

    def _build_selected_alternates(self, parent, node: DecisionNode):
        frame = ttk.LabelFrame(parent, text="Selected Alternates")
        frame.pack(fill="x", padx=8, pady=6)

        self.selected_list = tk.Listbox(frame, height=5)
        self.selected_list.pack(fill="x", padx=4, pady=4)

        selected = self._get(node, "selected_alternates", default=[])
        for alt in selected:
            self.selected_list.insert("end", self._alt_label(alt))

    def _build_decision_controls(self, parent, node: DecisionNode, win: tk.Toplevel):
        frame = ttk.Frame(parent)
        frame.pack(fill="x", padx=8, pady=10)

        ttk.Button(frame, text="Mark Ready", command=lambda: self._mark_ready(node, win)).pack(side="right")
        ttk.Button(frame, text="Cancel", command=win.destroy).pack(side="right", padx=(0, 6))

    def _mark_ready(self, node: DecisionNode, win: tk.Toplevel):
        bt = self._node_base_type(node)
        if bt == "NEW" and self._count_selected_alts(node) == 0:
            messagebox.showwarning("Incomplete", "NEW parts require at least one alternate selected.")
            return

        # Status set (works if DecisionStatus exists; otherwise stores string)
        try:
            node.status = DecisionStatus.READY_FOR_EXPORT
        except Exception:
            node.status = "READY_FOR_EXPORT"

        try:
            node.locked = True
        except Exception:
            pass

        self.render_table(self.decision_nodes)
        win.destroy()

    # =====================================================
    # Candidate manipulation (minimal, safe)
    # =====================================================
    def _add_manual_alt(self, node: DecisionNode):
        mfgpn = simpledialog.askstring("Add Manual Alternate", "Manufacturer Part # (or vendor item):")
        if not mfgpn:
            return

        # Create a very lightweight “alt object” shape using dict-like fields.
        # Your DecisionNode layer can later convert this into a proper NPRAlternateRow.
        manual_alt = {
            "manufacturer_part_number": mfgpn,
            "internal_part_number": "",
            "description": "Manual alternate",
            "source": "manual",
        }

        candidates = self._get(node, "alternate_candidates", default=None)
        if candidates is None:
            messagebox.showerror("Error", "Node has no alternate_candidates list.")
            return

        try:
            candidates.append(manual_alt)
        except Exception:
            messagebox.showerror("Error", "Could not append manual alternate (unexpected candidate container).")
            return

        self.render_table(self.decision_nodes)

    def _remove_alt(self, node: DecisionNode):
        sel = self.alt_list.curselection()
        if not sel:
            return
        i = int(sel[0])

        candidates = self._get(node, "alternate_candidates", default=[])
        try:
            if 0 <= i < len(candidates):
                candidates.pop(i)
        except Exception:
            return

        self.render_table(self.decision_nodes)

    def _select_alt(self, node: DecisionNode):
        sel = self.alt_list.curselection()
        if not sel:
            return
        i = int(sel[0])

        candidates = self._get(node, "alternate_candidates", default=[])
        selected = self._get(node, "selected_alternates", default=None)
        if selected is None:
            messagebox.showerror("Error", "Node has no selected_alternates list.")
            return

        try:
            if 0 <= i < len(candidates):
                picked = candidates[i]
                selected.append(picked)
        except Exception:
            return

        self.render_table(self.decision_nodes)

    # =====================================================
    # Filtering / clipboard helpers
    # =====================================================
    def filter_table(self):
        self.render_table(self.decision_nodes)

    def on_tree_click(self, event):
        self.last_clicked_item = self.tree.identify_row(event.y)
        self.last_clicked_col = self.tree.identify_column(event.x)

    def copy_selected_cell(self, event=None):
        item = self.last_clicked_item
        col = self.last_clicked_col
        if not item or not col:
            return
        values = self.tree.item(item, "values")
        try:
            idx = int(col.replace("#", "")) - 1
            val = values[idx] if idx >= 0 else ""
        except Exception:
            val = ""
        if val is None:
            val = ""
        self.root.clipboard_clear()
        self.root.clipboard_append(str(val))
        self.status_var.set("Copied cell to clipboard.")

    # =====================================================
    # Tooltip (unchanged behavior; safe)
    # =====================================================
    def _create_tooltip(self, text, x, y):
        self._hide_tooltip()
        self.tooltip = tk.Toplevel(self.root)
        self.tooltip.overrideredirect(True)
        self.tooltip.attributes("-topmost", True)
        self.tooltip.attributes("-alpha", 0.97)
        frame = tk.Frame(self.tooltip, bg="#fefefe", padx=6, pady=4, highlightthickness=1, highlightbackground="#d0d0d0")
        frame.pack()
        label = tk.Label(frame, text=text, font=("Segoe UI", 9), bg="#fefefe", fg="#000")
        label.pack()
        self.tooltip.geometry(f"+{x+12}+{y+12}")

    def _hide_tooltip(self):
        if self.tooltip:
            try:
                self.tooltip.destroy()
            except Exception:
                pass
            self.tooltip = None
        if self.hover_after_id:
            try:
                self.root.after_cancel(self.hover_after_id)
            except Exception:
                pass
            self.hover_after_id = None

    def _on_hover(self, event):
        row = self.tree.identify_row(event.y)
        col = self.tree.identify_column(event.x)
        if not row or not col:
            self._hide_tooltip()
            self.hover_row = None
            self.hover_col = None
            return
        if row == self.hover_row and col == self.hover_col:
            return
        self._hide_tooltip()
        self.hover_row, self.hover_col = row, col
        self.hover_after_id = self.root.after(600, lambda: self._show_hover_tooltip(event.x_root, event.y_root))

    def _show_hover_tooltip(self, x, y):
        if not self.hover_row or not self.hover_col:
            return
        item = self.tree.item(self.hover_row)
        values = item.get("values", ())
        try:
            idx = int(self.hover_col[1:]) - 1
            value = values[idx]
        except Exception:
            value = ""
        if value:
            self._create_tooltip(str(value), x, y)

    # =====================================================
    # Settings
    # =====================================================
    def open_settings(self):
        if self.settings_window and self.settings_window.winfo_exists():
            self.settings_window.lift()
            return

        win = tk.Toplevel(self.root)
        win.title("Settings")
        win.geometry("420x220")
        win.transient(self.root)
        self.settings_window = win

        ttk.Label(win, text="Confidence Threshold", font=("Segoe UI", 10, "bold")).pack(pady=(12, 4))
        slider = ttk.Scale(win, from_=0.0, to=1.0, variable=self.confidence_threshold, orient="horizontal")
        slider.pack(fill="x", padx=20)

        ttk.Label(win, text="(Used by matching engine tiers; decision view currently shows all nodes.)", foreground="#555").pack(pady=(6, 0))

        ttk.Button(win, text="Close", command=win.destroy).pack(pady=14)

    # =============================================
    # NPR WORKSPACE TAB (kept as-is; can delete later)
    # =============================================
    def _init_npr_workspace_tab(self):
        frame = ttk.Frame(self.notebook)
        self.notebook.add(frame, text="NPR Workspace")

        top = tk.Frame(frame)
        top.pack(fill="x", pady=4)

        tk.Button(top, text="Build NPR Workspace", command=self.build_npr_workspace).pack(side="left", padx=5)
        tk.Button(top, text="Add Manual Alternate", command=self.add_manual_alternate).pack(side="left", padx=5)
        tk.Button(top, text="Export NPR", command=self.export_npr).pack(side="left", padx=5)

        tk.Label(top, text="Template:").pack(side="left", padx=(20, 4))
        self.template_label = tk.Label(top, text=str(self.npr_template_path))
        self.template_label.pack(side="left")

        self.npr_notebook = ttk.Notebook(frame)
        self.npr_notebook.pack(fill="both", expand=True)

        # Page 1
        self.page1 = ttk.Frame(self.npr_notebook)
        self.npr_notebook.add(self.page1, text="Page 1 — NEW Parts")
        self.page1_tree = ttk.Treeview(self.page1, columns=("Include", "BOM UID", "BOM MPN", "Description", "Type"), show="headings", height=18)
        for c in ("Include", "BOM UID", "BOM MPN", "Description", "Type"):
            self.page1_tree.heading(c, text=c)
        self.page1_tree.pack(fill="both", expand=True)

        # Page 2
        self.page2 = ttk.Frame(self.npr_notebook)
        self.npr_notebook.add(self.page2, text="Page 2 — Secondary Rows")

        self.page2_tree = ttk.Treeview(
            self.page2,
            columns=("Include", "Kind", "Parent BOM MPN", "Internal PN", "Exists?", "MFG Name", "MFG PN", "Supplier", "Unit Cost"),
            show="headings",
            height=18
        )
        for c in self.page2_tree["columns"]:
            self.page2_tree.heading(c, text=c)
        self.page2_tree.pack(fill="both", expand=True)

        # Page 3 (preview)
        self.page3 = ttk.Frame(self.npr_notebook)
        self.npr_notebook.add(self.page3, text="Page 3 — NPR Preview")

        self.page3_tree = ttk.Treeview(self.page3, columns=(""), show="headings", height=18)
        self.page3_tree.pack(fill="both", expand=True)

    def build_npr_workspace(self):
        if not self.inventory or not self.npr_list or not self.match_pairs:
            messagebox.showerror("Error", "Load inventory + parts and run matching first.")
            return

        ws = NPRWorkspace()

        # Page 1: NEW parts (NO_MATCH in your engine)
        for npr_part, match in self.match_pairs:
            if match and getattr(match, "match_type", None) == MatchType.NO_MATCH:
                ws.primary_new_items.append(
                    NPRPrimaryNewItem(
                        bom_uid=str(getattr(npr_part, "partnum", "")),
                        bom_mpn=str(getattr(npr_part, "mfgpn", "")),
                        description=str(getattr(npr_part, "description", "")),
                        component_type=str(getattr(npr_part, "part_type", "")),
                        populated=False,
                        include_in_export=True,
                    )
                )

        # Page 2: context rows for matches with inventory_part
        for npr_part, match in self.match_pairs:
            inv = getattr(match, "inventory_part", None) if match else None
            if inv is None:
                continue

            row_id = f"CTX-{getattr(npr_part, 'partnum', '')}-{getattr(inv, 'itemnum', '')}"
            ws.secondary_rows.append(
                NPRSecondaryRow(
                    row_id=row_id,
                    kind=NPRRowKind.CONTEXT_EXISTS,
                    parent_bom_uid=str(getattr(npr_part, "partnum", "")),
                    parent_bom_mpn=str(getattr(npr_part, "mfgpn", "")),
                    parent_description=str(getattr(npr_part, "description", "")),
                    internal_part_number=str(getattr(inv, "itemnum", "")),
                    exists_in_inventory=True,
                    include_in_export=True,
                    source="inventory",
                )
            )

        self.npr_workspace = ws
        self.refresh_npr_pages()
        self.notebook.select(self.notebook.index("end") - 1)

    def refresh_npr_pages(self):
        ws = self.npr_workspace
        if not ws:
            return

        # Page 1
        for i in self.page1_tree.get_children():
            self.page1_tree.delete(i)
        for item in ws.primary_new_items:
            self.page1_tree.insert("", "end", values=(
                "YES" if item.include_in_export else "NO",
                item.bom_uid,
                item.bom_mpn,
                item.description,
                item.component_type,
            ))

        # Page 2
        for i in self.page2_tree.get_children():
            self.page2_tree.delete(i)
        for row in ws.secondary_rows:
            self.page2_tree.insert("", "end", values=(
                "YES" if row.include_in_export else "NO",
                row.kind.value,
                row.parent_bom_mpn,
                row.internal_part_number,
                "EXISTS" if row.exists_in_inventory else "NEW",
                row.manufacturer_name,
                row.manufacturer_part_number,
                row.supplier,
                "" if row.unit_cost is None else row.unit_cost,
            ))

        # Page 3 preview
        try:
            headers = get_template_headers(self.npr_template_path)
        except Exception as e:
            messagebox.showerror("Template Error", str(e))
            return

        preview_rows = ws.build_excel_preview_rows(headers)

        self.page3_tree["columns"] = headers
        for h in headers:
            self.page3_tree.heading(h, text=h)

        for i in self.page3_tree.get_children():
            self.page3_tree.delete(i)

        for r in preview_rows:
            self.page3_tree.insert("", "end", values=[r.get(h, "") for h in headers])

    def add_manual_alternate(self):
        messagebox.showinfo(
            "Add Alternate",
            "Manual alternate entry is not implemented here yet.\n"
            "Use the Decision Inspector (double-click a decision) to add manual alternates."
        )

    def export_npr(self):
        if not self.npr_workspace:
            messagebox.showerror("Error", "Build NPR Workspace first.")
            return

        # Ask save location
        out_path = filedialog.asksaveasfilename(
            defaultextension=".xlsx",
            filetypes=[("Excel Files", "*.xlsx")],
            initialfile=f"NPR_EXPORT_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
        )
        if not out_path:
            return

        meta = NPRExportMetadata(
            created_by="NPR Tool",
            created_at=datetime.now().isoformat(timespec="seconds"),
            notes="Exported from NPR Tool",
        )

        try:
            export_npr_from_rows(
                template_path=self.npr_template_path,
                output_path=out_path,
                workspace=self.npr_workspace,
                metadata=meta,
            )
            messagebox.showinfo("Export Complete", f"Saved:\n{out_path}")
        except Exception as e:
            messagebox.showerror("Export Error", str(e))


def main():
    root = tk.Tk()
    app = NPRToolUI(root)
    root.mainloop()


if __name__ == "__main__":
    main()
