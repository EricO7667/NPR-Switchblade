"""
===============================================================
Part Matching Tool — UI Module (v1.1)
===============================================================
---------------------------------------------------------------
🔹 VERSION HISTORY
---------------------------------------------------------------
Run with python -m npr_tool.ui_main
v1.1 — Current (2025-12-10)
• Added functional live search filtering (`filter_table`) with reload-on-clear.
• Fixed Treeview item ID duplication ("Item already exists" bug).
• Confidence threshold slider in Settings now filters matches dynamically.
• Settings window now limited to one instance at a time (no duplicates).
• Improved double-click “Show Raw Fields” toggle for inline raw Excel data comparison.
• Restored inline parsed field comparisons (Description, MFG PN, Manufacturer, etc.)
• Retained all previous tooltip, copy-cell, and export functionalities.
• Fully modular design — ready for integration with future Substitute & API tabs.

v1.0 (previous build)
• Added expandable inline match comparison system.
• Introduced “Show Raw Fields” toggle (initial release of detail expansion).
• Added tabbed UI structure for future Substitutes and API data.
• Implemented threaded matching execution (non-blocking UI).
• Added confidence threshold slider and dynamic status bar.
• Fixed tooltip and copy-to-clipboard functionality.

v0.2.0 (legacy builds)
• Core matching and parsing engine integration with UI.
• Introduced color-coded match highlighting (Exact, Prefix, Parsed, Missing).
• Added Excel import/export support for existing/missing parts.
• Introduced parsed resistor/capacitor comparison engine.

---------------------------------------------------------------
🔹 FILE OVERVIEW
---------------------------------------------------------------
- ui_main.py:       This UI controller (Tkinter-based frontend)
- data_loader.py:   Loads Inventory and NPR Excel files, normalizes headers
- matching_engine.py: Core matching logic between NPR and Inventory
- data_models.py:   Defines data classes (NPRPart, InventoryPart, MatchType)
- parsing_engine.py: Extracts structured attributes (value, tolerance, wattage, etc.)

---------------------------------------------------------------
🔹 NEXT FEATURES (Planned)
---------------------------------------------------------------
• Add Substitute Sheet Integration (multi-list handling)
• Digi-Key API integration for part lookup and metadata scraping
• Improved match confidence visualization (bar/heatmap)
• Persistent user settings and configuration file
• CSV export for raw match dataset

===============================================================
"""

"""
===============================================================
Part Matching Tool — UI Module (v1.2)
===============================================================
Refactor goal:
- Keep UI behavior the same
- Make backend scalable (component rules, parsing rules, matching rules)
- Fix table filtering column mismatch bug

Run: python -m npr_tool.ui_main
"""

import os
import threading
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import pandas as pd
import copy
from pathlib import Path
from .parsing_engine import parse_description
from .config_loader import load_config

from .data_loader import DataLoader
from .matching_engine import MatchingEngine
from .data_models import MatchType
from .config_loader import load_config


from datetime import datetime
from tkinter import simpledialog

from .npr_workspace import NPRWorkspace, NPRPrimaryNewItem, NPRSecondaryRow, NPRRowKind
from .npr_export import NPRExportMetadata, export_npr_from_rows, get_template_headers




class NPRToolUI:
    """Enhanced scalable UI with original functionality preserved."""

    def __init__(self, root):
        self.root = root
        self.root.title("Part Matching Tool — v1.2")
        self.root.geometry("1650x950")

        # Core Data
        self.inventory = []
        self.npr_list = []
        self.match_results = []

        # State
        self.row_index_map = {}
        self.detail_rows = {}
        self.expanded_state = {}
        self.raw_expanded = {}
        self.filter_text = tk.StringVar()
        self.status_var = tk.StringVar(value="Ready.")
        self.confidence_threshold = tk.DoubleVar(value=0.0)

        self.tooltip = None
        self.hover_after_id = None
        self.hover_row = None
        self.hover_col = None

        self.last_clicked_item = None
        self.last_clicked_col = None

        self.npr_workspace: NPRWorkspace | None = None
        from pathlib import Path

        BASE_DIR = Path(__file__).parent

        self.npr_template_path = (
            BASE_DIR / "NPR_Master2023_v4 Form(pulled on 12-16-2025).xlsx"
        )

        self._build_ui()
        self._init_npr_workspace_tab()


        

    # =====================================================
    # UI BUILD
    # =====================================================
    def _build_ui(self):
        tk.Label(
            self.root,
            text="Part Matching Tool",
            font=("Segoe UI", 22, "bold"),
        ).pack(pady=10)

        toolbar = tk.Frame(self.root)
        toolbar.pack(pady=5)
        tk.Button(toolbar, text="Load Inventory", command=self.load_inventory).grid(row=0, column=0, padx=5)
        tk.Button(toolbar, text="Load Parts", command=self.load_npr).grid(row=0, column=1, padx=5)
        tk.Button(toolbar, text="Run Matching", command=self.run_matching_async).grid(row=0, column=2, padx=5)
        tk.Button(toolbar, text="Settings ⚙️", command=self.open_settings).grid(row=0, column=5, padx=5)

        # Search Bar
        search_frame = tk.Frame(self.root)
        search_frame.pack(fill="x", pady=4)
        tk.Label(search_frame, text="Search / Filter:").pack(side="left", padx=5)
        entry = tk.Entry(search_frame, textvariable=self.filter_text, width=40)
        entry.pack(side="left", padx=5)
        entry.bind("<KeyRelease>", lambda e: self.filter_table())

        # Tabs
        self.notebook = ttk.Notebook(self.root)
        self.notebook.pack(fill="both", expand=True)

        self.tab_matches = ttk.Frame(self.notebook)
        self.tab_substitutes = ttk.Frame(self.notebook)
        self.tab_api = ttk.Frame(self.notebook)

        self.notebook.add(self.tab_matches, text="Matches")
        self.notebook.add(self.tab_substitutes, text="Substitutes")
        self.notebook.add(self.tab_api, text="API Data")

        self._build_match_tab()
        self._build_placeholder_tab(self.tab_substitutes, "Substitute data not yet connected.")
        self._build_placeholder_tab(self.tab_api, "API data not yet connected.")

        tk.Label(self.root, textvariable=self.status_var, anchor="w", relief="sunken").pack(fill="x", side="bottom")

    # =====================================================
    # MATCH TAB
    # =====================================================
    def _build_match_tab(self):
        frame = tk.Frame(self.tab_matches)
        frame.pack(fill="both", expand=True)

        self.columns = (
            "Internal Part #",
            "Inventory MFG #",
            "LookUp Part's MFG #",
            "Description",
            "Match Type",
            "Confidence",
        )

        self.tree = ttk.Treeview(frame, columns=self.columns, show="headings", height=25, selectmode="browse")

        for col in self.columns:
            self.tree.heading(col, text=col)
            if "Description" in col:
                self.tree.column(col, anchor="w", width=400)
            else:
                self.tree.column(col, anchor="w", width=220)

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
        self.tree.tag_configure("exact", background="#d4f7d0")
        self.tree.tag_configure("itemnum_exact", background="#fff3b0")
        self.tree.tag_configure("prefix", background="#e8ccff")
        self.tree.tag_configure("parsed", background="#d0e7ff")
        self.tree.tag_configure("substitute", background="#e0ffe6")
        self.tree.tag_configure("api", background="#f0f0ff")
        self.tree.tag_configure("missing", background="#f7d0d0")

        self.tree.tag_configure("detail_section_header", background="#d9e6ff", font=("Segoe UI", 9, "bold"))
        self.tree.tag_configure("detail_summary", background="#e0f0ff", font=("Segoe UI", 9, "italic"))
        self.tree.tag_configure("detail_header", background="#e9e9e9", font=("Segoe UI", 9, "bold"))
        self.tree.tag_configure("detail_row", background="#f5f5f5", font=("Segoe UI", 9))
        self.tree.tag_configure("raw_toggle", background="#f0f0ff", font=("Segoe UI", 9, "italic"))

    def _build_placeholder_tab(self, parent, text):
        frame = tk.Frame(parent)
        frame.pack(expand=True)
        tk.Label(frame, text=text, font=("Segoe UI", 11, "italic"), fg="#555").pack(pady=50)


    # =============================================
    # NPR BUILDING WORKSAPCE TABS
    #==============================================

    def _init_npr_workspace_tab(self):
        frame = ttk.Frame(self.notebook)
        self.notebook.add(frame, text="NPR Workspace")

        top = tk.Frame(frame)
        top.pack(fill="x", pady=4)

        tk.Button(top, text="Build NPR Workspace", command=self.build_npr_workspace).pack(side="left", padx=5)
        tk.Button(top, text="Add Manual Alternate", command=self.add_manual_alternate).pack(side="left", padx=5)
        tk.Button(top, text="Export NPR", command=self.export_npr).pack(side="left", padx=5)

        tk.Label(top, text="Template:").pack(side="left", padx=(20, 4))
        self.template_label = tk.Label(top, text=self.npr_template_path)
        self.template_label.pack(side="left")

        self.npr_notebook = ttk.Notebook(frame)
        self.npr_notebook.pack(fill="both", expand=True)

        # Page 1
        self.page1 = ttk.Frame(self.npr_notebook)
        self.npr_notebook.add(self.page1, text="Page 1 — NEW Parts")

        self.page1_tree = ttk.Treeview(
            self.page1,
            columns=("Include", "BOM UID", "BOM MPN", "Description", "Type"),
            show="headings",
            height=18
        )
        for c in ("Include", "BOM UID", "BOM MPN", "Description", "Type"):
            self.page1_tree.heading(c, text=c)
        self.page1_tree.pack(fill="both", expand=True)

        # Page 2
        self.page2 = ttk.Frame(self.npr_notebook)
        self.npr_notebook.add(self.page2, text="Page 2 — EXISTS + Alternates")

        self.page2_tree = ttk.Treeview(
            self.page2,
            columns=("Include", "Kind", "Parent BOM", "Internal PN", "EXISTS/NEW", "MFG", "MPN", "Supplier", "Cost"),
            show="headings",
            height=18
        )
        for c in ("Include", "Kind", "Parent BOM", "Internal PN", "EXISTS/NEW", "MFG", "MPN", "Supplier", "Cost"):
            self.page2_tree.heading(c, text=c)
        self.page2_tree.pack(fill="both", expand=True)

        # Page 3
        self.page3 = ttk.Frame(self.npr_notebook)
        self.npr_notebook.add(self.page3, text="Page 3 — Final Excel Preview (Read-Only)")

        self.page3_tree = ttk.Treeview(self.page3, show="headings", height=18)
        self.page3_tree.pack(fill="both", expand=True)


    def build_npr_workspace(self):
        """
        Build NPR Workspace from existing NPRPart + MatchResult pairs.

        Page 1:
          - NEW parts (NO_MATCH)

        Page 2:
          - EXISTS inventory context rows
        """

        if not self.match_results:
            messagebox.showwarning("NPR", "Run parsing and matching first.")
            return

        ws = NPRWorkspace()

        # --------------------------------------------
        # Page 1 — NEW / unmatched BOM parts
        # --------------------------------------------
        for npr_part, match in self.match_results:

            inventory_part = match.inventory_part if match else None

            if inventory_part is None:
                ws.primary_new_items.append(
                    NPRPrimaryNewItem(
                        bom_uid=npr_part.partnum,
                        bom_mpn=npr_part.mfgpn,
                        description=npr_part.description,
                        component_type=npr_part.part_type,
                        populated=bool(npr_part.parsed),
                    )
                )

        # --------------------------------------------
        # Page 2 — EXISTS inventory context rows
        # --------------------------------------------
        for npr_part, match in self.match_results:

            inventory_part = match.inventory_part if match else None
            if inventory_part is None:
                continue

            row_id = f"CTX-{npr_part.partnum}-{inventory_part.itemnum}"

            ws.secondary_rows.append(
                NPRSecondaryRow(
                    row_id=row_id,
                    kind=NPRRowKind.CONTEXT_EXISTS,
                    parent_bom_uid=npr_part.partnum,
                    parent_bom_mpn=npr_part.mfgpn,
                    parent_description=npr_part.description,
                    internal_part_number=inventory_part.itemnum,
                    exists_in_inventory=True,
                    include_in_export=True,
                    source="inventory",
                )
            )

        self.npr_workspace = ws
        self.refresh_npr_pages()
        self.npr_notebook.select(self.page1)

        # --------------------------------------------------
        # Build Page 2: EXISTS inventory context rows
        # --------------------------------------------------
        for npr_part, match in zip(self.npr_list, self.match_results):

            inventory_part = getattr(match, "inventory_part", None) if match else None
            if inventory_part is None:
                continue

            row_id = f"CTX-{getattr(npr_part,'itemnum','')}-{getattr(inventory_part,'itemnum','')}"

            ws.secondary_rows.append(
                NPRSecondaryRow(
                    row_id=row_id,
                    kind=NPRRowKind.CONTEXT_EXISTS,
                    parent_bom_uid=str(getattr(npr_part, "itemnum", "")),
                    parent_bom_mpn=str(getattr(npr_part, "manufacturer_part", "")),
                    parent_description=str(getattr(npr_part, "description", "")),
                    internal_part_number=str(getattr(inventory_part, "itemnum", "")),
                    exists_in_inventory=True,
                    include_in_export=True,
                    source="inventory",
                )
            )

        self.npr_workspace = ws
        self.refresh_npr_pages()
        self.npr_notebook.select(self.page1)


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
                item.component_type
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
                "" if row.unit_cost is None else row.unit_cost
            ))

        # Page 3 (preview)
        try:
            headers = get_template_headers(self.npr_template_path)
        except Exception as e:
            messagebox.showerror("Template Error", str(e))
            return

        preview_rows = ws.build_excel_preview_rows(headers)

        # configure columns dynamically
        self.page3_tree["columns"] = headers
        for h in headers:
            self.page3_tree.heading(h, text=h)

        for i in self.page3_tree.get_children():
            self.page3_tree.delete(i)

        for r in preview_rows:
            self.page3_tree.insert("", "end", values=[r.get(h, "") for h in headers])



    def add_manual_alternate(self):
        """
        Placeholder for manual alternate entry.
        Will be expanded to open a dialog / form.
        """
        messagebox.showinfo(
            "Add Alternate",
            "Manual alternate entry is not implemented yet.\n"
            "This will allow engineers to add alternates manually."
        )


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

        ### TODO: should load the config not here but globally for testing purposes keep
        cfg = load_config("./config/components.yaml")

        # Explicit parsing — no magic
        for inv in self.inventory:
            inv.parsed = parse_description(inv.description, cfg)

        for npr in self.npr_list:
            npr.parsed = parse_description(npr.description, cfg)

        engine = MatchingEngine(self.inventory, config=cfg)
        self.match_results = engine.match_npr_list(self.npr_list)

        self.status_var.set(f"Matched {len(self.match_results)} parts.")
        self.render_table()

    # =====================================================
    # RENDER TABLE
    # =====================================================
    def render_table(self):
        # Full reset to avoid stale expansion rows
        for iid in self.tree.get_children():
            self.tree.delete(iid)

        self.row_index_map.clear()
        self.detail_rows.clear()
        self.expanded_state.clear()
        self.raw_expanded.clear()

        conf_threshold = self.confidence_threshold.get()

        for idx, (npr, match) in enumerate(self.match_results):
            if match.confidence < conf_threshold:
                continue

            inv = match.inventory_part
            tag = self._tag_for_match(match)

            iid = f"row_{idx}"
            self.tree.insert(
                "",
                "end",
                iid=iid,
                values=(
                    inv.itemnum if inv else "",
                    inv.vendoritem if inv else "",
                    npr.mfgpn or "",
                    npr.description,
                    match.match_type.value,
                    f"{match.confidence:.2f}",
                ),
                tags=(tag,),
            )

            # Deepcopy so inline expand doesn't mutate original data
            self.row_index_map[iid] = (copy.deepcopy(npr), copy.deepcopy(match))
            self.raw_expanded[iid] = False

    # =====================================================
    # SEARCH / FILTER BAR
    # =====================================================
    def filter_table(self):
        """
        Filters displayed rows in the treeview based on search text.

        FIXED:
        - previously inserted the wrong columns/ordering (mismatched self.columns)
        """
        query = self.filter_text.get().lower().strip()

        if not query:
            self.render_table()
            return

        filtered_results = []
        for npr, match in self.match_results:
            inv = match.inventory_part
            row_text = " ".join(
                [
                    str(inv.itemnum if inv else ""),
                    str(inv.vendoritem if inv else ""),
                    str(npr.mfgpn or ""),
                    str(npr.description),
                    str(match.match_type.value),
                    f"{match.confidence:.2f}",
                ]
            ).lower()

            if query in row_text:
                filtered_results.append((npr, match))

        self.tree.delete(*self.tree.get_children())
        self.row_index_map.clear()

        for idx, (npr, match) in enumerate(filtered_results):
            inv = match.inventory_part
            tag = self._tag_for_match(match)
            iid = f"filter_{idx}"

            self.tree.insert(
                "",
                "end",
                iid=iid,
                values=(
                    inv.itemnum if inv else "",
                    inv.vendoritem if inv else "",
                    npr.mfgpn or "",
                    npr.description,
                    match.match_type.value,
                    f"{match.confidence:.2f}",
                ),
                tags=(tag,),
            )
            self.row_index_map[iid] = (copy.deepcopy(npr), copy.deepcopy(match))

    def _tag_for_match(self, match):
        mtype = match.match_type
        if mtype == MatchType.EXACT_MFG_PN:
            return "exact"
        if mtype == MatchType.EXACT_ITEMNUM:
            return "itemnum_exact"
        if mtype == MatchType.PREFIX_FAMILY:
            return "prefix"
        if mtype == MatchType.SUBSTITUTE:
            return "substitute"
        if mtype == MatchType.PARSED_MATCH:
            return "parsed"
        if mtype == MatchType.API_ASSISTED:
            return "api"
        return "missing"

    # =====================================================
    # EXPANDABLE DETAILS
    # =====================================================
    def _on_double_click(self, event):
        row = self.tree.identify_row(event.y)
        if not row:
            return

        tags = self.tree.item(row, "tags")
        if "raw_toggle" in tags:
            parent_id = None
            for pid, rows in self.detail_rows.items():
                if row in rows:
                    parent_id = pid
                    break
            if parent_id:
                self.raw_expanded[parent_id] = not self.raw_expanded.get(parent_id, False)
                self._expand_item(parent_id)
            return

        if row in self.row_index_map:
            if self.expanded_state.get(row, False):
                self._collapse_item(row)
            else:
                self._expand_item(row)

    def _collapse_item(self, iid):
        if iid in self.detail_rows:
            for rid in self.detail_rows[iid]:
                try:
                    self.tree.delete(rid)
                except Exception:
                    pass
        self.detail_rows[iid] = []
        self.expanded_state[iid] = False

    def _expand_item(self, iid):
        if iid not in self.row_index_map:
            return

        self._collapse_item(iid)
        self.detail_rows[iid] = []
        self.expanded_state[iid] = True

        npr, match = self.row_index_map[iid]
        inv = match.inventory_part
        insert_at = self.tree.index(iid) + 1

        def add_row(text, values, tag):
            nonlocal insert_at
            rid = self.tree.insert("", insert_at, values=("    " + text, *values), tags=(tag,))
            insert_at += 1
            self.detail_rows[iid].append(rid)
            return rid

        add_row("Match Details", ["", "", "", "", ""], "detail_section_header")

        summary_text = f"{match.match_type.value} (Confidence: {match.confidence:.2f})"
        add_row(
            "Summary",
            [
                npr.partnum or "",
                inv.itemnum if inv else "",
                summary_text,
                match.notes or "",
                "",
            ],
            "detail_summary",
        )

        # Add explainability if present (matching_engine now provides it)
        if getattr(match, "explain", None):
            expl = match.explain
            add_row("Explain", ["", "", "", "", ""], "detail_header")
            add_row("Tier", [str(expl.get("tier", "")), "", "", "", ""], "detail_row")

        add_row("Comparison", ["Requested Part for Lookup", "Inventory", "", "", ""], "detail_header")
        add_row("Description", [npr.description or "", inv.description if inv else "", "", "", ""], "detail_row")
        add_row("Manufacturer PN", [npr.mfgpn or "", inv.vendoritem if inv else "", "", "", ""], "detail_row")

        if getattr(npr, "mfgname", "") or getattr(inv, "mfgname", ""):
            add_row("Manufacturer", [npr.mfgname or "", inv.mfgname if inv else "", "", "", ""], "detail_row")

        parsed_npr = npr.parsed or {}
        parsed_inv = (inv.parsed if inv else {}) or {}
        skip = {"description", "mfgpn", "manufacturer", "mfg"}

        if parsed_npr or parsed_inv:
            add_row("Parsed Details", ["", "", "", "", ""], "detail_header")
            for key in sorted(set(parsed_npr) | set(parsed_inv)):
                if key.lower() in skip:
                    continue
                pv = str(parsed_npr.get(key, ""))
                iv = str(parsed_inv.get(key, ""))
                if pv or iv:
                    add_row(key.capitalize(), [pv, iv, "", "", ""], "detail_row")

        raw_open = self.raw_expanded.get(iid, False)
        toggle_label = "Hide Raw Fields" if raw_open else "Show Raw Fields"
        add_row(toggle_label, ["", "", "(double-click to toggle raw data)", "", ""], "raw_toggle")

        if raw_open:
            npr_raw = npr.raw_fields or {}
            inv_raw = inv.raw_fields if inv else {}
            add_row("Raw Field Comparison", ["New Part (Excel)", "Inventory (Excel)", "", "", ""], "detail_header")

            for key in sorted(set(npr_raw) | set(inv_raw)):
                pv = str(npr_raw.get(key, ""))
                iv = str(inv_raw.get(key, ""))
                if pv or iv:
                    add_row(key, [pv, iv, "", "", ""], "detail_row")

    # =====================================================
    # TOOLTIP
    # =====================================================
    def _create_tooltip(self, text, x, y):
        self._hide_tooltip()
        self.tooltip = tk.Toplevel(self.root)
        self.tooltip.overrideredirect(True)
        self.tooltip.attributes("-topmost", True)
        self.tooltip.attributes("-alpha", 0.97)
        frame = tk.Frame(
            self.tooltip,
            bg="#fefefe",
            padx=6,
            pady=4,
            highlightthickness=1,
            highlightbackground="#d0d0d0",
        )
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
    # COPY / EXPORT
    # =====================================================
    def on_tree_click(self, event):
        self.last_clicked_item = self.tree.identify_row(event.y)
        self.last_clicked_col = self.tree.identify_column(event.x)

    def copy_selected_cell(self, _=None):
        iid = self.last_clicked_item
        col = self.last_clicked_col
        if not iid:
            return
        item = self.tree.item(iid)
        vals = item.get("values", [])
        text = ""
        if col == "#0":
            text = item.get("text", "")
        else:
            try:
                idx = int(col[1:]) - 1
                text = vals[idx]
            except Exception:
                pass
        text = str(text or "\t".join(str(v) for v in vals))
        self.root.clipboard_clear()
        self.root.clipboard_append(text)

    def export_npr(self):
        if not self.npr_workspace:
            messagebox.showwarning("Export", "Build NPR Workspace first.")
            return

        try:
            headers = get_template_headers(self.npr_template_path)
            from .npr_export import build_excel_rows_from_workspace

            rows = build_excel_rows_from_workspace(self.npr_workspace, headers)


            output_path = Path(__file__).parent / "output.xlsx"

            export_npr_from_rows(
                template_path=self.npr_template_path,
                output_path=output_path,
                metadata=None,   # metadata writing can be skipped entirely
                excel_rows=rows,
            )

            messagebox.showinfo(
                "Export Complete",
                f"NPR exported successfully:\n{output_path}"
            )

        except Exception as e:
            messagebox.showerror("Export Error", str(e))

    # =====================================================
    # SETTINGS WINDOW
    # =====================================================
    def open_settings(self):
        if hasattr(self, "_settings_window") and self._settings_window.winfo_exists():
            self._settings_window.lift()
            return

        win = tk.Toplevel(self.root)
        self._settings_window = win
        win.title("Settings")
        win.geometry("320x180")
        win.resizable(False, False)

        tk.Label(win, text="Confidence Threshold", font=("Segoe UI", 10, "bold")).pack(pady=10)
        tk.Scale(
            win,
            variable=self.confidence_threshold,
            from_=0,
            to=1,
            resolution=0.05,
            orient="horizontal",
            length=200,
        ).pack()

        def apply_and_close():
            self.render_table()
            win.destroy()

        tk.Button(win, text="Apply & Close", command=apply_and_close).pack(pady=10)


if __name__ == "__main__":
    root = tk.Tk()
    app = NPRToolUI(root)
    root.mainloop()
