# same 3 panelization and split screen idea: upper scrollable panel, middle header panel, bottom split panel (left shows all matches to rect or accept) (right shows all infromaiton reguarding that part number hovered over on the left)
# big change: replace tkinter with customtkinter gui control.
# need to specify the inputs and utputs of the GUI
# the GUI is populated from the controller. the controller is a sperate python file
# It will be prepopulated with nothing from the database. Upon loading of the ERP inventory, Master invetory, BOM, then clicking RUN MATCHING the program will start and all elements will be populated
# The UI will be populated from the data base based on selctions from a drop down whihc allows "workspace" selcetions. all informaiton fromt he database will be populated (related to worksapce)
# The UI will have a button to lcick to export the BOM and NPR files
# becuase the UI is dynamic and is treate  as a worksapce, it WILL be adding and removing things from the database (calling functions from the controller to do that)

# This should be failry intuitive and easy to rework, we are simply building a GUI and all of its buttons simply call the a controller function.


# The controller is going to be difficult to handle now as it has many jobs. but the base line is: it will handle all buttons and funcitlities of the UI in order to populate the UI
# The contoller will grab information from the Database (querying) AND change values inside of the data base.
# The controller Will at one point call the matching engine. The matching engine is needed for core fucnitnilty (the biggest concern of the program)
# the infromation fromt he matching engine will be PASSED to the controller. the controller will then make changes to the database correspondingly.
# The controller is responsible for building the NPR workspace.
# the controller at no point operates under the scope of the dataloader, the parsing engine, the matching engine, or creation of UI elements themselves. right now, it does all of that.

# this should also be failry inituitive now that we know what the scope of the UI is. It's a large file, that acts kinda like an api for the ui (indicvidual files get called on by the ui)


# the matching engine needs to be rechecked in terms of how it interactes withthe database. right now it SHOULD BE a box (get static input, give processed output)
# anything that changing in the matching enigine should not effect the inputs and output. The inputs to the matching engine should be provided by the controller and ONLY the controller. 



# ui/ctk_workspace_ui.py
from __future__ import annotations

import threading
import traceback
from typing import Optional

import customtkinter as ctk
import tkinter as tk
from tkinter import ttk, filedialog, messagebox

from .decision_controller import DecisionController
from .data_models import DecisionNode, DecisionStatus, Alternate


class DecisionWorkspaceCTK:
    """
    Modern CTk UI shell that preserves the legacy behavior:
      - controller/DB is truth
      - UI holds only current_node_id
      - renders always pull fresh state from controller
    """

    def __init__(self, root: ctk.CTk, controller: DecisionController):
        ctk.set_appearance_mode("System")
        ctk.set_default_color_theme("blue")

        self.root = root
        self.controller = controller

        self.root.title("NPR Tool")
        self.root.geometry("1500x950")
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)

        self._stop_event = threading.Event()
        self.controller.stop_event = self._stop_event

        self.current_node_id: Optional[str] = None
        self._pinned_alt_id: Optional[str] = None
        self._last_specs_key = None

        # ---- theme/colors (keep your palette; tweak later) ----
        self.COLORS = {
            "bg_main": "#0B1220",     # darker modern background
            "card_bg": "#0F172A",
            "card_border": "#243046",
            "text": "#E5E7EB",
            "text_dim": "#9CA3AF",
            "primary": "#3B82F6",
            "success": "#22C55E",
            "warning": "#F59E0B",
            "danger": "#EF4444",
        }

        # Root background
        self.root.configure(fg_color=self.COLORS["bg_main"])

        self._init_ttk_styles()
        self._build_toolbar()
        self._build_layout()

        # initial draw
        self.refresh_node_table()
        self._render_empty_state()

    # ---------------------------------------------------------------------
    # ttk styles (Treeview + Panedwindow)
    # ---------------------------------------------------------------------
    def _init_ttk_styles(self):
        self.ttk_style = ttk.Style(self.root)
        try:
            self.ttk_style.theme_use("clam")
        except Exception:
            pass

        self.ttk_style.configure(
            "Treeview",
            background="#0B1220",
            fieldbackground="#0B1220",
            foreground="#E5E7EB",
            rowheight=28,
            borderwidth=0,
            font=("Segoe UI", 9),
        )
        self.ttk_style.configure(
            "Treeview.Heading",
            background="#111827",
            foreground="white",
            relief="flat",
            font=("Segoe UI", 9, "bold"),
        )
        self.ttk_style.map(
            "Treeview.Heading",
            background=[("active", self.COLORS["primary"])],
        )

    # ---------------------------------------------------------------------
    # Toolbar
    # ---------------------------------------------------------------------
    def _build_toolbar(self):
        bar = ctk.CTkFrame(self.root, corner_radius=0, fg_color="#0B1220")
        bar.pack(fill="x", side="top", padx=10, pady=(10, 6))

        title = ctk.CTkLabel(
            bar,
            text="NPR Tool Workspace",
            font=ctk.CTkFont(size=18, weight="bold"),
        )
        title.pack(side="left", padx=(10, 16))

        def btn(txt, cmd, primary=False):
            return ctk.CTkButton(
                bar,
                text=txt,
                command=cmd,
                fg_color=(self.COLORS["primary"] if primary else "#111827"),
                hover_color=(self.COLORS["primary"] if not primary else "#2563EB"),
                text_color="white",
                height=32,
                corner_radius=10,
            )

        btn("Load Master Inventory", self._load_inventory).pack(side="left", padx=6)
        btn("Load ERP Inventory", self._load_items_sheet).pack(side="left", padx=6)
        btn("Load BOM", self._load_bom).pack(side="left", padx=6)
        btn("Open Workspace", self._open_workspace).pack(side="left", padx=6)
        btn("Save Workspace", self._save_workspace).pack(side="left", padx=6)
        btn("Run Matching", self._run_matching, primary=True).pack(side="left", padx=6)
        btn("Export NPR", self._export_npr).pack(side="left", padx=6)
        btn("Re-run Matching", self._rematch_workspace).pack(side="left", padx=6)

        self.status_var = tk.StringVar(value="Ready.")
        status = ctk.CTkLabel(bar, textvariable=self.status_var, text_color=self.COLORS["text_dim"])
        status.pack(side="right", padx=10)

    # ---------------------------------------------------------------------
    # Layout (ttk Panedwindows inside CTk frames)
    # ---------------------------------------------------------------------
    def _build_layout(self):
        wrapper = ctk.CTkFrame(self.root, corner_radius=12, fg_color="#0B1220")
        wrapper.pack(fill="both", expand=True, padx=10, pady=(0, 10))

        # vertical split: table / header / bottom
        self.vpane = ttk.Panedwindow(wrapper, orient="vertical")
        self.vpane.pack(fill="both", expand=True)

        # ---- table pane ----
        table_host = ctk.CTkFrame(wrapper, corner_radius=12, fg_color="#0F172A")
        self.vpane.add(table_host, weight=3)
        self._build_node_table(table_host)

        # ---- header pane ----
        self.header_host = ctk.CTkFrame(wrapper, corner_radius=12, fg_color="#111827")
        self.vpane.add(self.header_host, weight=2)
        self._build_header(self.header_host)

        # ---- bottom pane (cards/specs) ----
        bottom_host = ctk.CTkFrame(wrapper, corner_radius=12, fg_color="#0B1220")
        self.vpane.add(bottom_host, weight=6)
        self._build_bottom_panes(bottom_host)

    def _build_node_table(self, parent: ctk.CTkFrame):
        parent.grid_columnconfigure(0, weight=1)
        parent.grid_rowconfigure(0, weight=1)

        container = tk.Frame(parent, bg="#0F172A")
        container.grid(row=0, column=0, sticky="nsew", padx=10, pady=10)

        cols = ("ID", "Type", "MPN", "Status", "Confidence")
        self.node_tree = ttk.Treeview(container, columns=cols, show="headings")
        for c in cols:
            self.node_tree.heading(c, text=c)
            self.node_tree.column(c, width=180, anchor="center")

        vs = ttk.Scrollbar(container, orient="vertical", command=self.node_tree.yview)
        self.node_tree.configure(yscrollcommand=vs.set)

        self.node_tree.pack(side="left", fill="both", expand=True)
        vs.pack(side="right", fill="y")

        self.node_tree.bind("<<TreeviewSelect>>", self._on_node_select)

    def _build_header(self, parent: ctk.CTkFrame):
        parent.grid_columnconfigure(0, weight=1)

        self.h_title = ctk.CTkLabel(parent, text="Select a part", font=ctk.CTkFont(size=22, weight="bold"))
        self.h_title.pack(pady=(14, 4))

        self.h_desc = ctk.CTkLabel(parent, text="", text_color="#CBD5E1")
        self.h_desc.pack(pady=(0, 6))

        self.h_meta = ctk.CTkLabel(parent, text="", text_color=self.COLORS["primary"], font=ctk.CTkFont(size=14, weight="bold"))
        self.h_meta.pack(pady=(0, 10))

        # PN row (we’ll port your editor behavior next)
        pn_row = ctk.CTkFrame(parent, fg_color="transparent")
        pn_row.pack(pady=(0, 10))

        self.suggested_var = tk.StringVar(value="")
        self.company_pn_var = tk.StringVar(value="")

        ctk.CTkLabel(pn_row, text="Suggested CNS:", text_color="#CBD5E1").pack(side="left", padx=(0, 8))
        self.suggested_entry = ctk.CTkEntry(pn_row, width=160, textvariable=self.suggested_var)
        self.suggested_entry.configure(state="readonly")
        self.suggested_entry.pack(side="left", padx=(0, 16))

        ctk.CTkLabel(pn_row, text="Company PN:", text_color="#CBD5E1").pack(side="left", padx=(0, 8))
        self.company_pn_entry = ctk.CTkEntry(pn_row, width=260, textvariable=self.company_pn_var)
        self.company_pn_entry.pack(side="left", padx=(0, 10))

        self.apply_pn_btn = ctk.CTkButton(pn_row, text="Apply", command=self._apply_company_pn, width=80)
        self.apply_pn_btn.pack(side="left", padx=(0, 10))

        self.mark_ready_btn = ctk.CTkButton(parent, text="Mark Ready", command=self._on_mark_ready, width=140)
        self.mark_ready_btn.pack(pady=(0, 14))

    def _build_bottom_panes(self, parent: ctk.CTkFrame):
        parent.grid_rowconfigure(0, weight=1)
        parent.grid_columnconfigure(0, weight=1)

        hpane = ttk.Panedwindow(parent, orient="horizontal")
        hpane.grid(row=0, column=0, sticky="nsew", padx=6, pady=6)

        # left cards
        self.cards_host = ctk.CTkFrame(parent, corner_radius=12, fg_color="#0F172A")
        hpane.add(self.cards_host, weight=3)

        self.cards_scroll = ctk.CTkScrollableFrame(self.cards_host, fg_color="#0F172A")
        self.cards_scroll.pack(fill="both", expand=True, padx=10, pady=10)

        # right specs
        self.specs_host = ctk.CTkFrame(parent, corner_radius=12, fg_color="#0F172A")
        hpane.add(self.specs_host, weight=2)

        self.specs_scroll = ctk.CTkScrollableFrame(self.specs_host, fg_color="#0F172A")
        self.specs_scroll.pack(fill="both", expand=True, padx=10, pady=10)

        self.specs_title = ctk.CTkLabel(self.specs_scroll, text="Information", font=ctk.CTkFont(size=14, weight="bold"))
        self.specs_title.pack(anchor="w", pady=(0, 8))

    # ---------------------------------------------------------------------
    # Rendering
    # ---------------------------------------------------------------------
    def _render_empty_state(self):
        self.h_title.configure(text="No Selection")
        self.h_desc.configure(text="")
        self.h_meta.configure(text="Select a BOM line")

        for w in self.cards_scroll.winfo_children():
            w.destroy()
        for w in self.specs_scroll.winfo_children():
            w.destroy()
        self.specs_title = ctk.CTkLabel(self.specs_scroll, text="Information", font=ctk.CTkFont(size=14, weight="bold"))
        self.specs_title.pack(anchor="w", pady=(0, 8))

    def refresh_node_table(self):
        # clear
        for i in self.node_tree.get_children():
            self.node_tree.delete(i)

        nodes = list(self.controller.get_nodes())
        if not nodes:
            return

        for node in nodes:
            explain = getattr(node, "explain", {}) or {}
            display_mpn = (
                explain.get("winning_mpn")
                or getattr(node, "assigned_part_number", "")
                or getattr(node, "internal_part_number", "")
                or getattr(node, "bom_mpn", "")
            )
            conf_display = f"{(getattr(node, 'confidence', 0.0) or 0.0) * 100:.1f}%"
            self.node_tree.insert(
                "", "end",
                iid=node.id,
                values=(node.id, node.base_type, display_mpn, getattr(node.status, "value", str(node.status)), conf_display),
            )

    # ---------------------------------------------------------------------
    # Event handlers (wire to your existing controller methods)
    # ---------------------------------------------------------------------
    def _on_node_select(self, _e=None):
        sel = self.node_tree.selection()
        if not sel:
            return
        node_id = sel[0]
        try:
            node = self.controller.get_node(node_id)
        except Exception:
            self.current_node_id = None
            self._render_empty_state()
            return

        self.current_node_id = node_id
        self._render_header_state(node)
        self._render_cards(node)
        self._render_specs_for_node(node)

    def _render_header_state(self, node: DecisionNode):
        # Port your exact legacy header logic here next
        self.h_title.configure(text=f"Company PN: {getattr(node, 'internal_part_number', '') or '—'}")
        self.h_desc.configure(text=f"BOM MPN: {getattr(node, 'bom_mpn', '') or '—'}")
        self.h_meta.configure(text=str(getattr(node.status, "value", node.status)))

        self.suggested_var.set((getattr(node, "suggested_pb", "") or "").strip())
        self.company_pn_var.set((getattr(node, "assigned_part_number", "") or getattr(node, "internal_part_number", "") or "").strip())

    def _render_cards(self, node: DecisionNode):
        for w in self.cards_scroll.winfo_children():
            w.destroy()

        self._pinned_alt_id = None
        alts = list(getattr(node, "alternates", []) or [])
        internal_active = [a for a in alts if (not getattr(a, "rejected", False)) and getattr(a, "source", "") == "inventory"]
        external_active = [a for a in alts if (not getattr(a, "rejected", False)) and getattr(a, "source", "") != "inventory"]
        rejected_all = [a for a in alts if getattr(a, "rejected", False)]

        if not alts:
            ctk.CTkLabel(self.cards_scroll, text="No alternates/candidates yet.", text_color=self.COLORS["text_dim"]).pack(anchor="w", padx=8, pady=8)
            return

        self._build_card_section(self.cards_scroll, f"Internal Matches ({len(internal_active)})", internal_active, node)
        self._build_card_section(self.cards_scroll, f"External Alternates ({len(external_active)})", external_active, node)
        if rejected_all:
            self._build_card_section(self.cards_scroll, f"Rejected ({len(rejected_all)})", rejected_all, node)

    def _render_specs_for_node(self, node: DecisionNode):
        alts = list(getattr(node, "alternates", []) or [])
        active = [a for a in alts if not getattr(a, "rejected", False)]

        pick = None
        for a in active:
            if getattr(a, "selected", False):
                pick = a
                break
        if pick is None:
            for a in active:
                if getattr(a, "source", "") == "inventory":
                    pick = a
                    break
        if pick is None and active:
            pick = active[0]

        self._render_specs_for_alt(node, pick)

    def _build_card_section(self, parent, title: str, alternates: list, node: DecisionNode):
        section = ctk.CTkFrame(parent, corner_radius=10, fg_color="#111827")
        section.pack(fill="x", padx=4, pady=6)

        ctk.CTkLabel(section, text=title, font=ctk.CTkFont(size=13, weight="bold"),
                     text_color=self.COLORS["text"]).pack(anchor="w", padx=10, pady=(8, 4))

        if not alternates:
            ctk.CTkLabel(section, text="None", text_color=self.COLORS["text_dim"]).pack(anchor="w", padx=12, pady=(0, 8))
            return

        grid = ctk.CTkFrame(section, fg_color="transparent")
        grid.pack(fill="x", padx=8, pady=(0, 8))
        cols = 2
        try:
            width = max(600, int(self.cards_host.winfo_width() or 600))
            cols = 1 if width < 900 else 2
        except Exception:
            cols = 2
        for c in range(cols):
            grid.grid_columnconfigure(c, weight=1)

        for i, alt in enumerate(alternates):
            r, c = divmod(i, cols)
            card = self._create_card(node, alt)
            card.grid(row=r, column=c, sticky="ew", padx=6, pady=6, in_=grid)

    def _create_card(self, node: DecisionNode, alt: Alternate):
        is_rejected = bool(getattr(alt, "rejected", False))
        is_selected = bool(getattr(alt, "selected", False))
        is_pinned = (self._pinned_alt_id == getattr(alt, "id", None))

        border = self.COLORS["card_border"]
        if is_selected:
            border = self.COLORS["success"]
        elif is_rejected:
            border = self.COLORS["danger"]
        elif is_pinned:
            border = self.COLORS["primary"]

        outer = ctk.CTkFrame(self.cards_scroll, corner_radius=12, fg_color=border)
        inner = ctk.CTkFrame(outer, corner_radius=10, fg_color=self.COLORS["card_bg"])
        inner.pack(fill="both", expand=True, padx=1, pady=1)

        head = ctk.CTkFrame(inner, fg_color="transparent")
        head.pack(fill="x", padx=10, pady=(8, 4))

        is_inventory = (getattr(alt, "source", "") == "inventory")
        company_pn = (getattr(alt, "internal_part_number", "") or "").strip()
        mfg_pn = (getattr(alt, "manufacturer_part_number", "") or "").strip()
        mfg_name = (getattr(alt, "manufacturer", "") or "").strip()

        title = company_pn if is_inventory else (mfg_pn or company_pn or "(no part number)")
        subtitle = mfg_pn if is_inventory else mfg_name

        left = ctk.CTkFrame(head, fg_color="transparent")
        left.pack(side="left", fill="x", expand=True)
        ctk.CTkLabel(left, text=title or "—", anchor="w", font=ctk.CTkFont(size=13, weight="bold")).pack(anchor="w")
        if subtitle:
            ctk.CTkLabel(left, text=subtitle, anchor="w", text_color=self.COLORS["text_dim"]).pack(anchor="w")

        conf = float(getattr(alt, "confidence", 0.0) or 0.0)
        if (getattr(alt, "source", "") != "inventory") and conf == 0.0 and not is_rejected:
            conf = 1.0
        ctk.CTkLabel(head, text=f"{int(conf*100)}%", width=56, corner_radius=8,
                     fg_color="#1E3A8A", text_color="white").pack(side="right")

        desc = (getattr(alt, "description", "") or "").strip() or "(no description)"
        ctk.CTkLabel(inner, text=desc, wraplength=520, justify="left",
                     text_color=self.COLORS["text_dim"]).pack(anchor="w", padx=10, pady=(0, 6))

        stock = self._display_stock_for_alt(alt)
        ctk.CTkLabel(inner, text=f"Source: {getattr(alt, 'source', '') or '-'}    Stock: {stock}",
                     text_color=self.COLORS["text_dim"]).pack(anchor="w", padx=10, pady=(0, 6))

        row = ctk.CTkFrame(inner, fg_color="transparent")
        row.pack(fill="x", padx=10, pady=(0, 10))

        ctk.CTkButton(row, text=("Unpin" if is_pinned else "Pin"), width=70, height=28,
                      fg_color="#1F2937", hover_color="#374151",
                      command=lambda a=alt: self._pin_card(node, a)).pack(side="left", padx=(0, 6))
        ctk.CTkButton(row, text="Copy", width=70, height=28,
                      fg_color="#1F2937", hover_color="#374151",
                      command=lambda a=alt: self._copy_to_clipboard(self._card_copy_text(a), toast="Copied card details")).pack(side="left")

        if not getattr(node, "locked", False):
            if not is_selected and not is_rejected:
                ctk.CTkButton(row, text="Reject", width=78, height=28,
                              fg_color="#374151", hover_color="#4B5563",
                              command=lambda a=alt: self._reject_alt(node, a)).pack(side="right", padx=(6, 0))
                ctk.CTkButton(row, text="Add", width=70, height=28,
                              fg_color=self.COLORS["success"], hover_color="#16A34A",
                              command=lambda a=alt: self._add_alt(node, a)).pack(side="right")
            elif is_rejected:
                ctk.CTkButton(row, text="Unreject", width=88, height=28,
                              fg_color="#374151", hover_color="#4B5563",
                              command=lambda a=alt: self._unreject_alt(node, a)).pack(side="right")
            elif is_selected:
                ctk.CTkLabel(row, text="SELECTED", text_color=self.COLORS["success"],
                             font=ctk.CTkFont(weight="bold")).pack(side="right")

        def _bind_recursive(widget):
            try:
                widget.bind("<Button-1>", lambda _e, a=alt: self._pin_card(node, a))
            except Exception:
                pass
            for ch in getattr(widget, "winfo_children", lambda: [])():
                _bind_recursive(ch)
        _bind_recursive(inner)
        return outer

    def _pin_card(self, node: DecisionNode, alt: Alternate):
        aid = getattr(alt, "id", None)
        self._pinned_alt_id = None if self._pinned_alt_id == aid else aid
        self._render_cards(node)
        if self._pinned_alt_id:
            self._render_specs_for_alt(node, alt)
        else:
            self._render_specs_for_node(node)

    def _render_specs_for_alt(self, node: DecisionNode, alt: Optional[Alternate]):
        for w in self.specs_scroll.winfo_children():
            w.destroy()
        title_txt = "Information"
        if alt is not None:
            pn = (getattr(alt, "internal_part_number", "") or getattr(alt, "manufacturer_part_number", "") or "").strip()
            if pn:
                title_txt = f"Information • {pn}"
        self.specs_title = ctk.CTkLabel(self.specs_scroll, text=title_txt, font=ctk.CTkFont(size=14, weight="bold"))
        self.specs_title.pack(anchor="w", pady=(0, 8))

        if alt is None:
            ctk.CTkLabel(self.specs_scroll, text="No details available.", text_color=self.COLORS["text_dim"]).pack(anchor="w")
            return

        specs = self._specs_from_inventory(alt.raw) if getattr(alt, "raw", None) is not None else self._specs_from_alternate(alt)

        if getattr(alt, "source", "") == "inventory" and getattr(alt, "internal_part_number", ""):
            cpn = (getattr(alt, "internal_part_number", "") or "").strip().lower()
            lines, seen = [], set()
            for a in (getattr(node, "alternates", []) or []):
                if getattr(a, "source", "") != "inventory":
                    continue
                if (getattr(a, "internal_part_number", "") or "").strip().lower() != cpn:
                    continue
                mpn = (getattr(a, "manufacturer_part_number", "") or "").strip()
                if not mpn or mpn.lower() in seen:
                    continue
                seen.add(mpn.lower())
                desc = (getattr(a, "description", "") or "").strip()
                lines.append(f"{mpn} — {desc}" if desc else mpn)
            if lines:
                specs["AlternatesCount"] = str(len(lines))
                specs["AlternatesList"] = "\n".join(lines)

        self._render_specs(specs)

    def _render_specs(self, specs: dict):
        def add_section(title: str):
            ctk.CTkLabel(self.specs_scroll, text=title.upper(), text_color=self.COLORS["primary"],
                         font=ctk.CTkFont(size=11, weight="bold")).pack(anchor="w", pady=(8, 2))

        def add_kv(label: str, value):
            if value in (None, ""):
                return
            row = ctk.CTkFrame(self.specs_scroll, fg_color="#111827", corner_radius=8)
            row.pack(fill="x", pady=3)
            ctk.CTkLabel(row, text=label, width=140, anchor="w", text_color=self.COLORS["text_dim"]).pack(side="left", padx=(8, 4), pady=6)
            val = str(value)
            box = ctk.CTkTextbox(row, height=(34 if "\n" not in val else min(120, 24 * (val.count("\n") + 2))), fg_color="#0B1220")
            box.pack(side="left", fill="x", expand=True, padx=(0, 8), pady=4)
            box.insert("1.0", val)
            box.configure(state="disabled")
            try:
                box.bind("<Button-1>", lambda _e, t=val, l=label: self._copy_to_clipboard(t, toast=f"Copied {l}"))
            except Exception:
                pass

        if not specs:
            ctk.CTkLabel(self.specs_scroll, text="No details available.", text_color=self.COLORS["text_dim"]).pack(anchor="w")
            return

        for title, keys in [
            ("Identity", ["ItemNumber", "VendorItem", "Description"]),
            ("Manufacturing", ["MfgName", "MfgId", "PrimaryVendorNumber"]),
            ("Logistics", ["TotalQty", "LastCost", "AvgCost", "ItemLeadTime", "DefaultWhse", "TariffCodeHTSUS"]),
        ]:
            add_section(title)
            for k in keys:
                add_kv(k, specs.get(k))

        alts_raw = str(specs.get("AlternatesList", "") or "").strip()
        if alts_raw or specs.get("AlternatesCount"):
            add_section("Alternates")
            add_kv("AlternatesCount", specs.get("AlternatesCount") or "")
            for line in alts_raw.splitlines()[:100]:
                add_kv("MPN", line)

    def _specs_from_inventory(self, inv):
        raw = dict(getattr(inv, "raw_fields", {}) or {})
        def pick(*keys):
            for k in keys:
                v = raw.get(k, "")
                if v is None:
                    continue
                s = str(v).strip()
                if s:
                    return s
            return ""

        specs = {
            "ItemNumber": (getattr(inv, "itemnum", "") or "").strip() or pick("itemnum", "item_number", "itemnumber"),
            "VendorItem": (getattr(inv, "vendoritem", "") or "").strip() or pick("vendoritem", "vendor_item", "mfgpn"),
            "Description": (getattr(inv, "desc", "") or "").strip() or pick("desc", "description"),
            "MfgName": (getattr(inv, "mfgname", "") or "").strip() or pick("mfgname", "manufacturer_name"),
            "MfgId": (getattr(inv, "mfgid", "") or "").strip() or pick("mfgid", "manufacturer_id"),
            "PrimaryVendorNumber": pick("primaryvendornumber", "supplier", "vendor"),
            "TotalQty": pick("totalqty", "total_qty", "qty_on_hand", "on_hand", "quantity"),
            "LastCost": pick("lastcost", "last_cost"),
            "AvgCost": pick("avgcost", "avg_cost", "average_cost"),
            "ItemLeadTime": pick("itemleadtime", "item_lead_time", "lead_time"),
            "DefaultWhse": pick("defaultwhse", "default_whse", "warehouse"),
            "TariffCodeHTSUS": pick("tariffcodehtsus", "htsus", "tariff_code"),
        }

        ui_mpns = list(getattr(inv, "_ui_group_mpns", []) or [])
        base_vendoritem = (getattr(inv, "vendoritem", "") or "").strip()
        subs = getattr(inv, "substitutes", None) or []
        lines, seen = [], set()

        def push_line(mpn, desc=""):
            mpn = (mpn or "").strip()
            if not mpn or mpn.lower() in seen:
                return
            seen.add(mpn.lower())
            d = (desc or "").strip()
            lines.append(f"{mpn} — {d}" if d else mpn)

        push_line(base_vendoritem, getattr(inv, "desc", "") or "")
        for mpn in ui_mpns:
            push_line(mpn)
        for s in subs:
            push_line(getattr(s, "mfgpn", ""), getattr(s, "description", ""))

        if lines:
            specs["AlternatesCount"] = str(len(lines))
            specs["AlternatesList"] = "\n".join(lines)
        return specs

    def _specs_from_alternate(self, alt):
        specs = {}
        if getattr(alt, "internal_part_number", ""):
            specs["ItemNumber"] = alt.internal_part_number
        if getattr(alt, "manufacturer_part_number", ""):
            specs["VendorItem"] = alt.manufacturer_part_number
        if getattr(alt, "manufacturer", ""):
            specs["MfgName"] = alt.manufacturer
        if getattr(alt, "description", ""):
            specs["Description"] = alt.description
        if getattr(alt, "stock", None) not in (None, ""):
            specs["TotalQty"] = alt.stock
        if getattr(alt, "unit_cost", None) not in (None, ""):
            specs["AvgCost"] = alt.unit_cost
        if getattr(alt, "supplier", ""):
            specs["PrimaryVendorNumber"] = alt.supplier
        return specs

    def _display_stock_for_alt(self, alt: Alternate):
        try:
            if getattr(alt, "raw", None) is not None:
                return self._specs_from_inventory(alt.raw).get("TotalQty") or "-"
            v = getattr(alt, "stock", None)
            return "-" if v in (None, "") else str(v)
        except Exception:
            return "-"

    def _card_copy_text(self, alt: Alternate) -> str:
        parts = []
        if getattr(alt, "internal_part_number", ""):
            parts.append(f"Company PN: {alt.internal_part_number}")
        if getattr(alt, "manufacturer_part_number", ""):
            parts.append(f"MFG PN: {alt.manufacturer_part_number}")
        if getattr(alt, "manufacturer", ""):
            parts.append(f"MFG: {alt.manufacturer}")
        parts.append(f"Source: {getattr(alt, 'source', '') or '-'}")
        parts.append(f"Stock: {self._display_stock_for_alt(alt)}")
        if getattr(alt, "description", ""):
            parts.append(f"Description: {alt.description}")
        return "\n".join(parts)

    def _add_alt(self, node: DecisionNode, alt: Alternate):
        try:
            self.controller.select_alternate(node.id, alt.id)
            fresh = self.controller.get_node(node.id)
            self._render_header_state(fresh)
            self._render_cards(fresh)
            picked = next((a for a in (fresh.alternates or []) if a.id == alt.id), None)
            self._render_specs_for_alt(fresh, picked)
            self.refresh_node_table()
        except Exception as e:
            messagebox.showerror("Select Alternate Failed", str(e))

    def _reject_alt(self, node: DecisionNode, alt: Alternate):
        try:
            self.controller.reject_alternate(node.id, alt.id)
            fresh = self.controller.get_node(node.id)
            self._render_header_state(fresh)
            self._render_cards(fresh)
            self._render_specs_for_node(fresh)
            self.refresh_node_table()
        except Exception as e:
            messagebox.showerror("Reject Alternate Failed", str(e))

    def _unreject_alt(self, node: DecisionNode, alt: Alternate):
        try:
            self.controller.unreject_alternate(node.id, alt.id)
            fresh = self.controller.get_node(node.id)
            self._render_header_state(fresh)
            self._render_cards(fresh)
            self._render_specs_for_node(fresh)
            self.refresh_node_table()
        except Exception as e:
            messagebox.showerror("Unreject Alternate Failed", str(e))
    # ---------------------------------------------------------------------
    # Controller-backed actions (same names as legacy UI)
    # ---------------------------------------------------------------------
    def _load_inventory(self):
        path = filedialog.askopenfilename(filetypes=[("Excel Files", "*.xlsx")])
        if not path:
            return
        try:
            n = self.controller.load_inventory(path)
            self.status_var.set(f"Loaded master inventory: {n} unique company parts.")
            self.current_node_id = None
            self.refresh_node_table()
            self._render_empty_state()
        except Exception as e:
            messagebox.showerror("Load Master Inventory Failed", str(e))

    def _load_items_sheet(self):
        path = filedialog.askopenfilename(filetypes=[("Excel Files", "*.xlsx")])
        if not path:
            return
        try:
            n = self.controller.load_items_inventory(path)
            self.status_var.set(f"Loaded ERP inventory: merged into {n} company parts.")
            self.current_node_id = None
            self.refresh_node_table()
            self._render_empty_state()
        except Exception as e:
            messagebox.showerror("Load ERP Inventory Failed", str(e))

    def _load_bom(self):
        path = filedialog.askopenfilename(filetypes=[("Excel Files", "*.xlsx")])
        if not path:
            return
        try:
            n = self.controller.load_npr(path)
            self.status_var.set(f"Loaded BOM/NPR list: {n} parts.")
            self.current_node_id = None
            self.refresh_node_table()
            self._render_empty_state()
        except Exception as e:
            messagebox.showerror("Load BOM Failed", str(e))

    def _run_matching(self):
        def worker():
            try:
                n = self.controller.run_matching()
                self.root.after(0, lambda: self._on_match_done(n))
            except Exception as e:
                self.root.after(0, lambda: messagebox.showerror("Run Matching Failed", str(e)))

        threading.Thread(target=worker, daemon=True).start()

    def _on_match_done(self, n: int):
        self.refresh_node_table()
        self.current_node_id = None
        self._render_empty_state()
        self.status_var.set(f"Matching complete. Built {n} decision nodes.")

    def _open_workspace(self):
        # We’ll port your legacy modal into a CTkToplevel next
        messagebox.showinfo("TODO", "Open Workspace modal will be ported next.")

    def _save_workspace(self):
        try:
            self.controller.flush_workspace_to_db()
            self.status_var.set("Workspace saved.")
        except Exception as e:
            messagebox.showerror("Save Failed", str(e))

    def _rematch_workspace(self):
        def worker():
            try:
                n = self.controller.rematch_workspace_preserve_decisions()
                self.root.after(0, lambda: self._on_match_done(n))
            except Exception as e:
                self.root.after(0, lambda: messagebox.showerror("Rematch Failed", str(e)))

        threading.Thread(target=worker, daemon=True).start()

    def _export_npr(self):
        path = filedialog.asksaveasfilename(defaultextension=".xlsx", filetypes=[("Excel Files", "*.xlsx")])
        if not path:
            return
        try:
            self.controller.flush_workspace_to_db()
            self.controller.export_npr(path)
            messagebox.showinfo("Export Successful", f"NPR file saved to:\n{path}")
        except PermissionError as e:
            messagebox.showwarning("File In Use", str(e))
        except Exception as e:
            messagebox.showerror("Export Failed", str(e))

    def _apply_company_pn(self):
        if not self.current_node_id:
            return
        pn = (self.company_pn_var.get() or "").strip()
        if not pn:
            messagebox.showwarning("Missing PN", "Enter a Company Part Number first.")
            return
        try:
            self.controller.set_assigned_part_number(self.current_node_id, pn)
            node = self.controller.get_node(self.current_node_id)
            self._render_header_state(node)
            self.refresh_node_table()
        except Exception as e:
            messagebox.showerror("Apply PN Failed", str(e))

    def _on_mark_ready(self):
        if not self.current_node_id:
            messagebox.showwarning("No Selection", "Select a node first.")
            return
        try:
            self.controller.mark_ready(self.current_node_id)
            node = self.controller.get_node(self.current_node_id)
            self._render_header_state(node)
            self.refresh_node_table()
        except Exception as e:
            messagebox.showerror("Mark Ready Failed", str(e))

    def _on_close(self):
        try:
            self._stop_event.set()
        except Exception:
            pass
        try:
            self.controller.flush_workspace_to_db()
        except Exception:
            pass
        try:
            self.root.destroy()
        except Exception:
            pass

    def run(self):
        self.root.mainloop()
