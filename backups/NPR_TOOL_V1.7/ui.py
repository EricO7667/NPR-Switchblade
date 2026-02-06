# ui/testui.py
import tkinter as tk
from tkinter import ttk, filedialog, messagebox

from typing import Optional
from .data_models import DecisionNode, DecisionStatus
from .data_models import Alternate
from .decision_controller import DecisionController


class DecisionWorkspaceUI:
    def __init__(self, root: tk.Tk, controller: DecisionController):
        self.root = root
        self.controller = controller

        self.root.title("NPR Tool")
        self.root.geometry("1500x950")

        self.current_node_id: Optional[str] = None

        self.COLORS = {
            "bg_main": "#F9FAFB",           # Slightly lighter, fresher background
            "header_bg": "#111827",         # Even darker slate
            "header_text": "#F9FAFB",
            "card_bg": "#FFFFFF",
            "card_border": "#D1D5DB",
            "primary": "#2563EB",           # Stronger blue
            "success": "#16A34A",           # More vivid green
            "warning": "#F59E0B",
            "text_dark": "#111827",
            "text_dim": "#6B7280",
            "highlight": "#EFF6FF",
            "danger": "#DC2626",            # New - for rejected
            "hover_card_bg": "#F0F4FF",     # Better hover contrast
        }


        # SET GLOBAL BACKGROUND
        self.root.configure(bg=self.COLORS["bg_main"])

        self._build_layout()
        self.refresh_node_table()

        self._init_row_styles()
        self.last_hovered_card = None
        # Add this inside __init__ or as class constants



        self.SPEC_FIELDS = [
            "ItemNumber",
            "Description",
            "PrimaryVendorNumber",
            "VendorItem",
            "MfgId",
            "MfgName",
            "MfgItemCount",
            "LastCost",
            "StdCost",
            "AvgCost",
            "ItemLeadTime",
            "DefaultWhse",
            "TotalQty",
            "TariffCodeHTSUS",
        ]


    # =====================================================
    # Layout
    # =====================================================

    def _build_layout(self):
        self._build_toolbar()
        self._build_node_table()
        self._build_header()
        self._build_main_panes()

    def _init_row_styles(self):
            style = ttk.Style(self.root)

            # IMPORTANT: Switch to 'clam' to allow custom colors on headers
            style.theme_use("clam") 

            # 1. Configure the Treeview (The Table)
            style.configure("Treeview", 
                            background="white", 
                            fieldbackground="white", 
                            foreground=self.COLORS["text_dark"], 
                            rowheight=30,
                            font=("Segoe UI", 9))

            # 2. Configure the Headings (The Column Titles) to match your Dark Header
            style.configure("Treeview.Heading", 
                            background=self.COLORS["header_bg"], # Matches the dark middle bar
                            foreground="white", 
                            relief="flat",
                            font=("Segoe UI", 9, "bold"))

            style.map("Treeview.Heading", 
                      background=[("active", self.COLORS["primary"])]) # Blue highlight on hover
            
            # Add style for the detail panel frame background
            style.configure("Detail.TFrame", background=self.COLORS["card_bg"], relief="flat")      

            # 3. Status-based row colors (Pastels for readability)
            self.node_tree.tag_configure("needs_alternate", background="#FEF2F2") # Very light red
            self.node_tree.tag_configure("needs_decision", background="#FFFBEB")  # Very light yellow
            self.node_tree.tag_configure("auto_matched", background="#ECFDF5")    # Very light green
            self.node_tree.tag_configure("ready", background="#D1FAE5")           # Stronger green
            self.node_tree.tag_configure("locked", foreground="#9CA3AF")          # Muted text
    

    def _build_toolbar(self):
            # Use a Frame with a white background and a subtle bottom border
            bar_container = tk.Frame(self.root, bg="white", height=60)
            bar_container.pack(fill="x", side="top")

            # Add a subtle bottom border using a 1px frame
            border = tk.Frame(self.root, bg=self.COLORS["card_border"], height=1)
            border.pack(fill="x", side="top")

            # Inner padding frame
            bar = tk.Frame(bar_container, bg="white", padx=15, pady=10)
            bar.pack(fill="both", expand=True)

            # Title
            tk.Label(bar, text="NPR Tool Workspace", font=("Segoe UI", 14, "bold"), 
                     bg="white", fg=self.COLORS["text_dark"]).pack(side="left", padx=(0, 20))

            # Helper to make clean flat buttons
            def add_btn(text, cmd, primary=False):
                bg = self.COLORS["primary"] if primary else "white"
                fg = "white" if primary else self.COLORS["text_dark"]


                btn = tk.Button(bar, text=text, command=cmd, bg=bg, fg=fg, 
                                relief="solid", bd=1, padx=15, pady=6, cursor="hand2", 
                                font=("Segoe UI", 10, "bold"))
                btn.pack(side="left", padx=5)

            add_btn("Load Inventory", self._load_inventory)
            add_btn("Load BOM", self._load_bom)
            add_btn("Run Matching", self._run_matching, primary=True) # Highlight the main action

            add_btn("Export NPR", self._export_npr) # You can pack this side="right" if you prefer

            # Status Line (Moved into the bar for cleaner look, or keep at bottom)
            self.status_var = tk.StringVar(value="Ready.")


    def _load_inventory(self):
        path = filedialog.askopenfilename(filetypes=[("Excel Files", "*.xlsx")])
        if not path:
            return
        try:
            n = self.controller.load_inventory(path)
            self.status_var.set(f"Loaded inventory: {n} parts.")
        except Exception as e:
            messagebox.showerror("Load Inventory Failed", str(e))


    def _load_bom(self):
        path = filedialog.askopenfilename(filetypes=[("Excel Files", "*.xlsx")])
        if not path:
            return
        try:
            n = self.controller.load_npr(path)
            self.status_var.set(f"Loaded BOM/NPR list: {n} parts.")
        except Exception as e:
            messagebox.showerror("Load BOM Failed", str(e))


    def _run_matching(self):
        try:
            n = self.controller.run_matching()
            self.refresh_node_table()
            self.status_var.set(f"Matching complete. Built {n} decision nodes.")
        except Exception as e:
            messagebox.showerror("Run Matching Failed", str(e))





    def _build_node_table(self):
        frame = ttk.Frame(self.root)
        frame.pack(fill="x", padx=8)

        cols = ("ID", "Type", "MANUFACTURER PART NUMBER", "Status", "Matches")
        self.node_tree = ttk.Treeview(frame, columns=cols, show="headings", height=9)

        for c in cols:
            self.node_tree.heading(c, text=c)
            self.node_tree.column(c, anchor="center")

        self.node_tree.bind("<<TreeviewSelect>>", self._on_node_select)

        sb = ttk.Scrollbar(frame, orient="vertical", command=self.node_tree.yview)
        self.node_tree.configure(yscrollcommand=sb.set)

        self.node_tree.pack(side="left", fill="x", expand=True)
        sb.pack(side="right", fill="y")



    def _build_header(self):
        # 1. Use tk.Frame for the background color (easier than ttk styles)
        self.header = tk.Frame(self.root, bg=self.COLORS["header_bg"], pady=15, padx=20)
        self.header.pack(fill="x")

        shadow = tk.Frame(self.root, bg="#E5E7EB", height=1)
        shadow.pack(fill="x", padx=0)

        # Inner container for centering
        content_frame = tk.Frame(self.header, bg=self.COLORS["header_bg"])
        content_frame.pack(anchor="center")

        # ---- Title (BOM MPN) ----
        self.h_title = self.make_readonly_text(
            content_frame,
            text="Select a part",
            font=("Segoe UI", 24, "bold"),
            bg=self.COLORS["header_bg"],
            fg=self.COLORS["header_text"],
            height=1,
            width=40,
            justify="center"
        )
        self.h_title.pack(anchor="center")

        # ---- Description ----
        self.h_desc = self.make_readonly_text(
            content_frame,
            text="",
            font=("Segoe UI", 12),
            bg=self.COLORS["header_bg"],
            fg="#D1D5DB",
            height=2,
            width=110
        )
        self.h_desc.pack(anchor="center", pady=(5, 5))

        # ---- Meta Data Row (Status bubbles) ----
        # ✅ FIX: Create the frame before adding the text widget
        self.h_meta_frame = tk.Frame(content_frame, bg=self.COLORS["header_bg"])
        self.h_meta_frame.pack(anchor="center", pady=(5, 5))

        self.h_meta = self.make_readonly_text(
            self.h_meta_frame,
            text="",
            font=("Segoe UI", 15, "bold"),
            bg="#374151",
            fg="#60A5FA",
            height=1,
            width=45
        )
        self.h_meta.pack(side="left", padx=10)

        # ---- Context (Inventory Match) ----
        self.inv_context = self.make_readonly_text(
            content_frame,
            text="",
            font=("Consolas", 20),
            bg=self.COLORS["header_bg"],
            fg="#9CA3AF",
            height=2,
            width=110
        )
        self.inv_context.pack(anchor="center", pady=(5, 0))

        # ---- Action Button (Floating Top Right) ----
        self.mark_ready_btn = ttk.Button(
            self.header,
            text="Mark Ready",
            command=self._mark_ready,
        )
        self.mark_ready_btn.place(relx=0.98, rely=0.5, anchor="e")



    def make_readonly_text(self, parent, **kwargs):
        text = kwargs.pop("text", "")
        justify = kwargs.pop("justify", None)  # "left", "center", "right"
    
        widget = tk.Text(
            parent,
            wrap="word",
            bd=0,
            highlightthickness=0,
            padx=0,
            pady=0,
            cursor="arrow",
            **kwargs
        )
    
        # Configure the justify tag correctly
        if justify:
            widget.tag_configure("justify", justify=justify)
        widget.tag_configure("center_all", justify="center")
    
        def set_text(value, force_justify=None):
            widget.config(state="normal")
            widget.delete("1.0", "end")
    
            # Normalize newlines (Tkinter weirdly treats trailing newline as new line start)
            value = value.rstrip() + "\n"
    
            # Choose which tag to use
            tag_to_use = "justify" if (justify and not force_justify) else None
            if force_justify == "center":
                tag_to_use = "center_all"
    
            # Insert and apply the tag to ALL lines
            widget.insert("1.0", value)
            if tag_to_use:
                widget.tag_add(tag_to_use, "1.0", "end")
    
            widget.config(state="disabled")
    
        # Monkey-patch config so `.config(text="...")` still works
        orig_config = widget.config
    
        def config_proxy(*args, **kw):
            if "text" in kw:
                set_text(kw.pop("text"))
            return orig_config(*args, **kw)
    
        widget.set_text = set_text
        widget.set_text_centered = lambda v: set_text(v, force_justify="center")
        widget.config = config_proxy
    
        set_text(text)
        return widget
    



        
    # Builds the right-side scrollable detail/specs panel   
    def _build_detail_panel(self):
        self.specs_title = ttk.Label(
            self.detail,
            text="Information",
            font=("Segoe UI", 14, "bold"),
        )
        self.specs_title.pack(anchor="w", pady=(0, 6))

        # Scrollable container
        self.specs_canvas = tk.Canvas(self.detail, borderwidth=0, highlightthickness=0)
        self.specs_scroll = ttk.Scrollbar(
            self.detail, orient="vertical", command=self.specs_canvas.yview
        )
        self.specs_inner = ttk.Frame(self.specs_canvas)

        self.specs_inner.bind(
            "<Configure>",
            lambda e: self.specs_canvas.configure(
                scrollregion=self.specs_canvas.bbox("all")
            ),
        )

        self.specs_canvas.create_window(
            (0, 0), window=self.specs_inner, anchor="nw"
        )
        self.specs_canvas.configure(yscrollcommand=self.specs_scroll.set)

        self.specs_canvas.pack(side="left", fill="both", expand=True)
        self.specs_scroll.pack(side="right", fill="y")


    def _build_main_panes(self):
        # Outer container for better breathing room & soft background
        pane_wrapper = tk.Frame(self.root, bg=self.COLORS["bg_main"])
        pane_wrapper.pack(fill="both", expand=True, padx=12, pady=(0, 12))

        # Horizontal split pane inside wrapper
        pane = ttk.Panedwindow(pane_wrapper, orient="horizontal")
        pane.pack(fill="both", expand=True)

        # ========== LEFT SIDE: Cards ==========
        self.cards_frame = ttk.Frame(pane)
        pane.add(self.cards_frame, weight=3)

        self.cards_canvas = tk.Canvas(
            self.cards_frame,
            bg=self.COLORS["card_bg"],  # Use soft card background
            highlightthickness=0
        )
        self.cards_canvas.pack(side="left", fill="both", expand=True)

        sb = ttk.Scrollbar(self.cards_frame, orient="vertical", command=self.cards_canvas.yview)
        sb.pack(side="right", fill="y")

        self.cards_canvas.configure(yscrollcommand=sb.set)

        self.cards_inner = ttk.Frame(self.cards_canvas, padding=10)
        self.cards_canvas.create_window((0, 0), window=self.cards_inner, anchor="nw")

        self.cards_inner.bind(
            "<Configure>",
            lambda e: self.cards_canvas.configure(scrollregion=self.cards_canvas.bbox("all"))
        )

        # ========== RIGHT SIDE: Detail Panel (framed) ==========
        detail_border = tk.Frame(pane, bg=self.COLORS["card_border"], padx=1, pady=1)
        pane.add(detail_border, weight=2)

        self.detail = ttk.Frame(detail_border, padding=10, style="Detail.TFrame")
        self.detail.pack(fill="both", expand=True)

        self._build_detail_panel()


    def _render_specs(self, specs: dict):
            # Clear previous
            for w in self.specs_inner.winfo_children():
                w.destroy()

            # Helper to create a data row
            def add_row(parent, label, value, is_header=False):
                row = ttk.Frame(parent)
                row.pack(fill="x", pady=1 if not is_header else 5)

                if is_header:
                    ttk.Label(row, text=label.upper(), font=("Segoe UI", 8, "bold"), 
                             foreground=self.COLORS["primary"]).pack(anchor="w", pady=(10, 2))
                    return

                lbl = ttk.Label(row, text=label, width=20, font=("Segoe UI", 9), foreground=self.COLORS["text_dim"])
                lbl.pack(side="left", anchor="nw")

                val = ttk.Label(row, text=str(value), font=("Segoe UI", 9, "bold"), foreground=self.COLORS["text_dark"], wraplength=350)
                val.pack(side="left", fill="x", expand=True)

            # 1. Identity Group
            add_row(self.specs_inner, "IDENTITY", "", is_header=True)
            for k in ["ItemNumber", "VendorItem", "Description"]:
                if k in specs: add_row(self.specs_inner, k, specs[k])

            # 2. Manufacturing Group
            add_row(self.specs_inner, "MANUFACTURING", "", is_header=True)
            for k in ["MfgName", "MfgId", "PrimaryVendorNumber"]:
                 if k in specs: add_row(self.specs_inner, k, specs[k])

            # 3. Logistics & Cost
            add_row(self.specs_inner, "LOGISTICS", "", is_header=True)
            for k in ["TotalQty", "LastCost", "AvgCost", "ItemLeadTime", "DefaultWhse"]:
                 if k in specs: add_row(self.specs_inner, k, specs[k])



    def _set_inspector_text(self, title: str, blocks: list[tuple[str, dict]]):
        self.inspect_title.config(text=title)

        t = self.inspect_text
        t.configure(state="normal")
        t.delete("1.0", "end")

        for heading, kv in blocks:
            # Section header
            t.insert("end", f"{heading}\n", "section")
            t.insert("end", f"{'─' * len(heading)}\n", "muted")

            for k, v in kv.items():
                if v is None or v == "":
                    continue

                t.insert("end", f"{k}: ", "key")

                # numeric formatting
                if isinstance(v, (int, float)):
                    t.insert("end", f"{v}\n", "number")
                else:
                    t.insert("end", f"{v}\n", "value")

            t.insert("end", "\n")

        t.configure(state="disabled")


    def _specs_from_inventory(self, inv):
        return dict(inv.raw_fields)


    def _specs_from_alternate(self, alt):
        specs = {}

        if alt.manufacturer_part_number:
            specs["VendorItem"] = alt.manufacturer_part_number
        if alt.manufacturer:
            specs["MfgName"] = alt.manufacturer
        if alt.description:
            specs["Description"] = alt.description
        if alt.stock is not None:
            specs["TotalQty"] = alt.stock
        if alt.unit_cost is not None:
            specs["AvgCost"] = alt.unit_cost
        if alt.supplier:
            specs["PrimaryVendorNumber"] = alt.supplier

        # ---- HTSUS / Tariff Code ----
        # Prefer explicit field, fall back to raw/meta if present
        if hasattr(alt, "tariff_htsus") and alt.tariff_htsus:
            specs["TariffCodeHTSUS"] = alt.tariff_htsus
        else:
            meta = getattr(alt, "meta", None)
            if isinstance(meta, dict):
                for k in ("TariffCodeHTSUS", "HTSUS", "tariff_code_htsus_"):
                    if k in meta and meta[k]:
                        specs["TariffCodeHTSUS"] = meta[k]
                        break

        return specs

    def refresh_node_table(self):
        for i in self.node_tree.get_children():
            self.node_tree.delete(i)

        for node in self.controller.get_nodes():
            alt_txt = f"{len(node.selected_alternates())}/{len(node.candidate_alternates())}"
            appr = "YES" if node.needs_approval else "NO"

            tags = []

            if node.locked:
                tags.append("locked")

            if node.status == DecisionStatus.NEEDS_ALTERNATE:
                tags.append("needs_alternate")
            elif node.status == DecisionStatus.NEEDS_DECISION:
                tags.append("needs_decision")
            elif node.status == DecisionStatus.AUTO_MATCHED:
                tags.append("auto_matched")
            elif node.status == DecisionStatus.READY_FOR_EXPORT:
                tags.append("ready")

            self.node_tree.insert(
                "",
                "end",
                iid=node.id,
                values=(
                    node.id,
                    node.base_type,
                    node.bom_mpn,
                    node.status.value,
                    f"{len(node.selected_alternates())}/{len(node.candidate_alternates())}",
                    "YES" if node.needs_approval else "NO",
                ),
                tags=tags,
            )

    def _render_cards(self, node: DecisionNode):
        self.last_hovered_card = None
        for w in self.cards_inner.winfo_children():
            w.destroy()
            
        style = ttk.Style()
        style.configure("TLabelframe.Label", font=("Segoe UI", 9, "bold"), foreground=self.COLORS["text_dim"])

        internal_frame = ttk.LabelFrame(self.cards_inner, text="🔒 Internal Matches")
        internal_frame.pack(fill="x", padx=16, pady=(10, 6))

        external_frame = ttk.LabelFrame(self.cards_inner, text="🔍 External Alternates")
        external_frame.pack(fill="x", padx=16, pady=(10, 6))

        # DigiKey search bar (external only)
        search_bar = ttk.Frame(external_frame)
        search_bar.pack(fill="x", padx=6, pady=4)

        ttk.Button(
            search_bar,
            text="Search DigiKey",
            command=lambda: self._search_digikey(node)
        ).pack(side="left")

        # Card grids
        self._render_card_grid(
            internal_frame,
            [a for a in node.candidate_alternates() if a.source == "inventory"],
            node
        )

        self._render_card_grid(
            external_frame,
            [a for a in node.candidate_alternates() if a.source != "inventory"],
            node
        )

    def _render_card_grid(self, parent, alternates, node, cols=3):
        # Dedicated grid container (NO pack children inside it)
        grid_container = ttk.Frame(parent)
        grid_container.pack(fill="x", pady=6)

        for i, alt in enumerate(alternates):
            card = self._create_card(node, alt)
            r, c = divmod(i, cols)
            card.grid(
                row=r,
                column=c,
                padx=10,
                pady=10,
                sticky="nsew",
                in_=grid_container,  # 👈 critical
            )

        for c in range(cols):
            grid_container.columnconfigure(c, weight=1)




    # =====================================================
    # Cards
    # =====================================================

    def _create_card(self, node: DecisionNode, alt: Alternate) -> tk.Frame:
            # VISUAL HACK: Use a frame with a slightly darker color as the "Border"
            # and pack a white frame inside it with 1px padding. cleaner than relief="ridge".
            border_color = self.COLORS["card_border"]

            # Color logic based on status
            if alt.selected: border_color = self.COLORS["success"]
            elif alt.rejected: border_color = "#FECACA" # Light red

            container = tk.Frame(self.cards_inner, bg=border_color, padx=1, pady=1)

            # The actual card content
            frame = tk.Frame(container, bg="white", padx=20, pady=15, relief="flat", bd=0)

            frame.pack(fill="both", expand=True)

            # -- HOVER LOGIC --
            def on_enter(_):
                # Only highlight if not selected/rejected
                if not alt.selected and not alt.rejected:
                    container.config(bg=self.COLORS["primary"]) # Blue border on hover
                    frame.config(bg=self.COLORS["hover_card_bg"])  # New background

                # Specs Logic
                self.specs_title.config(text="Specs")
                if alt.raw is not None:
                    specs = self._specs_from_inventory(alt.raw)
                else:
                    specs = self._specs_from_alternate(alt)
                self._render_specs(specs)

            def on_leave(_):
                if not alt.selected and not alt.rejected:
                    container.config(bg=self.COLORS["card_border"])
                    frame.config(bg="white")

            frame.bind("<Enter>", on_enter)
            frame.bind("<Leave>", on_leave)

            # -- CARD CONTENT --

            # Header Row (MPN + Confidence)
            header_row = tk.Frame(frame, bg=frame["bg"])
            header_row.pack(fill="x", pady=(0, 5))

            mpn_text = alt.manufacturer_part_number or alt.internal_part_number
            tk.Label(header_row, text=mpn_text, font=("Segoe UI", 12, "bold"), 
                     bg=frame["bg"], fg="#1F2937").pack(side="left")

            tk.Label(header_row, text=f"{int(alt.confidence * 100)}%", 
                     font=("Segoe UI", 10, "bold"), bg="#DBEAFE", fg="#1D4ED8", padx=6).pack(side="right")

            # Description
            tk.Label(frame, text=alt.description, font=("Segoe UI", 9), 
                     fg="#4B5563", bg=frame["bg"], wraplength=350, justify="left").pack(anchor="w", pady=(0, 8))

            # Details Grid (Small info)
            details_row = tk.Frame(frame, bg=frame["bg"])
            details_row.pack(fill="x", pady=(0, 10))

            # Quick helper for small gray text
            def small_lbl(txt): 
                return tk.Label(details_row, text=txt, font=("Segoe UI", 8), fg="#6B7280", bg=frame["bg"])

            small_lbl(f"Stock: {alt.stock or '-'}").pack(side="left", padx=(0, 10))
            small_lbl(f"Source: {alt.source}").pack(side="left")

            # Buttons (Only show if not locked)
            btn_frame = tk.Frame(frame, bg=frame["bg"])
            btn_frame.pack(fill="x")

            if node.locked:
                return container  # or skip buttons entirely


            # Using ttk.Style to make small buttons or just standard buttons
            if not alt.selected and not alt.rejected:
                # We use standard tk.Button here to control background color better than ttk
                tk.Button(btn_frame, text="Add", bg="#10B981", fg="white", bd=0, padx=10, pady=2, cursor="hand2",
                          command=lambda: self._add_alt(node, alt)).pack(side="right", padx=2)

                tk.Button(btn_frame, text="Reject", bg="#F3F4F6", fg="#EF4444", bd=0, padx=10, pady=2, cursor="hand2",
                          command=lambda: self._reject_alt(node, alt)).pack(side="right", padx=2)
            elif alt.selected:
                tk.Label(btn_frame, text="SELECTED", fg=self.COLORS["success"], font=("Segoe UI", 9, "bold"), bg=frame["bg"]).pack(side="right")
            elif alt.rejected:
                 tk.Label(btn_frame, text="REJECTED", fg="#EF4444", font=("Segoe UI", 9, "bold"), bg=frame["bg"]).pack(side="right")

            return container

    # =====================================================
    # Actions
    # =====================================================

    def _on_node_select(self, event=None):
        selection = self.node_tree.selection()
        if not selection:
            return

        node_id = selection[0]
        node = self.controller.get_node(node_id)
        if not node:
            return

        # -------------------------
        # Header: BOM identity
        # -------------------------
        self.h_title.set_text_centered(node.bom_mpn)



        #####THIS IS BREAKPOINT STOP CLT ZING ########

        
        self.h_desc.set_text_centered(node.description or "")

        self.h_meta.config(
            text=f"Status: {node.status.value}   |   Confidence: {int(node.confidence * 100)}%"
        )

        # Enable / disable Mark Ready
        self.mark_ready_btn.state(
            ["!disabled"] if not node.locked else ["disabled"]
        )

        # -------------------------
        # Inventory context (EXACT DataLoader contract)
        # -------------------------
        if node.internal_part_number:
            inv = None

            # Find the inventory-backed alternate that represents the context match
            for alt in node.alternates:
                if (
                    alt.source == "inventory"
                    and alt.internal_part_number == node.internal_part_number
                    and alt.raw is not None
                ):
                    inv = alt.raw
                    break

            if inv:
                rf = inv.raw_fields or {}

                stock = rf.get("TotalQty", "—")
                avg_cost = rf.get("AvgCost", "—")
                lead_time = rf.get("ItemLeadTime", "—")

                self.inv_context.config(
                    text=(
                        f"Internal PN: {inv.itemnum}   |   "
                        f"Mfg: {inv.mfgname}   |   "
                        f"Vendor Item: {inv.vendoritem}\n"
                        f"Stock: {stock} pcs   |   "
                        f"Avg Cost: {avg_cost}   |   "
                        f"Lead Time: {lead_time}"
                    )
                )
            else:
                self.inv_context.config(
                    text="Internal inventory match exists, but detailed context is unavailable."
                )
        else:
            self.inv_context.config(
                text="No internal inventory match for this BOM part."
            )

        # -------------------------
        # Cards + detail panel
        # -------------------------
        self._render_cards(node)



    def _add_alt(self, node: DecisionNode, alt: Alternate):
        # Mutate state
        for a in node.alternates:
            a.selected = False
        alt.selected = True
        alt.rejected = False

        # 🔁 Refresh UI
        self._render_cards(node)
        
        self.refresh_node_table()



    def _reject_alt(self, node: DecisionNode, alt: Alternate):
        alt.rejected = True
        alt.selected = False

        # 🔁 Refresh UI
        self._render_cards(node)
      
        self.refresh_node_table()

    def _mark_ready(self):
        node = self.controller.get_node(self.current_node_id)
        if node is None:
            messagebox.showerror("Error", "No node selected.")
            return

        # Allow ready if no alternates exist
        if len(node.candidate_alternates()) == 0:
            self.controller.set_status(node.id, DecisionStatus.READY)
            messagebox.showinfo("Marked Ready", f"{node.id} marked ready (no alternates).")
            self.refresh_node_table()
            return

        # Else, only allow if one or more alternates selected
        if not node.selected_alternates():
            messagebox.showwarning("Cannot Mark Ready", "You must select an alternate first.")
            return

        self.controller.set_status(node.id, DecisionStatus.READY)
        messagebox.showinfo("Marked Ready", f"{node.id} marked ready.")
        self.refresh_node_table()



    def get_selected_node(self):
        """Return the currently selected DecisionNode from the Treeview via the controller."""
        selected = self.node_tree.selection()
        if not selected:
            return None

        # Extract the first selected item's ID
        node_id = self.node_tree.item(selected[0], "values")[0]
        try:
            node = self.controller.get_node(node_id)
            self.current_node_id = node_id
            return node
        except KeyError:
            return None



    def _export_npr(self):
        path = filedialog.asksaveasfilename(
            defaultextension=".xlsx",
            filetypes=[("Excel Files", "*.xlsx")],
            title="Save NPR Export As..."
        )
        if not path:
            return
    
        try:
            ws = self.controller.build_npr_workspace_from_nodes()
            self.controller.export_npr(path)
            messagebox.showinfo("Export Successful", f"NPR file saved to:\n{path}")
        except PermissionError as e:
            messagebox.showwarning("File In Use", str(e))
        except Exception as e:
            messagebox.showerror("Export Failed", str(e))


    # =====================================================

    def _refresh_current(self):
        if not self.current_node_id:
            return
        node = self.controller.get_node(self.current_node_id)
        self._render_cards(node)
        self.refresh_node_table()
