

from __future__ import annotations

import threading
import traceback
from typing import Optional

import customtkinter as ctk
import tkinter as tk
from tkinter import ttk, filedialog, messagebox

from .data_models import DecisionNode, DecisionStatus, Alternate
from typing import TYPE_CHECKING
import time

if TYPE_CHECKING:
    from .NEW_decision_controller import DecisionController


class DecisionWorkspaceCTK:
    """
    Modern CTk UI shell that preserves the legacy behavior:
      - controller/DB is truth
      - UI holds only current_node_id
      - renders always pull fresh state from controller
    """

    def __init__(self, root: ctk.CTk, controller: "DecisionController"):
        ctk.set_appearance_mode("System")
        ctk.set_default_color_theme("blue")

        self.root = root
        self.controller = controller

        self.root.title("NPR Tool")
        self.root.geometry("1500x950")
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)

        self._stop_event = threading.Event()
        self.controller.stop_event = self._stop_event

        # Shared progress callbacks used by inventory loading and matching-engine cache builds.
        self.root.loading_progress_callback = self._loading_progress_callback
        self.root.loading_phase_callback = self._loading_phase_callback
        self._loading_win = None
        self._loading_phase_var = None
        self._loading_detail_var = None
        self._loading_bar = None
        self._loading_started_at = 0.0

        self.current_node_id: Optional[str] = None
        self._node_tree_menu = None
        self._node_tree_ctx_item = None
        self._node_tree_ctx_col = None
        self._pinned_alt_id: Optional[str] = None
        self._last_specs_key = None
        self._suspend_node_select = False
        self._pin_click_guard_alt = None
        self._pin_click_guard_until = 0

        self.COLORS = {
            "bg_main": "#0B1220",    
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

        self._suspend_desc_trace = False
        self._suspend_approval_toggle = False
        self.desc_var.trace_add("write", self._on_desc_var_changed)

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

        # row color tags for node state cues
        try:
            self.node_row_colors = {
                "ready": "#123A24",
                "selected": "#0F2E4A",
                "attention": "#3A1D1D",
                "default": "#0B1220",
            }
        except Exception:
            self.node_row_colors = {}




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


    def _on_desc_var_changed(self, *_args):
        if getattr(self, "_suspend_desc_trace", False):
            return
        if not getattr(self, "current_node_id", None):
            return
        try:
            node = self.controller.get_node(self.current_node_id)
            if bool(getattr(node, "locked", False)):
                return
            self.controller.set_node_description(self.current_node_id, self.desc_var.get())
        except Exception:
            pass

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
        self.node_tree.bind("<Control-c>", self._copy_selected_node_rows)
        self.node_tree.bind("<Button-3>", self._on_node_tree_right_click)
        try:
            self.node_tree.tag_configure("row_ready", background="#123A24", foreground="#E5E7EB")
            self.node_tree.tag_configure("row_selected", background="#0F2E4A", foreground="#E5E7EB")
            self.node_tree.tag_configure("row_attention", background="#3A1D1D", foreground="#FDECEC")
        except Exception:
            pass

    def _build_header(self, parent: ctk.CTkFrame):
        parent.grid_columnconfigure(0, weight=1)

        self.h_title = ctk.CTkLabel(parent, text="Select a part", font=ctk.CTkFont(size=22, weight="bold"))
        self.h_title.pack(pady=(14, 4))

        self.h_desc = ctk.CTkLabel(parent, text="", text_color="#CBD5E1")
        self.h_desc.pack(pady=(0, 6))

        self.h_meta = ctk.CTkLabel(parent, text="", text_color=self.COLORS["primary"], font=ctk.CTkFont(size=14, weight="bold"))
        self.h_meta.pack(pady=(0, 10))

        pn_row = ctk.CTkFrame(parent, fg_color="transparent")
        pn_row.pack(pady=(0, 10))

        self.suggested_var = tk.StringVar(value="")
        self.company_pn_var = tk.StringVar(value="")

        self.desc_var = tk.StringVar(value='')
        ctk.CTkLabel(pn_row, text="Suggested CNS:", text_color="#CBD5E1").pack(side="left", padx=(0, 8))
        self.suggested_entry = ctk.CTkEntry(pn_row, width=160, textvariable=self.suggested_var)
        self.suggested_entry.configure(state="readonly")
        self.suggested_entry.pack(side="left", padx=(0, 16))

        ctk.CTkLabel(pn_row, text="Company PN:", text_color="#CBD5E1").pack(side="left", padx=(0, 8))
        self.company_pn_entry = ctk.CTkEntry(pn_row, width=260, textvariable=self.company_pn_var)
        self.company_pn_entry.pack(side="left", padx=(0, 10))

        self.apply_pn_btn = ctk.CTkButton(pn_row, text="Apply", command=self._apply_company_pn, width=80)
        self.apply_pn_btn.pack(side="left", padx=(0, 10))

        # Company BOM section (drives formatted BOM export bucket)
        self.bom_section_var = tk.StringVar(value="SURFACE MOUNT")
        self._suspend_bom_section_event = False
        ctk.CTkLabel(pn_row, text="BOM Section:", text_color="#CBD5E1").pack(side="left", padx=(8, 6))
        self.bom_section_menu = ctk.CTkOptionMenu(
            pn_row,
            width=220,
            variable=self.bom_section_var,
            values=["SURFACE MOUNT", "THROUGH-HOLE", "AUXILIARY - ASSEMBLY", "AUXILIARY - MECH", "AUXILIARY - PRODUCTION", "AUXILIARY - OTHER"],
            command=self._on_bom_section_changed,
        )
        self.bom_section_menu.pack(side="left", padx=(0, 8))

        # Description override (affects export + right panel; persisted in DB via controller)
        desc_row = ctk.CTkFrame(parent, fg_color="transparent")
        desc_row.pack(pady=(0, 10), fill="x")
        ctk.CTkLabel(desc_row, text="Description:", text_color="#CBD5E1").pack(side="left", padx=(0, 8))
        self.desc_entry = ctk.CTkEntry(desc_row, width=720, textvariable=self.desc_var)
        self.desc_entry.pack(side="left", padx=(0, 10))

        approval_row = ctk.CTkFrame(parent, fg_color="transparent")
        approval_row.pack(pady=(0, 10), fill="x")
        self.include_approval_var = tk.BooleanVar(value=False)
        self.include_approval_chk = ctk.CTkCheckBox(
            approval_row,
            text="Include this part on the ALTS approval sheet",
            variable=self.include_approval_var,
            command=self._on_toggle_approval_export,
        )
        self.include_approval_chk.pack(anchor="w")

        btn_row = ctk.CTkFrame(parent, fg_color="transparent")
        btn_row.pack(pady=(0, 14))

        self.mark_ready_btn = ctk.CTkButton(btn_row, text="Mark Ready", command=self._on_mark_ready, width=140)
        self.mark_ready_btn.pack(side="left", padx=(0, 8))

        self.unmark_ready_btn = ctk.CTkButton(btn_row, text="Unmark Ready", command=self._on_unmark_ready, width=140, fg_color="#374151", hover_color="#4B5563")
        self.unmark_ready_btn.pack(side="left", padx=(0, 8))

        self.auto_reject_btn = ctk.CTkButton(
            btn_row,
            text="Auto Reject All",
            command=self._on_auto_reject_all,
            width=150,
            fg_color="#7F1D1D",
            hover_color="#991B1B",
        )
        self.auto_reject_btn.pack(side="left")

        self.load_fake_external_btn = ctk.CTkButton(
            btn_row,
            text="Load Fake External Alts",
            command=self._load_fake_external_alts,
            width=190,
            fg_color="#1D4ED8",
            hover_color="#1E40AF",
        )
        self.load_fake_external_btn.pack(side="left", padx=(8, 0))

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

        # smoother wheel routing: scroll only the cards panel when cursor is over it
        try:
            self.cards_scroll.bind_all("<MouseWheel>", self._on_global_mousewheel, add="+")
        except Exception:
            pass

    # ---------------------------------------------------------------------
    # Rendering
    # ---------------------------------------------------------------------
    def _on_global_mousewheel(self, event):
        try:
            w = self.root.winfo_containing(event.x_root, event.y_root)
            if not w:
                return
            # Only intercept when pointer is inside the cards panel; otherwise let defaults handle it.
            p = w
            inside_cards = False
            while p is not None:
                if p == self.cards_scroll:
                    inside_cards = True
                    break
                p = getattr(p, 'master', None)
            if not inside_cards:
                return
            delta = 0
            if getattr(event, 'delta', 0):
                delta = -1 if event.delta > 0 else 1
            if delta:
                self.cards_scroll._parent_canvas.yview_scroll(delta, 'units')
                return "break"
        except Exception:
            return

    def _render_empty_state(self):
        self.h_title.configure(text="No Selection")
        self.h_desc.configure(text="")
        self.h_meta.configure(text="Select a BOM line")
        try:
            self.auto_reject_btn.configure(state="disabled")
        except Exception:
            pass

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
            # color cue for upper panel rows
            tags = []
            try:
                alts = list(getattr(node, "alternates", []) or [])
                selected_count = sum(1 for a in alts if getattr(a, "selected", False))
                active_count = sum(1 for a in alts if not getattr(a, "rejected", False))
                node_status = getattr(getattr(node, "status", None), "value", str(getattr(node, "status", "")))
                if str(node_status) == "READY_FOR_EXPORT":
                    tags = ["row_ready"]
                elif selected_count > 0:
                    tags = ["row_selected"]
                elif alts and active_count == 0:
                    tags = ["row_attention"]
            except Exception:
                tags = []
            self.node_tree.insert(
                "", "end",
                iid=node.id,
                values=(node.id, node.base_type, display_mpn, getattr(node.status, "value", str(node.status)), conf_display),
                tags=tuple(tags),
            )

    # ---------------------------------------------------------------------
    # Event handlers 
    # ---------------------------------------------------------------------
    def _on_node_select(self, _e=None):
        if getattr(self, "_suspend_node_select", False):
            return
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
        try:
            self.desc_var.set(getattr(node, 'description', '') or '')
        except Exception:
            pass
        try:
            alts = list(getattr(node, "alternates", []) or [])
            picked = next((a for a in alts if getattr(a, "selected", False) and not getattr(a, "rejected", False)), None)
            self._pinned_alt_id = getattr(picked, "id", None) if picked else None
        except Exception:
            self._pinned_alt_id = None
        self._render_header_state(node)
        self._render_cards(node)
        self._render_specs_for_node(node)

    def _node_tree_column_name(self, col_id: str) -> str:
        try:
            if not col_id:
                return ""
            idx = int(str(col_id).replace("#", "")) - 1
            cols = list(self.node_tree["columns"])
            if 0 <= idx < len(cols):
                return str(cols[idx])
        except Exception:
            pass
        return ""

    def _copy_selected_node_rows(self, _event=None):
        try:
            sel = list(self.node_tree.selection() or [])
            if not sel:
                return "break"
            cols = list(self.node_tree["columns"])
            lines = ["	".join(cols)]
            for item in sel:
                vals = [str(v) for v in (self.node_tree.item(item, "values") or [])]
                lines.append("	".join(vals))
            self._copy_to_clipboard("\n".join(lines), toast=f"Copied {len(sel)} row(s)")
        except Exception as e:
            messagebox.showerror("Copy Failed", str(e))
        return "break"

    def _copy_node_tree_cell(self):
        item = getattr(self, "_node_tree_ctx_item", None)
        col = getattr(self, "_node_tree_ctx_col", None)
        if not item or not col:
            return
        try:
            values = list(self.node_tree.item(item, "values") or [])
            idx = int(str(col).replace("#", "")) - 1
            if idx < 0 or idx >= len(values):
                return
            header = self._node_tree_column_name(col)
            self._copy_to_clipboard(str(values[idx]), toast=f"Copied {header or 'cell'}")
        except Exception as e:
            messagebox.showerror("Copy Failed", str(e))

    def _copy_node_tree_row(self):
        item = getattr(self, "_node_tree_ctx_item", None)
        if not item:
            return
        try:
            cols = list(self.node_tree["columns"])
            vals = [str(v) for v in (self.node_tree.item(item, "values") or [])]
            text = "\n".join(f"{c}: {v}" for c, v in zip(cols, vals))
            self._copy_to_clipboard(text, toast="Copied row")
        except Exception as e:
            messagebox.showerror("Copy Failed", str(e))

    def _copy_node_tree_column(self):
        col = getattr(self, "_node_tree_ctx_col", None)
        if not col:
            return
        try:
            idx = int(str(col).replace("#", "")) - 1
            if idx < 0:
                return
            header = self._node_tree_column_name(col)
            values = []
            for item in self.node_tree.get_children():
                row_vals = list(self.node_tree.item(item, "values") or [])
                if idx < len(row_vals):
                    values.append(str(row_vals[idx]))
            payload = "\n".join(([header] if header else []) + values)
            self._copy_to_clipboard(payload, toast=f"Copied column {header or idx+1}")
        except Exception as e:
            messagebox.showerror("Copy Failed", str(e))

    def _on_node_tree_right_click(self, event):
        menu = None
        try:
            item = self.node_tree.identify_row(event.y)
            col = self.node_tree.identify_column(event.x)
            if item:
                self.node_tree.selection_set(item)
                self.node_tree.focus(item)
            self._node_tree_ctx_item = item
            self._node_tree_ctx_col = col

            menu = tk.Menu(self.root, tearoff=0)
            menu.add_command(label="Copy Cell", command=self._copy_node_tree_cell)
            menu.add_command(label="Copy Row", command=self._copy_node_tree_row)
            menu.add_command(label="Copy Column", command=self._copy_node_tree_column)
            menu.tk_popup(event.x_root, event.y_root)
        finally:
            try:
                if menu is not None:
                    menu.grab_release()
            except Exception:
                pass

    def _render_header_state(self, node: DecisionNode):
        # Header values are driven ONLY by committed state.
        # - Clicking a card (pin/view) must never affect header fields.
        # - If an alternate has been ADDED (selected), show that alternate's CPN + description.
        # - Otherwise show the BOM line's current values (assigned PN + BOM description).
    
        alts = list(getattr(node, "alternates", []) or [])
        selected_alt = None
        try:
            for a in alts:
                if bool(getattr(a, "selected", False)) and not bool(getattr(a, "rejected", False)):
                    selected_alt = a
                    break
        except Exception:
            selected_alt = None
    
        # Reject-all / all-cards-rejected unlocks manual editing (for PN)
        try:
            all_rejected = bool(alts) and all(bool(getattr(a, "rejected", False)) for a in alts)
        except Exception:
            all_rejected = False
    
        # Committed display values (RESTORED: selected card drives desc_val when selected)
        if selected_alt is not None:
            pn_val = (getattr(selected_alt, "internal_part_number", "") or "").strip() or \
                     (getattr(node, "internal_part_number", "") or "").strip()
            desc_val = (getattr(selected_alt, "description", "") or "").strip() or \
                       (getattr(node, "description", "") or "").strip()
        else:
            pn_val = (getattr(node, "assigned_part_number", "") or "").strip()
            desc_val = (getattr(node, "description", "") or "").strip()
    
        self.h_title.configure(text=f"Company PN: {pn_val or '—'}")
        self.h_desc.configure(text=f"BOM MPN: {getattr(node, 'bom_mpn', '') or '—'}")
        self.h_meta.configure(text=str(getattr(node.status, "value", node.status)))
    
        # Suggested CNS: prefer current visible PN when present, else suggested_pb
        _suggest = (getattr(node, "suggested_pb", "") or "").strip()
        if pn_val:
            _suggest = pn_val
        self.suggested_var.set(_suggest)
    
        # Populate header edit fields
        self._suspend_desc_trace = True
        try:
            self.company_pn_var.set(pn_val)
            self.desc_var.set(desc_val)
        finally:
            self._suspend_desc_trace = False
    
        # BOM section widget
        try:
            self._suspend_bom_section_event = True
            sec = self.controller.get_node_bom_section(node.id)
            self.bom_section_var.set(sec or "SURFACE MOUNT")
        except Exception:
            self.bom_section_var.set("SURFACE MOUNT")
        finally:
            self._suspend_bom_section_event = False
    
        locked = bool(getattr(node, "locked", False))
        status_text = str(getattr(getattr(node, "status", None), "value", getattr(node, "status", "")))
        is_ready = (status_text == "READY_FOR_EXPORT")

        try:
            self._suspend_approval_toggle = True
            self.include_approval_var.set(bool(getattr(node, "needs_approval", False)))
        finally:
            self._suspend_approval_toggle = False
    
        # PN stays editable when the node is not hard-locked and there is no
        # selected internal inventory card. This preserves manual editability
        # for external-only selections while keeping internal-selected cases gated.
        selected_is_internal = bool(
            selected_alt is not None and getattr(selected_alt, "source", "") == "inventory"
        )
        pn_unlock = (not locked) and (all_rejected or not selected_is_internal)
    
        # Description should stay editable unless the node is hard-locked.
        desc_unlock = (not locked)
    
        try:
            self.mark_ready_btn.configure(state=("disabled" if is_ready else "normal"))
            self.unmark_ready_btn.configure(state=("normal" if is_ready else "disabled"))
            self.bom_section_menu.configure(state=("disabled" if locked else "normal"))
            self.auto_reject_btn.configure(state=("disabled" if locked else "normal"))
    
            self.company_pn_entry.configure(state=("normal" if pn_unlock else "disabled"))
            self.apply_pn_btn.configure(state=("normal" if pn_unlock else "disabled"))
    
            self.desc_entry.configure(state=("normal" if desc_unlock else "disabled"))
            self.include_approval_chk.configure(state=("disabled" if locked else "normal"))
            self.load_fake_external_btn.configure(state=("disabled" if locked else "normal"))
        except Exception:
            pass


    def _render_cards(self, node: DecisionNode):
        for w in self.cards_scroll.winfo_children():
            w.destroy()

        prev_pinned = self._pinned_alt_id
        alts = list(getattr(node, "alternates", []) or [])
        valid_ids = {getattr(a, 'id', None) for a in alts}
        self._pinned_alt_id = prev_pinned if prev_pinned in valid_ids else None

        internal_active = [a for a in alts if (not getattr(a, "rejected", False)) and getattr(a, "source", "") == "inventory"]
        external_active = [a for a in alts if (not getattr(a, "rejected", False)) and getattr(a, "source", "") != "inventory"]
        rejected_all = [a for a in alts if getattr(a, "rejected", False)]

        if not alts:
            ctk.CTkLabel(self.cards_scroll, text="No alternates/candidates yet.", text_color=self.COLORS["text_dim"]).pack(anchor="w", padx=8, pady=8)
            return

        self._build_card_section(self.cards_scroll, f"Internal Matches ({len(internal_active)})", internal_active, node)
        self._build_card_section(self.cards_scroll, f"External Alternates ({len(external_active)})", external_active, node)
        if rejected_all:
            self._build_card_section(self.cards_scroll, f"REJECTED ({len(rejected_all)})", rejected_all, node)

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


    def _card_layout_metrics(self):
        try:
            panel_width = max(720, int(self.cards_host.winfo_width() or 720))
        except Exception:
            panel_width = 720

        if panel_width < 950:
            cols = 1
        elif panel_width < 1450:
            cols = 2
        else:
            cols = 3

        usable_width = max(420, panel_width - 36)
        gap_total = (cols - 1) * 12
        col_width = max(290, min(360, (usable_width - gap_total) // cols))
        wrap_width = max(210, col_width - 26)

        return cols, col_width, wrap_width

    def _build_card_section(self, parent, title: str, alternates: list, node: DecisionNode):
        section = ctk.CTkFrame(parent, corner_radius=10, fg_color="#111827")
        section.pack(fill="x", padx=4, pady=6)

        ctk.CTkLabel(
            section,
            text=title,
            font=ctk.CTkFont(size=13, weight="bold"),
            text_color=self.COLORS["text"]
        ).pack(anchor="w", padx=10, pady=(8, 4))

        if not alternates:
            ctk.CTkLabel(
                section,
                text="None",
                text_color=self.COLORS["text_dim"]
            ).pack(anchor="w", padx=12, pady=(0, 8))
            return

        holder = ctk.CTkFrame(section, fg_color="transparent")
        holder.pack(fill="x", padx=8, pady=(0, 8), anchor="w")
        holder.grid_anchor("nw")

        cols, card_width, _wrap_width = self._card_layout_metrics()
        used_cols = max(1, min(cols, len(alternates)))

        for c in range(used_cols):
            holder.grid_columnconfigure(c, weight=0, minsize=card_width)

        for i, alt in enumerate(alternates):
            r = i // used_cols
            c = i % used_cols
            card = self._create_card(holder, node, alt, card_width=card_width)
            card.grid(row=r, column=c, padx=6, pady=6, sticky="nw")


    def _create_card(self, parent, node: DecisionNode, alt: Alternate, card_width: int | None = None):
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

        _cols, _default_card_width, wrap_width = self._card_layout_metrics()
        if card_width is None:
            card_width = _default_card_width

        outer = ctk.CTkFrame(
            parent,
            corner_radius=12,
            fg_color=border,
            width=card_width,
            height=250
        )
        outer.grid_propagate(False)
        outer.pack_propagate(False)

        inner = ctk.CTkFrame(
            outer,
            corner_radius=10,
            fg_color=self.COLORS["card_bg"]
        )
        inner.pack(fill="both", expand=True, padx=1, pady=1)

        is_inventory = (getattr(alt, "source", "") == "inventory")
        company_pn = (getattr(alt, "internal_part_number", "") or "").strip()
        mfg_pn = (getattr(alt, "manufacturer_part_number", "") or "").strip()
        mfg_name = (getattr(alt, "manufacturer", "") or "").strip()

        rep_vendoritem = str(((getattr(alt, 'meta', {}) or {}).get('company_pn_rep_vendoritem', '') or '')).strip()
        matched_ui = (getattr(alt, '_matched_mpn_ui', '') or '').strip()

        title = company_pn if is_inventory else (mfg_pn or company_pn or "(no part number)")
        subtitle = (matched_ui or rep_vendoritem or mfg_pn) if is_inventory else mfg_name

        head = ctk.CTkFrame(inner, fg_color="transparent")
        head.pack(fill="x", padx=10, pady=(8, 4))

        left = ctk.CTkFrame(head, fg_color="transparent")
        left.pack(side="left", fill="x", expand=True)

        ctk.CTkLabel(
            left,
            text=title or "—",
            anchor="w",
            font=ctk.CTkFont(size=13, weight="bold")
        ).pack(anchor="w")

        if subtitle:
            ctk.CTkLabel(
                left,
                text=subtitle,
                anchor="w",
                text_color=self.COLORS["text_dim"]
            ).pack(anchor="w")

        try:
            alt_count = int(((getattr(alt, 'meta', {}) or {}).get('company_pn_mfgpn_count', 0) or 0))
        except Exception:
            alt_count = 0

        if is_inventory and alt_count >= 1:
            ctk.CTkLabel(
                left,
                text=f"MFGPNs: {alt_count}",
                text_color=self.COLORS["text_dim"]
            ).pack(anchor="w")

        conf = float(getattr(alt, "confidence", 0.0) or 0.0)
        if (getattr(alt, "source", "") != "inventory") and conf == 0.0 and not is_rejected:
            conf = 1.0

        ctk.CTkLabel(
            head,
            text=f"{int(conf * 100)}%",
            width=54,
            corner_radius=8,
            fg_color="#1E3A8A",
            text_color="white"
        ).pack(side="right", padx=(8, 0))

        desc = (getattr(alt, "description", "") or "").strip() or "(no description)"
        if len(desc) > 92:
            desc = desc[:89].rstrip() + "..."

        ctk.CTkLabel(
            inner,
            text=desc,
            wraplength=wrap_width,
            justify="left",
            text_color=self.COLORS["text_dim"]
        ).pack(anchor="w", padx=10, pady=(0, 6))

        stock = self._display_stock_for_alt(alt)
        ctk.CTkLabel(
            inner,
            text=f"Source: {getattr(alt, 'source', '') or '-'}    Stock: {stock}",
            text_color=self.COLORS["text_dim"]
        ).pack(anchor="w", padx=10, pady=(0, 4))

        badge_row = ctk.CTkFrame(inner, fg_color="transparent", height=24)
        badge_row.pack(fill="x", padx=10, pady=(0, 4))
        badge_row.pack_propagate(False)

        if is_selected:
            ctk.CTkLabel(
                badge_row,
                text="LOCKED IN",
                fg_color="#14532D",
                corner_radius=6,
                padx=8,
                text_color="white"
            ).pack(side="left")
        elif is_rejected:
            ctk.CTkLabel(
                badge_row,
                text="REJECTED",
                fg_color="#7F1D1D",
                corner_radius=6,
                padx=8,
                text_color="white"
            ).pack(side="left")
        elif is_pinned:
            ctk.CTkLabel(
                badge_row,
                text="VIEWING",
                fg_color="#1E3A8A",
                corner_radius=6,
                padx=8,
                text_color="white"
            ).pack(side="left")

        actions = ctk.CTkFrame(inner, fg_color="transparent")
        actions.pack(side="bottom", fill="x", padx=10, pady=(2, 10))

        left_actions = ctk.CTkFrame(actions, fg_color="transparent")
        left_actions.pack(side="left", anchor="w")

        right_actions = ctk.CTkFrame(actions, fg_color="transparent")
        right_actions.pack(side="right", anchor="e")

        ctk.CTkButton(
            left_actions,
            text="Copy",
            width=64,
            height=26,
            fg_color="#1F2937",
            hover_color="#374151",
            command=lambda a=alt: self._copy_to_clipboard(self._card_copy_text(a), toast="Copied card details")
        ).pack(side="left")

        if not getattr(node, "locked", False):
            if is_rejected:
                ctk.CTkButton(
                    right_actions,
                    text="Unreject",
                    width=82,
                    height=26,
                    fg_color="#374151",
                    hover_color="#4B5563",
                    command=lambda a=alt: self._unreject_alt(node, a)
                ).pack(side="right")
            elif is_selected:
                ctk.CTkButton(
                    right_actions,
                    text="Reject",
                    width=72,
                    height=26,
                    fg_color="#374151",
                    hover_color="#4B5563",
                    command=lambda a=alt: self._reject_alt(node, a)
                ).pack(side="right", padx=(0, 6))

                ctk.CTkButton(
                    right_actions,
                    text="Unlock",
                    width=72,
                    height=26,
                    fg_color="#14532D",
                    hover_color="#166534",
                    command=lambda a=alt: self._add_alt(node, a)
                ).pack(side="right")
            else:
                ctk.CTkButton(
                    right_actions,
                    text="Reject",
                    width=72,
                    height=26,
                    fg_color="#374151",
                    hover_color="#4B5563",
                    command=lambda a=alt: self._reject_alt(node, a)
                ).pack(side="right", padx=(0, 6))

                ctk.CTkButton(
                    right_actions,
                    text="Add",
                    width=64,
                    height=26,
                    fg_color=self.COLORS["success"],
                    hover_color="#16A34A",
                    command=lambda a=alt: self._add_alt(node, a)
                ).pack(side="right")

        def _bind_recursive(widget):
            try:
                if widget is not actions and not isinstance(widget, ctk.CTkButton):
                    widget.bind("<Button-1>", lambda _e=None, a=alt: self._pin_card(node, a))
            except Exception:
                pass

            if widget is actions:
                return

            for ch in getattr(widget, "winfo_children", lambda: [])():
                _bind_recursive(ch)

        _bind_recursive(inner)
        return outer


    def _pin_card(self, node: DecisionNode, alt: Alternate):
        # card click = focus/view this card (persistent highlight until another card is clicked)
        import time
        aid = getattr(alt, "id", None)
        now = time.monotonic()
        if self._pin_click_guard_alt == aid and now < getattr(self, '_pin_click_guard_until', 0):
            return "break"
        self._pin_click_guard_alt = aid
        self._pin_click_guard_until = now + 0.08
        if self._pinned_alt_id != aid:
            self._pinned_alt_id = aid
            self._render_cards(node)
        self._render_specs_for_alt(node, alt)
        return "break"

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

        export_mfgpn_options = []
        try:
            payload = self.controller.get_alt_detail_payload(node.id, alt.id)
            if isinstance(payload, dict):
                specs = dict(payload.get('specs') or {})
                export_mfgpn_options = list(payload.get('export_mfgpn_options') or [])
            else:
                specs = self._specs_from_inventory(alt.raw) if getattr(alt, 'raw', None) is not None else self._specs_from_alternate(alt)
        except Exception:
            specs = self._specs_from_inventory(alt.raw) if getattr(alt, 'raw', None) is not None else self._specs_from_alternate(alt)

        self._render_specs(specs)

        # Choose a specific MFG PN under the same company PN (controller-backed list)
        try:
            if getattr(alt, 'source', '') == 'inventory':
                if not export_mfgpn_options:
                    lines = [ln.strip() for ln in str(specs.get('AlternatesList', '') or '').splitlines() if ln.strip()]
                    export_mfgpn_options = [ln.split('—',1)[0].strip() if '—' in ln else ln.strip() for ln in lines]
                export_mfgpn_options = [m for m in export_mfgpn_options if str(m).strip()]
                if export_mfgpn_options:
                    ctk.CTkLabel(self.specs_scroll, text='Choose export MFG PN', text_color=self.COLORS['primary'],
                                 font=ctk.CTkFont(size=11, weight='bold')).pack(anchor='w', pady=(10,4))
                    seen = set()
                    for mpn in export_mfgpn_options[:40]:
                        mpn = str(mpn).strip()
                        if not mpn or mpn.lower() in seen:
                            continue
                        seen.add(mpn.lower())
                        b = ctk.CTkButton(self.specs_scroll, text=mpn, height=28, fg_color='#111827', hover_color='#1F2937',
                                          command=lambda m=mpn, a=alt: self._set_group_mfgpn(node, a, m))
                        b.pack(anchor='w', pady=2)
        except Exception:
            pass

    def _render_specs(self, specs: dict):
        def add_kv(label: str, value):
            if value in (None, ""):
                return
            row = ctk.CTkFrame(self.specs_scroll, fg_color="#111827", corner_radius=8)
            row.pack(fill="x", pady=3)
            ctk.CTkLabel(row, text=label, width=160, anchor="w", text_color=self.COLORS["text_dim"]).pack(side="left", padx=(8, 4), pady=6)
            val = str(value)
            box = ctk.CTkTextbox(row, height=(34 if "\n" not in val else min(120, 24 * (val.count("\n") + 2))), fg_color="#0B1220")
            box.pack(side="left", fill="x", expand=True, padx=(0, 8), pady=4)
            box.insert("1.0", val)
            box.configure(state="disabled")
            try:
                box.bind("<Button-1>", lambda _e=None, t=val, l=label: self._copy_to_clipboard(t, toast=f"Copied {l}"))
            except Exception:
                pass

        def pick(*keys):
            for key in keys:
                value = specs.get(key)
                if value not in (None, ""):
                    return value
            return ""

        if not specs:
            ctk.CTkLabel(self.specs_scroll, text="No details available.", text_color=self.COLORS["text_dim"]).pack(anchor="w")
            return

        ordered_fields = [
            ("Company Part Number", pick("CompanyPartNumber", "ItemNumber")),
            ("Manufacturer Part Number", pick("ManufacturerPartNumber", "MasterManufacturerPN", "VendorItem")),
            ("Manufacturer", pick("ManufacturerName", "MasterManufacturerName", "MfgName")),
            ("Manufacturer ID", pick("ManufacturerId", "MasterMfgId", "MfgId")),
            ("Active", pick("Active", "MasterActive")),
            ("Tariff Code", pick("TariffCode", "MasterTariffCode", "TariffCodeHTSUS")),
            ("Tariff Rate", pick("TariffRate", "MasterTariffRate")),
            ("Last Cost", pick("LastCost", "MasterLastCost")),
            ("Standard Cost", pick("StandardCost", "MasterStandardCost", "StdCost")),
            ("Average Cost", pick("AverageCost", "MasterAverageCost", "AvgCost")),
            ("Manufacturer Count", pick("ManufacturerCount", "MfgItemCount")),
            ("Revision", pick("Revision")),
            ("Lead Time", pick("LeadTime", "ItemLeadTime")),
            ("Total Quantity", pick("TotalQuantity", "TotalQty")),
        ]

        rendered = 0
        for label, value in ordered_fields:
            if value in (None, ""):
                continue
            add_kv(label, value)
            rendered += 1

        if rendered == 0:
            fallback_fields = [
                ("Description", pick("Description")),
                ("Supplier", pick("PrimaryVendorNumber", "Supplier")),
                ("Stock", pick("Stock", "TotalQty")),
                ("Cost", pick("UnitCost", "LastCost", "AvgCost")),
                ("Notes", pick("Notes")),
            ]
            for label, value in fallback_fields:
                add_kv(label, value)

        alts_raw = str(specs.get("AlternatesList", "") or "").strip()
        alts_count = specs.get("AlternatesCount") or ""
        if alts_count or alts_raw:
            row = ctk.CTkFrame(self.specs_scroll, fg_color="#111827", corner_radius=8)
            row.pack(fill="x", pady=(8, 3))
            summary = f"{alts_count} available" if alts_count else "Available"
            ctk.CTkLabel(row, text="Other MFG PNs", width=160, anchor="w", text_color=self.COLORS["text_dim"]).pack(side="left", padx=(8, 4), pady=6)
            ctk.CTkLabel(row, text=summary, anchor="w", text_color="#E5E7EB").pack(side="left", padx=(0, 8), pady=6)

    def _specs_from_inventory(self, inv):
        _raw = dict(getattr(inv, "raw_fields", {}) or {})
        # Normalize keys for case-insensitive lookups (inventory exports vary by header casing).
        raw = {str(k).strip().lower(): v for k, v in _raw.items()}
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


    def _set_group_mfgpn(self, node: DecisionNode, alt: Alternate, mfgpn: str):
        try:
            self.controller.set_preferred_inventory_mfgpn(node.id, alt.id, mfgpn)
            self._refresh_after_alt_action(node.id, focus_alt_id=getattr(alt, 'id', None), refresh_table=True)
        except Exception as e:
            messagebox.showerror('Set MFG PN Failed', str(e))

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

    def _set_tree_selection_silent(self, node_id: str):
        try:
            cur = self.node_tree.selection()
            if cur and cur[0] == node_id:
                return
            self._suspend_node_select = True
            self.node_tree.selection_set(node_id)
        finally:
            try:
                self.root.after(1, lambda: setattr(self, "_suspend_node_select", False))
            except Exception:
                self._suspend_node_select = False

    def _refresh_after_alt_action(self, node_id: str, focus_alt_id: str | None = None, refresh_table: bool = True):
        fresh = self.controller.get_node(node_id)
        self._render_header_state(fresh)
        self._render_cards(fresh)
        picked = None
        if focus_alt_id:
            picked = next((a for a in (fresh.alternates or []) if a.id == focus_alt_id), None)
        if picked is None:
            self._render_specs_for_node(fresh)
        else:
            self._render_specs_for_alt(fresh, picked)
        if refresh_table:
            self.refresh_node_table()
        self._set_tree_selection_silent(node_id)
        return fresh

    def _load_fake_external_alts(self):
        if not self.current_node_id:
            messagebox.showwarning("No Selection", "Select a BOM line first.")
            return
        try:
            created = self.controller.seed_fake_external_alternates(self.current_node_id)
            fresh = self.controller.get_node(self.current_node_id)
            self._render_header_state(fresh)
            self._render_cards(fresh)
            self._render_specs_for_node(fresh)
            self.refresh_node_table()
            if created:
                self.status_var.set(f"Loaded {len(created)} fake external alternates for testing.")
            else:
                self.status_var.set("Fake external alternates already loaded for this node.")
        except Exception as e:
            messagebox.showerror("Load Fake External Alternates Failed", str(e))

    def _on_toggle_approval_export(self):
        if getattr(self, "_suspend_approval_toggle", False):
            return
        if not self.current_node_id:
            return
        try:
            self.controller.set_node_approval_export(self.current_node_id, bool(self.include_approval_var.get()))
            fresh = self.controller.get_node(self.current_node_id)
            self._render_header_state(fresh)
        except Exception as e:
            messagebox.showerror("Approval Flag Failed", str(e))

    def _add_alt(self, node: DecisionNode, alt: Alternate):
        try:
            if getattr(alt, 'selected', False):
                self.controller.unselect_alternate(node.id, alt.id)
            else:
                is_external = (getattr(alt, 'source', '') or '').lower() != 'inventory'
                if is_external:
                    try:
                        umbrella_count = int(self.controller.get_internal_umbrella_count(node.id) or 0)
                    except Exception:
                        umbrella_count = 0
                    if umbrella_count >= 3:
                        ok = messagebox.askyesno(
                            "External Alternate Confirmation",
                            "The selected internal company part already has 3 or more internal manufacturer part numbers under it. Include additional external parts in the NPR anyway?"
                        )
                        if not ok:
                            return
                self.controller.select_alternate(node.id, alt.id)
            self._pinned_alt_id = alt.id
            self._refresh_after_alt_action(node.id, focus_alt_id=alt.id, refresh_table=True)
        except Exception as e:
            messagebox.showerror("Select Alternate Failed", str(e))

    def _reject_alt(self, node: DecisionNode, alt: Alternate):
        try:
            self.controller.reject_alternate(node.id, alt.id)
            self._pinned_alt_id = alt.id
            self._refresh_after_alt_action(node.id, focus_alt_id=alt.id, refresh_table=True)
        except Exception as e:
            messagebox.showerror("Reject Alternate Failed", str(e))

    def _unreject_alt(self, node: DecisionNode, alt: Alternate):
        try:
            self.controller.unreject_alternate(node.id, alt.id)
            self._pinned_alt_id = alt.id
            self._refresh_after_alt_action(node.id, focus_alt_id=alt.id, refresh_table=True)
        except Exception as e:
            messagebox.showerror("Unreject Alternate Failed", str(e))
    # ---------------------------------------------------------------------
    # Loading/progress popup helpers
    # ---------------------------------------------------------------------
    def _show_loading_popup(self, title: str, phase: str = "Working..."):
        self._close_loading_popup()
        self._loading_started_at = time.time()
        win = ctk.CTkToplevel(self.root)
        win.title(title)
        win.geometry("460x150")
        win.transient(self.root)
        win.grab_set()
        win.resizable(False, False)

        self._loading_phase_var = tk.StringVar(value=phase)
        self._loading_detail_var = tk.StringVar(value="Starting...")

        frame = ctk.CTkFrame(win, corner_radius=12)
        frame.pack(fill="both", expand=True, padx=16, pady=16)

        ctk.CTkLabel(frame, textvariable=self._loading_phase_var, font=ctk.CTkFont(size=16, weight="bold")).pack(anchor="w", pady=(4, 8), padx=10)
        self._loading_bar = ctk.CTkProgressBar(frame)
        self._loading_bar.pack(fill="x", padx=10, pady=(0, 8))
        self._loading_bar.set(0.0)
        ctk.CTkLabel(frame, textvariable=self._loading_detail_var, text_color=self.COLORS["text_dim"]).pack(anchor="w", padx=10)

        self._loading_win = win
        try:
            win.update_idletasks()
            x = self.root.winfo_rootx() + (self.root.winfo_width() // 2) - 230
            y = self.root.winfo_rooty() + (self.root.winfo_height() // 2) - 75
            win.geometry(f"+{max(0, x)}+{max(0, y)}")
        except Exception:
            pass

    def _close_loading_popup(self):
        try:
            if self._loading_win is not None and self._loading_win.winfo_exists():
                self._loading_win.grab_release()
                self._loading_win.destroy()
        except Exception:
            pass
        self._loading_win = None
        self._loading_bar = None
        self._loading_phase_var = None
        self._loading_detail_var = None

    def _loading_phase_callback(self, message: str, determinate: bool = True):
        def _apply():
            if self._loading_phase_var is not None:
                self._loading_phase_var.set(str(message or "Working..."))
        try:
            self.root.after(0, _apply)
        except Exception:
            _apply()

    def _loading_progress_callback(self, *args):
        def _apply(ratio: float, detail: str):
            if self._loading_bar is not None:
                try:
                    self._loading_bar.set(max(0.0, min(1.0, float(ratio))))
                except Exception:
                    pass
            if self._loading_detail_var is not None and detail:
                self._loading_detail_var.set(detail)

        ratio = 0.0
        detail = ""
        try:
            if len(args) == 1:
                ratio = float(args[0])
                detail = f"{int(ratio * 100)}% complete"
            elif len(args) >= 2:
                cur = float(args[0] or 0)
                total = float(args[1] or 1)
                ratio = 0.0 if total <= 0 else cur / total
                msg = str(args[2]) if len(args) >= 3 else "Working..."
                detail = f"{msg} ({int(cur)}/{int(total)})"
        except Exception:
            ratio = 0.0
            detail = "Working..."
        try:
            self.root.after(0, lambda: _apply(ratio, detail))
        except Exception:
            _apply(ratio, detail)

    # ---------------------------------------------------------------------
    # Controller-backed actions (same names as legacy UI)
    # ---------------------------------------------------------------------
    def _load_inventory(self):
        path = filedialog.askopenfilename(filetypes=[("Excel Files", "*.xlsx")])
        if not path:
            return

        self._show_loading_popup("Loading Master Inventory", "Reading workbook...")
        self.status_var.set("Loading master inventory...")

        def worker():
            try:
                n = self.controller.load_inventory(
                    path,
                    progress_cb=self._loading_progress_callback,
                    phase_cb=self._loading_phase_callback,
                )
                self.root.after(0, lambda: self._on_inventory_loaded(n))
            except Exception as e:
                self.root.after(0, lambda err=str(e): self._on_inventory_load_failed(err))

        threading.Thread(target=worker, daemon=True, name="load-master-inventory").start()

    def _on_inventory_loaded(self, n: int):
        self._close_loading_popup()
        self.status_var.set(f"Loaded master inventory: {n} unique company parts.")
        self.current_node_id = None
        self.refresh_node_table()
        self._render_empty_state()

    def _on_inventory_load_failed(self, err: str):
        self._close_loading_popup()
        messagebox.showerror("Load Master Inventory Failed", str(err))

    def _load_items_sheet(self):
        path = filedialog.askopenfilename(filetypes=[("Excel Files", "*.xlsx")])
        if not path:
            return

        self._show_loading_popup("Loading ERP Inventory", "Reading workbook...")
        self.status_var.set("Loading ERP inventory...")

        def worker():
            try:
                n = self.controller.load_items_inventory(
                    path,
                    progress_cb=self._loading_progress_callback,
                    phase_cb=self._loading_phase_callback,
                )
                self.root.after(0, lambda: self._on_items_inventory_loaded(n))
            except Exception as e:
                self.root.after(0, lambda err=str(e): self._on_items_inventory_load_failed(err))

        threading.Thread(target=worker, daemon=True, name="load-erp-inventory").start()

    def _on_items_inventory_loaded(self, n: int):
        self._close_loading_popup()
        self.status_var.set(f"Loaded ERP inventory: merged into {n} company parts.")
        self.current_node_id = None
        self.refresh_node_table()
        self._render_empty_state()

    def _on_items_inventory_load_failed(self, err: str):
        self._close_loading_popup()
        messagebox.showerror("Load ERP Inventory Failed", str(err))

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
        self._show_loading_popup("Running Matching", "Preparing semantic cache...")
        def worker():
            try:
                n = self.controller.run_matching()
                self.root.after(0, lambda: self._on_match_done(n))
            except Exception as e:
                self.root.after(0, lambda err=str(e): self._on_match_failed(err))

        threading.Thread(target=worker, daemon=True).start()

    def _on_match_done(self, n: int):
        self._close_loading_popup()
        self.refresh_node_table()
        self.current_node_id = None
        self._render_empty_state()
        self.status_var.set(f"Matching complete. Built {n} decision nodes.")

    def _on_match_failed(self, err: str):
        self._close_loading_popup()
        messagebox.showerror("Run Matching Failed", str(err))

    def _open_workspace(self):
        try:
            rows = self.controller.list_workspaces(status='ACTIVE') or []
        except Exception as e:
            messagebox.showerror('Open Workspace Failed', str(e))
            return
        if not rows:
            messagebox.showinfo('Open Workspace', 'No active workspaces found.')
            return

        win = ctk.CTkToplevel(self.root)
        win.title('Open Workspace')
        win.geometry('720x420')
        win.grab_set()

        ctk.CTkLabel(win, text='Select Workspace', font=ctk.CTkFont(size=16, weight='bold')).pack(pady=(10,6))
        lb = tk.Listbox(win, height=14, bg='#0F172A', fg='white', selectbackground='#2563EB')
        lb.pack(fill='both', expand=True, padx=12, pady=8)
        for r in rows:
            wid = str(r.get('workspace_id',''))
            name = str(r.get('name','') or '')
            stamp = str(r.get('updated_at','') or r.get('created_at','') or '')
            lb.insert('end', f"{wid}   |   {name}   |   {stamp}")

        def do_open():
            sel = lb.curselection()
            if not sel:
                return
            wid = str(rows[sel[0]].get('workspace_id',''))
            try:
                n = self.controller.open_workspace(wid)
                self.current_node_id = None
                self.refresh_node_table()
                self._render_empty_state()
                self.status_var.set(f'Opened workspace {wid} ({n} nodes).')
                win.destroy()
            except Exception as e:
                messagebox.showerror('Open Workspace Failed', str(e))

        ctk.CTkButton(win, text='Open', command=do_open).pack(pady=(0,10))

    def _copy_to_clipboard(self, text, toast=None):
        try:
            s = "" if text is None else str(text)
            self.root.clipboard_clear()
            self.root.clipboard_append(s)
            try:
                self.root.update_idletasks()
            except Exception:
                pass
            if toast:
                try:
                    self.status_var.set(str(toast))
                except Exception:
                    pass
        except Exception as e:
            messagebox.showerror("Copy Failed", str(e))

    def _set_status(self, msg: str, ok: bool = True):
        try:
            if hasattr(self, "status_var") and self.status_var is not None:
                self.status_var.set(str(msg or ""))
        except Exception:
            pass


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

    def _on_bom_section_changed(self, _choice=None):
        if getattr(self, "_suspend_bom_section_event", False):
            return
        node_id = self.current_node_id
        if not node_id:
            return
        try:
            sec = self.controller.set_node_bom_section(node_id, self.bom_section_var.get())
            self._suspend_bom_section_event = True
            self.bom_section_var.set(sec)
            self._suspend_bom_section_event = False
            self._set_status(f"BOM section set to {sec}", ok=True)
        except Exception as e:
            self._suspend_bom_section_event = True
            try:
                self.bom_section_var.set(self.controller.get_node_bom_section(node_id))
            except Exception:
                pass
            self._suspend_bom_section_event = False
            messagebox.showerror("BOM Section Failed", str(e))

    def _apply_company_pn(self):
        if not self.current_node_id:
            return
        pn = (self.company_pn_var.get() or self.suggested_var.get() or "").strip()
        if not pn:
            messagebox.showwarning("Missing PN", "Enter a Company Part Number first.")
            return
        try:
            self.controller.set_assigned_part_number(self.current_node_id, pn)
            node = self.controller.get_node(self.current_node_id)
            self._render_header_state(node)
            self._render_cards(node)
            self._render_specs_for_node(node)
            self.refresh_node_table()
        except Exception as e:
            messagebox.showerror("Apply PN Failed", str(e))

    # TODO:
    # post application of the applu_description make need to chnage the description of the node in UI to that of the chnaged description. In order to not change UI funcitlity of being a populated instance of the controller, INSTEAD of simply changing the panels and their descripion, inude and inidicator, maybe a badge orf sort, which indicated a changed description name, or pop open a hidden something in the header that shows that we changed that descirpion from the origianl one to the new one.The npr tool isnt set uop to handle changing the doccuments them selces (our inputs). therefore The descirption box for edition should only ever be opened up or placed into an unlocked state in the event that that all cards are rejected OR no cards are selected, the same applies for the apply button for the description. If a card is selcted and description apply button is selcted somehow then popup message an issue message and then reject the change. notifiy the user that becuase the part number is slected that this cannot be done as the part already exists in the company inventory. This same logic needs to be applied ot the comppnay part number button and chager. for now hide the COMN suggester, it is a feature which needs more work. 
    def _apply_description(self):
        node_id = self.current_node_id
        if not node_id:
            return
        desc = (self.desc_var.get() or "").strip()
        try:
            self.controller.set_node_description(node_id, desc)
        except Exception as e:
            self._set_status(f"Description update failed: {e}")
            return

        # Refresh current node views without flicker
        try:
            fresh = self.controller.get_node(node_id)
            self._render_header_state(fresh)
            self._render_cards(fresh)
            # keep right panel on pinned alt if possible
            if self._pinned_alt_id:
                payload = self.controller.get_alt_detail_payload(node_id, self._pinned_alt_id)
                self._render_specs(payload)
            else:
                # pick first active card
                first = None
                try:
                    for a in (fresh.alternates or []):
                        if not getattr(a, 'rejected', False):
                            first = a
                            break
                except Exception:
                    first = None
                if first:
                    payload = self.controller.get_alt_detail_payload(node_id, getattr(first,'id',None))
                    self._render_specs(payload)
            self._set_status("Description updated")
        except Exception:
            pass

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
            msg = str(e)
            if "Description not confirmed" in msg:
                messagebox.showwarning("Description Not Confirmed", msg)
                return
            messagebox.showerror("Mark Ready Failed", msg)


    def _on_auto_reject_all(self):
        if not self.current_node_id:
            messagebox.showwarning("No Selection", "Select a node first.")
            return
        try:
            node = self.controller.get_node(self.current_node_id)
            if not node:
                return
            if getattr(node, "locked", False):
                messagebox.showwarning("Node Locked", "Unmark Ready before rejecting alternates.")
                return
            alts = list(getattr(node, "alternates", []) or [])
            todo = [a for a in alts if not getattr(a, "rejected", False)]
            if not todo:
                self.status_var.set("No active cards to reject.")
                return
            # batch controller updates, then single UI refresh for smoothness
            for a in todo:
                try:
                    self.controller.reject_alternate(node.id, a.id)
                except Exception:
                    pass
            self._pinned_alt_id = None
            fresh = self._refresh_after_alt_action(node.id, focus_alt_id=None, refresh_table=True)
            remaining = sum(1 for a in (getattr(fresh, 'alternates', []) or []) if not getattr(a, 'rejected', False)) if fresh else 0
            self.status_var.set(f"Auto rejected cards for node {node.id}. Remaining active: {remaining}.")
        except Exception as e:
            messagebox.showerror("Auto Reject Failed", str(e))

    def _on_unmark_ready(self):
        if not self.current_node_id:
            messagebox.showwarning("No Selection", "Select a node first.")
            return
        try:
            self.controller.unmark_ready(self.current_node_id)
            node = self.controller.get_node(self.current_node_id)
            self._render_header_state(node)
            self._render_cards(node)
            self._render_specs_for_node(node)
            self.refresh_node_table()
        except Exception as e:
            messagebox.showerror("Unmark Ready Failed", str(e))

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
