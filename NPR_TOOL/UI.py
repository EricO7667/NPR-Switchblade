

from __future__ import annotations

import threading
import traceback
from typing import Optional

import customtkinter as ctk
import tkinter as tk
from tkinter import ttk, filedialog, messagebox, simpledialog

from .data_models import DecisionNode, DecisionStatus, Alternate, DecisionCard, CardSection, NodeHeaderState, CardDetailState
from typing import TYPE_CHECKING
import time

if TYPE_CHECKING:
    from .decision_controller import DecisionController


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
        self._focused_card_id: Optional[str] = None
        self._pinned_alt_id: Optional[str] = None
        self._last_specs_key = None
        self._suspend_node_select = False
        self._pin_click_guard_card = None
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
        self._suspend_company_pn_trace = False
        self._suspend_approval_toggle = False
        self.company_pn_var.trace_add("write", self._on_company_pn_var_changed)
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


    def _on_company_pn_var_changed(self, *_args):
        if getattr(self, "_suspend_company_pn_trace", False):
            return
        if not getattr(self, "current_node_id", None):
            return
        try:
            self.controller.stage_header_company_pn(self.current_node_id, self.company_pn_var.get())
        except Exception:
            pass

    def _on_desc_var_changed(self, *_args):
        if getattr(self, "_suspend_desc_trace", False):
            return
        if not getattr(self, "current_node_id", None):
            return
        try:
            node = self.controller.get_node(self.current_node_id)
            if bool(getattr(node, "locked", False)):
                return
            self.controller.stage_header_description(self.current_node_id, self.desc_var.get())
            self.controller.apply_header_description(self.current_node_id)
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
        try:
            self.cards_host.bind("<Configure>", self._on_bottom_panel_configure, add="+")
        except Exception:
            pass

        # right specs
        self.specs_host = ctk.CTkFrame(parent, corner_radius=12, fg_color="#0F172A")
        hpane.add(self.specs_host, weight=2)

        self.specs_scroll = ctk.CTkScrollableFrame(self.specs_host, fg_color="#0F172A")
        self.specs_scroll.pack(fill="both", expand=True, padx=10, pady=10)
        try:
            self.specs_host.bind("<Configure>", self._on_bottom_panel_configure, add="+")
        except Exception:
            pass

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

    def _on_bottom_panel_configure(self, _event=None):
        try:
            if hasattr(self, "_panel_refresh_after_id") and self._panel_refresh_after_id:
                self.root.after_cancel(self._panel_refresh_after_id)
        except Exception:
            pass
        try:
            self._panel_refresh_after_id = self.root.after(120, self._refresh_current_node_panels)
        except Exception:
            self._refresh_current_node_panels()

    def _refresh_current_node_panels(self):
        self._panel_refresh_after_id = None
        if not self.current_node_id:
            return
        try:
            fresh = self.controller.get_node(self.current_node_id)
            detail = self.controller.get_node_detail_state(self.current_node_id, card_id=self._focused_card_id, auto_focus=False)
            self._render_cards(fresh)
            self._render_detail_state(fresh, detail)
        except Exception:
            pass

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
            detail = self.controller.get_node_detail_state(node.id, auto_focus=True)
        except Exception:
            detail = CardDetailState(node_id=node.id)

        self._focused_card_id = getattr(detail, "card_id", "") or None
        self._pinned_alt_id = getattr(detail, "alt_id", "") or None

        try:
            self.desc_var.set(getattr(node, 'description', '') or '')
        except Exception:
            pass

        self._render_header_state(node)
        self._render_cards(node)
        self._render_detail_state(node, detail)

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


    def _resolve_header_state(self, node_or_id, rebuild: bool = False) -> NodeHeaderState:
        """Fetch the controller-owned upper-panel state for a node."""
        node_id = getattr(node_or_id, "id", node_or_id)
        return self.controller.get_node_header_state(str(node_id), rebuild=bool(rebuild))


    def _render_header_state(self, node: DecisionNode):
        """Render the upper panel strictly from controller-owned header state."""
        try:
            header = self._resolve_header_state(node, rebuild=True)
        except Exception:
            header = self.controller.get_node_header_state(getattr(node, "id", node), rebuild=True)

        self.h_title.configure(text=getattr(header, "title_text", "Company PN: —"))
        self.h_desc.configure(text=getattr(header, "subtitle_text", "BOM MPN: —"))
        self.h_meta.configure(text=getattr(header, "status_text", ""))

        self.suggested_var.set(getattr(header, "suggested_company_part_number", "") or "")

        self._suspend_desc_trace = True
        self._suspend_company_pn_trace = True
        try:
            self.company_pn_var.set(getattr(header, "company_part_number", "") or "")
            self.desc_var.set(getattr(header, "description_text", "") or "")
        finally:
            self._suspend_company_pn_trace = False
            self._suspend_desc_trace = False

        try:
            self._suspend_bom_section_event = True
            self.bom_section_var.set(getattr(header, "bom_section", "") or "SURFACE MOUNT")
        except Exception:
            self.bom_section_var.set("SURFACE MOUNT")
        finally:
            self._suspend_bom_section_event = False

        try:
            self._suspend_approval_toggle = True
            self.include_approval_var.set(bool(getattr(header, "include_approval", False)))
        finally:
            self._suspend_approval_toggle = False

        controls = getattr(header, "controls", None)

        try:
            self.mark_ready_btn.configure(
                state=("normal" if bool(getattr(controls, "mark_ready_enabled", True)) else "disabled")
            )
            self.unmark_ready_btn.configure(
                state=("normal" if bool(getattr(controls, "unmark_ready_enabled", False)) else "disabled")
            )
            self.bom_section_menu.configure(
                state=("normal" if bool(getattr(controls, "bom_section_editable", False)) else "disabled")
            )
            self.auto_reject_btn.configure(
                state=("normal" if bool(getattr(controls, "auto_reject_enabled", True)) else "disabled")
            )
            self.company_pn_entry.configure(
                state=("normal" if bool(getattr(controls, "company_pn_editable", False)) else "disabled")
            )
            self.apply_pn_btn.configure(
                state=("normal" if bool(getattr(controls, "apply_pn_enabled", False)) else "disabled")
            )
            self.desc_entry.configure(
                state=("normal" if bool(getattr(controls, "description_editable", False)) else "disabled")
            )
            self.include_approval_chk.configure(
                state=("normal" if bool(getattr(controls, "approval_editable", False)) else "disabled")
            )
            self.load_fake_external_btn.configure(
                state=("normal" if bool(getattr(controls, "load_external_enabled", False)) else "disabled")
            )
        except Exception:
            pass


    def _render_cards(self, node: DecisionNode):
        for w in self.cards_scroll.winfo_children():
            w.destroy()

        try:
            cards = list(self.controller.build_node_cards(node.id, focused_card_id=self._focused_card_id) or [])
        except Exception:
            cards = []

        valid_card_ids = {c.card_id for c in cards if getattr(c, "card_id", "")}
        self._focused_card_id = self._focused_card_id if self._focused_card_id in valid_card_ids else None
        focused_card = None
        if self._focused_card_id:
            focused_card = next((c for c in cards if getattr(c, "card_id", "") == self._focused_card_id), None)
        self._pinned_alt_id = getattr(focused_card, "alt_id", None) if focused_card is not None else None

        if not cards:
            ctk.CTkLabel(self.cards_scroll, text="No alternates/candidates yet.", text_color=self.COLORS["text_dim"]).pack(anchor="w", padx=8, pady=8)
            return

        section_map = [
            (CardSection.INTERNAL_MATCHES, "Internal Matches"),
            (CardSection.EXTERNAL_ALTERNATES, "External Alternates"),
            (CardSection.REJECTED, "REJECTED"),
        ]
        for section, label in section_map:
            group = [card for card in cards if getattr(card, "section", None) == section]
            if section == CardSection.REJECTED and not group:
                continue
            self._build_card_section(parent=self.cards_scroll, title=f"{label} ({len(group)})", cards=group, node=node)

    def _build_card_section(self, parent, title: str, cards: list[DecisionCard], node: DecisionNode):
        section = ctk.CTkFrame(parent, corner_radius=10, fg_color="#111827")
        section.pack(fill="x", padx=4, pady=6)

        ctk.CTkLabel(
            section,
            text=title,
            font=ctk.CTkFont(size=13, weight="bold"),
            text_color=self.COLORS["text"]
        ).pack(anchor="w", padx=10, pady=(8, 4))

        if not cards:
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
        used_cols = max(1, min(cols, len(cards)))

        for c in range(used_cols):
            holder.grid_columnconfigure(c, weight=0, minsize=card_width)

        for i, card in enumerate(cards):
            r = i // used_cols
            c = i % used_cols
            card_widget = self._create_card(parent=holder, node=node, card=card, card_width=card_width)
            card_widget.grid(row=r, column=c, padx=6, pady=6, sticky="nw")

    def _create_card(self, parent, node: DecisionNode, card: DecisionCard, card_width: int | None = None):
        alt = getattr(card, "alternate", None)
        if alt is None:
            raise ValueError("DecisionCard is missing its backing Alternate reference.")

        border = self.COLORS["card_border"]
        border_role = getattr(getattr(card, "display", None), "border_role", "default")
        if border_role == "selected":
            border = self.COLORS["success"]
        elif border_role == "rejected":
            border = self.COLORS["danger"]
        elif border_role == "pinned":
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

        head = ctk.CTkFrame(inner, fg_color="transparent")
        head.pack(fill="x", padx=10, pady=(8, 4))

        left = ctk.CTkFrame(head, fg_color="transparent")
        left.pack(side="left", fill="x", expand=True)

        ctk.CTkLabel(
            left,
            text=getattr(card.display, "title", "—") or "—",
            anchor="w",
            font=ctk.CTkFont(size=13, weight="bold")
        ).pack(anchor="w")

        subtitle = getattr(card.display, "subtitle", "") or ""
        if subtitle:
            ctk.CTkLabel(
                left,
                text=subtitle,
                anchor="w",
                text_color=self.COLORS["text_dim"]
            ).pack(anchor="w")

        if getattr(card, "is_inventory", False) and int(getattr(card.display, "mfgpn_count", 0) or 0) >= 1:
            ctk.CTkLabel(
                left,
                text=f"MFGPNs: {int(getattr(card.display, 'mfgpn_count', 0) or 0)}",
                text_color=self.COLORS["text_dim"]
            ).pack(anchor="w")

        ctk.CTkLabel(
            head,
            text=getattr(card.display, "confidence_text", "0%"),
            width=54,
            corner_radius=8,
            fg_color="#1E3A8A",
            text_color="white"
        ).pack(side="right", padx=(8, 0))

        ctk.CTkLabel(
            inner,
            text=getattr(card.display, "description", "(no description)"),
            wraplength=wrap_width,
            justify="left",
            text_color=self.COLORS["text_dim"]
        ).pack(anchor="w", padx=10, pady=(0, 6))

        ctk.CTkLabel(
            inner,
            text=f"Source: {getattr(card.display, 'source_label', '-') or '-'}    Stock: {getattr(card.display, 'stock_label', '-') or '-'}",
            text_color=self.COLORS["text_dim"]
        ).pack(anchor="w", padx=10, pady=(0, 4))

        badge_row = ctk.CTkFrame(inner, fg_color="transparent", height=24)
        badge_row.pack(fill="x", padx=10, pady=(0, 4))
        badge_row.pack_propagate(False)

        for badge in list(getattr(card.display, "badges", []) or []):
            badge_upper = str(badge).upper()
            fg = "#1E3A8A"
            if badge_upper == "LOCKED IN":
                fg = "#14532D"
            elif badge_upper == "REJECTED":
                fg = "#7F1D1D"
            ctk.CTkLabel(
                badge_row,
                text=badge,
                fg_color=fg,
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
            command=lambda c=card: self._copy_to_clipboard(self._card_copy_text_from_card(c), toast="Copied card details")
        ).pack(side="left")

        if not bool(getattr(node, "locked", False)):
            if bool(getattr(card.state, "rejected", False)):
                ctk.CTkButton(
                    right_actions,
                    text="Unreject",
                    width=82,
                    height=26,
                    fg_color="#374151",
                    hover_color="#4B5563",
                    command=lambda c=card: self._unreject_card(node, c)
                ).pack(side="right")
            elif bool(getattr(card.state, "selected", False)):
                ctk.CTkButton(
                    right_actions,
                    text="Unlock",
                    width=72,
                    height=26,
                    fg_color="#14532D",
                    hover_color="#166534",
                    command=lambda c=card: self._toggle_card_selection(node, c)
                ).pack(side="right")
            else:
                ctk.CTkButton(
                    right_actions,
                    text="Reject",
                    width=72,
                    height=26,
                    fg_color="#374151",
                    hover_color="#4B5563",
                    command=lambda c=card: self._reject_card(node, c)
                ).pack(side="right", padx=(0, 6))

                ctk.CTkButton(
                    right_actions,
                    text="Add",
                    width=64,
                    height=26,
                    fg_color=self.COLORS["success"],
                    hover_color="#16A34A",
                    command=lambda c=card: self._toggle_card_selection(node, c)
                ).pack(side="right")

        def _bind_recursive(widget):
            try:
                if widget is not actions and not isinstance(widget, ctk.CTkButton):
                    widget.bind("<Button-1>", lambda _e=None, c=card: self._pin_card(node, c))
            except Exception:
                pass

            if widget is actions:
                return

            for ch in getattr(widget, "winfo_children", lambda: [])():
                _bind_recursive(ch)

        _bind_recursive(inner)
        return outer


    def _pin_card(self, node: DecisionNode, card: DecisionCard):
        import time
        cid = getattr(card, "card_id", None)
        now = time.monotonic()
        if self._pin_click_guard_card == cid and now < getattr(self, '_pin_click_guard_until', 0):
            return "break"
        self._pin_click_guard_card = cid
        self._pin_click_guard_until = now + 0.08
        try:
            focused = self.controller.focus_card(node.id, cid)
        except Exception:
            return "break"
        self._focused_card_id = getattr(focused, "card_id", None)
        self._pinned_alt_id = getattr(focused, "alt_id", None)
        fresh = self.controller.get_node(node.id)
        try:
            detail = self.controller.get_node_detail_state(node.id, card_id=cid, auto_focus=True)
        except Exception:
            detail = CardDetailState(node_id=node.id)
        self._render_cards(fresh)
        self._render_detail_state(fresh, detail)
        return "break"

    # LEGACY: retained during the card-model migration.
    def _render_cards_legacy(self, node: DecisionNode):
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

        self._build_card_section_legacy(self.cards_scroll, f"Internal Matches ({len(internal_active)})", internal_active, node)
        self._build_card_section_legacy(self.cards_scroll, f"External Alternates ({len(external_active)})", external_active, node)
        if rejected_all:
            self._build_card_section_legacy(self.cards_scroll, f"REJECTED ({len(rejected_all)})", rejected_all, node)

    def _render_specs_for_node(self, node: DecisionNode):
        try:
            detail = self.controller.get_node_detail_state(node.id, auto_focus=True)
        except Exception:
            detail = CardDetailState(node_id=getattr(node, "id", ""))
        self._render_detail_state(node, detail)

    def _render_specs_for_card(self, node: DecisionNode, card: Optional[DecisionCard]):
        try:
            detail = self.controller.get_node_detail_state(
                node.id,
                card_id=(getattr(card, "card_id", "") if card is not None else None),
                auto_focus=True,
            )
        except Exception:
            detail = CardDetailState(node_id=getattr(node, "id", ""))
        self._render_detail_state(node, detail)

    def _render_specs_for_alt(self, node: DecisionNode, alt: Optional[Alternate]):
        if alt is None:
            self._render_specs_for_node(node)
            return
        try:
            card = self.controller.get_card_for_alt(node.id, getattr(alt, "id", ""))
        except Exception:
            card = None
        if card is None:
            self._render_specs_for_node(node)
            return
        self._render_specs_for_card(node, card)

    def _render_detail_state(self, node: DecisionNode, detail: CardDetailState):
        for w in self.specs_scroll.winfo_children():
            w.destroy()

        self._last_specs_key = (
            getattr(detail, "node_id", ""),
            getattr(detail, "card_id", ""),
            getattr(detail, "alt_id", ""),
        )
        self._focused_card_id = getattr(detail, "card_id", "") or None
        self._pinned_alt_id = getattr(detail, "alt_id", "") or None

        title_txt = getattr(detail, "title_text", "Information") or "Information"
        self.specs_title = ctk.CTkLabel(self.specs_scroll, text=title_txt, font=ctk.CTkFont(size=14, weight="bold"))
        self.specs_title.pack(anchor="w", pady=(0, 8))

        if not bool(getattr(detail, "has_card", False)):
            ctk.CTkLabel(self.specs_scroll, text="No details available.", text_color=self.COLORS["text_dim"]).pack(anchor="w")
            return

        specs = dict(getattr(detail, "specs", {}) or {})
        self._render_specs(specs)

        export_mfgpn_options = [str(m).strip() for m in list(getattr(detail, "export_mfgpn_options", []) or []) if str(m).strip()]
        selected_export_mfgpn = str(getattr(detail, "selected_export_mfgpn", "") or specs.get("VendorItem", "") or "").strip()
        if bool(getattr(detail, "is_inventory", False)) and export_mfgpn_options:
            ctk.CTkLabel(
                self.specs_scroll,
                text='Choose export MFG PN',
                text_color=self.COLORS['primary'],
                font=ctk.CTkFont(size=11, weight='bold')
            ).pack(anchor='w', pady=(10, 4))
            seen = set()
            for mpn in export_mfgpn_options[:40]:
                mpn = str(mpn).strip()
                if not mpn or mpn.lower() in seen:
                    continue
                seen.add(mpn.lower())
                is_selected = bool(selected_export_mfgpn and mpn.lower() == selected_export_mfgpn.lower())
                b = ctk.CTkButton(
                    self.specs_scroll,
                    text=mpn,
                    height=28,
                    fg_color=(self.COLORS["success"] if is_selected else '#111827'),
                    hover_color=('#16A34A' if is_selected else '#1F2937'),
                    command=lambda m=mpn, cid=getattr(detail, "card_id", ""): self._set_group_mfgpn_for_card(node.id, cid, m),
                )
                b.pack(anchor='w', pady=2)

        selected_external_only = False
        try:
            export_state = self.controller.build_committed_export_state(node)
            selected_external_only = bool(export_state.has_selected_external) and not bool(export_state.has_internal)
        except Exception:
            selected_external_only = False

        if selected_external_only:
            try:
                current_exclude = bool(self.controller.get_exclude_customer_part_number_in_npr(node.id))
            except Exception:
                current_exclude = False
            self.exclude_customer_pn_npr_var.set(current_exclude)
            ctk.CTkCheckBox(
                self.specs_scroll,
                text="Do not include customer part number in NPR",
                variable=self.exclude_customer_pn_npr_var,
                command=lambda nid=node.id: self._toggle_exclude_customer_pn_npr(nid),
            ).pack(anchor="w", pady=(10, 2))

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
            ("Supplier", pick("PrimaryVendorNumber", "Supplier")),
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


    def _set_group_mfgpn_for_card(self, node_id: str, card_id: str, mfgpn: str):
        try:
            focus_card = self.controller.set_preferred_inventory_mfgpn_for_card(node_id, card_id, mfgpn)
            self._focused_card_id = getattr(focus_card, 'card_id', None)
            self._pinned_alt_id = getattr(focus_card, 'alt_id', None)
            self._refresh_after_card_action(node_id, focus_card_id=getattr(focus_card, 'card_id', None), refresh_table=True)
        except Exception as e:
            messagebox.showerror('Set MFG PN Failed', str(e))

    def _set_group_mfgpn(self, node: DecisionNode, alt: Alternate, mfgpn: str):
        try:
            card = self.controller.get_card_for_alt(node.id, getattr(alt, 'id', None))
            if card is None:
                raise ValueError('Unable to resolve card for alternate.')
            self._set_group_mfgpn_for_card(node.id, card.card_id, mfgpn)
        except Exception as e:
            messagebox.showerror('Set MFG PN Failed', str(e))

    def _display_stock_for_card(self, card: DecisionCard) -> str:
        alt = getattr(card, 'alternate', None)
        if alt is None:
            return '-'
        return self._display_stock_for_alt(alt)

    def _display_stock_for_alt(self, alt: Alternate):
        try:
            if getattr(alt, "raw", None) is not None:
                return self._specs_from_inventory(alt.raw).get("TotalQty") or "-"
            v = getattr(alt, "stock", None)
            return "-" if v in (None, "") else str(v)
        except Exception:
            return "-"

    def _card_copy_text_from_card(self, card: DecisionCard) -> str:
        if card is None:
            return ""
        parts = []
        if getattr(card, "company_part_number", ""):
            parts.append(f"Company PN: {card.company_part_number}")
        if getattr(card, "manufacturer_part_number", ""):
            parts.append(f"MFG PN: {card.manufacturer_part_number}")
        if getattr(card, "manufacturer", ""):
            parts.append(f"MFG: {card.manufacturer}")
        parts.append(f"Source: {getattr(card, 'source', '') or '-'}")
        parts.append(f"Stock: {self._display_stock_for_card(card)}")
        description = getattr(getattr(card, 'display', None), 'description', '') or getattr(getattr(card, 'alternate', None), 'description', '')
        if description:
            parts.append(f"Description: {description}")
        return "\n".join(parts)

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
        focus_card_id = None
        if focus_alt_id:
            try:
                card = self.controller.get_card_for_alt(node_id, focus_alt_id)
                focus_card_id = getattr(card, 'card_id', None) if card is not None else None
            except Exception:
                focus_card_id = None
        return self._refresh_after_card_action(node_id, focus_card_id=focus_card_id, refresh_table=refresh_table)

    def _refresh_after_card_action(self, node_id: str, focus_card_id: str | None = None, refresh_table: bool = True):
        fresh = self.controller.get_node(node_id)
        try:
            detail = self.controller.get_node_detail_state(node_id=node_id, card_id=focus_card_id, auto_focus=True)
        except Exception:
            detail = CardDetailState(node_id=node_id)
        self._focused_card_id = getattr(detail, "card_id", "") or None
        self._pinned_alt_id = getattr(detail, "alt_id", "") or None
        self._render_header_state(fresh)
        self._render_cards(fresh)
        self._render_detail_state(fresh, detail)
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
            self.controller.stage_header_approval(self.current_node_id, bool(self.include_approval_var.get()))
            self.controller.apply_header_approval(self.current_node_id)
            fresh = self.controller.get_node(self.current_node_id)
            self._render_header_state(fresh)
            self.refresh_node_table()
        except Exception as e:
            messagebox.showerror("Approval Flag Failed", str(e))

    def _toggle_card_selection(self, node: DecisionNode, card: DecisionCard):
        try:
            alt = getattr(card, "alternate", None)
            if alt is None:
                raise ValueError("Card has no backing alternate.")
            if bool(getattr(card.state, 'selected', False)):
                self.controller.unselect_card(node.id, card.card_id)
            else:
                is_external = (getattr(card, 'source', '') or '').lower() != 'inventory'
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
                self.controller.select_card(node.id, card.card_id)
            self._focused_card_id = card.card_id
            self._pinned_alt_id = getattr(card, 'alt_id', None)
            self._refresh_after_card_action(node.id, focus_card_id=card.card_id, refresh_table=True)
        except Exception as e:
            messagebox.showerror("Select Card Failed", str(e))

    def _reject_card(self, node: DecisionNode, card: DecisionCard):
        try:
            resolved = self.controller.reject_card(node.id, card.card_id)
            resolved_id = getattr(resolved, 'card_id', None) if resolved is not None and not bool(getattr(getattr(resolved, 'state', None), 'rejected', False)) else None
            resolved_alt_id = getattr(resolved, 'alt_id', None) if resolved_id else None
            self._focused_card_id = resolved_id
            self._pinned_alt_id = resolved_alt_id
            self._refresh_after_card_action(node.id, focus_card_id=resolved_id, refresh_table=True)
        except Exception as e:
            messagebox.showerror("Reject Card Failed", str(e))

    def _unreject_card(self, node: DecisionNode, card: DecisionCard):
        try:
            self.controller.unreject_card(node.id, card.card_id)
            self._focused_card_id = card.card_id
            self._pinned_alt_id = getattr(card, 'alt_id', None)
            self._refresh_after_card_action(node.id, focus_card_id=card.card_id, refresh_table=True)
        except Exception as e:
            messagebox.showerror("Unreject Card Failed", str(e))

    # Legacy alternate-driven handlers kept for fallback paths during migration.
    def _add_alt(self, node: DecisionNode, alt: Alternate):
        try:
            card = self.controller.get_card_for_alt(node.id, getattr(alt, 'id', ''))
            if card is None:
                raise ValueError("Unable to resolve card for alternate.")
            self._toggle_card_selection(node, card)
        except Exception as e:
            messagebox.showerror("Select Alternate Failed", str(e))

    def _reject_alt(self, node: DecisionNode, alt: Alternate):
        try:
            card = self.controller.get_card_for_alt(node.id, getattr(alt, 'id', ''))
            if card is None:
                raise ValueError("Unable to resolve card for alternate.")
            self._reject_card(node, card)
        except Exception as e:
            messagebox.showerror("Reject Alternate Failed", str(e))

    def _unreject_alt(self, node: DecisionNode, alt: Alternate):
        try:
            card = self.controller.get_card_for_alt(node.id, getattr(alt, 'id', ''))
            if card is None:
                raise ValueError("Unable to resolve card for alternate.")
            self._unreject_card(node, card)
        except Exception as e:
            messagebox.showerror("Unreject Alternate Failed", str(e))

    def _toggle_exclude_customer_pn_npr(self, node_id: str):
        try:
            self.controller.set_exclude_customer_part_number_in_npr(node_id, bool(self.exclude_customer_pn_npr_var.get()))
            self._set_status("Updated NPR customer part number option", ok=True)
        except Exception as e:
            messagebox.showerror("NPR Option Failed", str(e))

    def _on_bom_section_changed(self, _choice=None):
        if getattr(self, "_suspend_bom_section_event", False):
            return
        node_id = self.current_node_id
        if not node_id:
            return
        try:
            self.controller.stage_header_bom_section(node_id, self.bom_section_var.get())
            header = self.controller.apply_header_bom_section(node_id)
            sec = getattr(header, "bom_section", self.bom_section_var.get()) or "SURFACE MOUNT"
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
            self.controller.stage_header_company_pn(self.current_node_id, pn)
            self.controller.apply_header_company_pn(self.current_node_id)
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
            self.controller.stage_header_description(node_id, desc)
            self.controller.apply_header_description(node_id)
        except Exception as e:
            self._set_status(f"Description update failed: {e}")
            return

        try:
            fresh = self.controller.get_node(node_id)
            self._render_header_state(fresh)
            self._render_cards(fresh)
            self._render_specs_for_node(fresh)
            self._set_status("Description updated")
        except Exception:
            pass

    def _on_mark_ready(self):
        if not self.current_node_id:
            messagebox.showwarning("No Selection", "Select a node first.")
            return
        try:
            self.controller.mark_ready_from_header(self.current_node_id)
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

            self.controller.auto_reject_all_from_header(node.id)
            self._focused_card_id = None
            self._pinned_alt_id = None
            fresh = self._refresh_after_card_action(node.id, focus_card_id=None, refresh_table=True)
            remaining = sum(1 for a in (getattr(fresh, 'alternates', []) or []) if not getattr(a, 'rejected', False)) if fresh else 0
            self.status_var.set(f"Auto rejected cards for node {node.id}. Remaining active: {remaining}.")
        except Exception as e:
            messagebox.showerror("Auto Reject Failed", str(e))

    def _on_unmark_ready(self):
        if not self.current_node_id:
            messagebox.showwarning("No Selection", "Select a node first.")
            return
        try:
            self.controller.unmark_ready_from_header(self.current_node_id)
            node = self.controller.get_node(self.current_node_id)
            self._render_header_state(node)
            self._render_cards(node)
            self._render_specs_for_node(node)
            self.refresh_node_table()
        except Exception as e:
            messagebox.showerror("Unmark Ready Failed", str(e))

    def _set_status(self, message: str, ok: Optional[bool] = None):
        try:
            self.status_var.set(str(message or ""))
            self.root.update_idletasks()
        except Exception:
            pass

    def _copy_to_clipboard(self, text: str, toast: Optional[str] = None):
        try:
            self.root.clipboard_clear()
            self.root.clipboard_append(str(text or ""))
            if toast:
                self._set_status(toast)
        except Exception as e:
            messagebox.showerror("Copy Failed", str(e))

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

        ctk.CTkLabel(
            frame,
            textvariable=self._loading_phase_var,
            font=ctk.CTkFont(size=16, weight="bold"),
        ).pack(anchor="w", pady=(4, 8), padx=10)

        self._loading_bar = ctk.CTkProgressBar(frame)
        self._loading_bar.pack(fill="x", padx=10, pady=(0, 8))
        self._loading_bar.set(0.0)

        ctk.CTkLabel(
            frame,
            textvariable=self._loading_detail_var,
            text_color=self.COLORS["text_dim"],
        ).pack(anchor="w", padx=10)

        btn_row = ctk.CTkFrame(frame, fg_color="transparent")
        btn_row.pack(fill="x", padx=10, pady=(10, 0))
        ctk.CTkButton(
            btn_row,
            text="Stop",
            width=90,
            fg_color="#7F1D1D",
            hover_color="#991B1B",
            command=self._stop_matching,
        ).pack(side="right")

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

    def _stop_matching(self):
        try:
            self.controller.request_stop_matching()
            self._set_status("Stop requested. Waiting for matching to halt...")
            if self._loading_detail_var is not None:
                self._loading_detail_var.set("Stop requested...")
        except Exception as e:
            messagebox.showerror("Stop Matching Failed", str(e))

    def _loading_phase_callback(self, message: str, determinate: bool = True):
        def _apply():
            text = str(message or "Working...")
            try:
                if self._loading_phase_var is not None:
                    self._loading_phase_var.set(text)
            except Exception:
                pass
            self._set_status(text)

        try:
            self.root.after(0, _apply)
        except Exception:
            _apply()

    def _loading_progress_callback(self, *args):
        ratio = 0.0
        detail = ""
        try:
            if len(args) == 1:
                ratio = float(args[0] or 0.0)
                detail = f"{int(max(0.0, min(1.0, ratio)) * 100)}% complete"
            elif len(args) >= 2:
                cur = float(args[0] or 0)
                total = float(args[1] or 1)
                ratio = 0.0 if total <= 0 else cur / total
                msg = str(args[2]) if len(args) >= 3 else "Working..."
                detail = f"{msg} ({int(cur)}/{int(total)})"
            else:
                detail = "Working..."
        except Exception:
            ratio = 0.0
            detail = "Working..."

        ratio = max(0.0, min(1.0, float(ratio)))

        def _apply():
            try:
                if self._loading_bar is not None:
                    self._loading_bar.set(ratio)
            except Exception:
                pass
            try:
                if self._loading_detail_var is not None and detail:
                    self._loading_detail_var.set(detail)
            except Exception:
                pass
            self._set_status(detail or f"{int(ratio * 100)}% complete")

        try:
            self.root.after(0, _apply)
        except Exception:
            _apply()

    def _load_inventory(self):
        path = filedialog.askopenfilename(
            title="Select Master Inventory Workbook",
            filetypes=[("Excel files", "*.xlsx *.xlsm *.xls"), ("All files", "*.*")],
        )
        if not path:
            return

        self._show_loading_popup("Loading Master Inventory", "Reading workbook...")
        self._set_status("Loading master inventory...")

        def worker():
            try:
                count = self.controller.load_inventory(
                    path,
                    progress_cb=self._loading_progress_callback,
                    phase_cb=self._loading_phase_callback,
                )
                self.root.after(0, lambda: self._on_inventory_loaded(count))
            except Exception as e:
                self.root.after(0, lambda err=str(e): self._on_inventory_load_failed(err))

        threading.Thread(target=worker, daemon=True, name="load-master-inventory").start()

    def _on_inventory_loaded(self, count: int):
        self._close_loading_popup()
        self._set_status(f"Loaded master inventory: {count} items")
        self.current_node_id = None
        self.refresh_node_table()
        self._render_empty_state()

    def _on_inventory_load_failed(self, err: str):
        self._close_loading_popup()
        messagebox.showerror("Load Master Inventory Failed", str(err))

    def _load_items_sheet(self):
        path = filedialog.askopenfilename(
            title="Select ERP Inventory Workbook",
            filetypes=[("Excel files", "*.xlsx *.xlsm *.xls"), ("All files", "*.*")],
        )
        if not path:
            return

        self._show_loading_popup("Loading ERP Inventory", "Reading workbook...")
        self._set_status("Loading ERP inventory...")

        def worker():
            try:
                count = self.controller.load_items_inventory(
                    path,
                    progress_cb=self._loading_progress_callback,
                    phase_cb=self._loading_phase_callback,
                )
                self.root.after(0, lambda: self._on_items_inventory_loaded(count))
            except Exception as e:
                self.root.after(0, lambda err=str(e): self._on_items_inventory_load_failed(err))

        threading.Thread(target=worker, daemon=True, name="load-erp-inventory").start()

    def _on_items_inventory_loaded(self, count: int):
        self._close_loading_popup()
        self._set_status(f"Loaded ERP inventory rows: {count}")
        self.current_node_id = None
        self.refresh_node_table()
        self._render_empty_state()

    def _on_items_inventory_load_failed(self, err: str):
        self._close_loading_popup()
        messagebox.showerror("Load ERP Inventory Failed", str(err))

    def _load_bom(self):
        path = filedialog.askopenfilename(
            title="Select BOM Workbook",
            filetypes=[("Excel files", "*.xlsx *.xlsm *.xls"), ("All files", "*.*")],
        )
        if not path:
            return
        try:
            count = self.controller.load_npr(path)
            self.current_node_id = None
            self._focused_card_id = None
            self._pinned_alt_id = None
            self.refresh_node_table()
            self._render_empty_state()
            self._set_status(f"Loaded BOM lines: {count}")
        except Exception as e:
            messagebox.showerror("Load BOM Failed", str(e))

    def _save_workspace(self):
        try:
            saved = self.controller.save_workspace_state()
            self._set_status(f"Workspace saved ({saved} node(s)).")
        except Exception as e:
            messagebox.showerror("Save Workspace Failed", str(e))

    def _choose_workspace_id(self, workspaces: list[dict]) -> str:
        if not workspaces:
            return ""
        if len(workspaces) == 1:
            return str(workspaces[0].get("workspace_id") or "")

        chosen = {"workspace_id": ""}
        win = ctk.CTkToplevel(self.root)
        win.title("Open Workspace")
        win.geometry("720x360")
        win.transient(self.root)
        win.grab_set()

        frame = ctk.CTkFrame(win, corner_radius=12)
        frame.pack(fill="both", expand=True, padx=12, pady=12)

        ctk.CTkLabel(frame, text="Select a workspace", font=ctk.CTkFont(size=16, weight="bold")).pack(anchor="w", padx=10, pady=(10, 6))

        lb = tk.Listbox(frame, height=12)
        lb.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        for ws in workspaces:
            wsid = str(ws.get("workspace_id") or "")
            label = str(ws.get("name") or "")
            updated = str(ws.get("updated_at") or "")
            lb.insert("end", f"{wsid}  |  {label}  |  {updated}")
        if workspaces:
            lb.selection_set(0)

        def _accept(_event=None):
            sel = lb.curselection()
            if not sel:
                return
            chosen["workspace_id"] = str(workspaces[int(sel[0])].get("workspace_id") or "")
            try:
                win.grab_release()
            except Exception:
                pass
            win.destroy()

        def _cancel():
            try:
                win.grab_release()
            except Exception:
                pass
            win.destroy()

        btns = ctk.CTkFrame(frame, fg_color="transparent")
        btns.pack(fill="x", padx=10, pady=(0, 10))
        ctk.CTkButton(btns, text="Open", command=_accept).pack(side="right")
        ctk.CTkButton(btns, text="Cancel", fg_color="#374151", hover_color="#4B5563", command=_cancel).pack(side="right", padx=(0, 8))

        lb.bind("<Double-1>", _accept)
        win.wait_window()
        return str(chosen["workspace_id"] or "").strip()

    def _open_workspace(self):
        try:
            workspaces = self.controller.list_workspaces(status="ACTIVE")
        except Exception as e:
            messagebox.showerror("Open Workspace Failed", str(e))
            return

        if not workspaces:
            messagebox.showinfo("Open Workspace", "No saved workspaces were found.")
            return

        if len(workspaces) == 1:
            workspace_id = str(workspaces[0].get("workspace_id") or "")
        else:
            lines = [f"{ws.get('workspace_id', '')}  |  {ws.get('name', '')}" for ws in workspaces[:15]]
            workspace_id = simpledialog.askstring(
                "Open Workspace",
                "Enter the workspace ID to open:\n\n" + "\n".join(lines),
                initialvalue=str(workspaces[0].get("workspace_id") or ""),
                parent=self.root,
            ) or ""

        workspace_id = workspace_id.strip()
        if not workspace_id:
            return

        try:
            count = self.controller.open_workspace(workspace_id)
            self.current_node_id = None
            self._focused_card_id = None
            self._pinned_alt_id = None
            self.refresh_node_table()

            children = list(self.node_tree.get_children())
            if children:
                first = children[0]
                self._suspend_node_select = True
                self.node_tree.selection_set(first)
                self.node_tree.focus(first)
                self._suspend_node_select = False
                self._on_node_select()
            else:
                self._render_empty_state()

            self._set_status(f"Opened workspace {workspace_id} ({count} node(s)).")
        except Exception as e:
            messagebox.showerror("Open Workspace Failed", str(e))

    def _run_matching(self):
        try:
            has_workspace = bool(getattr(self.controller, "workspace_id", None))
            has_nodes = bool(getattr(self.controller, "nodes", []) or [])
        except Exception:
            has_workspace = False
            has_nodes = False

        if has_workspace and has_nodes:
            ok = messagebox.askyesno(
                "Re-run Matching",
                "Re-running matching inside the current workspace can replace the current candidate set. Continue?"
            )
            if not ok:
                return

        try:
            self.controller.reset_stop_matching()
        except Exception:
            pass

        self._show_loading_popup("Running Matching", "Preparing semantic cache...")

        def worker():
            try:
                count = self.controller.run_matching()
                self.root.after(0, lambda: self._on_match_done(count))
            except Exception as e:
                self.root.after(0, lambda err=str(e): self._on_match_failed(err))

        threading.Thread(target=worker, daemon=True, name="run-matching").start()

    def _on_match_done(self, count: int):
        try:
            self.controller.reset_stop_matching()
        except Exception:
            pass
        self._close_loading_popup()
        self.refresh_node_table()
        children = list(self.node_tree.get_children())
        if children:
            first = children[0]
            self._suspend_node_select = True
            self.node_tree.selection_set(first)
            self.node_tree.focus(first)
            self._suspend_node_select = False
            self._on_node_select()
        else:
            self._render_empty_state()
        self._set_status(f"Matching complete: {count} node(s).")

    def _on_match_failed(self, err: str):
        try:
            self.controller.reset_stop_matching()
        except Exception:
            pass
        self._close_loading_popup()
        messagebox.showerror("Run Matching Failed", str(err))

    def _rematch_workspace(self):
        ok = messagebox.askyesno(
            "Re-run Matching",
            "Re-run matching for the active workspace and merge fresh suggestions into current decisions?"
        )
        if not ok:
            return
        try:
            self.controller.reset_stop_matching()
        except Exception:
            pass
        try:
            current = self.current_node_id
            count = self.controller.rematch_workspace_preserve_decisions()
            self.refresh_node_table()
            target = current if current and self.node_tree.exists(current) else None
            if target is None:
                children = list(self.node_tree.get_children())
                target = children[0] if children else None
            if target:
                self._suspend_node_select = True
                self.node_tree.selection_set(target)
                self.node_tree.focus(target)
                self._suspend_node_select = False
                self._on_node_select()
            else:
                self._render_empty_state()
            self._set_status(f"Re-run matching complete: {count} node(s).")
        except Exception as e:
            messagebox.showerror("Re-run Matching Failed", str(e))

    def _export_npr(self):
        path = filedialog.asksaveasfilename(
            title="Export NPR Workbook",
            defaultextension=".xlsx",
            filetypes=[("Excel files", "*.xlsx"), ("All files", "*.*")],
        )
        if not path:
            return
        try:
            out = self.controller.export_npr(path)
            self._set_status(f"Exported workbook: {out}")
        except Exception as e:
            messagebox.showerror("Export Failed", str(e))
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
