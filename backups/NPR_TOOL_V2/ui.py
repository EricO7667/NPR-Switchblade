# ui/testui.py
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import traceback
from typing import Optional
from .data_models import DecisionNode, DecisionStatus
from .data_models import Alternate
from .decision_controller import DecisionController
import re, os
import time
class DecisionWorkspaceUI:
    def __init__(self, root: tk.Tk, controller: DecisionController):
        self.root = root
        self.controller = controller

        self.root.title("NPR Tool")
        self.root.geometry("1500x950")
        

        # Debug controls (wired to DecisionController via NPR_DEBUG_* env vars)
        dbg_enabled = str(os.getenv("NPR_DEBUG_PARSE", "")).strip().lower() not in ("", "0", "false", "no")
        self.debug_enabled_var = tk.BooleanVar(value=dbg_enabled)
        self.debug_filter_var = tk.StringVar(value=str(os.getenv("NPR_DEBUG_FILTER", "") or "").strip())
        try:
            self.debug_max_var = tk.IntVar(value=int(os.getenv("NPR_DEBUG_MAX", "200") or 200))
        except Exception:
            self.debug_max_var = tk.IntVar(value=200)
        try:
            self.debug_explain_max_var = tk.IntVar(value=int(os.getenv("NPR_DEBUG_EXPLAIN_MAX", "4000") or 4000))
        except Exception:
            self.debug_explain_max_var = tk.IntVar(value=4000)        # Debug controls (wired to DecisionController via NPR_DEBUG_* env var


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
            "meta_bg": "#374151",
            "meta_text": "#60A5FA",
        }

        self._init_ttk_styles()
        # SET GLOBAL BACKGROUND
        self.root.configure(bg=self.COLORS["bg_main"])
        
        # 🧭 Loading overlay progress binding
        self._bind_progress_to_loading_bar()

        self._build_layout()
        self.refresh_node_table()

        self._init_row_styles()
        self.last_hovered_card = None
        # Add this inside __init__ or as class constants
        self._pn_editor_visible = False
        


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

    def _init_ttk_styles(self):
        """Initialize ttk styles to match NPR Tool color scheme."""
        self.ttk_style = ttk.Style(self.root)

        # On Windows, many themes ignore Progressbar colors. 'clam' usually respects them.
        try:
            self.ttk_style.theme_use("clam")
        except Exception:
            pass

        self.ttk_style.configure(
            "NPR.Horizontal.TProgressbar",
            troughcolor=self.COLORS["card_border"],   # track background
            background=self.COLORS["primary"],        # fill color
            bordercolor=self.COLORS["card_border"],
            lightcolor=self.COLORS["primary"],
            darkcolor=self.COLORS["primary"],
            thickness=14,  # adjust to taste
        )

    def _dbg(self, msg: str):
        """Conditional debug printer that respects the Debug checkbox."""
        try:
            # If the debug checkbox or env variable is off — do nothing
            if not getattr(self, "debug_enabled_var", None):
                return
            if not self.debug_enabled_var.get():
                return

            print(f"[UI DBG] {msg}")
        except Exception:
            # Silently ignore edge cases (e.g., during early init)
            pass

    # ---------------------------------------------------------
    # Loading Overlay
    # ---------------------------------------------------------
    def _show_loading_overlay(self, message="Loading..."):
        """Create a centered loading popup window with a real progress bar."""
        import time
        self._load_start_time = time.time()
        self._loading_active = True

        # Popup
        self.loading_win = tk.Toplevel(self.root)
        self.loading_win.title("Loading")
        self.loading_win.geometry("400x180")
        self.loading_win.configure(bg="white")
        self.loading_win.transient(self.root)
        self.loading_win.grab_set()
        self.loading_win.resizable(False, False)

        # Message
        self.loading_msg_label = tk.Label(
            self.loading_win,
            text=message,
            bg="white",
            font=("Segoe UI", 12, "bold"),
            fg="#2563EB"
        )
        self.loading_msg_label.pack(pady=(20, 10))

        # Progress bar (real determinate mode)
        self.progress = ttk.Progressbar(
            self.loading_win,
            orient="horizontal",
            length=300,
            mode="determinate",
            style="NPR.Horizontal.TProgressbar",
            maximum=100
        )
        self.progress.pack(pady=(5, 5))
        self.progress["value"] = 0

        # Progress percentage label
        self.progress_label = tk.Label(
            self.loading_win,
            text="0%",
            bg="white",
            font=("Segoe UI", 10)
        )
        self.progress_label.pack(pady=(0, 5))

        # Elapsed time label
        self.elapsed_label = tk.Label(
            self.loading_win,
            text="Elapsed: 0s",
            bg="white",
            font=("Segoe UI", 9)
        )
        self.elapsed_label.pack()

        # Center window on parent
        self.loading_win.update_idletasks()
        x = self.root.winfo_x() + (self.root.winfo_width() // 2) - 200
        y = self.root.winfo_y() + (self.root.winfo_height() // 2) - 90
        self.loading_win.geometry(f"+{x}+{y}")

        def _update_elapsed():
            try:
                if not getattr(self, "_loading_active", False):
                    return  # overlay closed or stopped

                win = getattr(self, "loading_win", None)
                label = getattr(self, "elapsed_label", None)

                if not win or not str(win):
                    return
                if not win.winfo_exists():
                    return
                if not label or not label.winfo_exists():
                    return

                elapsed = int(time.time() - self._load_start_time)
                label.config(text=f"Elapsed: {elapsed}s")

                self.root.after(1000, _update_elapsed)
            except tk.TclError:
                return
            except Exception as e:
                print(f"[UI] Elapsed update skipped: {e}")

        _update_elapsed()

        self.loading_win.update()


    def _hide_loading_overlay(self):
        try:
            if getattr(self, "progress", None) and self.progress.winfo_exists():
                self.progress.stop()
        except Exception:
            pass

        try:
            if getattr(self, "loading_win", None) and self.loading_win.winfo_exists():
                self.loading_win.destroy()
        except Exception:
            pass

        self.progress = None
        self.loading_win = None
        self._loading_active = False
        self.loading_msg_label = None


    def _set_loading_message(self, message: str):
        try:
            if getattr(self, "loading_msg_label", None) and self.loading_msg_label.winfo_exists():
                self.loading_msg_label.config(text=message)
                self.loading_msg_label.update_idletasks()
        except Exception:
            pass


    def _bind_progress_to_loading_bar(self):
        """Links backend progress + phase updates to our UI overlay."""
        def update_progress(ratio: float):
            try:
                if hasattr(self, "progress") and self.progress and self.progress.winfo_exists():
                    self.progress["mode"] = "determinate"
                    self.progress["value"] = min(100, max(0, ratio * 100))
                    self.progress.update_idletasks()
                    if getattr(self, "progress_label", None):
                        self.progress_label.config(text=f"{int(min(100, max(0, ratio * 100)))}%")
            except Exception:
                pass

        def set_phase(message: str, reset: bool = True):
            """Update overlay message; optionally reset progress to 0%."""
            try:
                self._set_loading_message(message)
                if reset and hasattr(self, "progress") and self.progress and self.progress.winfo_exists():
                    self.progress["mode"] = "determinate"
                    self.progress["value"] = 0
                    self.progress.update_idletasks()
                    if getattr(self, "progress_label", None):
                        self.progress_label.config(text="0%")
            except Exception:
                pass

        self.root.loading_progress_callback = update_progress
        self.root.loading_phase_callback = set_phase



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

            # 3. Status-based row colors (Pastels for readability) TODO: correlate with colors in the init
            self.node_tree.tag_configure("needs_alternate", background="#FEF2F2") # Very light red
            self.node_tree.tag_configure("needs_decision", background="#FFFBEB")  # Very light yellow
            self.node_tree.tag_configure("full_matched", background="#ECFDF5")    # Very light green
            self.node_tree.tag_configure("ready", background="#D1FAE5")           # Stronger green
            self.node_tree.tag_configure("locked", foreground="#9CA3AF")          # Muted text

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
        print("[UI DBG] Clicked Run Matching")
        try:
            self._apply_debug_env()
            # Show loading overlay while backend loads embeddings or matches
            self._show_loading_overlay("Running matching engine... please wait")

            # Run the match logic in a thread so UI stays responsive
            def run_task():
                try:
                    n = self.controller.run_matching()
                    self.root.after(0, lambda: self._on_match_done(n))
                except Exception as e:
                    self.root.after(0, lambda err=e: messagebox.showerror("Run Matching Failed", str(err)))
                finally:
                    self.root.after(0, self._hide_loading_overlay)

            import threading
            threading.Thread(target=run_task, daemon=True).start()

        except Exception as e:
            print("[UI DBG] Run Matching FAILED.")
            traceback.print_exc()
            messagebox.showerror("Run Matching Failed", str(e))

    def _on_match_done(self, n):
        #print(f"[UI DBG] controller.run_matching OK -> nodes={n}")
        self.refresh_node_table()
        self.status_var.set(f"Matching complete. Built {n} decision nodes.")

    def _apply_debug_env(self) -> None:
        """
        Maps UI debug widgets -> NPR_DEBUG_* env vars consumed by DecisionController.
        Keeping it as env-vars avoids threading config through every layer.
        """
        enabled = bool(self.debug_enabled_var.get())
        if enabled:
            os.environ["NPR_DEBUG_PARSE"] = "1"
            flt = (self.debug_filter_var.get() or "").strip()
            if flt:
                os.environ["NPR_DEBUG_FILTER"] = flt
            else:
                os.environ.pop("NPR_DEBUG_FILTER", None)

            mx = int(self.debug_max_var.get() or 200)
            ex_mx = int(self.debug_explain_max_var.get() or 4000)
            os.environ["NPR_DEBUG_MAX"] = str(mx)
            os.environ["NPR_DEBUG_EXPLAIN_MAX"] = str(ex_mx)
        else:
            os.environ.pop("NPR_DEBUG_PARSE", None)
            os.environ.pop("NPR_DEBUG_FILTER", None)
            os.environ.pop("NPR_DEBUG_MAX", None)
            os.environ.pop("NPR_DEBUG_EXPLAIN_MAX", None)

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

            # ---- Debug Controls (right side) ----
            dbg = tk.Frame(bar, bg="white")
            dbg.pack(side="right", padx=(10, 0))

            tk.Label(dbg, text="Debug", font=("Segoe UI", 10, "bold"),
                     bg="white", fg=self.COLORS["text_dim"]).pack(side="left", padx=(0, 6))

            tk.Checkbutton(
                dbg,
                variable=self.debug_enabled_var,
                bg="white",
                activebackground="white",
                highlightthickness=0,
                bd=0,
                command=self._apply_debug_env,
            ).pack(side="left", padx=(0, 10))

            tk.Label(dbg, text="Filter", font=("Segoe UI", 9),
                     bg="white", fg=self.COLORS["text_dim"]).pack(side="left", padx=(0, 4))
            tk.Entry(dbg, textvariable=self.debug_filter_var, width=14,
                     relief="solid", bd=1).pack(side="left", padx=(0, 10))

            tk.Label(dbg, text="Max", font=("Segoe UI", 9),
                     bg="white", fg=self.COLORS["text_dim"]).pack(side="left", padx=(0, 4))
            tk.Spinbox(dbg, from_=1, to=5000, width=6,
                       textvariable=self.debug_max_var, command=self._apply_debug_env,
                       relief="solid", bd=1).pack(side="left", padx=(0, 10))

            tk.Label(dbg, text="ExplainMax", font=("Segoe UI", 9),
                     bg="white", fg=self.COLORS["text_dim"]).pack(side="left", padx=(0, 4))
            tk.Spinbox(dbg, from_=200, to=200000, increment=200, width=8,
                       textvariable=self.debug_explain_max_var, command=self._apply_debug_env,
                       relief="solid", bd=1).pack(side="left", padx=(0, 0))

    # ============================================================
    # HEADER (pure state rendering)
    # ============================================================
    def _build_header(self):
        self.header_frame = tk.Frame(self.root, bg=self.COLORS["header_bg"], pady=15, padx=20)
        self.header_frame.pack(fill="x")

        self.h_title = tk.Label(
            self.header_frame, text="Select a part",
            font=("Segoe UI", 24, "bold"),
            bg=self.COLORS["header_bg"], fg=self.COLORS["header_text"]
        )
        self.h_desc = tk.Label(
            self.header_frame, text="",
            font=("Segoe UI", 12),
            bg=self.COLORS["header_bg"], fg="#D1D5DB"
        )
        self.h_meta = tk.Label(
            self.header_frame, text="",
            font=("Segoe UI", 14, "bold"),
            bg=self.COLORS["meta_bg"], fg=self.COLORS["meta_text"]
        )

        self.h_title.pack(anchor="center")
        self.h_desc.pack(anchor="center", pady=(5, 5))
        self.h_meta.pack(anchor="center", pady=(5, 5))



        # --- CNS suggestion + manual PN entry row ---
        # --- PN controls row ---
        self.suggested_var = tk.StringVar(value="")
        self.company_pn_var = tk.StringVar(value="")

        pn_row = tk.Frame(self.header_frame, bg=self.COLORS["header_bg"])
        pn_row.pack(anchor="center", pady=(10, 0))

        tk.Label(
            pn_row, text="Suggested CNS:", bg=self.COLORS["header_bg"],
            fg="#D1D5DB", font=("Segoe UI", 10, "bold")
        ).pack(side="left", padx=(0, 6))

        self.suggested_entry = tk.Entry(
            pn_row, textvariable=self.suggested_var, width=18,
            font=("Segoe UI", 10),
            readonlybackground=self.COLORS["header_bg"],
            fg="white", bd=0, justify="center"
        )
        self.suggested_entry.config(state="readonly")
        self.suggested_entry.pack(side="left", padx=(0, 18))

        tk.Label(
            pn_row, text="Company PN:", bg=self.COLORS["header_bg"],
            fg="#D1D5DB", font=("Segoe UI", 10, "bold")
        ).pack(side="left", padx=(0, 6))

        # editable entry (starts hidden)
        self.company_pn_entry = tk.Entry(
            pn_row, textvariable=self.company_pn_var, width=26,
            font=("Segoe UI", 10),
            bg="white", fg=self.COLORS["text_dark"]
        )

        # apply button (starts hidden)
        self.apply_pn_btn = ttk.Button(
            pn_row, text="Apply", command=self._apply_company_pn
        )

        # edit toggle button (always visible, but we can disable/hide it for locked states)
        self.edit_pn_btn = ttk.Button(
            pn_row, text="Edit PN", command=self._open_pn_editor
        )
        self.edit_pn_btn.pack(side="left", padx=(0, 8))

        # lock label (only visible when locked)
        self.pn_lock_lbl = tk.Label(
            pn_row, text="", bg=self.COLORS["header_bg"],
            fg="#9CA3AF", font=("Segoe UI", 9, "bold")
        )
    

        # Add Mark Ready button on right side
        self.mark_ready_btn = ttk.Button(
            self.header_frame,
            text="Mark Ready",
            command=self._on_mark_ready
        )
        self.mark_ready_btn.pack(anchor="e", side="right", padx=10)

    def _render_header_state(self, node):

        # --- PATCH: unify winning mpn for header ---
        explain = getattr(node, "explain", {}) or {}
        winning_mpn = explain.get("winning_mpn", "")
        if winning_mpn:
            node._winning_mpn_ui = winning_mpn
        else:
            node._winning_mpn_ui = getattr(node, "assigned_part_number", "") or node.bom_mpn


        self._dbg(f"ENTER _render_header_state node None? {node is None}")
        if node:
            self._dbg(f"node.id={node.id} base_type={getattr(node,'base_type','')} conf={getattr(node,'confidence',None)}")

        
        """Render deterministic header state."""
        if not node:
            return self._update_header_visuals("No Selection", "", "Select a BOM line", "neutral")


        # STATE 0 — Unanchored BOM Part
        if (not getattr(node, "internal_part_number", None)) and node.confidence < 1.0:
            title = f"BOM MPN: {node._winning_mpn_ui or 'Unknown'}"
            desc = node.description or ""
            meta = "NEW PART"
            color = "warning"

        # STATE 1 — Anchored to EXISTING Company PN
        elif getattr(node, "internal_part_number", None) or node.confidence == 1.0:
            title = f"Company PN: {node.internal_part_number or 'Auto-Elevated'}"
            desc = f"BOM MPN: {node._winning_mpn_ui or ''}"
            meta = "EXISTING PART"
            color = "success"

        # STATE 2 — Anchored to NEW Company PN (future)
        elif getattr(node, "proposed_pn", None):
            title = f"Proposed Company PN: {node.proposed_pn}"
            desc = f"BOM MPN: {node._winning_mpn_ui or ''}"
            meta = "NEW PART (Proposed)"
            color = "info"

        else:
            title = f"BOM MPN: {node._winning_mpn_ui or 'Unknown'}"
            desc = node.description or ""
            meta = "STATE UNKNOWN"
            color = "neutral"
        self._dbg("EXIT _render_header_state")
        self._update_header_visuals(title, desc, meta, color)
        self._sync_pn_controls(node, force_open=False)
        #print(f"[UI PATCH] Header display -> node={node.id} display_mpn={node._winning_mpn_ui} winner={winning_mpn}")
    def _update_header_visuals(self, title, desc, meta, color):
        self.h_title.config(text=title, fg=self.COLORS["header_text"])
        self.h_desc.config(text=desc, fg="#D1D5DB")
        self.h_meta.config(text=meta, fg=self.COLORS.get(color, "#9CA3AF"))

    #def _apply_company_pn(self):
    #    node = getattr(self, "current_node", None)
    #    if not node:
    #        return
#
    #    pn = (self.company_pn_var.get() or "").strip()
    #    self.controller.set_assigned_part_number(node.id, pn)
#
    #    node = self.controller.get_node(node.id)
    #    self.current_node = node
    #    self._render_header_state(node)
    #    self.refresh_node_table()
    #    self._render_cards(node)

    def _apply_company_pn(self):
        node = getattr(self, "current_node", None)
        if not node:
            return

        # Hard lock safety
        if self._is_pn_locked(node):
            messagebox.showwarning("Locked", "This part is an existing full match. PN editing is locked.")
            return

        pn = (self.company_pn_var.get() or "").strip()
        if not pn:
            messagebox.showwarning("Missing PN", "Enter a Company Part Number first.")
            return

        # Optional safety: confirm overwriting an existing assigned PN
        existing = (getattr(node, "assigned_part_number", "") or "").strip()
        if existing and existing != pn:
            ok = messagebox.askyesno("Overwrite PN?", f"Replace:\n{existing}\n\nwith:\n{pn}\n?")
            if not ok:
                return

        try:
            self.controller.set_assigned_part_number(node.id, pn)
        except Exception as e:
            messagebox.showerror("Apply PN Failed", str(e))
            return

        node = self.controller.get_node(node.id)
        self.current_node = node

        # After apply: hide editor again (reduces accidental edits)
        self._sync_pn_controls(node, force_open=False)

        self._render_header_state(node)
        self.refresh_node_table()
        self._render_cards(node)


    def _is_pn_locked(self, node) -> bool:
        """
        Lock PN editing when this node is anchored to an existing internal PN.
        This matches your rule: only open when internal matches are gone.
        """
        if not node:
            return True

        # If it has an internal company PN anchor, treat as locked
        anchored = bool(getattr(node, "internal_part_number", "") or "")
        if anchored:
            return True

        # If locked for export, also lock
        if getattr(node, "locked", False):
            return True

        return False


    def _hide_pn_editor(self):
        if getattr(self, "_pn_editor_visible", False):
            try:
                self.company_pn_entry.pack_forget()
                self.apply_pn_btn.pack_forget()
            except Exception:
                pass
        self._pn_editor_visible = False


    def _show_pn_editor(self, editable: bool = True):
        # Only pack if not already visible
        if not getattr(self, "_pn_editor_visible", False):
            self.company_pn_entry.pack(side="left", padx=(0, 8))
            self.apply_pn_btn.pack(side="left")
            self._pn_editor_visible = True

        state = "normal" if editable else "readonly"
        try:
            self.company_pn_entry.config(state=state)
        except Exception:
            pass


    def _open_pn_editor(self):
        node = getattr(self, "current_node", None)
        if not node:
            return

        # If locked, do nothing
        if self._is_pn_locked(node):
            return

        # If empty, seed with "PB-" so they just type suffix
        if not (getattr(node, "assigned_part_number", "") or "").strip():
            pb = (getattr(node, "suggested_pb", "") or "").strip()
            if pb and not self.company_pn_var.get().strip():
                self.company_pn_var.set(f"{pb}-")

        self._show_pn_editor(editable=True)
        try:
            self.company_pn_entry.focus_set()
            self.company_pn_entry.icursor("end")
        except Exception:
            pass


    def _sync_pn_controls(self, node, force_open: bool = False):
        """
        Call this whenever selection/state changes:
          - node select
          - add/reject/unreject
          - mark ready
        """
        if not node:
            self.suggested_var.set("")
            self.company_pn_var.set("")
            self._hide_pn_editor()
            self.edit_pn_btn.state(["disabled"])
            self.pn_lock_lbl.config(text="")
            self.pn_lock_lbl.pack_forget()
            return

        # Suggested PB always updates
        self.suggested_var.set((getattr(node, "suggested_pb", "") or "").strip())

        # Display PN (assigned > internal)
        assigned = (getattr(node, "assigned_part_number", "") or "").strip()
        internal = (getattr(node, "internal_part_number", "") or "").strip()
        self.company_pn_var.set(assigned or internal)

        locked = self._is_pn_locked(node)

        # Default behavior: hide editor on first load unless force_open
        if locked:
            self._hide_pn_editor()
            self.edit_pn_btn.state(["disabled"])
            self.pn_lock_lbl.config(text="LOCKED (Existing Part)")
            if not self.pn_lock_lbl.winfo_ismapped():
                self.pn_lock_lbl.pack(side="left", padx=(8, 0))
        else:
            self.edit_pn_btn.state(["!disabled"])
            self.pn_lock_lbl.config(text="")
            if self.pn_lock_lbl.winfo_ismapped():
                self.pn_lock_lbl.pack_forget()

            if force_open:
                self._open_pn_editor()
            else:
                # Hide by default until user clicks "Edit PN"
                self._hide_pn_editor()


    # ============================================================
    # NODE TABLE
    # ============================================================
    def _build_node_table(self):
        frame = ttk.Frame(self.root)
        frame.pack(fill="both", expand=True)

        cols = ("ID", "Type", "MPN", "Status", "Confidence")
        self.node_tree = ttk.Treeview(frame, columns=cols, show="headings")
        for c in cols:
            self.node_tree.heading(c, text=c)
            self.node_tree.column(c, width=180, anchor="center")

        self.node_tree.bind("<<TreeviewSelect>>", self._on_node_select)
        self.node_tree.pack(fill="both", expand=True)

    def refresh_node_table(self):
        """Redraw table from controller."""
        self.node_tree.delete(*self.node_tree.get_children())

        for node in self.controller.nodes:
            self.node_tree.insert(
                "",
                "end",
                iid=node.id,
                values=(node.id, node.base_type, node.bom_mpn,
                        node.status, f"{node.confidence*100:.1f}%"),
            )
  
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


            # 4. Everything else (debug / extra fields)
            add_row(self.specs_inner, "OTHER", "", is_header=True)
            known = {"ItemNumber","VendorItem","Description","MfgName","MfgId","PrimaryVendorNumber",
                     "TotalQty","LastCost","AvgCost","ItemLeadTime","DefaultWhse","TariffCodeHTSUS"}
            for k in sorted(specs.keys()):
                if k in known:
                    continue
                if specs[k]:
                    add_row(self.specs_inner, k, specs[k])


    def _specs_from_inventory(self, inv):
        """
        Inventory loader normalizes headers (snake_case), but the UI renderer expects
        TitleCase keys (ItemNumber/VendorItem/etc). Bridge them here.
        """
        raw = dict(getattr(inv, "raw_fields", {}) or {})

        def pick(*keys) -> str:
            for k in keys:
                v = raw.get(k, "")
                if v is None:
                    continue
                v = str(v).strip()
                if v:
                    return v
            return ""

        specs = {}

        # Prefer the InventoryPart attributes (most reliable), then fall back to raw_fields
        specs["ItemNumber"] = (getattr(inv, "itemnum", "") or "").strip() or pick(
            "itemnum", "item_number", "itemnumber", "item_no"
        )
        specs["VendorItem"] = (getattr(inv, "vendoritem", "") or "").strip() or pick(
            "vendoritem", "vendor_item", "vendor_item_number", "manufacturer_part_number"
        )
        specs["Description"] = (getattr(inv, "desc", "") or "").strip() or pick(
            "desc", "description", "item_description"
        )

        specs["MfgName"] = (getattr(inv, "mfgname", "") or "").strip() or pick(
            "mfgname", "mfg_name", "manufacturer_name"
        )
        specs["MfgId"] = (getattr(inv, "mfgid", "") or "").strip() or pick(
            "mfgid", "mfg_id", "manufacturer_id"
        )
        specs["PrimaryVendorNumber"] = pick(
            "primaryvendornumber", "primary_vendor_number", "supplier", "vendor", "supplier_name"
        )

        specs["TotalQty"] = pick(
            "totalqty", "total_qty", "qty_on_hand", "on_hand", "quantity"
        )
        specs["LastCost"] = pick("lastcost", "last_cost")
        specs["AvgCost"] = pick("avgcost", "avg_cost", "average_cost")
        specs["ItemLeadTime"] = pick("itemleadtime", "item_lead_time", "lead_time", "lead_time_wks_")
        specs["DefaultWhse"] = pick("defaultwhse", "default_whse", "default_warehouse", "warehouse")

        # Optional tariff/HTSUS (if present in inventory export)
        specs["TariffCodeHTSUS"] = pick("tariffcodehtsus", "tariff_code_htsus_", "htsus")

        return specs



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
        """Redraw the NPR part node table with appropriate tags and correct winning MPN display."""
        # Clear all existing items
        for i in self.node_tree.get_children():
            self.node_tree.delete(i)

        # Rebuild the table from controller state
        for node in self.controller.get_nodes():
            tags = []

            # 🔒 Status-based visual tags
            if getattr(node, "locked", False):
                tags.append("locked")

            if node.status == DecisionStatus.NEEDS_ALTERNATE:
                tags.append("needs_alternate")
            elif node.status == DecisionStatus.NEEDS_DECISION:
                tags.append("needs_decision")
            elif node.status == DecisionStatus.FULL_MATCH:
                tags.append("full_matched")
            elif node.status == DecisionStatus.EXISTS:
                tags.append("exists")
            elif node.status == DecisionStatus.NEEDS_REVIEW:
                tags.append("needs_review")
            elif node.status == DecisionStatus.READY_FOR_EXPORT:
                tags.append("ready")

            # --- PATCH: Prefer winner or assigned MPN for display ---
            explain = getattr(node, "explain", {}) or {}
            winning_mpn = explain.get("winning_mpn", "")
            display_mpn = (
                winning_mpn
                or getattr(node, "assigned_part_number", "")
                or getattr(node, "internal_part_number", "")
                or node.bom_mpn
            )

            # Compute confidence display
            if len(node.candidate_alternates()) > 0:
                conf_display = f"{len(node.selected_alternates())}/{len(node.candidate_alternates())}"
            else:
                try:
                    conf_display = f"{node.confidence * 100:.1f}%"
                except Exception:
                    conf_display = "—"

            # Debug print for traceability
            #print(f"[UI TABLE] node={node.id} bom={node.bom_mpn} winning={winning_mpn} display={display_mpn}")

            # --- Insert the row (5 columns total: ID, Type, MPN, Status, Confidence) ---
            try:
                self.node_tree.insert(
                    "",
                    "end",
                    iid=node.id,
                    values=(
                        node.id,
                        node.base_type,
                        display_mpn,  # ✅ now shows winner if one exists
                        getattr(node.status, "value", str(node.status)),
                        conf_display,
                    ),
                    tags=tuple(tags),
                )
            except Exception as e:
                print(f"[UI ERROR] Failed to insert node {node.id}: {e}")



    def _render_cards(self, node: DecisionNode):
        self.last_hovered_card = None
        for w in self.cards_inner.winfo_children():
            w.destroy()

        style = ttk.Style()
        style.configure("TLabelframe.Label", font=("Segoe UI", 9, "bold"),
                        foreground=self.COLORS["text_dim"])

        # --- PATCH: Robust filter for alternates including winner ---
        explain = getattr(node, "explain", {}) or {}
        winning_mpn = explain.get("winning_mpn", "") or ""
        attempts = explain.get("attempts", [])
        winner_mpns = [
            a.get("customer_mpn", "").strip().lower()
            for a in attempts if a.get("is_winner")
        ] or [winning_mpn.lower()]

        def is_winner_alt(alt):
            """Return True if this alternate corresponds to a winning or matching MPN."""
            mfgpn = (getattr(alt, "manufacturer_part_number", "") or "").strip().lower()
            invpn = (getattr(alt, "internal_part_number", "") or "").strip().lower()
            return (
                mfgpn in winner_mpns
                or invpn in winner_mpns
                or (winning_mpn and mfgpn == winning_mpn.lower())
            )

        # ---- Collect all alternates ----
        all_internal = [
            a for a in node.alternates if (not a.rejected) and a.source == "inventory"
        ]
        all_external = [
            a for a in node.alternates if (not a.rejected) and a.source != "inventory"
        ]
        rejected_all = [a for a in node.alternates if a.rejected]

        # ---- Filter down to winners if any exist, else fallback ----
        internal_active = [a for a in all_internal if is_winner_alt(a)] or all_internal
        external_active = [a for a in all_external if is_winner_alt(a)] or all_external

        print(
            f"[UI PATCH] node={node.id} winning_mpn={winning_mpn} "
            f"winner_mpns={winner_mpns} internal={len(internal_active)} "
            f"external={len(external_active)} rejected={len(rejected_all)}"
        )

        # ---- Build card sections ----
        internal_frame = ttk.LabelFrame(
            self.cards_inner, text=f"🔒 Internal Matches ({len(internal_active)})"
        )
        internal_frame.pack(fill="x", padx=16, pady=(10, 6))

        external_frame = ttk.LabelFrame(
            self.cards_inner, text=f"🔍 External Alternates ({len(external_active)})"
        )
        external_frame.pack(fill="x", padx=16, pady=(10, 6))

        # ---- DigiKey search bar ----
        search_bar = ttk.Frame(external_frame)
        search_bar.pack(fill="x", padx=6, pady=4)
        ttk.Button(
            search_bar,
            text="Search DigiKey",
            command=lambda: self._search_digikey(node)
        ).pack(side="left")

        # ---- Render card grids ----
        self._render_card_grid(internal_frame, internal_active, node)
        self._render_card_grid(external_frame, external_active, node)

        # ---- Rejected section ----
        if rejected_all:
            rejected_frame = ttk.LabelFrame(
                self.cards_inner, text=f"🚫 Rejected ({len(rejected_all)})"
            )
            rejected_frame.pack(fill="x", padx=16, pady=(10, 6))
            self._render_card_grid(rejected_frame, rejected_all, node)

    

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
                self.specs_title.config(text="Part Specifications (Hover over part in left panel to view specs)")
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
            #print("ALT DBG:", alt.source, alt.manufacturer_part_number, alt.confidence, type(alt.confidence))
            

            # --- PATCH: Fix confidence display for customer-provided (external) alternates ---
            display_conf = alt.confidence
            if (alt.source != "inventory") and (not alt.rejected):
                # If external (e.g. customer-provided) and not rejected, assume full trust
                if display_conf == 0.0 or display_conf is None:
                    display_conf = 1.0
            
            tk.Label(
                header_row,
                text=f"{int(display_conf * 100)}%",
                font=("Segoe UI", 10, "bold"),
                bg="#DBEAFE",
                fg="#1D4ED8",
                padx=6
            ).pack(side="right")

            # Description
            tk.Label(frame, text=alt.description, font=("Segoe UI", 9), 
                     fg="#4B5563", bg=frame["bg"], wraplength=350, justify="left").pack(anchor="w", pady=(0, 8))

            # Details Grid (Small info)
            details_row = tk.Frame(frame, bg=frame["bg"])
            details_row.pack(fill="x", pady=(0, 10))

            # Quick helper for small gray text
            def small_lbl(txt): 
                return tk.Label(details_row, text=txt, font=("Segoe UI", 8), fg="#6B7280", bg=frame["bg"])

            # Stock display should match what specs panel shows
            stock_display = "-"
            if alt.raw is not None:
                inv_specs = self._specs_from_inventory(alt.raw)
                stock_display = inv_specs.get("TotalQty") or "-"
            else:
                stock_display = alt.stock if (alt.stock not in (None, "")) else "-"
            
            if alt.source == "customer_bom":
                small_lbl("Stock: —").pack(side="left", padx=(0, 10))
            else:           
                small_lbl(f"Stock: {stock_display}").pack(side="left", padx=(0, 10))

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
                tk.Label(
                    btn_frame, text="REJECTED",
                    fg="#EF4444", font=("Segoe UI", 9, "bold"),
                    bg=frame["bg"]
                ).pack(side="left")

                tk.Button(
                    btn_frame, text="Unreject",
                    bg="#F3F4F6", fg=self.COLORS["text_dark"],
                    bd=0, padx=10, pady=2, cursor="hand2",
                    command=lambda: self._unreject_alt(node, alt)
                ).pack(side="right", padx=2)


            return container

    # ============================================================
    # ACTIONS
    # ============================================================
    #def _on_node_select(self, event):
    #    selected = self.node_tree.selection()
    #    if not selected:
    #        return
    #    node_id = selected[0]
    #    node = next((n for n in self.controller.nodes if n.id == node_id), None)
    #    self.current_node = node
    #    self._preview_header(node)
    #    self._render_cards(node)


    def _on_node_select(self, event=None):
        self._dbg("ENTER _on_node_select")

        selected = self.node_tree.selection()
        self._dbg(f"selected={selected}")

        if not selected:
            self._dbg("EXIT _on_node_select (no selection)")
            return

        node_id = selected[0]
        self._dbg(f"node_id={node_id}")

        node = next((n for n in self.controller.nodes if n.id == node_id), None)
        self._dbg(f"node lookup result is None? {node is None}")

        if node:
            self._dbg(
                f"node.id={node.id} base_type={getattr(node,'base_type',None)} "
                f"internal_pn={getattr(node,'internal_part_number','')} "
                f"assigned_pn={getattr(node,'assigned_part_number','')} "
                f"suggested_pb={getattr(node,'suggested_pb','')}"
            )

        self.current_node = node
        self._dbg(f"self.current_node set? {self.current_node is not None}")

        # Header + cards
        try:
            self._preview_header(node)
            self._dbg("_preview_header ok")
        except Exception as e:
            self._dbg(f"_preview_header ERROR: {e}")
            raise

        try:
            self._render_cards(node)
            self._dbg("_render_cards ok")
        except Exception as e:
            self._dbg(f"_render_cards ERROR: {e}")
            raise

        self._dbg("EXIT _on_node_select")

    #def _preview_header(self, node):
    #    """Show live header preview without committing NPR state."""
    #    if not node:
    #        return self._update_header_visuals("No Selection", "", "Select a BOM line", "neutral")
#
    #    title = f"BOM MPN: {node.bom_mpn or 'Unknown'}"
    #    desc = node.description or ""
    #    meta = "Previewing Part Details"
    #    self._update_header_visuals(title, desc, meta, "info")
#
#
    #    # suggested PB
    #    suggested = getattr(node, "suggested_pb", "") or ""
    #    self.suggested_var.set(suggested)
#
    #    # editable PN (prefer assigned, else internal if exists)
    #    current_pn = getattr(node, "assigned_part_number", "") or getattr(node, "internal_part_number", "")
    #    self.company_pn_var.set(current_pn)

    def _preview_header(self, node):
        self._dbg("ENTER _preview_header")
        self._dbg(f"node is None? {node is None}")

        if not node:
            self._dbg("EXIT _preview_header (no node)")
            return self._update_header_visuals("No Selection", "", "Select a BOM line", "neutral")

        self._dbg(f"node.id={node.id}")

        # --- PATCH: prefer winner or assigned PN over BOM MPN ---
        explain = getattr(node, "explain", {}) or {}
        winning_mpn = explain.get("winning_mpn", "")
        assigned = getattr(node, "assigned_part_number", "") or getattr(node, "internal_part_number", "")
        display_mpn = winning_mpn or assigned or node.bom_mpn or "Unknown"

        # --- PATCH: use part description instead of "BOM Input" ---
        desc = getattr(node, "description", "") or "(no description available)"

        title = f"Resolved MPN: {display_mpn}"
        meta = "Previewing Part Details"

        self._update_header_visuals(title, desc, meta, "info")

        # ---- Update suggested + company PN fields ----
        try:
            suggested = getattr(node, "suggested_pb", "") or ""
            assigned = getattr(node, "assigned_part_number", "") or getattr(node, "internal_part_number", "")
            self._dbg(f"suggested_pb='{suggested}' assigned_or_internal='{assigned}'")

            if hasattr(self, "suggested_var"):
                self.suggested_var.set(suggested)
                self._dbg("suggested_var set")
            else:
                self._dbg("suggested_var missing on UI")

            if hasattr(self, "company_pn_var"):
                self.company_pn_var.set(assigned)
                self._dbg("company_pn_var set")
            else:
                self._dbg("company_pn_var missing on UI")

        except Exception as e:
            self._dbg(f"ERROR setting header PN vars: {e}")
            raise

        self._dbg("EXIT _preview_header")
        self._sync_pn_controls(node, force_open=False)




    # ============================================================
    # ALT (CARD) ACTIONS
    # ============================================================

    def _add_alt(self, node, alt):
        """Toggle-select an alternate via the controller (single source of truth)."""
        try:
            # Toggle behavior: if already selected -> unselect, else select
            if getattr(alt, "selected", False):
                self.controller.unselect_alternate(node.id, alt.id)  # recompute inside
            else:
                self.controller.select_alternate(node.id, alt.id)    # recompute inside

        except Exception as e:
            messagebox.showerror("Selection Error", str(e))
            return

        # Pull the canonical node (optional but nice)
        node = self.controller.get_node(node.id)

        self._render_header_state(node)
        self.refresh_node_table()
        self._render_cards(node)


    def _reject_alt(self, node, alt):
        try:
            self.controller.reject_alternate(node.id, alt.id)
        except Exception as e:
            messagebox.showerror("Reject Error", str(e))
            return

        node = self.controller.get_node(node.id)

        self._render_header_state(node)
        force_open = (not getattr(node, "internal_part_number", "")) and (getattr(node, "base_type", "").upper() == "NEW")
        self._sync_pn_controls(node, force_open=force_open)

        self.refresh_node_table()
        self._render_cards(node)

    def _unreject_alt(self, node, alt):
        try:
            self.controller.unreject_alternate(node.id, alt.id)
        except Exception as e:
            messagebox.showerror("Unreject Error", str(e))
            return
    
        node = self.controller.get_node(node.id)
        self.current_node = node
    
        self._render_header_state(node)
    
        # If unrejecting reintroduces an anchored internal match, lock/hide editor again.
        # Otherwise, if it's NEW/unanchored, keep it closed unless you want to auto-open.
        force_open = (not getattr(node, "internal_part_number", "")) and (getattr(node, "base_type", "").upper() == "NEW")
        self._sync_pn_controls(node, force_open=force_open)
    
        self.refresh_node_table()
        self._render_cards(node)


    def _on_mark_ready(self):
        n = self.current_node
        if not n:
            messagebox.showwarning("No Selection", "Select a node first.")
            return

        try:
            self.controller.mark_ready(n.id)  # enforces rules + locks
        except Exception as e:
            messagebox.showerror("Mark Ready Failed", str(e))
            return

        # Refresh from canonical state
        n = self.controller.get_node(n.id)
        self.current_node = n
        self._render_header_state(n)
        self.refresh_node_table()

    def _export_npr(self):
        path = filedialog.asksaveasfilename(
            defaultextension=".xlsx",
            filetypes=[("Excel Files", "*.xlsx")],
            title="Save NPR Export As..."
        )
        if not path:
            return
    
        try:
            #ws = self.controller.build_npr_workspace_from_nodes()
            self.controller.export_npr(path)
            messagebox.showinfo("Export Successful", f"NPR file saved to:\n{path}")
        except PermissionError as e:
            messagebox.showwarning("File In Use", str(e))
        except Exception as e:
            messagebox.showerror("Export Failed", str(e))