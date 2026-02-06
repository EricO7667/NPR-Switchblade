import os
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import pandas as pd

from .data_loader import DataLoader
from .matching_engine import MatchingEngine
from .data_models import MatchType
from .parsing_engine import DescriptionParser


class NPRToolUI:
    """
    Part Matching Tool UI with inline expandable detail rows.

    Features:
    - Bottom details panel removed.
    - Inline expander per row (double-click to toggle).
    - Dynamic side-by-side comparison of parsed fields (no hard-coded keys).
    - Dynamic raw-field comparison, collapsed by default via "Show/Hide Raw Fields".
    - Ctrl+C copies JUST the clicked cell (row+column), or entire row if no cell selected.
    """

    def __init__(self, root):
        self.root = root
        self.root.title("Part Matching Tool")
        self.root.geometry("1500x950")

        # Loaded data
        self.inventory = []       # List[InventoryPart]
        self.npr_list = []        # List[PartRecord]
        self.match_results = []   # List[(NPRPart, MatchResult)]

        # Map Treeview item -> index into match_results
        self.row_index_map = {}

        # Track which main item is expanded
        self.expanded_item = None

        # Track whether raw fields are expanded per main item
        self.raw_expanded = {}  # main_item_id -> bool



        self.detail_rows = {}     # main_item_id -> [detail_row_ids]
        self.expanded_state = {}  # main_item_id -> bool
        self.child_to_parent = {} # detail_row_id -> main_item_id

        # Track last clicked cell for copy (item, column)
        self.last_clicked_item = None
        self.last_clicked_col = None  # '#0', '#1', ...

        self.tooltip = None
        self.hover_after_id = None
        self.hover_row = None
        self.hover_col = None

        self._build_ui()

    # -----------------------------------------------------
    # UI SETUP
    # -----------------------------------------------------
    def _build_ui(self):
        tk.Label(
            self.root,
            text="Part Matching Tool",
            font=("Segoe UI", 22, "bold")
        ).pack(pady=10)

        # Load Buttons
        load_frame = tk.Frame(self.root)
        load_frame.pack(pady=5)

        tk.Button(
            load_frame,
            text="Load Inventory",
            command=self.load_inventory
        ).grid(row=0, column=0, padx=5)

        tk.Button(
            load_frame,
            text="Load Parts",
            command=self.load_npr
        ).grid(row=0, column=1, padx=5)

        tk.Button(
            load_frame,
            text="Run Matching",
            command=self.run_matching
        ).grid(row=0, column=2, padx=5)

        tk.Button(
            load_frame,
            text="Export Existing Parts",
            command=self.export_existing
        ).grid(row=0, column=3, padx=5)

        tk.Button(
            load_frame,
            text="Export Missing Parts",
            command=self.export_missing
        ).grid(row=0, column=4, padx=5)

        # Filters
        filter_frame = tk.Frame(self.root)
        filter_frame.pack(pady=8)

        tk.Button(
            filter_frame,
            text="Show ALL",
            width=14,
            command=lambda: self.render_table("all")
        ).grid(row=0, column=0, padx=5)

        tk.Button(
            filter_frame,
            text="Show EXISTS",
            width=14,
            command=lambda: self.render_table("exists")
        ).grid(row=0, column=1, padx=5)

        tk.Button(
            filter_frame,
            text="Show MISSING",
            width=14,
            command=lambda: self.render_table("missing")
        ).grid(row=0, column=2, padx=5)

        # Table + Scrollbar
        table_frame = tk.Frame(self.root)
        table_frame.pack(fill="both", expand=True)

        columns = (
            "Internal Part #",
            "Manufacturer Part #",
            "Part Description",
            "Match Type",
            "Confidence"
        )

        self.tree = ttk.Treeview(
            table_frame,
            columns=columns,
            show="headings",
            height=22,
            selectmode="browse"
        )

    


        # Tree column (#0) is used for detail labels
        self.tree.column("#0", width=240, anchor="w")
        self.tree.heading("#0", text="Details / Field")

        for col in columns:
            self.tree.heading(col, text=col)
            if col == "Part Description":
                self.tree.column(col, width=420, anchor="w")
            else:
                self.tree.column(col, width=220, anchor="w")

        scrollbar = ttk.Scrollbar(table_frame, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=scrollbar.set)

        self.tree.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")

        # Row colors for main match rows
        self.tree.tag_configure("exact", background="#d4f7d0")      # green
        self.tree.tag_configure("internal", background="#fff3b0")   # yellow
        self.tree.tag_configure("missing", background="#f7d0d0")    # red
        self.tree.tag_configure("parsed", background="#d0e7ff")     # light blue
        self.tree.tag_configure("prefix", background="#e8ccff")     # purple-ish

        # Styles for inline detail rows
        self.tree.tag_configure("detail_section_header", background="#d9e6ff", font=("Segoe UI", 9, "bold"))
        self.tree.tag_configure("detail_summary", background="#e0f0ff", font=("Segoe UI", 9, "italic"))
        self.tree.tag_configure("detail_header", background="#e9e9e9", font=("Segoe UI", 9, "bold"))
        self.tree.tag_configure("detail_row", background="#f5f5f5", font=("Segoe UI", 9))
        self.tree.tag_configure("raw_toggle", background="#f0f0ff", font=("Segoe UI", 9, "italic"))

        # Double-click to toggle inline details
        self.tree.bind("<Double-1>", self._on_double_click)


        # Record last clicked cell for copy
        self.tree.bind("<Button-1>", self.on_tree_click, add="+")

        # Ctrl+C to copy selected cell (or row)
        self.tree.bind("<Control-c>", self.copy_selected_cell)

        # Hover detection
        self.tree.bind("<Motion>", self._on_hover)
        self.tree.bind("<Leave>", lambda e: self._hide_tooltip())


    def _on_double_click(self, event):
        """Override Treeview's default expand/collapse behavior."""
        self.toggle_expander(event)
        return "break"


    # -----------------------------------------------------
    # LOADERS
    # -----------------------------------------------------
    def load_inventory(self):
        path = filedialog.askopenfilename(filetypes=[("Excel Files", "*.xlsx")])
        if not path:
            return
        try:
            self.inventory = DataLoader.load_inventory(path)
            messagebox.showinfo("Success", "Inventory loaded.")
        except Exception as e:
            messagebox.showerror("Error", str(e))

    def load_npr(self):
        path = filedialog.askopenfilename(filetypes=[("Excel Files", "*.xlsx")])
        if not path:
            return
        try:
            self.npr_list = DataLoader.load_npr(path)
            messagebox.showinfo("Success", "Parts file loaded.")
        except Exception as e:
            messagebox.showerror("Error", str(e))

    # -----------------------------------------------------
    # RUN MATCHING
    # -----------------------------------------------------
    def run_matching(self):
        if not self.inventory or not self.npr_list:
            messagebox.showerror("Error", "Load inventory and parts files first.")
            return

        DescriptionParser.enrich_inventory_parts(self.inventory)
        DescriptionParser.enrich_npr_parts(self.npr_list)

        engine = MatchingEngine(self.inventory)
        self.match_results = engine.match_npr_list(self.npr_list)

        self.render_table("all")

    # -----------------------------------------------------
    # TABLE RENDERING
    # -----------------------------------------------------
    def render_table(self, mode="all"):
        for row in self.tree.get_children():
            self.tree.delete(row)

        self.row_index_map.clear()
        self.expanded_item = None
        self.raw_expanded.clear()
        self.last_clicked_item = None
        self.last_clicked_col = None

        

        for idx, (npr, match) in enumerate(self.match_results):
            exists = match.match_type != MatchType.NO_MATCH

            if match.match_type == MatchType.EXACT_MFG_PN:
                tag = "exact"
            elif match.match_type == MatchType.EXACT_ITEMNUM:
                tag = "internal"
            elif match.match_type == MatchType.PARSED_MATCH:
                tag = "parsed"
            elif match.match_type == MatchType.PREFIX_FAMILY:
                tag = "prefix"
            else:
                tag = "missing"

            if mode == "exists" and not exists:
                continue
            if mode == "missing" and exists:
                continue

            inv = match.inventory_part
            internal_num = inv.itemnum if inv else ""
            vendor_num = inv.vendoritem if inv else ""

            item_id = self.tree.insert(
                "",
                "end",
                text="",
                values=("",  # Details column text
                    internal_num,
                    vendor_num,
                    npr.description,
                    match.match_type.value,
                    f"{match.confidence:.2f}"
                ),
                tags=(tag,)
            )

            self.row_index_map[item_id] = idx
            self.raw_expanded[item_id] = False

        item_id = self.tree.insert(
            "",
            "end",
            text="",
            values=(
                "",
                internal_num,
                vendor_num,
                npr.description,
                match.match_type.value,
                f"{match.confidence:.2f}"
            ),
            tags=(tag,)
        )
        
        self.row_index_map[item_id] = idx
        self.raw_expanded[item_id] = False
        self.detail_rows[item_id] = []
        self.expanded_state[item_id] = False



    def _create_tooltip(self, text, x, y):
        """Create a light modern tooltip near the cursor."""
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
            highlightbackground="#d0d0d0"
        )
        frame.pack()

        label = tk.Label(
            frame,
            text=text,
            font=("Segoe UI", 9),
            bg="#fefefe",
            fg="#000000",
            justify="left"
        )
        label.pack()

        self.tooltip.geometry(f"+{x+12}+{y+12}")

    def _hide_tooltip(self):
        """Remove any visible tooltip and cancel timers."""
        if self.tooltip:
            try:
                self.tooltip.destroy()
            except:
                pass
            self.tooltip = None

        if self.hover_after_id:
            try:
                self.root.after_cancel(self.hover_after_id)
            except:
                pass
            self.hover_after_id = None


    def _on_hover(self, event):
        """Triggered whenever the mouse moves inside the Treeview."""
        row = self.tree.identify_row(event.y)
        col = self.tree.identify_column(event.x)

        # If hovering nothing → hide tooltip
        if not row or not col:
            self._hide_tooltip()
            self.hover_row = None
            self.hover_col = None
            return

        # If still over same exact cell → do nothing
        if row == self.hover_row and col == self.hover_col:
            return

        # Cell changed → hide tooltip and start new delay
        self._hide_tooltip()
        self.hover_row = row
        self.hover_col = col

        # Delay appearance of tooltip (600 ms)
        self.hover_after_id = self.root.after(
            600,
            lambda: self._show_hover_tooltip(event.x_root, event.y_root)
        )


    def _show_hover_tooltip(self, x, y):
        """Compute tooltip text and display it."""
        if not self.hover_row or not self.hover_col:
            return


        item = self.tree.item(self.hover_row)
        text = item.get("text", "")
        values = item.get("values", ())

        # Determine column label
        if self.hover_col == "#0":
            
            value = text
        else:
            try:
                idx = int(self.hover_col[1:]) - 1
                #col_label = self.tree["columns"][idx]
                value = values[idx]
            except:
                #col_label = "Unknown"
                value = ""

        #tooltip_text = f"{col_label}:\n{value}"
        tooltip_text = f"{value}"
        # Create tooltip
        self._create_tooltip(tooltip_text, x, y)



    def toggle_expander(self, event):
        row = self.tree.identify_row(event.y)
        if not row:
            return

        tags = self.tree.item(row, "tags")

        # --------------------------------------------------------
        # 1. RAW TOGGLE ROW HANDLING
        # --------------------------------------------------------
        if "raw_toggle" in tags:
            parent_id = None

            # Find parent owning this toggle row
            for pid, rows in self.detail_rows.items():
                if row in rows:
                    parent_id = pid
                    break

            if parent_id:
                # Flip raw state
                self.raw_expanded[parent_id] = not self.raw_expanded.get(parent_id, False)
                # Re-expand to redraw raw fields
                self.expand_item(parent_id)

            return  # Prevent any further behavior


        # --------------------------------------------------------
        # 2. IGNORE DOUBLE-CLICKS ON DETAIL ROWS (NON-TOGGLE)
        # --------------------------------------------------------
        for pid, rows in self.detail_rows.items():
            if row in rows:
                # If it is a detail row BUT NOT raw_toggle, ignore
                return


        # --------------------------------------------------------
        # 3. MAIN ROW TOGGLE: EXPAND / COLLAPSE
        # --------------------------------------------------------
        if self.expanded_state.get(row, False):
            self.collapse_item(row)
        else:
            self.expand_item(row)



    def collapse_item(self, item_id):
        # Remove flat detail rows
        if item_id in self.detail_rows:
            for rid in self.detail_rows[item_id]:
                try:
                    self.tree.delete(rid)
                except:
                    pass

        self.detail_rows[item_id] = []
        self.expanded_state[item_id] = False


    def expand_item(self, item_id):
        if item_id not in self.row_index_map:
            return

        # Prevent duplicates
        if self.expanded_state.get(item_id, False):
            self.collapse_item(item_id)
            return

        # First collapse any existing rows
        self.collapse_item(item_id)

        self.detail_rows[item_id] = []
        self.expanded_state[item_id] = True

        idx = self.row_index_map[item_id]
        npr, match = self.match_results[idx]
        inv = match.inventory_part

        # Base insertion index
        insert_at = self.tree.index(item_id) + 1

        # Helper for orderly insertion
        def add_row(text, values, tag):
            nonlocal insert_at
            row = self.tree.insert(
                "",
                insert_at,
                values=("    " + text, *values),
                tags=(tag,)
            )
            insert_at += 1
            self.detail_rows[item_id].append(row)
            return row

        # SECTION HEADER
        add_row(
            "Match Details (Inline view – columns reused below, not main table)",
            ["", "", "", "", ""],
            "detail_section_header"
        )

        # SUMMARY
        part_num = getattr(npr, "partnum", "") or ""
        inv_num = getattr(inv, "itemnum", "") if inv else ""
        part_mfgpn = getattr(npr, "mfgpn", "") or ""
        inv_mfgpn = getattr(inv, "vendoritem", "") if inv else ""

        summary_text = (
            f"Match Type: {match.match_type.value}   |   "
            f"Confidence: {match.confidence:.2f}   |   "
            f"Notes: {match.notes or ''}"
        )

        add_row(
            "Summary",
            [part_num, inv_num, summary_text, "", ""],
            "detail_summary"
        )

        # PARSED HEADER
        add_row(
            "Parsed Comparison",
            ["Part Value", "Inventory Value", "", "", ""],
            "detail_header"
        )

        # DESCRIPTION FIRST
        add_row(
            "description",
            [npr.description or "", inv.description if inv else "", "", "", ""],
            "detail_row"
        )

        # MFG PN NEXT
        if part_mfgpn or inv_mfgpn:
            add_row(
                "MFG PN",
                [part_mfgpn, inv_mfgpn, "", "", ""],
                "detail_row"
            )

        # Dynamic parsed rows
        npr_parsed = getattr(npr, "parsed", {}) or {}
        inv_parsed = getattr(inv, "parsed", {}) or {}

        skip = {"description", "mfgpn", "manufacturer", "mfg"}

        for key in sorted(set(npr_parsed) | set(inv_parsed)):
            if key.lower() in skip:
                continue
            pv = self._display_value(npr_parsed.get(key, ""))
            iv = self._display_value(inv_parsed.get(key, ""))

            if pv or iv:
                add_row(key, [pv, iv, "", "", ""], "detail_row")

        # RAW TOGGLE
        raw_open = self.raw_expanded.get(item_id, False)
        toggle_label = "Hide Raw Fields" if raw_open else "Show Raw Fields"

        toggle_row = add_row(
            toggle_label,
            ["(double-click to toggle raw field comparison)", "", "", "", ""],
            "raw_toggle"
        )

        # Store where raw toggle lives:
        self.raw_toggle_row = toggle_row

        # RAW FIELDS IF OPEN
        if raw_open:
            npr_raw = getattr(npr, "raw_fields", {}) or {}
            inv_raw = getattr(inv, "raw_fields", {}) or {}

            if npr_raw or inv_raw:
                add_row(
                    "Raw Field Comparison",
                    ["Part Raw", "Inventory Raw", "", "", ""],
                    "detail_header"
                )

                for key in sorted(set(npr_raw) | set(inv_raw)):
                    pv = self._display_value(npr_raw.get(key, ""))
                    iv = self._display_value(inv_raw.get(key, ""))
                    if pv or iv:
                        add_row(key, [pv, iv, "", "", ""], "detail_row")




    def _insert_detail_row(self, parent_id, text, values, tag):
        """
        Insert a flat row directly beneath parent_id with indentation.
        """
        parent_index = self.tree.index(parent_id)
        indent_text = "    " + text  # manual indentation

        return self.tree.insert(
            "",
            parent_index + 1,
            values=(indent_text, *values),
            tags=(tag,)
        )

    # -----------------------------------------------------
    # CLICK / COPY SUPPORT
    # -----------------------------------------------------
    def on_tree_click(self, event):
        """
        Track which cell (row + column) was clicked so Ctrl+C
        can copy just that cell.
        """
        row_id = self.tree.identify_row(event.y)
        col_id = self.tree.identify_column(event.x)  # '#0', '#1', ...
        if not row_id:
            self.last_clicked_item = None
            self.last_clicked_col = None
            return

        self.last_clicked_item = row_id
        self.last_clicked_col = col_id

    def copy_selected_cell(self, event=None):
        """
        Copies the last-clicked cell to clipboard.
        If we don't know a specific cell, copy the whole row.
        """
        item_id = self.last_clicked_item or self.tree.focus()
        if not item_id:
            return

        col_id = self.last_clicked_col or "#0"

        # Get underlying data
        item = self.tree.item(item_id)
        text = item.get("text", "") or ""
        values = item.get("values", ()) or ()

        # Decide what to copy
        if col_id == "#0":
            to_copy = str(text)
        else:
            try:
                idx = int(col_id[1:]) - 1  # '#1' -> 0
                if 0 <= idx < len(values):
                    to_copy = str(values[idx])
                else:
                    to_copy = ""
            except Exception:
                to_copy = ""

        # Fallback to entire row if empty or something went weird
        if not to_copy:
            to_copy = "\t".join([str(text)] + [str(v) for v in values])

        try:
            self.root.clipboard_clear()
            self.root.clipboard_append(to_copy)
        except Exception:
            pass  # best-effort

    # -----------------------------------------------------
    # EXPORTERS
    # -----------------------------------------------------
    def export_existing(self):
        if not self.match_results:
            messagebox.showerror("Error", "Run matching first.")
            return

        out_dir = "./data"
        os.makedirs(out_dir, exist_ok=True)

        output_path = os.path.join(out_dir, "existing_parts.xlsx")

        rows = []
        for npr, match in self.match_results:
            if match.confidence < 1.0:
                continue

            inv = match.inventory_part
            if not inv:
                continue

            rows.append({
                "Internal Part #": inv.itemnum,
                "Item Description": inv.description,
                "Manufacturer Part #": inv.vendoritem,
                "Supplier": inv.mfgname,
                "Exists": "YES"
            })

        if not rows:
            messagebox.showinfo("Export Complete", "No fully matched existing parts to export.")
            return

        pd.DataFrame(rows).to_excel(output_path, index=False)
        messagebox.showinfo("Export Complete", f"Existing parts exported to:\n{output_path}")

    def export_missing(self):
        if not self.match_results:
            messagebox.showerror("Error", "Run matching first.")
            return

        out_dir = "./data"
        os.makedirs(out_dir, exist_ok=True)

        output_path = os.path.join(out_dir, "missing_parts.xlsx")

        rows = []
        for npr, match in self.match_results:
            if match.match_type != MatchType.NO_MATCH:
                continue

            rows.append({
                "Part Number": npr.partnum or "",
                "Item Description": npr.description,
                "Manufacturer Part #": npr.mfgpn,
                "Supplier": npr.supplier,
                "Exists": "NO",
                "Reason": match.notes
            })

        if not rows:
            messagebox.showinfo("Export Complete", "No missing parts to export.")
            return

        pd.DataFrame(rows).to_excel(output_path, index=False)
        messagebox.showinfo("Export Complete", f"Missing parts exported to:\n{output_path}")

    # -----------------------------------------------------
    # HELPER
    # -----------------------------------------------------
    @staticmethod
    def _display_value(val):
        if val is None:
            return ""
        return str(val)


if __name__ == "__main__":
    root = tk.Tk()
    app = NPRToolUI(root)
    root.mainloop()
