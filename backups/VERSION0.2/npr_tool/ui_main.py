import os
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import pandas as pd

from .data_loader import DataLoader
from .matching_engine import MatchingEngine
from .data_models import MatchType
from .parsing_engine import DescriptionParser


class NPRToolUI:
    def __init__(self, root):
        self.root = root
        self.root.title("NPR Matching Tool")
        self.root.geometry("1500x950")

        self.inventory = []
        self.npr_list = []
        self.match_results = []  # (NPRPart, MatchResult)

        self.row_index_map = {}  # map Treeview item -> match index

        self.details_visible = False

        self._build_ui()

    # -----------------------------------------------------
    # UI SETUP
    # -----------------------------------------------------
    def _build_ui(self):
        tk.Label(
            self.root,
            text="🧰 NPR Part Matching Tool",
            font=("Segoe UI", 22, "bold")
        ).pack(pady=10)

        # Load Buttons
        load_frame = tk.Frame(self.root)
        load_frame.pack(pady=5)

        tk.Button(load_frame, text="Load Inventory", command=self.load_inventory).grid(row=0, column=0, padx=5)
        tk.Button(load_frame, text="Load Parts", command=self.load_npr).grid(row=0, column=1, padx=5)
        tk.Button(load_frame, text="Run Matching", command=self.run_matching).grid(row=0, column=2, padx=5)
        tk.Button(load_frame, text="Export Existing Parts", command=self.export_existing).grid(row=0, column=3, padx=5)
        tk.Button(load_frame, text="Export Missing Parts", command=self.export_missing).grid(row=0, column=4, padx=5)
        tk.Button(load_frame, text="Download Blank Template", command=self.export_blank_template).grid(row=0, column=5, padx=5)



        # Filters
        filter_frame = tk.Frame(self.root)
        filter_frame.pack(pady=8)

        tk.Button(filter_frame, text="Show ALL", width=14, command=lambda: self.render_table("all")).grid(row=0, column=0, padx=5)
        tk.Button(filter_frame, text="Show EXISTS", width=14, command=lambda: self.render_table("exists")).grid(row=0, column=1, padx=5)
        tk.Button(filter_frame, text="Show MISSING", width=14, command=lambda: self.render_table("missing")).grid(row=0, column=2, padx=5)

        # Table + Scrollbar
        table_frame = tk.Frame(self.root)
        table_frame.pack(fill="both", expand=True)

        columns = (
            "Internal Part #",
            "Manufacturer Part #",
            "NPR Description",
            "Match Type",
            "Confidence"
        )

        self.tree = ttk.Treeview(table_frame, columns=columns, show="headings", height=22)

        for col in columns:
            self.tree.heading(col, text=col)
            self.tree.column(col, width=285, anchor="w")

        scrollbar = ttk.Scrollbar(table_frame, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=scrollbar.set)

        self.tree.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")

        # Row colors
        self.tree.tag_configure("exact", background="#d4f7d0")      # green
        self.tree.tag_configure("internal", background="#fff3b0")   # yellow
        self.tree.tag_configure("missing", background="#f7d0d0")    # red
        self.tree.tag_configure("parsed", background="#d0e7ff")     # light blue
        self.tree.tag_configure("prefix", background="#e8ccff")     # purple-ish


        # Bind double click
        self.tree.bind("<Double-1>", self.show_details)

        # -----------------------------------
        # DETAILS PANEL (scrollable, bordered)
        # -----------------------------------
        self.details_frame = tk.Frame(self.root, height=300, bd=2, relief="ridge")
        self.details_frame.pack_propagate(False)  # lock height

        self.details_title = tk.Label(
            self.details_frame, text="Part Details",
            font=("Segoe UI", 15, "bold")
        )
        self.details_title.pack(pady=5)

        # Scrollable area
        self.canvas = tk.Canvas(self.details_frame)
        self.scrollbar = ttk.Scrollbar(self.details_frame, orient="vertical", command=self.canvas.yview)
        self.canvas.configure(yscrollcommand=self.scrollbar.set)

        self.canvas.pack(side="left", fill="both", expand=True)
        self.scrollbar.pack(side="right", fill="y")

        self.content_frame = tk.Frame(self.canvas)
        self.canvas.create_window((0, 0), window=self.content_frame, anchor="nw")

        self.content_frame.bind("<Configure>", lambda e: self.canvas.configure(scrollregion=self.canvas.bbox("all")))

        # Side-by-side details
        self.npr_frame = tk.LabelFrame(self.content_frame, text="NPR Fields", font=("Segoe UI", 11, "bold"), padx=8, pady=8)
        self.inv_frame = tk.LabelFrame(self.content_frame, text="Inventory Fields", font=("Segoe UI", 11, "bold"), padx=8, pady=8)

        self.npr_frame.grid(row=0, column=0, sticky="nw", padx=20)
        self.inv_frame.grid(row=0, column=1, sticky="nw", padx=20)

        # Footer summary
        self.footer_frame = tk.Frame(self.details_frame)
        self.footer_frame.pack(fill="x", pady=4)

        self.match_info_label = tk.Label(self.footer_frame, text="", font=("Segoe UI", 11))
        self.match_info_label.pack()

    # -----------------------------------------------------
    # LOADERS
    # -----------------------------------------------------
    def load_inventory(self):
        path = filedialog.askopenfilename(filetypes=[("Excel Files", "*.xlsx")])
        if not path:
            return
        try:
            self.inventory = DataLoader.load_inventory(path)
            messagebox.showinfo("Success", "Inventory Loaded.")
        except Exception as e:
            messagebox.showerror("Error", str(e))

    def load_npr(self):
        path = filedialog.askopenfilename(filetypes=[("Excel Files", "*.xlsx")])
        if not path:
            return
        try:
            self.npr_list = DataLoader.load_npr(path)
            messagebox.showinfo("Success", "NPR Loaded.")
        except Exception as e:
            messagebox.showerror("Error", str(e))

    def export_blank_template(self):
        out_dir = "./data"
        os.makedirs(out_dir, exist_ok=True)
    
        output_path = os.path.join(out_dir, "blank_simple_template.xlsx")
    
        df = pd.DataFrame({
            "Description": [],
            "Manufacturer Part Number": []
        })
    
        df.to_excel(output_path, index=False)
    
        messagebox.showinfo("Template Created", f"Blank template created at:\n{output_path}")



    # -----------------------------------------------------
    # RUN MATCHING
    # -----------------------------------------------------
    def run_matching(self):
        if not self.inventory or not self.npr_list:
            messagebox.showerror("Error", "Load both files first.")
            return

        # Populate parsed description fields
        DescriptionParser.enrich_inventory_parts(self.inventory)
        DescriptionParser.enrich_npr_parts(self.npr_list)

        for p in self.inventory[:5]:
            print("INV:", p.itemnum, p.vendoritem, p.description)

        for p in self.npr_list[:5]:
            print("NPR:", p.partnum, p.mfgpn, p.description)

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
                values=(
                    internal_num,
                    vendor_num,
                    npr.description,
                    match.match_type.value,
                    f"{match.confidence:.2f}"
                ),
                tags=(tag,)
            )

            self.row_index_map[item_id] = idx

    # -----------------------------------------------------
    # DETAILS PANEL
    # -----------------------------------------------------
    def add_row(self, frame, row, label, value):
        tk.Label(frame, text=f"{label}:", font=("Segoe UI", 10, "bold")).grid(row=row, column=0, sticky="w", padx=4)
        tk.Label(frame, text=value, font=("Segoe UI", 10)).grid(row=row, column=1, sticky="w", padx=4)
        return row + 1

    def show_details(self, event):
        selected = self.tree.focus()
        if not selected:
            return

        index = self.row_index_map[selected]
        npr, match = self.match_results[index]
        inv = match.inventory_part

        # Clear previous widgets
        for frame in (self.npr_frame, self.inv_frame):
            for w in frame.winfo_children():
                w.destroy()

        # NPR DETAILS
        row = 0
        tk.Label(self.npr_frame, text="Raw Fields", font=("Segoe UI", 11, "bold")).grid(row=row, column=0, columnspan=2)
        row += 1
        for key, val in npr.raw_fields.items():
            row = self.add_row(self.npr_frame, row, key, val)

        row += 1
        tk.Label(self.npr_frame, text="Parsed Description", font=("Segoe UI", 11, "bold")).grid(row=row, column=0, columnspan=2)
        row += 1
        for key, val in npr.parsed.items():
            row = self.add_row(self.npr_frame, row, key, val)

        # INVENTORY DETAILS
        if inv:
            row = 0
            tk.Label(self.inv_frame, text="Raw Fields", font=("Segoe UI", 11, "bold")).grid(row=row, column=0, columnspan=2)
            row += 1
            for key, val in inv.raw_fields.items():
                row = self.add_row(self.inv_frame, row, key, val)

            row += 1
            tk.Label(self.inv_frame, text="Parsed Description", font=("Segoe UI", 11, "bold")).grid(row=row, column=0, columnspan=2)
            row += 1
            for key, val in inv.parsed.items():
                row = self.add_row(self.inv_frame, row, key, val)
        else:
            tk.Label(self.inv_frame, text="No matching inventory part", font=("Segoe UI", 10, "italic")).grid(row=0, column=0)

        # Footer summary
        self.match_info_label.config(
            text=f"Match Type: {match.match_type.value}   |   "
                 f"Confidence: {match.confidence:.2f}   |   "
                 f"Notes: {match.notes}"
        )

        if not self.details_visible:
            self.details_visible = True
            self.details_frame.pack(fill="x", pady=5)

    # -----------------------------------------------------
    # EXPORTERS
    # -----------------------------------------------------
    def export_existing(self):
        if not self.match_results:
            messagebox.showerror("Error", "Run matching first.")
            return

        out_dir = "./data"
        os.makedirs(out_dir, exist_ok=True)

        output_path = os.path.join(out_dir, "npr_existing_parts.xlsx")

        rows = []
        for npr, match in self.match_results:
            if match.confidence < 1.0:
                continue

            inv = match.inventory_part

            rows.append({
                "Internal Part #": inv.itemnum,
                "Item Description": inv.description,
                "Manufacturer Part #": inv.vendoritem,
                "Supplier": inv.mfgname,
                "Exists": "YES"
            })

        pd.DataFrame(rows).to_excel(output_path, index=False)
        messagebox.showinfo("Export Complete", f"Existing parts exported to:\n{output_path}")

    def export_missing(self):
        if not self.match_results:
            messagebox.showerror("Error", "Run matching first.")
            return

        out_dir = "./data"
        os.makedirs(out_dir, exist_ok=True)

        output_path = os.path.join(out_dir, "npr_missing_parts.xlsx")

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

        pd.DataFrame(rows).to_excel(output_path, index=False)
        messagebox.showinfo("Export Complete", f"Missing parts exported to:\n{output_path}")


if __name__ == "__main__":
    root = tk.Tk()
    app = NPRToolUI(root)
    root.mainloop()
