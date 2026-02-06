import tkinter as tk
from tkinter import ttk
import random

class NPRToolUI:
    def __init__(self, root, callbacks=None):
        self.root = root
        self.root.title("NPR Tool — Black Box UI v4 (Persistent State)")
        self.root.geometry("1400x950")

        self.style = ttk.Style()
        self.configure_styles()

        self.callbacks = callbacks or {}
        self.bom_data = []
        self.user_actions = {"accepted": [], "rejected": []}
        self.last_hovered = None

        # Persistent memory
        self.card_memory = {}     # {bom_id: {company_pn: "add"/"reject"/"pending"}}
        self.digikey_cache = {}   # {bom_id: [list of cached matches]}
        self.external_card_frames = {}

        self.create_toolbar()
        self.create_bom_table()
        self.create_bom_header()
        self.create_detail_panel()

    # === Styles ===
    def configure_styles(self):
        self.style.configure('Detail.TLabelframe', background='#f5f5f5')

    # === Public API ===
    def load_bom_data(self, bom_data):
        self.bom_data = bom_data
        for i in self.tree.get_children():
            self.tree.delete(i)
        for bom in bom_data:
            self.tree.insert('', tk.END, values=(
                bom.get('id', ''),
                "✅" if bom.get('exists') else "❌",
                bom.get('company_pn', '—'),
                bom.get('customer_pn', '—'),
                bom.get('status', 'Unknown'),
                f"{int(bom.get('confidence', 0)*100)}%",
                len(bom.get('matches', []))
            ))

    def get_results(self):
        return self.user_actions

    def load_digikey_matches(self, bom_id, results):
        """Externally inject DigiKey search results (cached)."""
        self.digikey_cache[bom_id] = results  # cache them permanently

        ext_refs = self.external_card_frames.get(bom_id)
        if not ext_refs:
            return
        container = ext_refs.get("container")
        if not container:
            return

        cols = 3
        start_row = container.grid_size()[1]
        saved_state = self.card_memory.get(bom_id, {})

        for i, match in enumerate(results):
            state = saved_state.get(match['company_pn'], 'pending')
            if state == 'reject':
                continue
            card = self.create_match_card(container, match, immutable=False)
            if state == 'add':
                card.config(bg='#c9f7d4', highlightbackground="#4CAF50", highlightthickness=2)
                for child in card.winfo_children():
                    for btn in child.winfo_children():
                        if isinstance(btn, ttk.Button):
                            btn.state(['disabled'])
            r, c = divmod(i, cols)
            card.grid(row=start_row + r, column=c, padx=10, pady=10, sticky='nsew')

        for i in range(cols):
            container.columnconfigure(i, weight=1)

    # === Toolbar ===
    def create_toolbar(self):
        toolbar = ttk.Frame(self.root)
        toolbar.pack(fill='x', pady=5)
        ttk.Label(toolbar, text="NPR Decision Workspace", font=("Segoe UI", 16, "bold")).pack(side='left', padx=10)
        ttk.Button(toolbar, text="Export NPR", command=self.export_npr).pack(side='left', padx=5)

    # === BOM Table ===
    def create_bom_table(self):
        frame = ttk.Frame(self.root)
        frame.pack(fill='x', padx=5)
        columns = ("Part ID", "Exists", "Company PN", "Customer PN", "Status", "Confidence", "Matches")
        self.tree = ttk.Treeview(frame, columns=columns, show='headings', height=10)
        for col in columns:
            self.tree.heading(col, text=col)
            self.tree.column(col, width=150, anchor='center')
        self.tree.bind('<<TreeviewSelect>>', self.on_bom_select)
        scrollbar = ttk.Scrollbar(frame, orient='vertical', command=self.tree.yview)
        self.tree.configure(yscrollcommand=scrollbar.set)
        self.tree.pack(side='left', fill='x', expand=True)
        scrollbar.pack(side='right', fill='y')

    # === Header ===
    def create_bom_header(self):
        self.header_frame = ttk.Frame(self.root, padding=10)
        self.header_frame.pack(fill='x', pady=5)
        self.header_inner = ttk.Frame(self.header_frame)
        self.header_inner.pack(anchor='center')
        self.header_part_label = ttk.Label(self.header_inner, text="Select a BOM part to view details", font=("Segoe UI", 13, "bold"))
        self.header_part_label.pack(anchor='center', pady=3)
        self.header_specs_label = ttk.Label(self.header_inner, text="", font=("Segoe UI", 11))
        self.header_specs_label.pack(anchor='center', pady=2)
        self.header_conf_label = ttk.Label(self.header_inner, text="", font=("Segoe UI", 10, 'italic'))
        self.header_conf_label.pack(anchor='center', pady=2)

    # === Detail Panel ===
    def create_detail_panel(self):
        self.detail_pane = ttk.Panedwindow(self.root, orient='horizontal')
        self.detail_pane.pack(fill='both', expand=True)
        left_frame = ttk.Frame(self.detail_pane)
        self.detail_pane.add(left_frame, weight=3)
        self.matches_canvas = tk.Canvas(left_frame, bg='#fafafa')
        self.matches_scrollbar = ttk.Scrollbar(left_frame, orient='vertical', command=self.matches_canvas.yview)
        self.matches_frame = ttk.Frame(self.matches_canvas)
        self.matches_frame.bind('<Configure>', lambda e: self.matches_canvas.configure(scrollregion=self.matches_canvas.bbox('all')))
        self.matches_canvas.create_window((0, 0), window=self.matches_frame, anchor='nw')
        self.matches_canvas.configure(yscrollcommand=self.matches_scrollbar.set)
        self.matches_canvas.pack(side='left', fill='both', expand=True)
        self.matches_scrollbar.pack(side='right', fill='y')
        self.right_frame = ttk.Frame(self.detail_pane, padding=15, style='Detail.TLabelframe')
        self.detail_pane.add(self.right_frame, weight=2)
        self._init_detail_content()

    def _init_detail_content(self):
        self.detail_title = ttk.Label(self.right_frame, text="Select a Part", font=("Segoe UI", 14, "bold"))
        self.detail_title.pack(anchor='w', pady=(0,5))
        self.detail_desc = ttk.Label(self.right_frame, text="No data loaded yet.", font=("Segoe UI", 10, "italic"), wraplength=400)
        self.detail_desc.pack(anchor='w', pady=(0,10))
        ttk.Separator(self.right_frame, orient='horizontal').pack(fill='x', pady=5)
        self.specs_frame = ttk.LabelFrame(self.right_frame, text="🔧 Specifications")
        self.specs_frame.pack(fill='x', pady=5)
        self.spec_labels = {k: ttk.Label(self.specs_frame, text="—") for k in ['Value','Package','Tolerance','Voltage']}
        for k,v in self.spec_labels.items():
            ttk.Label(self.specs_frame, text=f"{k}: ", font=("Segoe UI",9,"bold")).pack(anchor='w'); v.pack(anchor='w', padx=15)
        ttk.Separator(self.right_frame, orient='horizontal').pack(fill='x', pady=5)
        conf_frame = ttk.Frame(self.right_frame); conf_frame.pack(fill='x', pady=5)
        ttk.Label(conf_frame, text="📈 Confidence:", font=("Segoe UI",10,"bold")).pack(anchor='w')
        self.conf_progress = ttk.Progressbar(conf_frame, orient='horizontal', length=300, mode='determinate'); self.conf_progress.pack(anchor='w', pady=3)
        self.conf_value_label = ttk.Label(conf_frame, text="—", font=("Segoe UI",9,"italic")); self.conf_value_label.pack(anchor='w')
        ttk.Separator(self.right_frame, orient='horizontal').pack(fill='x', pady=5)
        self.stock_label = ttk.Label(self.right_frame, text="🏭 Stock: — pcs", font=("Segoe UI",10)); self.stock_label.pack(anchor='w', pady=2)
        self.relation_label = ttk.Label(self.right_frame, text="🔗 Relationship: —", font=("Segoe UI",10)); self.relation_label.pack(anchor='w', pady=2)

    # === Core Logic ===
    def on_bom_select(self, event):
        selected = self.tree.selection()
        if not selected:
            return
        bom_id = self.tree.item(selected[0])['values'][0]
        data = next((x for x in self.bom_data if x['id'] == bom_id), None)
        if not data:
            return
        self.update_bom_header(data)
        self.render_matches(bom_id, data)

    def render_matches(self, bom_id, data):
        self.current_bom = bom_id
        for w in self.matches_frame.winfo_children():
            w.destroy()
        internal_frame = ttk.LabelFrame(self.matches_frame, text="🔒 Internal Matches")
        internal_frame.pack(fill='x', padx=10, pady=5)
        external_frame = ttk.LabelFrame(self.matches_frame, text="🔍 External Alternates")
        external_frame.pack(fill='x', padx=10, pady=5)
        self.external_card_frames[bom_id] = {}

        # DigiKey search bar
        search_bar = ttk.Frame(external_frame)
        search_bar.pack(fill='x', padx=10, pady=5)
        already_searched = bom_id in self.digikey_cache
        search_btn = ttk.Button(search_bar, text="Search DigiKey", command=lambda: self._trigger_digikey(bom_id))
        search_btn.pack(side='left', padx=5)
        if already_searched:
            search_btn.state(['disabled'])
            ttk.Label(search_bar, text="(cached)").pack(side='left', padx=3)

        external_cards_container = ttk.Frame(external_frame)
        external_cards_container.pack(fill='both', expand=True, padx=10, pady=5)
        self.external_card_frames[bom_id]["container"] = external_cards_container

        saved_state = self.card_memory.get(bom_id, {})
        cols = 3

        for i, match in enumerate(data.get('matches', [])):
            state = saved_state.get(match['company_pn'], 'pending')
            if state == 'reject':
                continue
            parent = external_cards_container if match.get('source') == 'external' else internal_frame
            card = self.create_match_card(parent, match, match.get('source') == 'internal')
            if state == 'add':
                card.config(bg='#c9f7d4', highlightbackground="#4CAF50", highlightthickness=2)
                for child in card.winfo_children():
                    for btn in child.winfo_children():
                        if isinstance(btn, ttk.Button):
                            btn.state(['disabled'])
            r, c = divmod(i, cols)
            card.grid(row=r, column=c, padx=10, pady=10, sticky='nsew')

        for i in range(cols):
            internal_frame.columnconfigure(i, weight=1)
            external_cards_container.columnconfigure(i, weight=1)

        if already_searched:
            self.load_digikey_matches(bom_id, self.digikey_cache[bom_id])

    def _trigger_digikey(self, bom_id):
        if bom_id in self.digikey_cache:
            return
        if self.callbacks and "on_digikey_search" in self.callbacks:
            results = self.callbacks["on_digikey_search"](bom_id)
            if results:
                self.digikey_cache[bom_id] = results
                self.load_digikey_matches(bom_id, results)

    def create_match_card(self, parent, match, immutable=False):
        frame = tk.Frame(parent, bg='white', bd=1, relief='ridge', highlightthickness=0)
        def hover(e, m=match, f=frame):
            if self.last_hovered and self.last_hovered != f:
                try:
                    if self.last_hovered.winfo_exists():
                        self.last_hovered.config(bg='white', highlightbackground="#ccc", highlightthickness=1)
                except tk.TclError:
                    self.last_hovered = None
            f.config(bg='#f2f7ff', highlightbackground="#66a3ff", highlightthickness=2)
            self.last_hovered = f
            self.update_hover_detail(m)
        frame.bind('<Enter>', hover)
        top = ttk.Frame(frame)
        top.pack(fill='x', pady=(5,2), padx=5)
        ttk.Label(top, text=f"{match['company_pn']} ({match.get('manufacturer','Unknown')})",
                  font=("Segoe UI", 11, "bold")).pack(side='left')
        ttk.Label(top, text=f"{int(match['confidence']*100)}%", foreground=self.conf_color(match['confidence'])).pack(side='right')
        ttk.Label(frame, text=match['description'], wraplength=400, background='white').pack(anchor='w', padx=5, pady=3)
        ttk.Label(frame, text=f"{match['value']} | {match['package']} | {match['tolerance']} | {match['voltage']}",
                  font=("Segoe UI", 9), background='white').pack(anchor='w', padx=5)
        ttk.Label(frame, text=f"Stock: {match['stock']} pcs", font=("Segoe UI", 9, "italic"),
                  foreground='#555', background='white').pack(anchor='w', padx=5, pady=2)
        ttk.Separator(frame, orient='horizontal').pack(fill='x', pady=4)
        bf = ttk.Frame(frame)
        bf.pack(anchor='e', pady=3)
        ttk.Button(bf, text="Add", command=lambda: self.handle_action('add', match, frame)).pack(side='left', padx=3)
        ttk.Button(bf, text="Reject", command=lambda: self.handle_action('reject', match, frame)).pack(side='left', padx=3)
        return frame

    def handle_action(self, action, match, frame=None):
        bom_id = getattr(self, "current_bom", None)
        if not bom_id:
            return
        if bom_id not in self.card_memory:
            self.card_memory[bom_id] = {}
        self.card_memory[bom_id][match['company_pn']] = action
        if action == 'add':
            self.user_actions['accepted'].append(match)
        elif action == 'reject':
            self.user_actions['rejected'].append(match)
        if frame:
            try:
                if action == 'add':
                    frame.config(bg='#c9f7d4', highlightbackground="#4CAF50", highlightthickness=2)
                    for child in frame.winfo_children():
                        for btn in child.winfo_children():
                            if isinstance(btn, ttk.Button):
                                btn.state(['disabled'])
                elif action == 'reject':
                    if self.last_hovered == frame:
                        self.last_hovered = None
                    self.fade_and_destroy(frame)
            except tk.TclError:
                pass
        cb = self.callbacks.get(f'on_{action}')
        if cb:
            cb(match)

    def fade_and_destroy(self, widget, steps=5):
        if not widget or not widget.winfo_exists():
            return
        def fade(step):
            if step <= 0:
                try:
                    widget.destroy()
                except tk.TclError:
                    pass
                return
            try:
                shade = 255 - int((step/steps)*40)
                widget.config(bg=f'#ff{shade:02x}{shade:02x}')
            except tk.TclError:
                return
            widget.after(40, fade, step-1)
        fade(steps)

    def export_npr(self):
        cb = self.callbacks.get('on_export')
        if cb:
            cb(self.user_actions)

    def update_hover_detail(self, m):
        self.detail_title.config(text=f"{m['company_pn']} ({m['manufacturer']})")
        self.detail_desc.config(text=m['description'])
        for k in ['Value','Package','Tolerance','Voltage']:
            self.spec_labels[k].config(text=m.get(k.lower(),'—'))
        self.conf_progress['value']=int(m['confidence']*100)
        self.conf_value_label.config(text=f"{int(m['confidence']*100)}%")
        self.stock_label.config(text=f"🏭 Stock: {m['stock']} pcs")
        self.relation_label.config(text=f"🔗 Relationship: {m['relationship']}")

    def update_bom_header(self, bom):
        self.header_part_label.config(text=f"BOM Part: {bom['customer_pn']} — {bom['description']}")
        self.header_specs_label.config(text=f"Manufacturer: {bom['manufacturer']} | Value: {bom['value']} | Package: {bom['package']}")
        self.header_conf_label.config(text=f"Confidence: {int(bom['confidence']*100)}% | Matches: {len(bom['matches'])}")

    def conf_color(self, conf):
        if conf >= 0.9: return 'green'
        elif conf >= 0.7: return 'orange'
        return 'red'

# === TEST MAIN ===
if __name__ == "__main__":
    def on_digikey_search(bom_id):
        print(f"🔍 Searching DigiKey for alternates for {bom_id}...")
        return [{
            "source": "external",
            "company_pn": f"DK-{random.randint(1000,9999)}",
            "manufacturer": random.choice(["TDK","Murata","Panasonic"]),
            "confidence": round(random.uniform(0.7,0.95),2),
            "description": "Chip Resistor 0603 ±1%",
            "value": "10kΩ",
            "package": "0603",
            "tolerance": "1%",
            "voltage": "50V",
            "stock": random.randint(1000,10000),
            "relationship": "Alternate"
        } for _ in range(3)]

    def on_add(m): print(f"✅ Accepted: {m['company_pn']}")
    def on_reject(m): print(f"❌ Rejected: {m['company_pn']}")
    def on_export(r):
        print("📦 Export:", r)

    root = tk.Tk()
    ui = NPRToolUI(root, callbacks={
        "on_add": on_add,
        "on_reject": on_reject,
        "on_export": on_export,
        "on_digikey_search": on_digikey_search
    })

    # === Test Data ===
    ui.load_bom_data([
        {
            "id": "BOM-001",
            "customer_pn": "R-10K",
            "description": "10kΩ ±1% 0603 Resistor",
            "manufacturer": "Vishay",
            "company_pn": "RES-1002",
            "exists": True,
            "status": "Exists",
            "confidence": 0.94,
            "value": "10kΩ",
            "package": "0603",
            "matches": [
                # Internal match with valid MPN (✅ Verified)
                {
                    "source": "internal",
                    "company_pn": "RES-1002",
                    "manufacturer": "Vishay",
                    "confidence": 0.94,
                    "description": "Thin Film Resistor ±1% 0603",
                    "value": "10kΩ",
                    "package": "0603",
                    "tolerance": "1%",
                    "voltage": "50V",
                    "stock": 10000,
                    "relationship": "Identical"
                },
                # Internal match missing manufacturer (⚠️ Description-only)
                {
                    "source": "internal",
                    "company_pn": "RES-1050",
                    "manufacturer": None,  # intentionally missing MPN
                    "confidence": 0.81,
                    "description": "Resistor 10kΩ 1% 0603 (parsed by description)",
                    "value": "10kΩ",
                    "package": "0603",
                    "tolerance": "1%",
                    "voltage": "50V",
                    "stock": 2400,
                    "relationship": "Parsed by Description"
                }
            ]
        },
        {
            "id": "BOM-002",
            "customer_pn": "C-1uF",
            "description": "Capacitor 1uF 16V X7R 0603",
            "manufacturer": "Samsung",
            "company_pn": None,
            "exists": False,
            "status": "New",
            "confidence": 0.77,
            "value": "1uF",
            "package": "0603",
            "matches": []
        }
    ])

    root.mainloop()
   
