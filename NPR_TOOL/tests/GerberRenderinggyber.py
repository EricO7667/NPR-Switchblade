from __future__ import annotations

import copy
import math
import os
import re
import tempfile
import tkinter as tk
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from tkinter import filedialog, messagebox

import tksvg



from pygerber.gerberx3.api.v2 import (
    ColorScheme,
    FileTypeEnum,
    GerberFile,
    OnParserErrorEnum,
)
# RGBA is located in the common utility module
from pygerber.common.rgba import RGBA
SVG_NS = "http://www.w3.org/2000/svg"
ET.register_namespace("", SVG_NS)


@dataclass
class LayerRecord:
    filename: str
    full_path: str
    file_type: FileTypeEnum
    parsed: object
    info: object


class VectorPartChecker:
    """Gerber-to-SVG viewer with parse-first loading and aligned SVG overlays.

    PATCHED VERSION:
    - Fixed CSS Class Collision using Scoped IDs.
    - Implemented custom RGBA color schemes.
    - Improved coordinate alignment for composite rendering.
    """

    def __init__(self, root: tk.Tk) -> None:
        self.root = root
        self.root.title("NPR Tool - Gerber Vector Viewer (Patched)")
        self.root.geometry("1280x920")
        self.root.configure(bg="#1e1e1e")

        self.loaded_files: list[LayerRecord] = []
        self.current_svg_path: str | None = None
        self.image_ref = None
        self.current_canvas_item: int | None = None
        self.last_skipped: list[str] = []
        # Changed to a dark background for better contrast with PCB layers
        self.svg_background = "#1a1a1a"
        self._resize_job: str | None = None

        self._build_ui()
        self._bind_events()

    def _build_ui(self) -> None:
        top = tk.Frame(self.root, bg="#333333", pady=8)
        top.pack(side=tk.TOP, fill=tk.X)

        tk.Button(top, text="Load Gerber Folder", command=self.load_folder, bg="#444444", fg="white", font=("Arial", 10, "bold"), relief=tk.FLAT, padx=10, pady=6).pack(side=tk.LEFT, padx=10)
        tk.Button(top, text="Export Current SVG", command=self.export_current_svg, bg="#444444", fg="white", font=("Arial", 10, "bold"), relief=tk.FLAT, padx=10, pady=6).pack(side=tk.LEFT, padx=(0, 10))
        tk.Button(top, text="Show Skipped", command=self.show_skipped, bg="#444444", fg="white", font=("Arial", 10, "bold"), relief=tk.FLAT, padx=10, pady=6).pack(side=tk.LEFT, padx=(0, 10))
        tk.Button(top, text="Select All Layers", command=self.select_all_layers, bg="#444444", fg="white", font=("Arial", 10, "bold"), relief=tk.FLAT, padx=10, pady=6).pack(side=tk.LEFT, padx=(0, 10))
        tk.Button(top, text="Clear Selection", command=self.clear_selection, bg="#444444", fg="white", font=("Arial", 10, "bold"), relief=tk.FLAT, padx=10, pady=6).pack(side=tk.LEFT, padx=(0, 10))

        tk.Label(top, text="Zoom", bg="#333333", fg="white", font=("Arial", 10, "bold")).pack(side=tk.LEFT, padx=(10, 4))
        self.zoom_scale = tk.Scale(top, from_=0.25, to=10.0, resolution=0.25, orient=tk.HORIZONTAL, bg="#333333", fg="white", highlightthickness=0, command=self.update_zoom, length=260)
        self.zoom_scale.set(1.0)
        self.zoom_scale.pack(side=tk.LEFT, padx=(0, 10))

        body = tk.Frame(self.root, bg="#1e1e1e")
        body.pack(fill=tk.BOTH, expand=True)

        left = tk.Frame(body, bg="#252525", width=380)
        left.pack(side=tk.LEFT, fill=tk.Y)
        left.pack_propagate(False)

        tk.Label(left, text="Detected Layers", bg="#252525", fg="white", font=("Arial", 11, "bold")).pack(anchor="w", padx=10, pady=(10, 6))
        tk.Label(left, text="Use Ctrl/Shift to select and overlay layers.", bg="#252525", fg="#bdbdbd", justify=tk.LEFT, anchor="w", font=("Arial", 9)).pack(fill=tk.X, padx=10, pady=(0, 8))

        self.layer_list = tk.Listbox(left, bg="#1a1a1a", fg="white", selectbackground="#4a6984", selectforeground="white", activestyle="none", font=("Consolas", 10), selectmode=tk.EXTENDED, exportselection=False)
        self.layer_list.pack(fill=tk.BOTH, expand=True, padx=10, pady=(0, 10))

        right = tk.Frame(body, bg=self.svg_background)
        right.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        self.canvas = tk.Canvas(right, bg=self.svg_background, highlightthickness=0)
        self.hbar = tk.Scrollbar(right, orient=tk.HORIZONTAL, command=self.canvas.xview)
        self.vbar = tk.Scrollbar(right, orient=tk.VERTICAL, command=self.canvas.yview)
        self.canvas.configure(xscrollcommand=self.hbar.set, yscrollcommand=self.vbar.set)
        self.vbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.hbar.pack(side=tk.BOTTOM, fill=tk.X)
        self.canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        self.status_var = tk.StringVar(value="Ready")
        tk.Label(self.root, textvariable=self.status_var, bg="#333333", fg="#aaaaaa", anchor=tk.W, padx=10).pack(side=tk.BOTTOM, fill=tk.X)

    def _bind_events(self) -> None:
        self.layer_list.bind("<<ListboxSelect>>", self.on_layer_selected)
        self.canvas.bind("<MouseWheel>", self._on_mousewheel_windows)
        self.canvas.bind("<Button-4>", self._on_mousewheel_linux_up)
        self.canvas.bind("<Button-5>", self._on_mousewheel_linux_down)
        self.canvas.bind("<Configure>", self._on_canvas_configure)

    def _cleanup_temp_svg(self) -> None:
        if self.current_svg_path and os.path.exists(self.current_svg_path):
            try: os.remove(self.current_svg_path)
            except OSError: pass
        self.current_svg_path = None

    def _looks_like_text_file(self, path: str) -> bool:
        try:
            with open(path, "rb") as f:
                chunk = f.read(4096)
            return b"\x00" not in chunk
        except OSError: return False

    def _try_load_layer(self, path: str) -> LayerRecord | None:
        parsed = GerberFile.from_file(path, file_type=FileTypeEnum.INFER).parse(on_parser_error=OnParserErrorEnum.Raise)
        return LayerRecord(os.path.basename(path), path, parsed.get_file_type(), parsed, parsed.get_info())

    def _sort_key(self, record: LayerRecord) -> tuple[int, str]:
        order = {FileTypeEnum.COPPER: 10, FileTypeEnum.PASTE: 20, FileTypeEnum.MASK: 30, FileTypeEnum.SOLDERMASK: 30, FileTypeEnum.SILK: 40, FileTypeEnum.LEGEND: 40, FileTypeEnum.EDGE: 50, FileTypeEnum.PROFILE: 50, FileTypeEnum.PLATED: 60, FileTypeEnum.NON_PLATED: 61}
        return (order.get(record.file_type, 95), record.filename.lower())

    def load_folder(self) -> None:
        folder = filedialog.askdirectory(title="Select Gerber Folder")
        if not folder: return
        self.loaded_files.clear(); self.layer_list.delete(0, tk.END); self.canvas.delete("all")
        self._cleanup_temp_svg(); self.image_ref = None; self.current_canvas_item = None
        self.last_skipped = []
        filenames = sorted(os.listdir(folder))
        for filename in filenames:
            full_path = os.path.join(folder, filename)
            if not os.path.isfile(full_path) or filename.lower().endswith((".gbrjob", ".zip", ".pdf")): continue
            if not self._looks_like_text_file(full_path): continue
            try:
                record = self._try_load_layer(full_path)
                if record: self.loaded_files.append(record)
            except Exception as exc: self.last_skipped.append(f"{filename} -> {exc}")
        self.loaded_files.sort(key=self._sort_key)
        for record in self.loaded_files: self.layer_list.insert(tk.END, f"{record.filename} [{record.file_type.value}]")
        self.layer_list.selection_set(0); self.render_selected_layer()

    def _pick_color_scheme(self, file_type: FileTypeEnum) -> ColorScheme:
        """Assigns colors to layers using RGBA class for distinct visual identification."""
        if file_type == FileTypeEnum.COPPER:
            return ColorScheme(
                background_color=RGBA.from_rgba(0, 0, 0, 0),
                solid_color=RGBA.from_rgba(184, 115, 51, 200), # Copper Orange
                clear_color=RGBA.from_rgba(0, 0, 0, 0),
                solid_region_color=RGBA.from_rgba(184, 115, 51, 200),
                clear_region_color=RGBA.from_rgba(0, 0, 0, 0),
            )
        if file_type in (FileTypeEnum.MASK, FileTypeEnum.SOLDERMASK):
            return ColorScheme(
                background_color=RGBA.from_rgba(0, 0, 0, 0),
                solid_color=RGBA.from_rgba(0, 100, 0, 150), # Deep Green
                clear_color=RGBA.from_rgba(0, 0, 0, 0),
                solid_region_color=RGBA.from_rgba(0, 100, 0, 150),
                clear_region_color=RGBA.from_rgba(0, 0, 0, 0),
            )
        if file_type in (FileTypeEnum.SILK, FileTypeEnum.LEGEND):
            return ColorScheme(
                background_color=RGBA.from_rgba(0, 0, 0, 0),
                solid_color=RGBA.from_rgba(255, 255, 255, 220), # White
                clear_color=RGBA.from_rgba(0, 0, 0, 0),
                solid_region_color=RGBA.from_rgba(255, 255, 255, 220),
                clear_region_color=RGBA.from_rgba(0, 0, 0, 0),
            )
        if file_type in (FileTypeEnum.EDGE, FileTypeEnum.PROFILE):
            return ColorScheme(
                background_color=RGBA.from_rgba(0, 0, 0, 0),
                solid_color=RGBA.from_rgba(255, 255, 0, 255), # Bright Yellow
                clear_color=RGBA.from_rgba(0, 0, 0, 0),
                solid_region_color=RGBA.from_rgba(255, 255, 0, 255),
                clear_region_color=RGBA.from_rgba(0, 0, 0, 0),
            )
        return ColorScheme.DEFAULT_GRAYSCALE

    def _scope_svg_styles(self, root: ET.Element, layer_id: str) -> None:
        """FIX: Scopes CSS classes to prevent layers from overriding each other's colors."""
        for style in root.findall(f".//{{{SVG_NS}}}style"):
            if style.text:
                css = style.text
                scoped_css = re.sub(r"\.([a-zA-Z0-9_-]+)\s*\{", rf"#{layer_id} .\1 {{", css)
                style.text = scoped_css

    def _build_composite_svg_text(self, records: list[LayerRecord], scale: float) -> str:
        if not records: return ""
        
        svg_datas = []
        for record in records:
            fd, temp_svg = tempfile.mkstemp(suffix=".svg")
            os.close(fd)
            try:
                record.parsed.render_svg(temp_svg, color_scheme=self._pick_color_scheme(record.file_type), scale=scale)
                svg_datas.append((record, ET.fromstring(Path(temp_svg).read_text(encoding="utf-8"))))
            finally:
                os.remove(temp_svg)

        min_x = min(float(r.info.min_x_mm) for r, _ in svg_datas)
        max_y = max(float(r.info.max_y_mm) for r, _ in svg_datas)
        
        # Determine global scale using the first layer's viewBox vs its mm width
        root0, rec0 = svg_datas[0][1], svg_datas[0][0]
        vb = root0.attrib.get("viewBox", "0 0 1 1").split()
        u_per_mm = float(vb[2]) / float(rec0.info.width_mm) if float(rec0.info.width_mm) > 0 else 1.0

        total_w = (max(float(r.info.max_x_mm) for r, _ in svg_datas) - min_x) * u_per_mm
        total_h = (max_y - min(float(r.info.min_y_mm) for r, _ in svg_datas)) * u_per_mm

        outer = ET.Element(f"{{{SVG_NS}}}svg", {
            "xmlns": SVG_NS, "version": "1.1",
            "viewBox": f"0 0 {total_w} {total_h}",
            "width": str(total_w), "height": str(total_h)
        })

        ET.SubElement(outer, f"{{{SVG_NS}}}rect", {"width": "100%", "height": "100%", "fill": self.svg_background})

        for i, (record, root) in enumerate(svg_datas):
            layer_id = f"layer_{i}"
            self._scope_svg_styles(root, layer_id)
            
            off_x = (float(record.info.min_x_mm) - min_x) * u_per_mm
            off_y = (max_y - float(record.info.max_y_mm)) * u_per_mm

            group = ET.SubElement(outer, f"{{{SVG_NS}}}g", {"id": layer_id})
            nested_svg = ET.SubElement(group, f"{{{SVG_NS}}}svg", {
                "x": str(off_x), "y": str(off_y),
                "width": root.attrib.get("width", "100%"),
                "height": root.attrib.get("height", "100%"),
                "viewBox": root.attrib.get("viewBox", "")
            })
            
            for child in list(root):
                if child.attrib.get("data-viewer-background") != "1": # Strip redundant backgrounds
                    nested_svg.append(copy.deepcopy(child))

        return '<?xml version="1.0" encoding="UTF-8"?>\n' + ET.tostring(outer, encoding="unicode")

    def on_layer_selected(self, _event=None) -> None:
        self.render_selected_layer()

    def render_selected_layer(self) -> None:
        indices = self.layer_list.curselection()
        if not indices: return
        records = [self.loaded_files[i] for i in indices]
        
        try:
            self._cleanup_temp_svg()
            scale = float(self.zoom_scale.get())
            svg_text = self._build_composite_svg_text(records, scale)
            
            fd, temp_path = tempfile.mkstemp(suffix=".svg")
            os.close(fd)
            Path(temp_path).write_text(svg_text, encoding="utf-8")
            
            self.current_svg_path = temp_path
            self.display_svg()
            self.status_var.set(f"Rendered {len(records)} layers at {scale}x")
        except Exception as e:
            messagebox.showerror("Error", f"Render failed: {e}")

    def display_svg(self) -> None:
        if not self.current_svg_path: return
        try:
            self.image_ref = tksvg.SvgImage(file=self.current_svg_path)
            self.canvas.delete("all")
            self.current_canvas_item = self.canvas.create_image(
                self.canvas.winfo_width()/2, self.canvas.winfo_height()/2, 
                anchor=tk.CENTER, image=self.image_ref
            )
            self._recenter_canvas_content()
        except Exception as e: print(f"Display error: {e}")

    def _recenter_canvas_content(self, _e=None) -> None:
        if self.current_canvas_item:
            self.canvas.coords(self.current_canvas_item, self.canvas.winfo_width()/2, self.canvas.winfo_height()/2)
            bbox = self.canvas.bbox(self.current_canvas_item)
            if bbox:
                self.canvas.configure(scrollregion=(bbox[0]-50, bbox[1]-50, bbox[2]+50, bbox[3]+50))

    def update_zoom(self, _v=None) -> None: self.render_selected_layer()
    
    def export_current_svg(self) -> None:
        if not self.current_svg_path: return
        path = filedialog.asksaveasfilename(defaultextension=".svg", filetypes=[("SVG files", "*.svg")])
        if path:
            with open(self.current_svg_path, "rb") as s, open(path, "wb") as d: d.write(s.read())

    def show_skipped(self) -> None:
        if not self.last_skipped: return
        top = tk.Toplevel(self.root)
        top.title("Skipped Files")
        txt = tk.Text(top, bg="#111", fg="#ddd", font=("Consolas", 10))
        txt.insert("1.0", "\n".join(self.last_skipped))
        txt.pack(fill=tk.BOTH, expand=True)

    def select_all_layers(self) -> None: self.layer_list.selection_set(0, tk.END); self.render_selected_layer()
    def clear_selection(self) -> None: self.layer_list.selection_clear(0, tk.END); self.canvas.delete("all")
    def _on_canvas_configure(self, _e) -> None: self._recenter_canvas_content()
    def _on_mousewheel_windows(self, e) -> None: self.canvas.yview_scroll(int(-1*(e.delta/120)), "units")
    def _on_mousewheel_linux_up(self, _e) -> None: self.canvas.yview_scroll(-1, "units")
    def _on_mousewheel_linux_down(self, _e) -> None: self.canvas.yview_scroll(1, "units")

if __name__ == "__main__":
    root = tk.Tk()
    app = VectorPartChecker(root)
    root.mainloop()