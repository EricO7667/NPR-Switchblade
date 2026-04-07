from __future__ import annotations

import csv
import io
import re
import shutil
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional
import xml.etree.ElementTree as ET

try:
    from PySide6.QtCore import Qt
    from PySide6.QtGui import QAction
    from PySide6.QtSvgWidgets import QSvgWidget
    from PySide6.QtWidgets import (
        QApplication,
        QFileDialog,
        QLabel,
        QListWidget,
        QListWidgetItem,
        QMainWindow,
        QMessageBox,
        QPushButton,
        QScrollArea,
        QSplitter,
        QToolBar,
        QVBoxLayout,
        QWidget,
    )
except ImportError as exc:  # pragma: no cover
    raise SystemExit("PySide6 is required. Install it with: pip install PySide6") from exc

try:
    from pygerber.gerberx3.api.v2 import FileTypeEnum, GerberFile, Project
except ImportError as exc:  # pragma: no cover
    raise SystemExit("PyGerber is required. Install it with: pip install pygerber") from exc


SVG_NS = "http://www.w3.org/2000/svg"
ET.register_namespace("", SVG_NS)

IGNORE_SUFFIXES = {".drl", ".drd", ".xln"}
GERBER_SUFFIXES = {
    ".gbr", ".gtl", ".gbl", ".gto", ".gbo", ".gts", ".gbs",
    ".gko", ".gml", ".gm1", ".gm2", ".art", ".pho", ".ger",
}
PNP_SUFFIXES = {".csv", ".txt", ".tsv", ".pos", ".mnt", ".xy"}


@dataclass(slots=True)
class Placement:
    refdes: str
    x: float
    y: float
    rotation: float = 0.0
    side: str = ""
    value: str = ""
    package: str = ""
    mpn: str = ""


class PlacementTable:
    """Parse common centroid / pick-and-place text formats into a normalized table."""

    def __init__(self, placements: Optional[list[Placement]] = None) -> None:
        self.placements = placements or []
        self.by_refdes = {p.refdes.upper(): p for p in self.placements if p.refdes}

    @classmethod
    def from_file(cls, path: Path) -> "PlacementTable":
        text = path.read_text(encoding="utf-8", errors="replace")
        delimiter = cls._guess_delimiter(text, path)
        rows = list(csv.DictReader(io.StringIO(text), delimiter=delimiter))
        placements: list[Placement] = []

        for row in rows:
            normalized = {cls._normalize_key(k): (v or "").strip() for k, v in row.items() if k}

            refdes = cls._pick(normalized, "refdes", "designator", "reference", "ref", "component")
            x_text = cls._pick(normalized, "x", "centerx", "midx", "midxmm", "posx", "xmm")
            y_text = cls._pick(normalized, "y", "centery", "midy", "midymm", "posy", "ymm")
            rotation_text = cls._pick(normalized, "rotation", "rot", "angle", "theta", default="0")
            side = cls._pick(normalized, "side", "layer", "boardside", "mountside")
            value = cls._pick(normalized, "value", "comment")
            package = cls._pick(normalized, "package", "footprint", "pattern")
            mpn = cls._pick(normalized, "mpn", "manufacturerpartnumber", "partnumber")

            if not refdes or not x_text or not y_text:
                continue

            try:
                placements.append(
                    Placement(
                        refdes=refdes,
                        x=cls._to_float(x_text),
                        y=cls._to_float(y_text),
                        rotation=cls._to_float(rotation_text),
                        side=side,
                        value=value,
                        package=package,
                        mpn=mpn,
                    )
                )
            except ValueError:
                continue

        return cls(placements)

    @staticmethod
    def _guess_delimiter(text: str, path: Path) -> str:
        suffix = path.suffix.lower()
        if suffix in {".tsv", ".mnt", ".xy"}:
            return "\t"
        lines = [line for line in text.splitlines() if line.strip()]
        first_line = lines[0] if lines else ""
        if first_line.count("\t") > first_line.count(","):
            return "\t"
        if first_line.count(";") > first_line.count(","):
            return ";"
        return ","

    @staticmethod
    def _normalize_key(value: str) -> str:
        return "".join(ch for ch in value.lower().strip() if ch.isalnum())

    @staticmethod
    def _pick(row: dict[str, str], *keys: str, default: str = "") -> str:
        for key in keys:
            value = row.get(key, "")
            if value:
                return value
        return default

    @staticmethod
    def _to_float(value: str) -> float:
        cleaned = value.strip().lower()
        cleaned = cleaned.replace("mm", "").replace("deg", "").replace("°", "")
        cleaned = cleaned.replace("in", "")
        cleaned = cleaned.replace(",", "")
        return float(cleaned)


class SvgViewer(QSvgWidget):
    """Minimal zoomable SVG widget."""

    def __init__(self) -> None:
        super().__init__()
        self._zoom = 1.0
        self._raw_svg = b""
        self.setMinimumSize(700, 700)

    def load_svg_bytes(self, data: bytes) -> None:
        self._raw_svg = data
        self._zoom = 1.0
        self.load(data)
        self.adjustSize()
        self.repaint()

    def clear(self) -> None:
        self.load_svg_bytes(b"")

    def wheelEvent(self, event) -> None:  # noqa: N802
        if not self._raw_svg:
            return super().wheelEvent(event)

        if event.angleDelta().y() > 0:
            self._zoom *= 1.15
        else:
            self._zoom /= 1.15

        self._zoom = max(0.1, min(self._zoom, 20.0))
        self.resize(self.sizeHint() * self._zoom)
        event.accept()

    def reset_zoom(self) -> None:
        self._zoom = 1.0
        if self._raw_svg:
            self.load(self._raw_svg)
            self.adjustSize()


@dataclass(slots=True)
class LayerRecord:
    path: Path
    file_type: FileTypeEnum

    @property
    def name(self) -> str:
        return self.path.name


class PyGerberRenderer:
    """
    Thin rendering service.

    Design constraints:
    - Use documented single-file SVG rendering directly.
    - Use Project for multi-layer grouping.
    - Only call multi-layer SVG methods when the installed PyGerber object actually
      exposes them. This avoids guessing undocumented APIs.
    """

    def __init__(self, temp_dir: Path) -> None:
        self.temp_dir = temp_dir
        self.temp_dir.mkdir(parents=True, exist_ok=True)

    def render_svg(self, layers: list[LayerRecord], output_path: Path) -> None:
        if not layers:
            raise ValueError("No layers selected for rendering.")

        output_path.parent.mkdir(parents=True, exist_ok=True)
        if output_path.exists():
            output_path.unlink(missing_ok=True)

        if len(layers) == 1:
            self._render_single_layer_svg(layers[0], output_path)
            self._ensure_background(output_path)
            return

        self._render_multi_layer_svg(layers, output_path)
        self._ensure_background(output_path)

    def _render_single_layer_svg(self, layer: LayerRecord, output_path: Path) -> None:
        GerberFile.from_file(layer.path, file_type=layer.file_type).parse().render_svg(str(output_path))

    def _render_multi_layer_svg(self, layers: list[LayerRecord], output_path: Path) -> None:
        project = Project(
            [GerberFile.from_file(layer.path, file_type=layer.file_type) for layer in layers]
        )
        parsed_project = project.parse()

        render_svg = getattr(parsed_project, "render_svg", None)
        if callable(render_svg):
            render_svg(str(output_path))
            return

        raise RuntimeError(
            "This PyGerber build does not expose a documented multi-layer SVG render method on "
            "the parsed Project object. Single-layer SVG rendering is supported. Multi-layer raster "
            "rendering is documented. For multi-layer SVG in this viewer, install a PyGerber release "
            "that exposes Project.parse().render_svg(...), or add a custom compositor once you confirm "
            "the exact local API."
        )

    def _ensure_background(self, svg_path: Path, fill: str = "#ffffff") -> None:
        """Inject a solid background rect if the SVG is transparent."""
        text = svg_path.read_text(encoding="utf-8", errors="replace")
        if 'data-viewer-background="1"' in text:
            return

        match = re.search(r"(<svg\b[^>]*>)", text, flags=re.IGNORECASE | re.DOTALL)
        if not match:
            return

        rect = (
            f'<rect data-viewer-background="1" x="0" y="0" width="100%" height="100%" '
            f'fill="{fill}" />'
        )
        updated = text[: match.end()] + rect + text[match.end():]
        svg_path.write_text(updated, encoding="utf-8")


class MainWindow(QMainWindow):
    def __init__(self) -> None:
        super().__init__()
        self.setWindowTitle("Gerber SVG Viewer")
        self.resize(1500, 960)

        self.folder: Optional[Path] = None
        self.layer_records: list[LayerRecord] = []
        self.placement_path: Optional[Path] = None
        self.placements = PlacementTable()

        self.temp_dir = Path(tempfile.mkdtemp(prefix="pygerber_svg_viewer_"))
        self.current_svg = self.temp_dir / "current_render.svg"
        self.renderer = PyGerberRenderer(self.temp_dir)

        self._build_ui()

    def closeEvent(self, event) -> None:  # noqa: N802
        try:
            shutil.rmtree(self.temp_dir, ignore_errors=True)
        finally:
            super().closeEvent(event)

    def _build_ui(self) -> None:
        toolbar = QToolBar("Main")
        self.addToolBar(toolbar)

        open_folder_action = QAction("Open Folder", self)
        open_folder_action.triggered.connect(self.open_folder)
        toolbar.addAction(open_folder_action)

        load_pnp_action = QAction("Load PnP", self)
        load_pnp_action.triggered.connect(self.open_pnp)
        toolbar.addAction(load_pnp_action)

        render_action = QAction("Render Checked", self)
        render_action.triggered.connect(self.render_checked_layers)
        toolbar.addAction(render_action)

        reset_zoom_action = QAction("Reset Zoom", self)
        reset_zoom_action.triggered.connect(self.reset_zoom)
        toolbar.addAction(reset_zoom_action)

        check_all_action = QAction("Check All", self)
        check_all_action.triggered.connect(self.check_all_layers)
        toolbar.addAction(check_all_action)

        uncheck_all_action = QAction("Uncheck All", self)
        uncheck_all_action.triggered.connect(self.uncheck_all_layers)
        toolbar.addAction(uncheck_all_action)

        export_action = QAction("Export Current SVG", self)
        export_action.triggered.connect(self.export_current_svg)
        toolbar.addAction(export_action)

        splitter = QSplitter(Qt.Horizontal)

        left_panel = QWidget()
        left_layout = QVBoxLayout(left_panel)

        self.folder_label = QLabel("No folder loaded")
        self.folder_label.setWordWrap(True)

        self.pnp_label = QLabel("No centroid / pick-and-place file loaded")
        self.pnp_label.setWordWrap(True)

        self.layer_list = QListWidget()
        self.layer_list.itemChanged.connect(self._item_check_changed)
        self.layer_list.itemDoubleClicked.connect(self._double_click_toggle)

        self.render_button = QPushButton("Render Checked Layers")
        self.render_button.clicked.connect(self.render_checked_layers)

        self.status_label = QLabel("Open a Gerber folder.")
        self.status_label.setWordWrap(True)

        left_layout.addWidget(self.folder_label)
        left_layout.addWidget(self.pnp_label)
        left_layout.addWidget(self.layer_list, 1)
        left_layout.addWidget(self.render_button)
        left_layout.addWidget(self.status_label)

        right_panel = QWidget()
        right_layout = QVBoxLayout(right_panel)

        self.viewer_status = QLabel("No render loaded")
        self.viewer_status.setWordWrap(True)

        self.overlay_status = QLabel(
            "Overlay status: SVG board rendering only. PnP parsing is supported, but interactive "
            "markers and refdes overlays are not drawn in this file yet."
        )
        self.overlay_status.setWordWrap(True)

        self.svg_view = SvgViewer()
        self.svg_view.setStyleSheet("background: white;")

        scroll_area = QScrollArea()
        scroll_area.setWidgetResizable(True)
        scroll_area.setWidget(self.svg_view)

        right_layout.addWidget(self.viewer_status)
        right_layout.addWidget(self.overlay_status)
        right_layout.addWidget(scroll_area, 1)

        splitter.addWidget(left_panel)
        splitter.addWidget(right_panel)
        splitter.setSizes([360, 1140])

        self.setCentralWidget(splitter)

    def open_folder(self) -> None:
        folder = QFileDialog.getExistingDirectory(self, "Select folder containing Gerber files")
        if not folder:
            return

        self.folder = Path(folder)
        paths = sorted([path for path in self.folder.iterdir() if looks_like_gerber(path)], key=layer_sort_key)
        self.layer_records = [LayerRecord(path=path, file_type=guess_file_type(path)) for path in paths]

        self._reload_layer_list()
        self._autodetect_pnp()

        self.folder_label.setText(f"Folder:\n{self.folder}")
        self.status_label.setText(
            f"Found {len(self.layer_records)} candidate Gerber files. Drill files ignored."
        )
        self.viewer_status.setText("No render loaded")

        if not self.layer_records:
            QMessageBox.warning(self, "No files", "No non-drill Gerber files were found in that folder.")
            self.svg_view.clear()
            return

        self.render_checked_layers()

    def _reload_layer_list(self) -> None:
        self.layer_list.blockSignals(True)
        self.layer_list.clear()
        for record in self.layer_records:
            item = QListWidgetItem(record.name)
            item.setData(Qt.UserRole, str(record.path))
            item.setToolTip(f"{record.file_type.name}: {record.path}")
            item.setFlags(item.flags() | Qt.ItemIsUserCheckable | Qt.ItemIsSelectable | Qt.ItemIsEnabled)
            item.setCheckState(Qt.Checked)
            self.layer_list.addItem(item)
        self.layer_list.blockSignals(False)

    def _autodetect_pnp(self) -> None:
        self.placement_path = None
        self.placements = PlacementTable()

        if self.folder is None:
            self.pnp_label.setText("No centroid / pick-and-place file loaded")
            return

        detected = next((path for path in self.folder.iterdir() if looks_like_pnp(path)), None)
        if detected is None:
            self.pnp_label.setText("No centroid / pick-and-place file loaded")
            return

        try:
            self.placements = PlacementTable.from_file(detected)
            self.placement_path = detected
            self.pnp_label.setText(
                f"PnP / centroid file:\n{detected.name}\nRows parsed: {len(self.placements.placements)}"
            )
        except Exception as exc:
            self.placement_path = None
            self.placements = PlacementTable()
            self.pnp_label.setText(f"PnP auto-detect failed for {detected.name}: {exc}")

    def open_pnp(self) -> None:
        file_path, _ = QFileDialog.getOpenFileName(
            self,
            "Select centroid / pick-and-place file",
            "",
            "Placement Files (*.csv *.txt *.tsv *.pos *.mnt *.xy);;All Files (*)",
        )
        if not file_path:
            return

        path = Path(file_path)
        try:
            self.placements = PlacementTable.from_file(path)
            self.placement_path = path
            self.pnp_label.setText(
                f"PnP / centroid file:\n{path.name}\nRows parsed: {len(self.placements.placements)}"
            )
            self._update_overlay_status()
        except Exception as exc:
            QMessageBox.critical(self, "PnP load failed", str(exc))

    def _double_click_toggle(self, item: QListWidgetItem) -> None:
        item.setCheckState(Qt.Unchecked if item.checkState() == Qt.Checked else Qt.Checked)

    def _item_check_changed(self, _item: QListWidgetItem) -> None:
        checked = self.get_checked_records()
        self.status_label.setText(f"{len(checked)} layer(s) checked.")
        self.render_checked_layers()

    def get_checked_records(self) -> list[LayerRecord]:
        records_by_path = {str(record.path): record for record in self.layer_records}
        checked: list[LayerRecord] = []
        for index in range(self.layer_list.count()):
            item = self.layer_list.item(index)
            if item.checkState() == Qt.Checked:
                record = records_by_path.get(str(item.data(Qt.UserRole)))
                if record is not None:
                    checked.append(record)
        return checked

    def check_all_layers(self) -> None:
        self.layer_list.blockSignals(True)
        for index in range(self.layer_list.count()):
            self.layer_list.item(index).setCheckState(Qt.Checked)
        self.layer_list.blockSignals(False)
        self.status_label.setText(f"{self.layer_list.count()} layer(s) checked.")
        self.render_checked_layers()

    def uncheck_all_layers(self) -> None:
        self.layer_list.blockSignals(True)
        for index in range(self.layer_list.count()):
            self.layer_list.item(index).setCheckState(Qt.Unchecked)
        self.layer_list.blockSignals(False)
        self.status_label.setText("0 layer(s) checked.")
        self.viewer_status.setText("No layers checked")
        self.svg_view.clear()

    def render_checked_layers(self) -> None:
        checked = self.get_checked_records()
        if not checked:
            self.viewer_status.setText("No layers checked")
            self.status_label.setText("0 layer(s) checked.")
            self.svg_view.clear()
            return

        self.status_label.setText(f"Rendering {len(checked)} layer(s) with PyGerber...")
        QApplication.processEvents()

        try:
            self.renderer.render_svg(checked, self.current_svg)
        except Exception as exc:
            QMessageBox.critical(self, "Render failed", str(exc))
            self.status_label.setText("Render failed.")
            return

        if not self.current_svg.exists():
            QMessageBox.critical(self, "Render failed", "PyGerber finished but did not create an SVG file.")
            self.status_label.setText("Render failed. No SVG output.")
            return

        data = self.current_svg.read_bytes()
        self.svg_view.load_svg_bytes(data)
        self.viewer_status.setText("Showing render of:\n" + "\n".join(record.name for record in checked))
        self.status_label.setText(f"Rendered {len(checked)} layer(s).")
        self._update_overlay_status()

    def _update_overlay_status(self) -> None:
        if self.placements.placements:
            self.overlay_status.setText(
                "Overlay status: centroid data parsed successfully. This rewrite keeps that data normalized "
                "and ready for refdes overlays, but does not draw interactive markers yet."
            )
        else:
            self.overlay_status.setText(
                "Overlay status: no centroid / pick-and-place data loaded. Base SVG rendering only."
            )

    def reset_zoom(self) -> None:
        self.svg_view.reset_zoom()

    def export_current_svg(self) -> None:
        if not self.current_svg.exists():
            QMessageBox.warning(self, "Nothing to export", "Render a layer set first.")
            return

        default_name = "board.svg"
        checked = self.get_checked_records()
        if len(checked) == 1:
            default_name = f"{checked[0].path.stem}.svg"

        file_path, _ = QFileDialog.getSaveFileName(
            self,
            "Export current SVG",
            default_name,
            "SVG Files (*.svg)",
        )
        if not file_path:
            return

        dst = Path(file_path)
        dst.write_bytes(self.current_svg.read_bytes())
        self.status_label.setText(f"Exported SVG to {dst}")


def looks_like_drill(path: Path) -> bool:
    name = path.name.lower()
    suffix = path.suffix.lower()
    if suffix in IGNORE_SUFFIXES:
        return True
    return any(token in name for token in ("drill", "ncdrill", "npth", "pth"))


def looks_like_gerber(path: Path) -> bool:
    if not path.is_file():
        return False
    if looks_like_drill(path):
        return False

    suffix = path.suffix.lower()
    if suffix in GERBER_SUFFIXES:
        return True

    name = path.name.lower()
    return any(token in name for token in (
        "top", "bottom", "copper", "silk", "soldermask", "mask", "paste", "outline", "edge"
    ))


def looks_like_pnp(path: Path) -> bool:
    if not path.is_file():
        return False
    if path.suffix.lower() not in PNP_SUFFIXES:
        return False
    name = path.name.lower()
    return any(token in name for token in ("pick", "place", "centroid", "xy", "mount", "pos"))


def layer_sort_key(path: Path) -> tuple[int, str]:
    name = path.name.lower()
    suffix = path.suffix.lower()

    if suffix in {".gko", ".gml", ".gm1", ".gm2"} or "outline" in name or "edge" in name:
        return (0, name)
    if suffix == ".gbl" or "bottom copper" in name:
        return (10, name)
    if suffix == ".gbs" or "bottom soldermask" in name or "bottom mask" in name:
        return (20, name)
    if suffix == ".gbo" or "bottom silk" in name or "bottom silkscreen" in name:
        return (30, name)
    if "inner" in name or "inr" in name:
        return (40, name)
    if suffix == ".gtl" or "top copper" in name:
        return (50, name)
    if suffix == ".gts" or "top soldermask" in name or "top mask" in name:
        return (60, name)
    if suffix == ".gto" or "top silk" in name or "top silkscreen" in name:
        return (70, name)
    return (100, name)


def guess_file_type(path: Path) -> FileTypeEnum:
    name = path.name.lower()
    suffix = path.suffix.lower()

    if suffix in {".gko", ".gml", ".gm1", ".gm2"} or "outline" in name or "edge" in name:
        return FileTypeEnum.PROFILE
    if suffix in {".gtl", ".gbl"} or "copper" in name:
        return FileTypeEnum.COPPER
    if suffix in {".gts", ".gbs"} or "mask" in name:
        return FileTypeEnum.MASK
    if suffix in {".gto", ".gbo"} or "silk" in name:
        return FileTypeEnum.SILK
    if "paste" in name:
        return FileTypeEnum.PASTE
    return FileTypeEnum.INFER_FROM_EXTENSION


def main() -> None:
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
