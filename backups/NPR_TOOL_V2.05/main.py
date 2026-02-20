# run_app.py
import tkinter as tk

from .decision_controller import DecisionController, ControllerConfig
from .ui import DecisionWorkspaceUI
import os
from pathlib import Path
from .db import connect_db, init_db
from .repositories import WorkspaceRepo, BomRepo, InventoryRepo, DecisionRepo


def main():

    #  1. Build absolute template path using os + Path
    base_dir = Path(os.path.dirname(os.path.abspath(__file__)))
    input_dir = base_dir.parent / "input"
    template_path = base_dir / "NPR_Master2023_v4_FormTEMPLATECOPY.xlsx"
    # CNS path 
    #cns_path = base_dir / "Components_12222025.xls"
    cns_path = input_dir / "Components.xls"

    conn = connect_db()
    init_db(conn)

    ws_repo = WorkspaceRepo(conn)
    bom_repo = BomRepo(conn)
    inv_repo = InventoryRepo(conn)
    dec_repo = DecisionRepo(conn)

    controller = DecisionController(
        ControllerConfig(
            components_yaml_path=str(base_dir / "config" / "components.yaml"),
            npr_template_path=template_path,
            created_by="NPR Tool v1"
        )
    )

    # Auto-load CNS at startup (safe / non-fatal)
    try:
        if cns_path.exists():
            summary = controller.load_cns_preview(str(cns_path))
            # optional: keep summary around for UI status display later
            controller.cns_summary = summary
        else:
            print(f" CNS file not found at startup: {cns_path}")
    except Exception as e:
        print(f"CNS load failed at startup: {e}")

    root = tk.Tk()
    DecisionWorkspaceUI(root, controller)
    root.mainloop()


if __name__ == "__main__":
    main()
