# run_app.py
import tkinter as tk
import os
from pathlib import Path

from .NEW_decision_controller import DecisionController, ControllerConfig
from .NEW_UI import DecisionWorkspaceCTK
import customtkinter as ctk
import tkinter as tk


def main():
    # Paths
    base_dir = Path(os.path.dirname(os.path.abspath(__file__)))
    input_dir = base_dir.parent / "input"

    template_path = base_dir / "NPR_Master2023_v4_FormTEMPLATECOPY.xlsx"
    cns_path = input_dir / "Components.xls"
    

    # Controller owns DB + repos
    controller = DecisionController(
        ControllerConfig(
            components_yaml_path=str(base_dir / "config" / "components.yaml"),
            npr_template_path=str(template_path),
            created_by="NPR Tool v2"
        )
    )

    # CNS is read-only reference data → safe at startup
    #try:
    #    if cns_path.exists():
    #        summary = controller.load_cns_preview(str(cns_path))
    #        controller.cns_summary = summary
    #    else:
    #        print(f"[WARN] CNS file not found: {cns_path}")
    #except Exception as e:
    #    print(f"[WARN] CNS load failed: {e}")

    # UI
    root = ctk.CTk()
    DecisionWorkspaceCTK(root, controller)
    root.mainloop()


if __name__ == "__main__":
    main()
