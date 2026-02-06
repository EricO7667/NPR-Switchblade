# run_app.py
import tkinter as tk

from .decision_controller import DecisionController, ControllerConfig
from .ui import DecisionWorkspaceUI


def main():
    controller = DecisionController(
        ControllerConfig(
            components_yaml_path="./config/components.yaml",
            npr_template_path="./NPR_Master2023_v4 Form(pulled on 12-16-2025).xlsx",
        )
    )

    root = tk.Tk()
    DecisionWorkspaceUI(root, controller)
    root.mainloop()


if __name__ == "__main__":
    main()
