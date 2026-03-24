import customtkinter as ctk

# Set the initial accent theme (requires restart to change this specific color)
ctk.set_default_color_theme("dark-blue") 

class ModernDashboard(ctk.CTk):
    def __init__(self):
        super().__init__()

        self.title("Modern Interface v2.0")
        self.geometry("800x500")

        # 1. TOP NAV BAR
        self.nav_bar = ctk.CTkFrame(self, height=50, corner_radius=0)
        self.nav_bar.pack(side="top", fill="x")

        self.settings_btn = ctk.CTkButton(
            self.nav_bar, text="⚙️ Appearance", width=120, height=32,
            fg_color="transparent", text_color=("gray10", "gray90"),
            hover_color=("gray80", "gray25"),
            command=self.toggle_dropdown
        )
        self.settings_btn.pack(side="left", padx=20, pady=10)

        # 2. MAIN CONTENT AREA (The "Info" Page)
        self.container = ctk.CTkFrame(self, fg_color="transparent")
        self.container.pack(expand=True, fill="both", padx=30, pady=30)
        self.container.grid_columnconfigure((0, 1), weight=1)

        # Left Column: Inputs & Controls
        self.label_1 = ctk.CTkLabel(self.container, text="User Profile", font=("Arial", 20, "bold"))
        self.label_1.grid(row=0, column=0, pady=(0, 20), sticky="w")

        self.entry = ctk.CTkEntry(self.container, placeholder_text="Enter Username", width=250)
        self.entry.grid(row=1, column=0, pady=10, sticky="w")

        self.checkbox = ctk.CTkCheckBox(self.container, text="Stay Logged In")
        self.checkbox.grid(row=2, column=0, pady=10, sticky="w")

        self.slider = ctk.CTkSlider(self.container, width=250)
        self.slider.grid(row=3, column=0, pady=20, sticky="w")

        # Right Column: Status & Progress
        self.label_2 = ctk.CTkLabel(self.container, text="System Status", font=("Arial", 20, "bold"))
        self.label_2.grid(row=0, column=1, pady=(0, 20), sticky="w")

        self.progress = ctk.CTkProgressBar(self.container, width=250)
        self.progress.grid(row=1, column=1, pady=10, sticky="w")
        self.progress.set(0.7)

        self.switch = ctk.CTkSwitch(self.container, text="Enable Hardware Acceleration")
        self.switch.grid(row=2, column=1, pady=10, sticky="w")

        self.btn_action = ctk.CTkButton(self.container, text="Apply Changes", width=200)
        self.btn_action.grid(row=3, column=1, pady=20, sticky="w")

        # 3. THE FLOATING SETTINGS DROPDOWN (Hidden initially)
        self.dropdown_menu = ctk.CTkFrame(self, width=160, corner_radius=10, border_width=1, border_color="gray50")
        self.menu_open = False
        self.setup_dropdown_items()

        # Bind clicking anywhere on the background to close the menu
        self.bind("<Button-1>", self.check_click_location)

    def setup_dropdown_items(self):
        ctk.CTkLabel(self.dropdown_menu, text="Mode Selector", font=("Arial", 11, "bold")).pack(pady=(10, 5))
        
        modes = ["System", "Dark", "Light"]
        for m in modes:
            btn = ctk.CTkButton(
                self.dropdown_menu, text=m, height=30, fg_color="transparent",
                text_color=("gray10", "gray90"), anchor="w",
                command=lambda val=m: self.change_mode(val)
            )
            btn.pack(fill="x", padx=10, pady=2)

    def toggle_dropdown(self):
        if not self.menu_open:
            self.dropdown_menu.place(x=20, y=45)
            self.dropdown_menu.lift()
            self.menu_open = True
        else:
            self.close_dropdown()

    def close_dropdown(self):
        self.dropdown_menu.place_forget()
        self.menu_open = False

    def change_mode(self, new_mode):
        ctk.set_appearance_mode(new_mode.lower())
        self.close_dropdown()

    def check_click_location(self, event):
        """Closes the menu if you click outside of it."""
        if self.menu_open:
            # If the click isn't within the dropdown frame coordinates
            if not (self.dropdown_menu.winfo_x() <= event.x <= self.dropdown_menu.winfo_x() + self.dropdown_menu.winfo_width() and
                    self.dropdown_menu.winfo_y() <= event.y <= self.dropdown_menu.winfo_y() + self.dropdown_menu.winfo_height()):
                self.close_dropdown()

if __name__ == "__main__":
    app = ModernDashboard()
    app.mainloop()