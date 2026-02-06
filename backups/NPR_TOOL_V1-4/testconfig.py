# NPR_TOOL/testconfig.py
from .config_loader import load_config

cfg = load_config("./config/components.yaml")
print(cfg.components.keys())