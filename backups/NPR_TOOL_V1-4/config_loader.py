from __future__ import annotations

import yaml
from dataclasses import dataclass
from typing import Dict, List, Any
from pathlib import Path



# =========================================================
# DATA MODELS (Config Side)
# =========================================================
@dataclass(frozen=True)
class FieldRuleConfig:
    field: str
    mode: str
    tolerance: float = 0.0


@dataclass(frozen=True)
class ComponentConfig:
    name: str
    enabled: bool
    detect_keywords: List[str]
    parsing: Dict[str, str]
    matching_rules: List[FieldRuleConfig]
    confidence_scale: float


@dataclass
class NPRConfig:
    components: Dict[str, ComponentConfig]
    tier_confidence: Dict[str, float]
    version: int


# =========================================================
# LOADER
# =========================================================
def load_config(path: str) -> NPRConfig:

    config_path = Path(path)

    if not config_path.is_absolute():
        # Resolve relative to THIS FILE, not CWD
        base_dir = Path(__file__).parent
        config_path = base_dir / path

    config_path = config_path.resolve()

    with open(config_path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    version = raw.get("version", 0)

    components: Dict[str, ComponentConfig] = {}
    for name, cfg in raw.get("components", {}).items():
        rules = []
        for r in cfg.get("matching", {}).get("rules", []):
            rules.append(
                FieldRuleConfig(
                    field=r["field"],
                    mode=r["mode"],
                    tolerance=float(r.get("tolerance", 0.0)),
                )
            )

        components[name.upper()] = ComponentConfig(
            name=name.upper(),
            enabled=bool(cfg.get("enabled", True)),
            detect_keywords=[k.upper() for k in cfg.get("detect_keywords", [])],
            parsing=cfg.get("parsing", {}),
            matching_rules=rules,
            confidence_scale=float(cfg.get("matching", {}).get("confidence_scale", 1.0)),
        )

    tiers = {
        k: float(v.get("confidence", 0.0))
        for k, v in raw.get("tiers", {}).items()
    }

    return NPRConfig(
        components=components,
        tier_confidence=tiers,
        version=version,
    )
