
from __future__ import annotations
import yaml
from dataclasses import dataclass, field
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
class GlobalSpecConfig:
    name: str
    enabled: bool = True
    extractor: str = ""
    kind: str = ""
    hardgate: bool = True
    mode: str = "eq"
    tolerance: float = 0.0
    aliases: List[str] = field(default_factory=list)
    units: str = ""


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
    global_specs: Dict[str, GlobalSpecConfig] = field(default_factory=dict)


# =========================================================
# HELPERS
# =========================================================
def _clean_upper_list(values: Any) -> List[str]:
    out: List[str] = []
    for v in values or []:
        if v is None:
            continue
        s = str(v).strip()
        if not s:
            continue
        out.append(s.upper())
    return out


def _clean_str_list(values: Any) -> List[str]:
    out: List[str] = []
    for v in values or []:
        if v is None:
            continue
        s = str(v).strip()
        if not s:
            continue
        out.append(s)
    return out


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
        raw = yaml.safe_load(f) or {}

    version = int(raw.get("version", 0) or 0)

    components: Dict[str, ComponentConfig] = {}
    for name, cfg in (raw.get("components", {}) or {}).items():
        cfg = cfg or {}
        rules = []
        for r in ((cfg.get("matching", {}) or {}).get("rules", []) or []):
            if not isinstance(r, dict):
                continue
            field = str(r.get("field", "") or "").strip()
            mode = str(r.get("mode", "") or "").strip()
            if not field or not mode:
                continue
            rules.append(
                FieldRuleConfig(
                    field=field,
                    mode=mode,
                    tolerance=float(r.get("tolerance", 0.0) or 0.0),
                )
            )

        components[str(name).upper()] = ComponentConfig(
            name=str(name).upper(),
            enabled=bool(cfg.get("enabled", True)),
            detect_keywords=_clean_upper_list(cfg.get("detect_keywords", [])),
            parsing=dict(cfg.get("parsing", {}) or {}),
            matching_rules=rules,
            confidence_scale=float((cfg.get("matching", {}) or {}).get("confidence_scale", 1.0) or 1.0),
        )

    tiers = {
        str(k): float((v or {}).get("confidence", 0.0) or 0.0)
        for k, v in (raw.get("tiers", {}) or {}).items()
    }

    global_specs: Dict[str, GlobalSpecConfig] = {}
    gs_root = (raw.get("global_specs", {}) or {})
    gs_fields = (gs_root.get("fields", {}) if isinstance(gs_root, dict) else {}) or {}
    for name, cfg in gs_fields.items():
        if not isinstance(cfg, dict):
            continue
        spec_name = str(name).strip()
        if not spec_name:
            continue
        global_specs[spec_name] = GlobalSpecConfig(
            name=spec_name,
            enabled=bool(cfg.get("enabled", True)),
            extractor=str(cfg.get("extractor", spec_name) or spec_name).strip(),
            kind=str(cfg.get("kind", "") or "").strip(),
            hardgate=bool(cfg.get("hardgate", True)),
            mode=str(cfg.get("mode", "eq") or "eq").strip(),
            tolerance=float(cfg.get("tolerance", 0.0) or 0.0),
            aliases=_clean_str_list(cfg.get("aliases", [])),
            units=str(cfg.get("units", "") or "").strip(),
        )

    return NPRConfig(
        components=components,
        tier_confidence=tiers,
        version=version,
        global_specs=global_specs,
    )
