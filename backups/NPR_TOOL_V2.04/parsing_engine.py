from __future__ import annotations
import re
from typing import Optional

# ==========================================================
# CORE IDEA
# ==========================================================
# This module provides:
#   1) A parser registry keyed by component "type" (RES, CAP, LED, ...)
#   2) A type detector (keyword-based) to decide which parser to run
#   3) Shared extractors (package, tolerance, voltage, etc.)
#
# IMPORTANT:
# - The goal is scalability. The parsing rules belong in "config" later.
# - For now, we keep sensible defaults but isolate them behind registries.


# ==========================================================
# NORMALIZATION
# ==========================================================
def normalize(text: str) -> str:
    """Uppercase + strip. Keep it simple and deterministic."""
    if not text:
        return ""
    return str(text).strip().upper()

def parse_description(text: str, config) -> dict:
    """
    Fully config-driven parsing.
    No component-specific code exists here.
    """
    parsed = {}
    text_norm = normalize(text)

    for comp in config.components.values():
        if not comp.enabled:
            continue
        
        # Only consider component if at least 2 detect_keywords appear in the description
        hits = [k for k in comp.detect_keywords if k in text_norm]
        if len(hits) < 2:
            continue

        if any(k in text_norm for k in comp.detect_keywords):
            parsed["type"] = comp.name

            for field, extractor_key in comp.parsing.items():
                extractor = EXTRACTOR_REGISTRY.get(extractor_key)
                if not extractor:
                    continue
                value = extractor(text_norm)
                if value is not None:
                    parsed[field] = value

            return parsed

    return {"type": "OTHER"}

# ==========================================================
# EXTRACTOR REGISTRY
# ==========================================================

def extract_voltage(text):
    return parse_voltage_v(text)

def extract_current(text):
    return parse_current_a(text)

def extract_power(text):
    return parse_power_w(text)

def extract_package(text):
    return parse_package(text)

def extract_mount(text):
    return parse_mount(text)

def extract_tolerance_percent(text):
    return parse_tolerance_percent(text)

def extract_resistance_ohms(text):
    return parse_res_value_ohms(text)

def extract_capacitance_farads(text):
    return parse_cap_value_farads(text)

def extract_dielectric(text):
    return parse_dielectric(text)

def extract_channel(text):
    if "NCH" in text or "N-CH" in text or "NCHANNEL" in text:
        return "N"
    if "PCH" in text or "P-CH" in text or "PCHANNEL" in text:
        return "P"
    return None

def extract_polarity(text):
    if "NPN" in text:
        return "NPN"
    if "PNP" in text:
        return "PNP"
    return None

def extract_color(text):
    for c in ("RED", "GREEN", "BLUE", "YELLOW", "WHITE", "AMBER"):
        if c in text:
            return c
    return None


EXTRACTOR_REGISTRY = {
    "volts": extract_voltage,
    "amps": extract_current,
    "watts": extract_power,
    "standard": extract_package,
    "auto": extract_mount,
    "percent": extract_tolerance_percent,
    "resistance_ohms": extract_resistance_ohms,
    "capacitance_farads": extract_capacitance_farads,
    "dielectric": extract_dielectric,
    "channel": extract_channel,
    "polarity": extract_polarity,
    "color": extract_color,
}


# ==========================================================
# SHARED EXTRACTORS
# ==========================================================
def parse_tolerance_percent(text: str) -> Optional[int]:
    m = re.search(r"(\d+)\s*%", text)
    return int(m.group(1)) if m else None


def parse_package(text: str) -> Optional[str]:
    # Keep this list small + safe. You can expand via config later.
    m = re.search(
        r"\b(0201|0402|0603|0805|1206|1210|1812|2010|2512|"
        r"TO-\d+|SOT-\d+|SC70-\d+|SOIC-\d+|TSSOP-\d+|DFN\d+|QFN\d+|DPAK|D2PAK)\b",
        text,
    )
    return m.group(1) if m else None


def parse_voltage_v(text: str) -> Optional[int]:
    m = re.search(r"(\d+)\s*V\b", text)
    return int(m.group(1)) if m else None


def parse_current_a(text: str) -> Optional[float]:
    m = re.search(r"(\d+(\.\d+)?)\s*A\b", text)
    return float(m.group(1)) if m else None


def parse_power_w(text: str) -> Optional[float]:
    m = re.search(r"(\d+(\.\d+)?)\s*W\b", text)
    return float(m.group(1)) if m else None


def parse_mount(text: str) -> str:
    # Keep behavior compatible with your existing logic.
    if "SMD" in text or "SMT" in text or "SM" in text:
        return "SMD"
    if "THT" in text or "TH" in text:
        return "TH"
    return ""


# ==========================================================
# DIELECTRIC (shared)
# ==========================================================
def parse_dielectric(text: str) -> Optional[str]:
    diel_list = ["NP0", "NPO", "C0G", "X5R", "X7R", "X6S", "Y5V", "Z5U"]
    for d in diel_list:
        if d in text:
            return "NP0" if d == "NPO" else d
    return None


# ==========================================================
# VALUE PARSERS (shared)
# ==========================================================
def parse_res_value_ohms(text: str) -> Optional[float]:
    """
    Returns resistance in ohms (float) if found.
    Supports:
      - 3.3K
      - 2K2
      - 10R0
      - 100 OHM
    """
    if m := re.search(r"(\d+(\.\d+)?)\s*K\b", text):
        return float(m.group(1)) * 1e3
    if m := re.search(r"(\d+)[Kk](\d+)", text):
        return float(f"{m.group(1)}.{m.group(2)}") * 1e3
    if m := re.search(r"(\d+)R(\d+)", text):
        return float(f"{m.group(1)}.{m.group(2)}")
    if m := re.search(r"(\d+(\.\d+)?)\s*OHM\b", text):
        return float(m.group(1))
    return None


def parse_cap_value_farads(text: str) -> Optional[float]:
    """
    Returns capacitance in farads if found.
    Supports PF/NF/UF.
    """
    if m := re.search(r"(\d+(\.\d+)?)\s*PF\b", text):
        return float(m.group(1)) * 1e-12
    if m := re.search(r"(\d+(\.\d+)?)\s*NF\b", text):
        return float(m.group(1)) * 1e-9
    if m := re.search(r"(\d+(\.\d+)?)\s*UF\b", text):
        return float(m.group(1)) * 1e-6
    return None
