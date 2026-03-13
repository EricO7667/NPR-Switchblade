from __future__ import annotations
import re
from typing import Any, Dict, List, Optional

# ==========================================================
# NORMALIZATION
# ==========================================================
def normalize(text: str) -> str:
    """Uppercase + cleanup while preserving engineering tokens."""
    if not text:
        return ""
    s = str(text).strip().upper()
    s = s.replace("Ω", "OHM")
    s = s.replace("Μ", "U")
    s = s.replace("µ", "U").replace("μ", "U")
    s = s.replace("±", "")
    s = s.replace(",", " ")
    s = re.sub(r"\s+", " ", s)
    return s


# ==========================================================
# TOKENIZATION HELPERS
# ==========================================================
def _seen_span(spans: List[tuple[int, int]], start: int, end: int) -> bool:
    for a, b in spans:
        if not (end <= a or start >= b):
            return True
    return False


def _add_token(tokens: List[Dict[str, Any]], spans: List[tuple[int, int]], start: int, end: int, token: Dict[str, Any]) -> None:
    if _seen_span(spans, start, end):
        return
    token = dict(token)
    token.setdefault("span", (start, end))
    spans.append((start, end))
    tokens.append(token)


# ==========================================================
# QUANTITY TOKENIZER
# ==========================================================
def parse_quantity_tokens(text: str) -> List[Dict[str, Any]]:
    """Parse general engineering quantity tokens from a component description."""
    text = normalize(text)
    tokens: List[Dict[str, Any]] = []
    spans: List[tuple[int, int]] = []

    # Fractional power first so 1/4W does not also become 4W.
    for m in re.finditer(r"(?<!\d)(\d+)\s*/\s*(\d+)\s*W\b", text):
        num = float(m.group(1))
        den = float(m.group(2))
        if den != 0:
            _add_token(tokens, spans, m.start(), m.end(), {
                "kind": "power", "value": num / den, "unit": "W", "raw": m.group(0)
            })

    # Resistance shorthand 4K7, 0R22, 4M7, 10R.
    for m in re.finditer(r"(?<![A-Z0-9])([0-9]+(?:\.[0-9]+)?)([RKM])([0-9]*)(?![A-Z0-9])", text):
        lead = m.group(1)
        marker = m.group(2)
        trail = m.group(3)
        if marker == "R":
            val = float(f"{lead}.{trail}" if trail else lead)
        elif marker == "K":
            val = float(f"{lead}.{trail}" if trail else lead) * 1e3
        else:  # M
            val = float(f"{lead}.{trail}" if trail else lead) * 1e6
        _add_token(tokens, spans, m.start(), m.end(), {
            "kind": "resistance", "value": val, "unit": "OHM", "raw": m.group(0)
        })

    # Explicit resistance forms.
    for m in re.finditer(r"(?<![A-Z0-9])([0-9]+(?:\.[0-9]+)?)\s*(M|K)?(?:OHM|OHMS?)\b", text):
        mag = float(m.group(1))
        pref = m.group(2) or ""
        mult = {"": 1.0, "K": 1e3, "M": 1e6}[pref]
        _add_token(tokens, spans, m.start(), m.end(), {
            "kind": "resistance", "value": mag * mult, "unit": "OHM", "raw": m.group(0)
        })

    # Capacitance.
    for m in re.finditer(r"(?<![A-Z0-9])([0-9]+(?:\.[0-9]+)?)\s*(PF|NF|UF|MF|F|MFD)\b", text):
        mag = float(m.group(1))
        unit = m.group(2)
        mult = {"PF": 1e-12, "NF": 1e-9, "UF": 1e-6, "MF": 1e-3, "MFD": 1e-6, "F": 1.0}[unit]
        _add_token(tokens, spans, m.start(), m.end(), {
            "kind": "capacitance", "value": mag * mult, "unit": "F", "raw": m.group(0)
        })

    # Inductance.
    for m in re.finditer(r"(?<![A-Z0-9])([0-9]+(?:\.[0-9]+)?)\s*(PH|NH|UH|MH|H)\b", text):
        mag = float(m.group(1))
        unit = m.group(2)
        mult = {"PH": 1e-12, "NH": 1e-9, "UH": 1e-6, "MH": 1e-3, "H": 1.0}[unit]
        _add_token(tokens, spans, m.start(), m.end(), {
            "kind": "inductance", "value": mag * mult, "unit": "H", "raw": m.group(0)
        })

    # Voltage, including qualifiers like VWM / VC / VDC.
    for m in re.finditer(r"(?<![A-Z0-9])([0-9]+(?:\.[0-9]+)?)\s*(MV|KV|VWM|VRWM|VC|VDC|VAC|V)(?![A-Z0-9])", text):
        mag = float(m.group(1))
        unit = m.group(2)
        mult = {"MV": 1e-3, "KV": 1e3}.get(unit, 1.0)
        _add_token(tokens, spans, m.start(), m.end(), {
            "kind": "voltage", "value": mag * mult, "unit": "V", "raw": m.group(0), "raw_unit": unit
        })

    # Current.
    for m in re.finditer(r"(?<![A-Z0-9])([0-9]+(?:\.[0-9]+)?)\s*(UA|MA|A)(?![A-Z0-9])", text):
        mag = float(m.group(1))
        unit = m.group(2)
        mult = {"UA": 1e-6, "MA": 1e-3, "A": 1.0}[unit]
        _add_token(tokens, spans, m.start(), m.end(), {
            "kind": "current", "value": mag * mult, "unit": "A", "raw": m.group(0)
        })

    # Power.
    for m in re.finditer(r"(?<![A-Z0-9])([0-9]+(?:\.[0-9]+)?)\s*(UW|MW|W)(?![A-Z0-9])", text):
        mag = float(m.group(1))
        unit = m.group(2)
        mult = {"UW": 1e-6, "MW": 1e-3, "W": 1.0}[unit]
        _add_token(tokens, spans, m.start(), m.end(), {
            "kind": "power", "value": mag * mult, "unit": "W", "raw": m.group(0)
        })

    # Frequency.
    for m in re.finditer(r"(?<![A-Z0-9])([0-9]+(?:\.[0-9]+)?)\s*(HZ|KHZ|MHZ|GHZ)(?![A-Z0-9])", text):
        mag = float(m.group(1))
        unit = m.group(2)
        mult = {"HZ": 1.0, "KHZ": 1e3, "MHZ": 1e6, "GHZ": 1e9}[unit]
        _add_token(tokens, spans, m.start(), m.end(), {
            "kind": "frequency", "value": mag * mult, "unit": "HZ", "raw": m.group(0)
        })

    # Tolerance.
    for m in re.finditer(r"(?<!\d)(\d+(?:\.\d+)?)\s*%", text):
        _add_token(tokens, spans, m.start(), m.end(), {
            "kind": "tolerance", "value": float(m.group(1)), "unit": "%", "raw": m.group(0)
        })

    # Pins.
    for m in re.finditer(r"(?<![A-Z0-9])([0-9]{1,3})\s*(?:POS|PIN|PINS|WAY|WAYS)\b", text):
        _add_token(tokens, spans, m.start(), m.end(), {
            "kind": "pins", "value": int(m.group(1)), "unit": "COUNT", "raw": m.group(0)
        })

    # Pitch in mm.
    for m in re.finditer(r"(?<![A-Z0-9])([0-9]+(?:\.[0-9]+)?)\s*MM\s*PITCH\b", text):
        _add_token(tokens, spans, m.start(), m.end(), {
            "kind": "pitch", "value": float(m.group(1)), "unit": "MM", "raw": m.group(0)
        })

    # Package tokens. Keep broad enough for common through-hole and SMT.
    pkg_pat = (
        r"\b(0201|0402|0603|0805|1206|1210|1812|2010|2512|"
        r"TO-\d+(?:-\d+)?|SOT-\d+|SC70-\d+|SOPA?\d*|SOIC-\d+|TSSOP-\d+|"
        r"MSOP-\d+|SSOP-\d+|DFN\d+|QFN\d+|QFP\d+|LQFP-?\d+|"
        r"DPAK|D2PAK|DIP-?\d+|PDIP-?\d+|SIP-?\d+|DO-\d+[A-Z]*|"
        r"SMA|SMB|SMC|AXIAL|RADIAL|TH|THT|SMD|SMT)\b"
    )
    for m in re.finditer(pkg_pat, text):
        _add_token(tokens, spans, m.start(), m.end(), {
            "kind": "package", "value": m.group(1), "unit": None, "raw": m.group(0)
        })

    tokens.sort(key=lambda t: t["span"][0])
    return tokens


# ==========================================================
# TOKEN SELECTION HELPERS
# ==========================================================
def _first_token_value(tokens: List[Dict[str, Any]], kind: str) -> Optional[Any]:
    for tok in tokens:
        if tok.get("kind") == kind:
            return tok.get("value")
    return None


def _first_token_raw(tokens: List[Dict[str, Any]], kind: str) -> Optional[str]:
    for tok in tokens:
        if tok.get("kind") == kind:
            return tok.get("value")
    return None


# ==========================================================
# SHARED EXTRACTORS
# ==========================================================
def parse_tolerance_percent(text: str, tokens: Optional[List[Dict[str, Any]]] = None) -> Optional[float]:
    tokens = tokens or parse_quantity_tokens(text)
    v = _first_token_value(tokens, "tolerance")
    return float(v) if v is not None else None


def parse_package(text: str, tokens: Optional[List[Dict[str, Any]]] = None) -> Optional[str]:
    tokens = tokens or parse_quantity_tokens(text)
    return _first_token_value(tokens, "package")


def parse_voltage_v(text: str, tokens: Optional[List[Dict[str, Any]]] = None) -> Optional[float]:
    tokens = tokens or parse_quantity_tokens(text)
    v = _first_token_value(tokens, "voltage")
    return float(v) if v is not None else None


def parse_current_a(text: str, tokens: Optional[List[Dict[str, Any]]] = None) -> Optional[float]:
    tokens = tokens or parse_quantity_tokens(text)
    v = _first_token_value(tokens, "current")
    return float(v) if v is not None else None


def parse_power_w(text: str, tokens: Optional[List[Dict[str, Any]]] = None) -> Optional[float]:
    tokens = tokens or parse_quantity_tokens(text)
    v = _first_token_value(tokens, "power")
    return float(v) if v is not None else None


def parse_mount(text: str, tokens: Optional[List[Dict[str, Any]]] = None) -> str:
    text = normalize(text)
    if any(k in text for k in ("SMD", "SMT", "SURFACE MOUNT")):
        return "SMD"
    if any(k in text for k in ("THT", "THROUGH HOLE", "THRU HOLE")):
        return "TH"
    if any(tok.get("value") in {"TH", "THT"} for tok in (tokens or [])):
        return "TH"
    return ""


def parse_dielectric(text: str, tokens: Optional[List[Dict[str, Any]]] = None) -> Optional[str]:
    text = normalize(text)
    diel_list = ["NP0", "NPO", "C0G", "X5R", "X7R", "X6S", "Y5V", "Z5U"]
    for d in diel_list:
        if d in text:
            return "NP0" if d == "NPO" else d
    return None


def parse_res_value_ohms(text: str, tokens: Optional[List[Dict[str, Any]]] = None) -> Optional[float]:
    tokens = tokens or parse_quantity_tokens(text)
    v = _first_token_value(tokens, "resistance")
    return float(v) if v is not None else None


def parse_cap_value_farads(text: str, tokens: Optional[List[Dict[str, Any]]] = None) -> Optional[float]:
    tokens = tokens or parse_quantity_tokens(text)
    v = _first_token_value(tokens, "capacitance")
    return float(v) if v is not None else None


def parse_inductance_henries(text: str, tokens: Optional[List[Dict[str, Any]]] = None) -> Optional[float]:
    tokens = tokens or parse_quantity_tokens(text)
    v = _first_token_value(tokens, "inductance")
    return float(v) if v is not None else None


def parse_frequency_hz(text: str, tokens: Optional[List[Dict[str, Any]]] = None) -> Optional[float]:
    tokens = tokens or parse_quantity_tokens(text)
    v = _first_token_value(tokens, "frequency")
    return float(v) if v is not None else None


def parse_pins(text: str, tokens: Optional[List[Dict[str, Any]]] = None) -> Optional[int]:
    tokens = tokens or parse_quantity_tokens(text)
    v = _first_token_value(tokens, "pins")
    return int(v) if v is not None else None


def parse_pitch_mm(text: str, tokens: Optional[List[Dict[str, Any]]] = None) -> Optional[float]:
    tokens = tokens or parse_quantity_tokens(text)
    v = _first_token_value(tokens, "pitch")
    return float(v) if v is not None else None


# ==========================================================
# EXTRACTOR WRAPPERS
# ==========================================================
def extract_voltage(text, tokens=None):
    return parse_voltage_v(text, tokens)


def extract_current(text, tokens=None):
    return parse_current_a(text, tokens)


def extract_power(text, tokens=None):
    return parse_power_w(text, tokens)


def extract_package(text, tokens=None):
    return parse_package(text, tokens)


def extract_mount(text, tokens=None):
    return parse_mount(text, tokens)


def extract_tolerance_percent(text, tokens=None):
    return parse_tolerance_percent(text, tokens)


def extract_resistance_ohms(text, tokens=None):
    return parse_res_value_ohms(text, tokens)


def extract_capacitance_farads(text, tokens=None):
    return parse_cap_value_farads(text, tokens)


def extract_inductance_henries(text, tokens=None):
    return parse_inductance_henries(text, tokens)


def extract_frequency_hz(text, tokens=None):
    return parse_frequency_hz(text, tokens)


def extract_pins(text, tokens=None):
    return parse_pins(text, tokens)


def extract_pitch_mm(text, tokens=None):
    return parse_pitch_mm(text, tokens)


def extract_dielectric(text, tokens=None):
    return parse_dielectric(text, tokens)


def extract_channel(text, tokens=None):
    text = normalize(text)
    if "NCH" in text or "N-CH" in text or "NCHANNEL" in text:
        return "N"
    if "PCH" in text or "P-CH" in text or "PCHANNEL" in text:
        return "P"
    return None


def extract_polarity(text, tokens=None):
    text = normalize(text)
    if "NPN" in text:
        return "NPN"
    if "PNP" in text:
        return "PNP"
    return None


def extract_color(text, tokens=None):
    text = normalize(text)
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
    "inductance_henries": extract_inductance_henries,
    "frequency_hz": extract_frequency_hz,
    "pins": extract_pins,
    "pitch": extract_pitch_mm,
    "dielectric": extract_dielectric,
    "channel": extract_channel,
    "polarity": extract_polarity,
    "color": extract_color,
}


# ==========================================================
# CORE PARSER
# ==========================================================
def parse_description(text: str, config) -> dict:
    """Config-driven parsing with shared quantity tokenization."""
    parsed: Dict[str, Any] = {}
    text_norm = normalize(text)
    tokens = parse_quantity_tokens(text_norm)

    for comp in config.components.values():
        if not comp.enabled:
            continue

        hits = [k for k in comp.detect_keywords if k in text_norm]
        if len(hits) < 2:
            continue

        if any(k in text_norm for k in comp.detect_keywords):
            parsed["type"] = comp.name
            for field, extractor_key in comp.parsing.items():
                extractor = EXTRACTOR_REGISTRY.get(extractor_key)
                if not extractor:
                    continue
                try:
                    value = extractor(text_norm, tokens=tokens)
                except TypeError:
                    value = extractor(text_norm)
                if value is not None and value != "":
                    parsed[field] = value
            return parsed

    return {"type": "OTHER"}
