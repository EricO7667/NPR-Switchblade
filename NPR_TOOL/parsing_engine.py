from __future__ import annotations
import re
from typing import Any, Dict, List, Optional, Tuple

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
    s = s.replace("_", " ")
    s = s.replace(",", " ")
    s = s.replace(";", " ")
    s = s.replace("(", " ").replace(")", " ")
    s = re.sub(r"\s+", " ", s)
    return s.strip()


# ==========================================================
# TOKENIZATION HELPERS
# ==========================================================
def _seen_span(spans: List[Tuple[int, int, str]], start: int, end: int, kind: str) -> bool:
    for a, b, k in spans:
        if k == kind and not (end <= a or start >= b):
            return True
    return False


def _add_token(tokens: List[Dict[str, Any]], spans: List[Tuple[int, int, str]], start: int, end: int, token: Dict[str, Any]) -> None:
    kind = str(token.get("kind", ""))
    if _seen_span(spans, start, end, kind):
        return
    payload = dict(token)
    payload.setdefault("span", (start, end))
    spans.append((start, end, kind))
    tokens.append(payload)


def _connector_context(text: str) -> bool:
    return any(k in text for k in (
        "CONN", "CONNECTOR", "HEADER", "RECEPTACLE", "SOCKET", "PLUG",
        "MICROFIT", "MINIFIT", "MOLEX", "JST", "TERMINAL BLOCK", "TERM BLK",
    ))


def _resistor_context(text: str, start: int, end: int) -> bool:
    lo = max(0, start - 16)
    hi = min(len(text), end + 16)
    window = text[lo:hi]
    return any(k in window for k in ("RES", "RESISTOR", "OHM", "OHMS"))


def _fraction_or_float(raw: str) -> Optional[float]:
    s = str(raw).strip().upper()
    try:
        if "/" in s:
            a, b = s.split("/", 1)
            den = float(b)
            if den == 0:
                return None
            return float(a) / den
        return float(s)
    except Exception:
        return None


def _normalize_package(raw: str) -> str:
    p = normalize(raw).replace(" ", "")
    m = re.fullmatch(r"(\d+)-(SOIC|TSSOP|SSOP|MSOP|QFN|UQFN|DFN|QFP|LQFP|TQFP|DIP|PDIP|SIP|SIL|USIL|SMD)", p)
    if m:
        n, fam = m.group(1), m.group(2)
        return f"{fam}-{n}" if fam != "SMD" else f"{n}-SMD"
    m = re.fullmatch(r"(\d+)(SOIC|TSSOP|SSOP|MSOP|QFN|UQFN|DFN|QFP|LQFP|TQFP|DIP|PDIP|SIP|SIL|USIL)", p)
    if m:
        n, fam = m.group(1), m.group(2)
        return f"{fam}-{n}"
    if p == "SOT235":
        return "SOT-23-5"
    return p


def _package_pattern() -> str:
    return (
        r"\b(0201|0402|0603|0805|1206|1210|1812|2010|2512|"
        r"\d+-SOIC|\d+SOIC|\d+-TSSOP|\d+TSSOP|\d+-SSOP|\d+SSOP|\d+-MSOP|\d+MSOP|"
        r"\d+-QFN|\d+QFN|\d+-UQFN|\d+UQFN|\d+-DFN|\d+DFN|\d+-QFP|\d+QFP|"
        r"\d+-LQFP|\d+LQFP|\d+-TQFP|\d+TQFP|\d+-SIL|\d+SIL|\d+-USIL|\d+USIL|"
        r"\d+-SMD|SOT-?\d+(?:-\d+)?|SC70-?\d+|SO\d+|SOIC-?\d+|TSSOP-?\d+|"
        r"MSOP-?\d+|SSOP-?\d+|QFN\d+|UQFN\d+|DFN\d+|QFP\d+|LQFP-?\d+|TQFP-?\d+|"
        r"DPAK|D2PAK|DIP-?\d+|PDIP-?\d+|SIP-?\d+|DO-\d+[A-Z]*|"
        r"SMA|SMB|SMC|AXIAL|RADIAL|TH|THT|SMD|SMT)\b"
    )


def _best_package(tokens: List[Dict[str, Any]]) -> Optional[str]:
    pkgs = [str(tok.get("value") or "") for tok in tokens if tok.get("kind") == "package" and tok.get("value")]
    if not pkgs:
        return None
    def score(pkg: str) -> tuple[int, int]:
        p = pkg.upper()
        specific = 0 if p in {"SMD", "SMT", "TH", "THT"} else 1
        return (specific, len(p))
    return sorted(pkgs, key=score, reverse=True)[0]


def _pins_from_package(pkg: Optional[str]) -> Optional[int]:
    if not pkg:
        return None
    p = str(pkg).upper().replace(" ", "")
    for pat in (
        r"(?:SOIC|TSSOP|SSOP|MSOP|QFN|UQFN|DFN|QFP|LQFP|TQFP|DIP|PDIP|SIP|SIL|USIL)-(\d+)$",
        r"(\d+)-SMD$",
        r"SO-(\d+)$",
        r"SOT-23-(\d+)$",
    ):
        m = re.search(pat, p)
        if m:
            try:
                return int(m.group(1))
            except Exception:
                pass
    return None


# ==========================================================
# QUANTITY TOKENIZER
# ==========================================================
def parse_quantity_tokens(text: str) -> List[Dict[str, Any]]:
    text = normalize(text)
    tokens: List[Dict[str, Any]] = []
    spans: List[Tuple[int, int, str]] = []
    is_connector = _connector_context(text)

    for m in re.finditer(r"(?<!\d)(\d+)\s*/\s*(\d+)\s*W\b", text):
        num = float(m.group(1))
        den = float(m.group(2))
        if den != 0:
            _add_token(tokens, spans, m.start(), m.end(), {"kind": "power", "value": num / den, "unit": "W", "raw": m.group(0)})

    for m in re.finditer(r"(?<![A-Z0-9])([0-9]+(?:\.[0-9]+)?)([RKM])([0-9]*)(?![A-Z0-9])", text):
        lead = m.group(1)
        marker = m.group(2)
        trail = m.group(3)
        raw = m.group(0)
        if not trail and not _resistor_context(text, m.start(), m.end()):
            continue
        if marker == "R":
            val = float(f"{lead}.{trail}" if trail else lead)
        elif marker == "K":
            val = float(f"{lead}.{trail}" if trail else lead) * 1e3
        else:
            val = float(f"{lead}.{trail}" if trail else lead) * 1e6
        _add_token(tokens, spans, m.start(), m.end(), {"kind": "resistance", "value": val, "unit": "OHM", "raw": raw})

    for m in re.finditer(r"(?<![A-Z0-9])([0-9]+(?:\.[0-9]+)?)\s*(M|K)?(?:OHM|OHMS?)\b", text):
        mag = float(m.group(1))
        pref = m.group(2) or ""
        mult = {"": 1.0, "K": 1e3, "M": 1e6}[pref]
        _add_token(tokens, spans, m.start(), m.end(), {"kind": "resistance", "value": mag * mult, "unit": "OHM", "raw": m.group(0)})

    for m in re.finditer(r"(?<![A-Z0-9])([0-9]+(?:\.[0-9]+)?)\s*(PF|NF|UF|MF|F|MFD)\b", text):
        mag = float(m.group(1))
        unit = m.group(2)
        mult = {"PF": 1e-12, "NF": 1e-9, "UF": 1e-6, "MF": 1e-3, "MFD": 1e-6, "F": 1.0}[unit]
        _add_token(tokens, spans, m.start(), m.end(), {"kind": "capacitance", "value": mag * mult, "unit": "F", "raw": m.group(0)})

    for m in re.finditer(r"(?<![A-Z0-9])([0-9]+(?:\.[0-9]+)?)\s*(PH|NH|UH|MH|H)\b", text):
        mag = float(m.group(1))
        unit = m.group(2)
        mult = {"PH": 1e-12, "NH": 1e-9, "UH": 1e-6, "MH": 1e-3, "H": 1.0}[unit]
        _add_token(tokens, spans, m.start(), m.end(), {"kind": "inductance", "value": mag * mult, "unit": "H", "raw": m.group(0)})

    for m in re.finditer(r"(?<![A-Z0-9])([0-9]+(?:\.[0-9]+)?)\s*(MV|KV|VPP|VPK|VPEAK|VRMS|VWM|VRWM|VC|VDC|VAC|V)(?![A-Z0-9])", text):
        mag = float(m.group(1))
        unit = m.group(2)
        mult = {"MV": 1e-3, "KV": 1e3}.get(unit, 1.0)
        _add_token(tokens, spans, m.start(), m.end(), {"kind": "voltage", "value": mag * mult, "unit": "V", "raw": m.group(0), "raw_unit": unit})

    for m in re.finditer(r"(?<![A-Z0-9])([0-9]+(?:\.[0-9]+)?)\s*(UA|MA|A)(?![A-Z0-9])", text):
        mag = float(m.group(1))
        unit = m.group(2)
        mult = {"UA": 1e-6, "MA": 1e-3, "A": 1.0}[unit]
        _add_token(tokens, spans, m.start(), m.end(), {"kind": "current", "value": mag * mult, "unit": "A", "raw": m.group(0)})

    for m in re.finditer(r"(?<![A-Z0-9])([0-9]+(?:\.[0-9]+)?)\s*(UW|MW|W)(?![A-Z0-9])", text):
        mag = float(m.group(1))
        unit = m.group(2)
        mult = {"UW": 1e-6, "MW": 1e-3, "W": 1.0}[unit]
        _add_token(tokens, spans, m.start(), m.end(), {"kind": "power", "value": mag * mult, "unit": "W", "raw": m.group(0)})

    for m in re.finditer(r"(?<![A-Z0-9])([0-9]+(?:\.[0-9]+)?)\s*(HZ|KHZ|MHZ|GHZ)(?![A-Z0-9])", text):
        mag = float(m.group(1))
        unit = m.group(2)
        mult = {"HZ": 1.0, "KHZ": 1e3, "MHZ": 1e6, "GHZ": 1e9}[unit]
        _add_token(tokens, spans, m.start(), m.end(), {"kind": "frequency", "value": mag * mult, "unit": "HZ", "raw": m.group(0)})

    for m in re.finditer(r"(?<!\d)(\d+(?:\.\d+)?)\s*%", text):
        _add_token(tokens, spans, m.start(), m.end(), {"kind": "tolerance", "value": float(m.group(1)), "unit": "%", "raw": m.group(0)})

    for m in re.finditer(r"\b(-?\d+(?:\.\d+)?)\s*C\b", text):
        _add_token(tokens, spans, m.start(), m.end(), {"kind": "temperature", "value": float(m.group(1)), "unit": "C", "raw": m.group(0)})

    for m in re.finditer(r"(?<![A-Z0-9])([0-9]{1,3})\s*(?:POS|PIN|PINS|WAY|WAYS)\b", text):
        _add_token(tokens, spans, m.start(), m.end(), {"kind": "pins", "value": int(m.group(1)), "unit": "COUNT", "raw": m.group(0)})
    for m in re.finditer(r"(?<![A-Z0-9])(\d{1,3})\s*P(?![A-Z0-9])", text):
        _add_token(tokens, spans, m.start(), m.end(), {"kind": "pins", "value": int(m.group(1)), "unit": "COUNT", "raw": m.group(0)})

    if not is_connector:
        for m in re.finditer(r"(?<![A-Z0-9])(\d+)\s*CH\b", text):
            _add_token(tokens, spans, m.start(), m.end(), {"kind": "channels", "value": int(m.group(1)), "unit": "COUNT", "raw": m.group(0)})
        if re.search(r"\bQUAD\b", text):
            _add_token(tokens, spans, 0, 0, {"kind": "channels", "value": 4, "unit": "COUNT", "raw": "QUAD"})
        elif re.search(r"\bDUAL\b", text):
            _add_token(tokens, spans, 0, 0, {"kind": "channels", "value": 2, "unit": "COUNT", "raw": "DUAL"})
        elif re.search(r"\bSINGLE\b", text):
            _add_token(tokens, spans, 0, 0, {"kind": "channels", "value": 1, "unit": "COUNT", "raw": "SINGLE"})

    for m in re.finditer(r"(?<![A-Z0-9])([0-9]+(?:\.[0-9]+)?)\s*MM\s*PITCH\b", text):
        _add_token(tokens, spans, m.start(), m.end(), {"kind": "pitch", "value": float(m.group(1)), "unit": "MM", "raw": m.group(0)})
    if is_connector:
        for m in re.finditer(r"(?<![A-Z0-9])([0-9]+(?:\.[0-9]+)?)\s*MM\b", text):
            raw = m.group(0)
            if re.search(r"(?:W|H|L|D|DIA|OD|ID|HEX|LS)\b", text[m.end():m.end()+4]):
                continue
            _add_token(tokens, spans, m.start(), m.end(), {"kind": "pitch", "value": float(m.group(1)), "unit": "MM", "raw": raw})
        for m in re.finditer(r"(?<![A-Z0-9])([0-9]+(?:\.[0-9]+)?|\d+\s*/\s*\d+)\s*(?:IN|INCH)\b", text):
            raw_num = m.group(1).replace(" ", "")
            val_in = _fraction_or_float(raw_num)
            if val_in is None:
                continue
            _add_token(tokens, spans, m.start(), m.end(), {"kind": "pitch", "value": val_in * 25.4, "unit": "MM", "raw": m.group(0), "raw_unit": "IN"})
        for m in re.finditer(r"(?<![A-Z0-9])(0\.\d+)(?![A-Z0-9])", text):
            raw = m.group(1)
            before = text[max(0, m.start() - 12):m.start()]
            after = text[m.end():min(len(text), m.end() + 12)]
            if not any(k in before + after for k in ("VERT", "RA", "R/A", "STRAIGHT", "STR", "HEADER", "PLUG", "CONN", "LOCK")):
                continue
            _add_token(tokens, spans, m.start(), m.end(), {"kind": "pitch", "value": float(raw) * 25.4, "unit": "MM", "raw": raw, "raw_unit": "IN_IMPLIED"})

    for m in re.finditer(r"(?<![A-Z0-9])(\d+(?:\.\d+)?)\s*MM\s*DIA\b", text):
        _add_token(tokens, spans, m.start(), m.end(), {"kind": "diameter", "value": float(m.group(1)), "unit": "MM", "raw": m.group(0)})
    for m in re.finditer(r"(?<![A-Z0-9])(\d+(?:\.\d+)?)\s*MMDX\s*(\d+(?:\.\d+)?)\s*MMLS\b", text):
        _add_token(tokens, spans, m.start(), m.end(), {"kind": "diameter", "value": float(m.group(1)), "unit": "MM", "raw": f"{m.group(1)}MMD"})
        _add_token(tokens, spans, m.start(), m.end(), {"kind": "lead_spacing", "value": float(m.group(2)), "unit": "MM", "raw": f"{m.group(2)}MMLS"})
    for m in re.finditer(r"(?<![A-Z0-9])(\d+(?:\.\d+)?)\s*MM\s*(?:LS|LEAD\s*SPACING)\b", text):
        _add_token(tokens, spans, m.start(), m.end(), {"kind": "lead_spacing", "value": float(m.group(1)), "unit": "MM", "raw": m.group(0)})
    for m in re.finditer(r"(?<![A-Z0-9])(\d+(?:\.\d+)?)\s*MM\s*[Xx]\s*(\d+(?:\.\d+)?)\s*MM(?:\s*[Xx]\s*(\d+(?:\.\d+)?)\s*MM)?", text):
        _add_token(tokens, spans, m.start(), m.end(), {"kind": "length", "value": float(m.group(1)), "unit": "MM", "raw": m.group(0)})
        _add_token(tokens, spans, m.start(), m.end(), {"kind": "width", "value": float(m.group(2)), "unit": "MM", "raw": m.group(0)})
        if m.group(3):
            _add_token(tokens, spans, m.start(), m.end(), {"kind": "height", "value": float(m.group(3)), "unit": "MM", "raw": m.group(0)})
    for m in re.finditer(r"(?<![A-Z0-9])(\d+\s*/\s*\d+|\d+(?:\.\d+)?)\s*HEX\s*SIZE\b", text):
        val = _fraction_or_float(m.group(1).replace(" ", ""))
        if val is not None:
            _add_token(tokens, spans, m.start(), m.end(), {"kind": "hex_size", "value": val, "unit": "IN", "raw": m.group(0)})

    for m in re.finditer(r"(?<![A-Z0-9])([0-9]+(?:\.[0-9]+)?)\s*(?:MCD|MILLICANDELA)\b", text):
        _add_token(tokens, spans, m.start(), m.end(), {"kind": "brightness_mcd", "value": float(m.group(1)), "unit": "MCD", "raw": m.group(0)})

    for m in re.finditer(r"\b(FLASH|EEPROM|SRAM|DRAM|FRAM|NOR FLASH|NAND FLASH)\b", text):
        _add_token(tokens, spans, m.start(), m.end(), {"kind": "memory_type", "value": m.group(1), "unit": None, "raw": m.group(0)})
    for m in re.finditer(r"(?<![A-Z0-9])([0-9]+(?:\.[0-9]+)?)\s*(KB|MB|GB|KBIT|MBIT|GBIT|BIT)\b", text):
        mag = float(m.group(1))
        unit = m.group(2)
        mult = {"BIT":1.0, "KBIT":1e3, "MBIT":1e6, "GBIT":1e9, "KB":1024.0, "MB":1024.0**2, "GB":1024.0**3}[unit]
        _add_token(tokens, spans, m.start(), m.end(), {"kind": "memory_size", "value": mag * mult, "unit": unit, "raw": m.group(0)})

    for m in re.finditer(_package_pattern(), text):
        raw = m.group(0)
        value = _normalize_package(raw)
        _add_token(tokens, spans, m.start(), m.end(), {"kind": "package", "value": value, "unit": None, "raw": raw})

    tokens.sort(key=lambda t: (t["span"][0], t["span"][1]))
    return tokens


# ==========================================================
# TOKEN SELECTION HELPERS
# ==========================================================
def _first_token_value(tokens: List[Dict[str, Any]], kind: str) -> Optional[Any]:
    for tok in tokens:
        if tok.get("kind") == kind:
            return tok.get("value")
    return None


def _all_token_values(tokens: List[Dict[str, Any]], kind: str) -> List[Any]:
    return [tok.get("value") for tok in tokens if tok.get("kind") == kind and tok.get("value") is not None]


# ==========================================================
# SHARED EXTRACTORS
# ==========================================================
def parse_tolerance_percent(text: str, tokens: Optional[List[Dict[str, Any]]] = None) -> Optional[float]:
    tokens = tokens or parse_quantity_tokens(text)
    v = _first_token_value(tokens, "tolerance")
    return float(v) if v is not None else None


def parse_package(text: str, tokens: Optional[List[Dict[str, Any]]] = None) -> Optional[str]:
    tokens = tokens or parse_quantity_tokens(text)
    return _best_package(tokens)


def parse_voltage_v(text: str, tokens: Optional[List[Dict[str, Any]]] = None) -> Optional[float]:
    tokens = tokens or parse_quantity_tokens(text)
    v = _first_token_value(tokens, "voltage")
    return float(v) if v is not None else None


def parse_voltage_kind(text: str, tokens: Optional[List[Dict[str, Any]]] = None) -> Optional[str]:
    tokens = tokens or parse_quantity_tokens(text)
    for tok in tokens:
        if tok.get("kind") != "voltage":
            continue
        raw_unit = str(tok.get("raw_unit") or "").upper()
        if raw_unit in {"VAC", "VRMS"}:
            return "AC"
        if raw_unit in {"VDC"}:
            return "DC"
        if raw_unit in {"VPK", "VPEAK", "VPP"}:
            return "PEAK"
        if raw_unit in {"VWM", "VRWM", "VC", "V", "MV", "KV"}:
            return raw_unit or None
    return None


def parse_current_a(text: str, tokens: Optional[List[Dict[str, Any]]] = None) -> Optional[float]:
    tokens = tokens or parse_quantity_tokens(text)
    v = _first_token_value(tokens, "current")
    return float(v) if v is not None else None


def parse_power_w(text: str, tokens: Optional[List[Dict[str, Any]]] = None) -> Optional[float]:
    tokens = tokens or parse_quantity_tokens(text)
    v = _first_token_value(tokens, "power")
    return float(v) if v is not None else None


def parse_frequency_hz(text: str, tokens: Optional[List[Dict[str, Any]]] = None) -> Optional[float]:
    tokens = tokens or parse_quantity_tokens(text)
    v = _first_token_value(tokens, "frequency")
    return float(v) if v is not None else None


def parse_temperature_c(text: str, tokens: Optional[List[Dict[str, Any]]] = None) -> Optional[float]:
    tokens = tokens or parse_quantity_tokens(text)
    vals = [float(v) for v in _all_token_values(tokens, "temperature")]
    if not vals:
        return None
    return max(vals)


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
    for d in ("NP0", "NPO", "C0G", "X5R", "X7R", "X6S", "Y5V", "Z5U"):
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


def parse_pins(text: str, tokens: Optional[List[Dict[str, Any]]] = None) -> Optional[int]:
    tokens = tokens or parse_quantity_tokens(text)
    v = _first_token_value(tokens, "pins")
    if v is None:
        return _pins_from_package(parse_package(text, tokens))
    return int(v)


def parse_pitch_mm(text: str, tokens: Optional[List[Dict[str, Any]]] = None) -> Optional[float]:
    tokens = tokens or parse_quantity_tokens(text)
    v = _first_token_value(tokens, "pitch")
    return float(v) if v is not None else None


def parse_channels(text: str, tokens: Optional[List[Dict[str, Any]]] = None) -> Optional[int]:
    tokens = tokens or parse_quantity_tokens(text)
    v = _first_token_value(tokens, "channels")
    return int(v) if v is not None else None


def parse_dimension_mm(kind: str, text: str, tokens: Optional[List[Dict[str, Any]]] = None) -> Optional[float]:
    tokens = tokens or parse_quantity_tokens(text)
    v = _first_token_value(tokens, kind)
    return float(v) if v is not None else None


def parse_hex_size_in(text: str, tokens: Optional[List[Dict[str, Any]]] = None) -> Optional[float]:
    tokens = tokens or parse_quantity_tokens(text)
    v = _first_token_value(tokens, "hex_size")
    return float(v) if v is not None else None


def parse_brightness_mcd(text: str, tokens: Optional[List[Dict[str, Any]]] = None) -> Optional[float]:
    tokens = tokens or parse_quantity_tokens(text)
    v = _first_token_value(tokens, "brightness_mcd")
    return float(v) if v is not None else None


def parse_memory_size(text: str, tokens: Optional[List[Dict[str, Any]]] = None) -> Optional[float]:
    tokens = tokens or parse_quantity_tokens(text)
    v = _first_token_value(tokens, "memory_size")
    return float(v) if v is not None else None


def parse_memory_type(text: str, tokens: Optional[List[Dict[str, Any]]] = None) -> Optional[str]:
    tokens = tokens or parse_quantity_tokens(text)
    v = _first_token_value(tokens, "memory_type")
    return str(v) if v is not None else None


def parse_interface_count(text: str, tokens: Optional[List[Dict[str, Any]]] = None) -> Optional[int]:
    t = normalize(text)
    m = re.search(r"\b(\d+)\s*(?:INTERFACE|INTERFACES|IF)\b", t)
    return int(m.group(1)) if m else None


# ==========================================================
# EXTRACTOR WRAPPERS / REGISTRY
# ==========================================================
def extract_voltage(text, tokens=None): return parse_voltage_v(text, tokens)
def extract_current(text, tokens=None): return parse_current_a(text, tokens)
def extract_power(text, tokens=None): return parse_power_w(text, tokens)
def extract_package(text, tokens=None): return parse_package(text, tokens)
def extract_mount(text, tokens=None): return parse_mount(text, tokens)
def extract_tolerance_percent(text, tokens=None): return parse_tolerance_percent(text, tokens)
def extract_resistance_ohms(text, tokens=None): return parse_res_value_ohms(text, tokens)
def extract_capacitance_farads(text, tokens=None): return parse_cap_value_farads(text, tokens)
def extract_inductance_henries(text, tokens=None): return parse_inductance_henries(text, tokens)
def extract_frequency_hz(text, tokens=None): return parse_frequency_hz(text, tokens)
def extract_pins(text, tokens=None): return parse_pins(text, tokens)
def extract_pitch_mm(text, tokens=None): return parse_pitch_mm(text, tokens)
def extract_dielectric(text, tokens=None): return parse_dielectric(text, tokens)
def extract_temperature_c(text, tokens=None): return parse_temperature_c(text, tokens)
def extract_length_mm(text, tokens=None): return parse_dimension_mm("length", text, tokens)
def extract_width_mm(text, tokens=None): return parse_dimension_mm("width", text, tokens)
def extract_height_mm(text, tokens=None): return parse_dimension_mm("height", text, tokens)
def extract_diameter_mm(text, tokens=None): return parse_dimension_mm("diameter", text, tokens)
def extract_lead_spacing_mm(text, tokens=None): return parse_dimension_mm("lead_spacing", text, tokens)
def extract_hex_size_in(text, tokens=None): return parse_hex_size_in(text, tokens)
def extract_channels(text, tokens=None): return parse_channels(text, tokens)
def extract_interface_count(text, tokens=None): return parse_interface_count(text, tokens)
def extract_memory_size(text, tokens=None): return parse_memory_size(text, tokens)
def extract_memory_type(text, tokens=None): return parse_memory_type(text, tokens)
def extract_brightness_mcd(text, tokens=None): return parse_brightness_mcd(text, tokens)

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
    "voltage": extract_voltage,
    "amps": extract_current,
    "current": extract_current,
    "watts": extract_power,
    "power": extract_power,
    "standard": extract_package,
    "package": extract_package,
    "auto": extract_mount,
    "mount": extract_mount,
    "percent": extract_tolerance_percent,
    "tolerance": extract_tolerance_percent,
    "resistance_ohms": extract_resistance_ohms,
    "resistance": extract_resistance_ohms,
    "capacitance_farads": extract_capacitance_farads,
    "capacitance": extract_capacitance_farads,
    "inductance_henries": extract_inductance_henries,
    "inductance": extract_inductance_henries,
    "frequency_hz": extract_frequency_hz,
    "frequency": extract_frequency_hz,
    "pins": extract_pins,
    "pitch": extract_pitch_mm,
    "dielectric": extract_dielectric,
    "temperature": extract_temperature_c,
    "temperature_c": extract_temperature_c,
    "length": extract_length_mm,
    "width": extract_width_mm,
    "height": extract_height_mm,
    "diameter": extract_diameter_mm,
    "lead_spacing": extract_lead_spacing_mm,
    "hex_size": extract_hex_size_in,
    "channels": extract_channels,
    "interfaces": extract_interface_count,
    "memory_size": extract_memory_size,
    "memory_type": extract_memory_type,
    "brightness_mcd": extract_brightness_mcd,
    "channel": extract_channel,
    "polarity": extract_polarity,
    "color": extract_color,
}


# ==========================================================
# GLOBAL SPEC PARSING
# ==========================================================
def parse_global_specs(text: str, config, tokens: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
    text_norm = normalize(text)
    tokens = tokens or parse_quantity_tokens(text_norm)
    out: Dict[str, Any] = {}
    for spec in getattr(config, "global_specs", {}).values():
        if not getattr(spec, "enabled", True):
            continue
        extractor_key = str(getattr(spec, "extractor", "") or spec.name).strip()
        extractor = EXTRACTOR_REGISTRY.get(extractor_key)
        if not extractor:
            continue
        try:
            value = extractor(text_norm, tokens=tokens)
        except TypeError:
            value = extractor(text_norm)
        if value is not None and value != "":
            out[spec.name] = value
    return out


# ==========================================================
# CORE PARSER
# ==========================================================
def parse_description(text: str, config) -> dict:
    """Config-driven parsing with shared quantity tokenization plus global quantitative spec extraction."""
    parsed: Dict[str, Any] = {}
    text_norm = normalize(text)
    tokens = parse_quantity_tokens(text_norm)

    for comp in getattr(config, "components", {}).values():
        if not getattr(comp, "enabled", True):
            continue
        keywords = [str(k).upper() for k in getattr(comp, "detect_keywords", [])]
        hits = [k for k in keywords if k and k in text_norm]
        if len(hits) < 2:
            continue
        parsed["type"] = comp.name
        for field, extractor_key in getattr(comp, "parsing", {}).items():
            extractor = EXTRACTOR_REGISTRY.get(extractor_key)
            if not extractor:
                continue
            try:
                value = extractor(text_norm, tokens=tokens)
            except TypeError:
                value = extractor(text_norm)
            if value is not None and value != "":
                parsed[field] = value
        break

    if "type" not in parsed:
        parsed["type"] = "OTHER"

    quantitative_specs = parse_global_specs(text_norm, config, tokens=tokens)
    if quantitative_specs:
        parsed["quantitative_specs"] = dict(quantitative_specs)
        for key, value in quantitative_specs.items():
            parsed.setdefault(key, value)

    return parsed
