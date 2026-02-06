import re

# ==========================================================
# HELPER FUNCTIONS
# ==========================================================
def normalize(s):
    if not s:
        return ""
    return s.strip().upper()


# ==========================================================
# CORE PARSER REGISTRY
# ==========================================================
PARSER_REGISTRY = {}

def register_parser(part_type, func):
    PARSER_REGISTRY[part_type.upper()] = func


def parse_description(text, part_type_hint=None):
    """Main entry point for component parsing."""
    if not text:
        return {"type": "OTHER"}

    t = normalize(text)

    # Dispatch via registry
    for key, parser in PARSER_REGISTRY.items():
        if key in t or part_type_hint == key:
            return parser(t)

    return {"type": "OTHER"}


# ==========================================================
# COMMON SUB-PARSERS
# ==========================================================
def parse_tolerance(text):
    m = re.search(r"(\d+)\s*%", text)
    return int(m.group(1)) if m else None


def parse_package(text):
    m = re.search(r"\b(0201|0402|0603|0805|1206|1210|1812|2010|2512|TO-\d+|SOT-\d+|DPAK|D2PAK)\b", text)
    return m.group(1) if m else None


def parse_voltage(text):
    m = re.search(r"(\d+)\s*V\b", text)
    return int(m.group(1)) if m else None


def parse_current(text):
    m = re.search(r"(\d+(\.\d+)?)\s*A\b", text)
    return float(m.group(1)) if m else None


def parse_power(text):
    m = re.search(r"(\d+(\.\d+)?)\s*W\b", text)
    return float(m.group(1)) if m else None


def parse_wattage(text):
    m = re.search(r"(\d+/\d+W)", text)
    return m.group(1) if m else None


def parse_mount(text):
    if "SM" in text or "SMD" in text or "SMD" in text:
        return "SMD"
    if "TH" in text or "THT" in text:
        return "TH"
    return ""


# ==========================================================
# RESISTOR PARSER
# ==========================================================
def parse_resistor(text):
    parsed = {"type": "RES"}

    # Value
    val = parse_res_value(text)
    if val is not None:
        parsed["value"] = val

    # Tolerance
    tol = parse_tolerance(text)
    if tol is not None:
        parsed["tolerance"] = tol

    # Wattage
    watt = parse_wattage(text)
    if watt:
        parsed["wattage"] = watt

    # Package
    pkg = parse_package(text)
    if pkg:
        parsed["package"] = pkg

    # Mount
    parsed["mount"] = parse_mount(text)
    return parsed


# ==========================================================
# CAPACITOR PARSER
# ==========================================================
def parse_capacitor(text):
    parsed = {"type": "CAP"}

    val = parse_cap_value(text)
    if val is not None:
        parsed["value"] = val

    volt = parse_voltage(text)
    if volt is not None:
        parsed["voltage"] = volt

    tol = parse_tolerance(text)
    if tol is not None:
        parsed["tolerance"] = tol

    diel = parse_dielectric(text)
    if diel:
        parsed["dielectric"] = diel

    pkg = parse_package(text)
    if pkg:
        parsed["package"] = pkg

    parsed["mount"] = parse_mount(text)
    return parsed


# ==========================================================
# DIODE PARSER (Zener, Schottky, TVS, etc.)
# ==========================================================
def parse_diode(text):
    parsed = {"type": "DIODE"}

    # TVS / Zener classification
    if "TVS" in text:
        parsed["subtype"] = "TVS"
    elif "ZENER" in text:
        parsed["subtype"] = "ZENER"
    elif "SCHOTTKY" in text:
        parsed["subtype"] = "SCHOTTKY"

    # Voltage & Power
    vwm = re.search(r"(\d+)\s*VWM", text)
    if vwm:
        parsed["working_voltage"] = int(vwm.group(1))

    vc = re.search(r"(\d+)\s*VC", text)
    if vc:
        parsed["clamping_voltage"] = int(vc.group(1))

    volt = parse_voltage(text)
    if volt:
        parsed["voltage"] = volt

    power = parse_power(text)
    if power:
        parsed["power"] = power

    parsed["package"] = parse_package(text)
    parsed["mount"] = parse_mount(text)

    return parsed


# ==========================================================
# LED PARSER
# ==========================================================
def parse_led(text):
    parsed = {"type": "LED"}

    # Color
    colors = ["RED", "GREEN", "BLUE", "YELLOW", "WHITE", "AMBER"]
    for c in colors:
        if c in text:
            parsed["color"] = c
            break

    # Package
    pkg = parse_package(text)
    if pkg:
        parsed["package"] = pkg

    # Mount
    parsed["mount"] = parse_mount(text)

    # Brightness (mcd)
    mcd = re.search(r"(\d+(\.\d+)?)\s*mC?D", text)
    if mcd:
        parsed["brightness_mcd"] = float(mcd.group(1))

    # Current (mA)
    ma = re.search(r"(\d+(\.\d+)?)\s*mA", text)
    if ma:
        parsed["current_ma"] = float(ma.group(1))

    # Wavelength (nm)
    nm = re.search(r"(\d+)\s*NM", text)
    if nm:
        parsed["wavelength_nm"] = int(nm.group(1))

    # Lens / Style
    if "CLEAR" in text:
        parsed["lens"] = "CLEAR"
    elif "DIFF" in text or "DIFFUSED" in text:
        parsed["lens"] = "DIFFUSED"

    return parsed


# ==========================================================
# TRANSISTOR PARSER
# ==========================================================
def parse_transistor(text):
    parsed = {"type": "TRANSISTOR"}

    if "PNP" in text:
        parsed["polarity"] = "PNP"
    elif "NPN" in text:
        parsed["polarity"] = "NPN"

    volt = parse_voltage(text)
    if volt:
        parsed["voltage"] = volt

    curr = parse_current(text)
    if curr:
        parsed["current"] = curr

    pkg = parse_package(text)
    if pkg:
        parsed["package"] = pkg

    parsed["mount"] = parse_mount(text)
    return parsed


# ==========================================================
# MOSFET PARSER
# ==========================================================
def parse_mosfet(text):
    parsed = {"type": "MOSFET"}

    # Channel type
    if "NCH" in text or "N-CH" in text or "NCHANNEL" in text:
        parsed["channel"] = "N"
    elif "PCH" in text or "P-CH" in text or "PCHANNEL" in text:
        parsed["channel"] = "P"

    # Voltage / Current / Power
    volt = parse_voltage(text)
    if volt:
        parsed["voltage"] = volt

    curr = parse_current(text)
    if curr:
        parsed["current"] = curr

    power = parse_power(text)
    if power:
        parsed["power"] = power

    pkg = parse_package(text)
    if pkg:
        parsed["package"] = pkg

    parsed["mount"] = parse_mount(text)
    return parsed


# ==========================================================
# TRIAC / SCR PARSER
# ==========================================================
def parse_triac(text):
    parsed = {"type": "TRIAC"}

    volt = parse_voltage(text)
    if volt:
        parsed["voltage"] = volt

    curr = parse_current(text)
    if curr:
        parsed["current"] = curr

    pkg = parse_package(text)
    if pkg:
        parsed["package"] = pkg

    parsed["mount"] = parse_mount(text)
    return parsed


# ==========================================================
# DIELECTRIC (shared)
# ==========================================================
def parse_dielectric(text):
    diel_list = ["NP0", "NPO", "C0G", "X5R", "X7R", "X6S", "Y5V", "Z5U"]
    for d in diel_list:
        if d in text:
            return "NP0" if d == "NPO" else d
    return None


# ==========================================================
# VALUE PARSERS (shared)
# ==========================================================
def parse_res_value(text):
    # 3.3K, 19.6K, 2K2
    m = re.search(r"(\d+(\.\d+)?)\s*[K]\b", text)
    if m:
        return float(m.group(1)) * 1e3

    m = re.search(r"(\d+)[Kk](\d+)", text)
    if m:
        return float(f"{m.group(1)}.{m.group(2)}") * 1e3

    m = re.search(r"(\d+)R(\d+)", text)
    if m:
        return float(f"{m.group(1)}.{m.group(2)}")

    m = re.search(r"(\d+(\.\d+)?)\s*OHM", text)
    if m:
        return float(m.group(1))

    return None


def parse_cap_value(text):
    if m := re.search(r"(\d+(\.\d+)?)\s*PF\b", text):
        return float(m.group(1)) * 1e-12
    if m := re.search(r"(\d+(\.\d+)?)\s*NF\b", text):
        return float(m.group(1)) * 1e-9
    if m := re.search(r"(\d+(\.\d+)?)\s*UF\b", text):
        return float(m.group(1)) * 1e-6
    return None


# ==========================================================
# DESCRIPTION PARSER CLASS (enrichment layer)
# ==========================================================
class DescriptionParser:
    @staticmethod
    def enrich_inventory_parts(inventory_parts):
        for inv in inventory_parts:
            inv.parsed = parse_description(inv.description)

    @staticmethod
    def enrich_npr_parts(npr_parts):
        for npr in npr_parts:
            npr.parsed = parse_description(npr.description)


# ==========================================================
# REGISTER PARSERS
# ==========================================================
register_parser("RES", parse_resistor)
register_parser("CAP", parse_capacitor)
register_parser("DIODE", parse_diode)
register_parser("LED", parse_led)
register_parser("TRANSISTOR", parse_transistor)
register_parser("MOSFET", parse_mosfet)
register_parser("TRIAC", parse_triac)





### to do:not hard code in values, switch over to a file which i pulls informational data like this from. keeping the parser seperate from rule creation and rule cretion as 
### a confgurationinstead. 