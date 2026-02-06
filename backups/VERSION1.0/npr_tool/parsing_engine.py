import re


# ---------------------------------------------------------
# Helper
# ---------------------------------------------------------
def normalize(s):
    if not s:
        return ""
    return s.strip().upper()


# ---------------------------------------------------------
# Top-level parser
# ---------------------------------------------------------
def parse_description(text, part_type_hint=None):
    """
    Returns a dictionary of parsed engineering fields.
    Example:
    {
        "type": "RES",
        "value": 5600.0,
        "tolerance": 1,
        "wattage": "1/8W",
        "package": "0805",
        "mount": "SMD"
    }
    """

    if not text:
        return {"type": "OTHER"}

    t = normalize(text)

    # ==============================================
    # RESISTOR DETECTION
    # ==============================================
    if "RES" in t or part_type_hint == "RES":
        return parse_resistor(t)

    # ==============================================
    # CAPACITOR DETECTION
    # ==============================================
    if "CAP" in t or "UF" in t or "PF" in t or "NF" in t or part_type_hint == "CAP":
        return parse_capacitor(t)

    # ==============================================
    # OTHERWISE OTHER
    # ==============================================
    return {"type": "OTHER"}


# ---------------------------------------------------------
# RESISTOR PARSER
# ---------------------------------------------------------
def parse_resistor(text):
    parsed = {"type": "RES"}

    # ---- Value (ohms) ----
    val = parse_res_value(text)
    if val is not None:
        parsed["value"] = val

    # ---- Tolerance (%) ----
    tol = parse_tolerance(text)
    if tol is not None:
        parsed["tolerance"] = tol

    # ---- Wattage ----
    watt = parse_wattage(text)
    if watt:
        parsed["wattage"] = watt

    # ---- Package ----
    pkg = parse_package(text)
    if pkg:
        parsed["package"] = pkg

    # ---- Mount type ----
    if "CF" in text:
        parsed["mount"] = "TH"
    elif "SM" in text or "SMD" in text:
        parsed["mount"] = "SMD"
    else:
        parsed["mount"] = ""


    return parsed


# ---------------------------------------------------------
# CAPACITOR PARSER
# ---------------------------------------------------------
def parse_capacitor(text):
    parsed = {"type": "CAP"}

    # ---- Cap value (F) ----
    val = parse_cap_value(text)
    if val is not None:
        parsed["value"] = val

    # ---- Voltage ----
    volt = parse_voltage(text)
    if volt is not None:
        parsed["voltage"] = volt

    # ---- Tolerance ----
    tol = parse_tolerance(text)
    if tol is not None:
        parsed["tolerance"] = tol

    # ---- Dielectric ----
    diel = parse_dielectric(text)
    if diel:
        parsed["dielectric"] = diel

    # ---- Package ----
    pkg = parse_package(text)
    if pkg:
        parsed["package"] = pkg

    # ---- Mount type ----
    parsed["mount"] = "SMD" if "0603" in text or "0805" in text or "SM" in text else "TH"

    return parsed


# ---------------------------------------------------------
# COMMON SUB-PARSERS
# ---------------------------------------------------------

def parse_tolerance(text):
    m = re.search(r"(\d+)\s*%", text)
    if m:
        return int(m.group(1))
    return None


def parse_package(text):
    m = re.search(r"\b(0201|0402|0603|0805|1206|1210|1812|2010|2512)\b", text)
    if m:
        return m.group(1)
    return None


def parse_wattage(text):
    # Only fraction formats by your requirement
    m = re.search(r"(\d+/\d+W)", text)
    if m:
        return m.group(1)
    return None


def parse_voltage(text):
    m = re.search(r"(\d+)\s*V\b", text)
    if m:
        return int(m.group(1))
    return None


def parse_dielectric(text):
    # Strict exact match (your requirement)
    diel_list = ["NP0", "NPO", "C0G", "X5R", "X7R", "X6S", "Y5V", "Z5U"]
    for d in diel_list:
        if d in text:
            # Normalize NPO -> NP0
            if d == "NPO":
                return "NP0"
            return d
    return None


# ---------------------------------------------------------
# RESISTOR VALUE PARSING
# ---------------------------------------------------------
def parse_res_value(text):
    # ---- FORMAT: 3.3K, 19.6K, 5.6K ----
    m = re.search(r"(\d+(\.\d+)?)\s*[K]\b", text)
    if m:
        return float(m.group(1)) * 1e3

    # ---- FORMAT: 2k4, 2k34 ----
    m = re.search(r"(\d+)[Kk](\d+)", text)
    if m:
        major = m.group(1)
        minor = m.group(2)
        value = float(f"{major}.{minor}") * 1000
        return value

    # ---- FORMAT: 243K (whole number) ----
    m = re.search(r"(\d+)[K]\b", text)
    if m:
        return float(m.group(1)) * 1000

    # ---- FORMAT: R decimal (4R7 → 4.7Ω) ----
    m = re.search(r"(\d+)R(\d+)", text)
    if m:
        return float(f"{m.group(1)}.{m.group(2)}")

    # ---- FORMAT: simple ohm number ----
    m = re.search(r"(\d+(\.\d+)?)\s*OHM", text)
    if m:
        return float(m.group(1))

    return None


# ---------------------------------------------------------
# CAP VALUE (FARADS)
# ---------------------------------------------------------
def parse_cap_value(text):
    # pF
    m = re.search(r"(\d+(\.\d+)?)\s*PF\b", text)
    if m:
        return float(m.group(1)) * 1e-12

    # nF
    m = re.search(r"(\d+(\.\d+)?)\s*NF\b", text)
    if m:
        return float(m.group(1)) * 1e-9

    # uF
    m = re.search(r"(\d+(\.\d+)?)\s*UF\b", text)
    if m:
        return float(m.group(1)) * 1e-6

    return None

# ======================================================================
# HIGH-LEVEL DESCRIPTION PARSER WRAPPER
# ======================================================================
class DescriptionParser:

    @staticmethod
    def enrich_inventory_parts(inventory_parts):
        """
        Populates inventory_part.parsed using parse_description()
        """
        for inv in inventory_parts:
            inv.parsed = parse_description(inv.description)

    @staticmethod
    def enrich_npr_parts(npr_parts):
        """
        Populates npr_part.parsed using parse_description()
        """
        for npr in npr_parts:
            npr.parsed = parse_description(npr.description)
