# ==========================================================
#  YAML_AutoGen_v9.py
#  CNS-driven YAML generator with Selective Hybrid Matching
#  Electronic families → parametric rules
#  Mechanical/docs → hybrid fuzzy text mode
# ==========================================================

import os
import re
import yaml
from collections import Counter, defaultdict
from ..data_loader import DataLoader

# ----------------------------------------------------------
#  Profile map: family pattern → relevant fields
# ----------------------------------------------------------
FAMILY_PROFILES = {
    "RES": ["resistance_ohms", "percent", "standard"],
    "CAP": ["capacitance_farads", "volts", "percent", "standard"],
    "IND": ["inductance_henries", "amps", "standard"],
    "DIODE": ["volts", "amps"],
    "TRANS": ["volts", "amps", "standard"],
    "POT": ["resistance_ohms", "percent"],
    "PSU": ["volts", "amps"],
    "CONN": ["pins", "pitch"],
    "SOCK": ["pins", "pitch"],
    "PLUG": ["pins", "pitch"],
    "SW": ["text"],
    "TAPE": ["text"],
    "BEZEL": ["text"],
    "HOUSING": ["text"],
    "MATERIAL": ["text"],
    "RIVET": ["text"],
    "BRACKET": ["text"],
    "BOLT": ["text"],
    "NUT": ["text"],
    "LABEL": ["text"],
    "DOC": ["text"],
    "SPEC": ["text"],
    "PCB": ["text"],
    "FUSE": ["volts", "amps"],
}


# ----------------------------------------------------------
#  Pattern-based field display names for parser
# ----------------------------------------------------------
FIELD_DISPLAY_NAMES = {
    "resistance_ohms": "resistance",
    "capacitance_farads": "capacitance",
    "inductance_henries": "inductance",
    "volts": "volts",
    "amps": "amps",
    "percent": "percent",
    "standard": "standard",
    "pins": "pins",
    "pitch": "pitch",
    "text": "description",
}


# ----------------------------------------------------------
#  Keyword cleanup helper
# ----------------------------------------------------------
def clean_tokens(tokens):
    return [
        t for t in tokens
        if len(t) > 2
        and not any(ch.isdigit() for ch in t)
        and not t.endswith(("MM", "IN"))
        and t.isalpha()
    ]


# ----------------------------------------------------------
#  Infer match mode
# ----------------------------------------------------------
def infer_rule_mode(field):
    f = field.lower()
    if any(k in f for k in ["tolerance", "percent", "%"]):
        return "max"
    if any(k in f for k in ["voltage", "current", "amp"]):
        return "max"
    return "eq"


# ----------------------------------------------------------
#  Build a family config from its CNS sheet
# ----------------------------------------------------------
def build_family_config(sheet_name, records):
    upper_name = sheet_name.upper()
    desc_texts = [r.description for r in records if getattr(r, "description", None)]
    raw_tokens = re.findall(r"[A-Z]+", " ".join(desc_texts).upper())
    tokens = clean_tokens(raw_tokens)
    common = [t for t, _ in Counter(tokens).most_common(10)]

    # 1️⃣ Determine profile
    profile_fields = ["text"]
    for k, fields in FAMILY_PROFILES.items():
        if k in upper_name:
            profile_fields = fields
            break

    # 2️⃣ Generate parsing & matching
    # Determine if this is a mechanical/doc family (text-only)
    is_text_only = profile_fields == ["text"]

    if is_text_only:
        parsing_fields = {"description": "text"}
        rules = [{"field": "description", "mode": "hybrid", "threshold": 0.75}]
        conf_scale = 0.5
    else:
        parsing_fields = {
            FIELD_DISPLAY_NAMES.get(f, f.split("_")[0]): f for f in profile_fields
        }
        rules = [{"field": name, "mode": infer_rule_mode(name)} for name in parsing_fields.keys()]
        conf_scale = round(0.4 + 0.1 * len(parsing_fields), 2)
        conf_scale = min(conf_scale, 1.0)

    # 3️⃣  Detect keywords (focused)
    keywords = list(set(clean_tokens(re.findall(r"[A-Z]+", upper_name)) + common))
    # Force at least profile-root word(s)
    if not any(k in upper_name for k in keywords):
        keywords.append(sheet_name.split()[0].upper())
    # Limit total
    detect_keywords = keywords[:12]

    return {
        "enabled": True,
        "detect_keywords": detect_keywords,
        "parsing": parsing_fields,
        "matching": {"confidence_scale": conf_scale, "rules": rules},
    }


# ----------------------------------------------------------
#  Main generator
# ----------------------------------------------------------
def generate_yaml(cns_path="CNS_Master.xlsx", output_path="components_autogen.yaml"):
    if not os.path.exists(cns_path):
        print(f"[!] CNS workbook not found at {cns_path}")
        return

    print(f"[+] Loading CNS workbook: {cns_path}")
    records = DataLoader.load_cns_workbook(cns_path)
    if not records:
        print("[!] No records found in CNS workbook.")
        return

    families = defaultdict(list)
    for rec in records:
        families[getattr(rec, "sheet_name", getattr(rec, "category", "UNKNOWN"))].append(rec)

    yaml_data = {"version": 1, "components": {}}
    for sheet_name, group in families.items():
        fam_key = re.sub(r"[^A-Z0-9]+", "_", sheet_name.upper()).strip("_")
        yaml_data["components"][fam_key] = build_family_config(sheet_name, group)
        print(f"[+] Generated config for: {fam_key}")

    with open(output_path, "w", encoding="utf-8") as f:
        yaml.safe_dump(yaml_data, f, sort_keys=False, allow_unicode=True, width=120)

    print(f"\n[✓] Auto-generation complete! Output: {output_path}")
    print(f"[✓] Total families generated: {len(yaml_data['components'])}")


# ----------------------------------------------------------
#  Entrypoint
# ----------------------------------------------------------
if __name__ == "__main__":
    CNS_FILE = r"C:\PersonalProjects\PartChecker\NPR_TOOL\Components_12222025.xls"
    OUTPUT_FILE = "components_autogen.yaml"
    generate_yaml(CNS_FILE, OUTPUT_FILE)
