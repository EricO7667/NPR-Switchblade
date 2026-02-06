===========================================================
NPR TOOL — BRICKLESS PAYLOAD ARCHITECTURE
===========================================================

Author: GPTavern / Grimoire
Version: v0.2 — Payload-Centric Rewrite
Date: 2025-12-23

===========================================================
OVERVIEW
===========================================================

The NPR Tool is a flexible, rule-driven parts processing engine 
for analyzing and matching Bill of Materials (BOM) data against
inventory data.

This new version abandons the earlier "brick + schema" design and
is now based on a single, powerful global object: the PAYLOAD.

The Payload holds all program state — including raw data, parsed
data, matched results, and user configuration — and exposes a 
series of simple methods to perform operations in sequence.

This design makes it trivial to:
- Debug and inspect state at any moment
- Load configuration dynamically from YAML
- Experiment with new logic and rules without touching code
- Scale back to modular bricks later if desired

-----------------------------------------------------------
In short:
    > Everything lives in one object.
    > The object IS the system.
-----------------------------------------------------------


===========================================================
PROJECT STRUCTURE
===========================================================

npr_tool/
│
├── payload.py            # Core class: data, logic, and config live here
├── config.yaml           # Parsing & matching rules
├── main.py               # Entry point / demo runner
│
└── data/
    ├── fake_bom.xlsx     # Example input BOM
    ├── fake_inventory.xlsx  # Example input inventory
    └── output.xlsx       # Example output file


===========================================================
THE PAYLOAD OBJECT
===========================================================

Everything revolves around the `Payload` class, which stores:

  raw_bom         → BOM as loaded from Excel
  parsed_bom      → BOM with detected component types & values
  matched_bom     → BOM matched against inventory
  inventory       → Inventory dataset
  config          → Rules & settings loaded from YAML
  metadata        → Context info (timestamps, etc.)
  history         → Logs of every action performed

The class also provides methods:

  .load_config(path)
  .load_bom(path)
  .load_inventory(path)
  .parse_descriptions()
  .match_inventory()
  .export(path)
  .summary()

These can be chained fluently:

```python
payload = (
    Payload()
    .load_config("config.yaml")
    .load_bom("data/fake_bom.xlsx")
    .load_inventory("data/fake_inventory.xlsx")
    .parse_descriptions()
    .match_inventory()
    .export("data/output.xlsx")
    .summary()
)


# ==========================================
# PARSING RULES
# ==========================================
parsing_rules:
  RES: ["res", "ohm"]
  CAP: ["cap", "farad", "uf", "nf", "pf"]
  LED: ["led"]
  DIODE: ["diode"]
  TRANSISTOR: ["transistor", "bjt"]
  MOSFET: ["mosfet"]
  TRIAC: ["triac"]

# ==========================================
# MATCHING RULES
# ==========================================
matching_rules:
  default:
    match_by:
      - ["type", "type"]
      - ["value", "value"]
    link_field: "inventory_match"

  RES:
    match_by:
      - ["type", "type"]
      - ["value", "value"]
      - ["tolerance", "tolerance"]

  CAP:
    match_by:
      - ["type", "type"]
      - ["value", "value"]
