====================================================================
NPR TOOL — MODULAR BILL OF MATERIALS INTELLIGENCE PIPELINE
====================================================================
"Every part number tells a story — this system makes sure we can read it."
====================================================================

OVERVIEW
--------------------------------------------------------------------
The NPR Tool (New Part Request Tool) is a modular, data-driven system
for processing, comparing, and cross-referencing Bill of Materials (BOM)
data against existing Inventory records.

The goal is to automate detection of new vs. existing parts, clean and
standardize messy Excel data, and build scalable, composable pipelines
made of modular "bricks."

Example flow:
    Excel → importer_excel → resolver_headers → parser_enrich
          → matcher_crosscheck → report_generator


====================================================================
ARCHITECTURE SUMMARY
====================================================================
The NPR Tool is built from independent processing units called BRICKS.

Each brick performs a single, well-defined transformation on the data.
Data flows through the pipeline immutably inside a PAYLOAD object.

    INPUT (Excel)
        ↓
    importer_excel
        ↓
    resolver_headers
        ↓
    parser_enrich
        ↓
    matcher_crosscheck
        ↓
    report_generator
        ↓
    OUTPUT (Reports / Snapshots)

====================================================================
DESIGN PRINCIPLES
====================================================================

  1. LOCAL-FIRST, CLOUD-READY
     - Works offline, but can scale to a server pipeline later.

  2. IMMUTABLE DATA FLOW
     - No in-place mutations. Every brick emits a new Payload.

  3. COMPOSABLE BRICKS
     - Each brick is self-contained and swappable. Add/remove freely.

  4. DECLARATIVE CONFIGURATION
     - Pipelines and schema mappings live in editable YAML files.

  5. EXTENSIBLE REGISTRY
     - New bricks register themselves automatically at runtime.

  6. TRACEABLE & AUDITABLE
     - Every step produces a snapshot of data + metadata for review.


====================================================================
PROJECT STRUCTURE
====================================================================

npr_tool/
│
├── core/
│   ├── data_models.py        ← Defines part + match structures
│   ├── parsing_engine.py     ← Extracts structured attributes
│   ├── matching_engine.py    ← NPR-to-inventory matching logic
│   ├── utils.py              ← Helper functions (cleaning, etc.)
│   └── registry.py           ← Central registry for all bricks
│
├── bricks/
│   ├── importer_excel.py     ← Loads Excel → Payload
│   ├── resolver_headers.py   ← Normalizes inconsistent headers
│   ├── parser_enrich.py      ← Parses text → structured attributes
│   ├── matcher_crosscheck.py ← Runs MatchingEngine
│   └── report_generator.py   ← Outputs Excel / CSV reports
│
├── configs/
│   ├── pipeline.yaml         ← Brick execution order
│   ├── schema_bom.yaml       ← NPR column mappings
│   ├── schema_inventory.yaml ← Inventory column mappings
│
├── runner/
│   ├── payload.py            ← Immutable Payload class
│   ├── runner_local.py       ← Pipeline executor
│
├── ui/
│   └── main_ui.py            ← Tkinter GUI frontend
│
├── __main__.py               ← CLI entrypoint
└── README.txt


====================================================================
HIGH-LEVEL DATA FLOW
====================================================================

1. INPUT
   - importer_excel loads a spreadsheet (BOM or Inventory)
   - Converts to list of dicts → wraps in Payload(schema="raw_excel")

2. NORMALIZATION
   - resolver_headers standardizes columns and cleans inconsistencies
   - Output schema: "normalized"

3. PARSING & ENRICHMENT
   - parser_enrich extracts structured attributes:
     (value, tolerance, wattage, dielectric, etc.)
   - Example:
       "100K OHM 1% 0603" → {type: "RES", value: 100000, tolerance: 1, package: "0603"}

4. CROSS-MATCHING
   - matcher_crosscheck compares NPR parts vs inventory using MatchingEngine
   - Matches classified as:
       • Exact MFG PN
       • Exact Item Number
       • Parsed Engineering Match
       • No Match

5. OUTPUT
   - report_generator exports results and snapshots


====================================================================
THE PAYLOAD OBJECT
====================================================================

Immutable data container passed between bricks.

Structure:
    Payload(
        data=[...],                # list of dicts or structured parts
        schema="parsed_parts",     # current stage
        metadata={
            "source": "path/to/excel",
            "rows": 512,
            "columns": ["desc", "mfgpn"],
            "step": 3,
            "brick": "parser_enrich"
        }
    )

Lifecycle:
    1. Created by importer_excel (schema="raw_excel")
    2. Transformed immutably by each brick using .with_update()
    3. Snapshots saved at each stage to /snapshots/


====================================================================
BRICKS: THE BUILDING BLOCKS
====================================================================

Each brick:
    - Is registered automatically via @registry.register("brick_name")
    - Receives a Payload and returns a new Payload
    - Never mutates input data

Example brick:
--------------------------------------------------------------------
@registry.register("importer_excel")
class ImporterExcel:
    def __init__(self, config):
        self.path = config["path"]

    def run(self, payload):
        df = pd.read_excel(self.path, dtype=str).fillna("")
        return payload.with_update(
            data=df.to_dict("records"),
            schema="raw_excel",
            metadata={"source": self.path, "rows": len(df)}
        )
--------------------------------------------------------------------


====================================================================
PIPELINE EXECUTION
====================================================================

Configured via: configs/pipeline.yaml

Example:
--------------------------------------------------------------------
pipeline:
  - name: importer_excel
    config:
      path: ./data/sample_bom.xlsx

  - name: resolver_headers
  - name: parser_enrich
  - name: matcher_crosscheck
  - name: report_generator
--------------------------------------------------------------------

Command:
    python -m npr_tool

Flow:
    LocalRunner → reads pipeline.yaml
                → loads bricks from registry
                → executes bricks in order
                → passes Payload between them
                → saves snapshots


====================================================================
REGISTRY SYSTEM
====================================================================

Location: core/registry.py

Purpose:
    - Keeps central map of all available bricks
    - Enables dynamic lookup by name
    - Supports zero-hardcoding modular expansion

Example:
--------------------------------------------------------------------
@registry.register("my_new_brick")
class MyNewBrick:
    def run(self, payload):
        return payload.with_update(schema="processed")
--------------------------------------------------------------------


====================================================================
RUNNER: ORCHESTRATION LAYER
====================================================================

File: runner/runner_local.py

Responsibilities:
    1. Load pipeline configuration (pipeline.yaml)
    2. Initialize empty payload
    3. Execute each brick sequentially
    4. Merge metadata and maintain step order
    5. Save payload snapshots to /snapshots/

Snapshots:
    snapshots/step_00_importer_excel.json
    snapshots/step_01_resolver_headers.json
    ...


====================================================================
ADDING A NEW BRICK
====================================================================

1. Create new file under /bricks/, e.g. bricks/my_custom_logic.py

2. Register the brick:
--------------------------------------------------------------------
from npr_tool.core.registry import registry

@registry.register("my_custom_logic")
class MyCustomLogic:
    def __init__(self, config):
        ...

    def run(self, payload):
        ...
        return payload.with_update(...)
--------------------------------------------------------------------

3. Add it to pipeline.yaml:
--------------------------------------------------------------------
- name: my_custom_logic
--------------------------------------------------------------------

Done. The registry auto-imports and registers it automatically.


====================================================================
IMMUTABILITY CONTRACT
====================================================================

Golden Rule:
    No brick may mutate the input payload directly.

Every brick must call:
    payload.with_update(data=new_data, schema="new_schema", metadata={...})

Why:
    • Prevents side effects
    • Enables reproducibility
    • Supports parallelism and caching later


====================================================================
DATA FLOW SNAPSHOT EXAMPLE
====================================================================

[ importer_excel ]
  ↓
Payload(schema="raw_excel", rows=512)

[ resolver_headers ]
  ↓
Payload(schema="normalized", columns=['part_number', 'description'])

[ parser_enrich ]
  ↓
Payload(schema="parsed_parts", added_fields=['value', 'tolerance'])

[ matcher_crosscheck ]
  ↓
Payload(schema="matched_results", match_rate=78%)

[ report_generator ]
  ↓
Output: /data/matched_parts.xlsx
Snapshots: /snapshots/step_*.json


====================================================================
FUTURE FEATURES
====================================================================

  - Parallel pipeline runner
  - Digi-Key / Octopart API integration
  - Web dashboard (FastAPI + React)
  - Machine-learned match confidence scoring
  - Persistent pipeline history viewer


====================================================================
PHILOSOPHY
====================================================================
Build pipelines like LEGO:
  Each brick is small, composable, and independent.

Let data tell its own story,
  and treat immutability as truth preservation.

====================================================================
END OF README
====================================================================
