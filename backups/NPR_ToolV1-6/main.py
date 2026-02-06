from payload import Payload
import pandas as pd
import os

os.makedirs("data", exist_ok=True)

# --- Create demo data ---
fake_bom = pd.DataFrame({
    "Part ID": ["R001", "C002", "U003"],
    "Description": [
        "10k 1% 0603 RES",
        "100nF 16V CAP",
        "LM358D IC OPAMP"
    ]
})
fake_inventory = pd.DataFrame({
    "part_number": ["RES-10K", "CAP-100NF", "OP-AMP-LM358"],
    "type": ["RES", "CAP", "OTHER"],
    "value": ["10k", "100nF", ""],
    "tolerance": ["1%", "16V", ""]
})
fake_bom.to_excel("data/fake_bom.xlsx", index=False)
fake_inventory.to_excel("data/fake_inventory.xlsx", index=False)

# --- Run pipeline ---
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

print("\n✅ Final Matched Data:")
print(payload.matched_bom)
