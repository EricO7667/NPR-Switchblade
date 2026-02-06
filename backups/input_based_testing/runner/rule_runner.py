import yaml
from core.payload import Payload
from core.parse_engine import parse_description
from core.loaders import load_bom

class RuleRunner:
    def __init__(self, pipeline_path: str):
        with open(pipeline_path, "r") as f:
            cfg = yaml.safe_load(f)
        self.pipeline = cfg.get("pipeline", [])

    def run(self):
        payload = Payload()

        for step in self.pipeline:
            name = step["name"]
            print(f"\n🔹 Running step: {name}")

            for rule in step.get("rules", []):
                rtype = rule["type"]

                if rtype == "load_bom":
                    payload = load_bom(rule["path"])
                    print(f"   ✔️ Loaded data with {len(payload.data)} records")

                elif rtype == "parse":
                    payload = payload.with_update(
                        data=parse_description(payload.data, rule["rules_file"]),
                        schema="parsed"
                    )
                    print("   ✔️ Parsed and enriched data")

        return payload
