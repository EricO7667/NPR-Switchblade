import yaml
from core.parsing_registry import registry

def parse_description(records, rules_path: str):
    with open(rules_path, "r") as f:
        cfg = yaml.safe_load(f)
    rules = cfg["parsing_rules"]

    enriched = []
    for row in records:
        desc = row.get("description", "")
        row_out = dict(row)

        for rule in rules:
            if not is_match(desc, rule["detect"]):
                continue

            for extractor_name in rule["extract"]:
                extractor_fn = registry.extractors.get(extractor_name)
                if extractor_fn:
                    row_out.update(extractor_fn(desc))

            row_out["type"] = rule["tag"]
            break

        enriched.append(row_out)
    return enriched


def is_match(desc: str, conditions: list):
    for cond in conditions:
        mode, value = cond.split(":", 1)
        detector_fn = registry.detectors.get(mode)
        if detector_fn and detector_fn(desc, value):
            return True
    return False
