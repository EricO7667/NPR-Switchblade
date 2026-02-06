import pandas as pd
import re
import yaml
from dataclasses import dataclass, field

@dataclass
class Payload:
    raw_bom: pd.DataFrame = None
    parsed_bom: pd.DataFrame = None
    matched_bom: pd.DataFrame = None
    inventory: pd.DataFrame = None
    config: dict = field(default_factory=dict)
    metadata: dict = field(default_factory=dict)
    history: list = field(default_factory=list)

    def log(self, msg):
        print(msg)
        self.history.append(msg)

    # ==========================================================
    # LOADERS
    # ==========================================================
    def load_config(self, path="config.yaml"):
        self.log(f"Loading configuration from {path}")
        with open(path, "r") as f:
            self.config = yaml.safe_load(f)
        self.log("Configuration loaded.")
        return self

    def load_bom(self, path):
        self.log(f"Loading BOM from {path}")
        df = pd.read_excel(path, dtype=str).fillna("")
        df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
        self.raw_bom = df
        self.log(f"Loaded {len(df)} BOM rows.")
        return self

    def load_inventory(self, path):
        self.log(f"Loading inventory from {path}")
        df = pd.read_excel(path, dtype=str).fillna("")
        df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
        self.inventory = df
        self.log(f"Loaded {len(df)} inventory rows.")
        return self

    # ==========================================================
    # PARSING
    # ==========================================================
    def parse_descriptions(self):
        if self.raw_bom is None:
            raise RuntimeError("No BOM loaded.")
        self.log("Parsing descriptions...")
        df = self.raw_bom.copy()
        df["type"] = None
        df["value"] = None
        df["tolerance"] = None

        rule_map = self.config.get("parsing_rules", {})

        for i, row in df.iterrows():
            desc = row.get("description", "")
            desc_lower = desc.lower()

            matched_type = None
            for part_type, patterns in rule_map.items():
                if any(pat.lower() in desc_lower for pat in patterns):
                    matched_type = part_type
                    break
            if not matched_type:
                matched_type = "OTHER"

            df.at[i, "type"] = matched_type

            if matched_type == "RES":
                val = re.search(r"(\d+(?:\.\d+)?k?)", desc)
                tol = re.search(r"(\d+%)", desc)
                if val:
                    df.at[i, "value"] = val.group(1)
                if tol:
                    df.at[i, "tolerance"] = tol.group(1)
            elif matched_type == "CAP":
                val = re.search(r"(\d+(?:\.\d+)?[munp]?f)", desc, re.I)
                volt = re.search(r"(\d+V)", desc)
                if val:
                    df.at[i, "value"] = val.group(1)
                if volt:
                    df.at[i, "tolerance"] = volt.group(1)

        self.parsed_bom = df
        self.log("Parsing complete.")
        return self

    # ==========================================================
    # MATCHING
    # ==========================================================
    def match_inventory(self):
        if self.parsed_bom is None or self.inventory is None:
            raise RuntimeError("Need parsed BOM and inventory first.")
        self.log("Matching parsed BOM to inventory...")

        df = self.parsed_bom.copy()
        inv = self.inventory
        df["inventory_match"] = None

        rules = self.config.get("matching_rules", {})
        default_rule = rules.get("default", {"match_by": [["type", "type"], ["value", "value"]], "link_field": "inventory_match"})

        for i, row in df.iterrows():
            part_type = row.get("type", "OTHER")
            rule = rules.get(part_type, default_rule)
            match_pairs = rule.get("match_by", [])
            link_field = rule.get("link_field", "inventory_match")

            subset = inv.copy()
            for pair in match_pairs:
                bom_field, inv_field = pair
                subset = subset[subset[inv_field].astype(str).str.lower() == str(row.get(bom_field, "")).lower()]

            if not subset.empty:
                df.at[i, link_field] = subset.iloc[0].get("part_number")

        self.matched_bom = df
        self.log("Matching complete.")
        return self

    # ==========================================================
    # EXPORT / VIEW
    # ==========================================================
    def export(self, path):
        self.log(f"Exporting to {path}")
    
        df = None
        if self.matched_bom is not None and not self.matched_bom.empty:
            df = self.matched_bom
        elif self.parsed_bom is not None and not self.parsed_bom.empty:
            df = self.parsed_bom
        elif self.raw_bom is not None and not self.raw_bom.empty:
            df = self.raw_bom
        else:
            raise ValueError("No BOM data available to export.")
    
        df.to_excel(path, index=False)
        self.log("Export complete.")
        return self


    def summary(self):
        self.log("\n--- PAYLOAD SUMMARY ---")
        if self.raw_bom is not None:
            self.log(f"Rows: {len(self.raw_bom)}")
            self.log(f"Detected types: {self.parsed_bom['type'].value_counts().to_dict() if self.parsed_bom is not None else 'N/A'}")
        self.log("------------------------")
        return self
