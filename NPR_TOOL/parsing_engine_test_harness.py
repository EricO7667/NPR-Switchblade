from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, List, Optional

import yaml

import parsing_engine as pe


# ==========================================================
# CONFIG LOADING
# ==========================================================
@dataclass
class ComponentConfig:
    name: str
    enabled: bool
    detect_keywords: List[str]
    parsing: Dict[str, str]
    matching: Dict[str, Any]


@dataclass
class ParserConfig:
    components: Dict[str, ComponentConfig]
    version: Optional[str] = None
    tiers: Optional[Dict[str, Any]] = None


DEFAULT_CONFIG_PATH = Path("/mnt/data/components.yaml")


COMMON_FIELDS = [
    "resistance_ohms",
    "capacitance_farads",
    "inductance_henries",
    "voltage_v",
    "current_a",
    "power_w",
    "frequency_hz",
    "tolerance_percent",
    "package",
]


def load_parser_config(path: str | Path = DEFAULT_CONFIG_PATH) -> ParserConfig:
    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}

    comps: Dict[str, ComponentConfig] = {}
    for name, cfg in (raw.get("components") or {}).items():
        comps[name] = ComponentConfig(
            name=name,
            enabled=bool(cfg.get("enabled", True)),
            detect_keywords=list(cfg.get("detect_keywords") or []),
            parsing=dict(cfg.get("parsing") or {}),
            matching=dict(cfg.get("matching") or {}),
        )

    return ParserConfig(
        components=comps,
        version=raw.get("version"),
        tiers=raw.get("tiers"),
    )


# ==========================================================
# PARSE HELPERS
# ==========================================================
def parse_line(line: str, config: Optional[ParserConfig] = None) -> Dict[str, Any]:
    text_norm = pe.normalize(line)
    tokens = pe.parse_quantity_tokens(text_norm)

    result: Dict[str, Any] = {
        "input": line,
        "normalized": text_norm,
        "tokens": tokens,
        "values": {
            "resistance_ohms": pe.parse_res_value_ohms(text_norm, tokens),
            "capacitance_farads": pe.parse_cap_value_farads(text_norm, tokens),
            "inductance_henries": _first_token(tokens, "inductance"),
            "voltage_v": pe.parse_voltage_v(text_norm, tokens),
            "current_a": pe.parse_current_a(text_norm, tokens),
            "power_w": pe.parse_power_w(text_norm, tokens),
            "frequency_hz": _first_token(tokens, "frequency"),
            "tolerance_percent": pe.parse_tolerance_percent(text_norm, tokens),
            "package": pe.parse_package(text_norm, tokens),
        },
    }

    if config is not None:
        try:
            full_parse = pe.parse_description(line, config)
        except Exception as exc:
            full_parse = {"error": str(exc)}
        result["full_parse"] = full_parse

    return result


# ==========================================================
# MATCHING HELPERS
# ==========================================================
def compare_lines(line_a: str, line_b: str, config: Optional[ParserConfig] = None) -> Dict[str, Any]:
    parsed_a = parse_line(line_a, config=config)
    parsed_b = parse_line(line_b, config=config)

    fields = COMMON_FIELDS
    comparisons = []
    total_score = 0.0
    hard_fail = False

    for field in fields:
        a_val = parsed_a["values"].get(field)
        b_val = parsed_b["values"].get(field)
        rule = classify_field_rule(field)
        status, note, delta, fail = compare_field(field, a_val, b_val, rule)
        if fail:
            hard_fail = True
        total_score += delta
        comparisons.append(
            {
                "field": field,
                "left": a_val,
                "right": b_val,
                "rule": rule,
                "status": status,
                "note": note,
                "score_delta": delta,
            }
        )

    return {
        "left": parsed_a,
        "right": parsed_b,
        "comparisons": comparisons,
        "hard_fail": hard_fail,
        "score": round(total_score, 4),
    }


def classify_field_rule(field: str) -> str:
    if field in {"resistance_ohms", "capacitance_farads", "inductance_henries", "frequency_hz", "package"}:
        return "exact"
    if field in {"voltage_v", "current_a", "power_w"}:
        return "candidate_gte"
    if field == "tolerance_percent":
        return "candidate_lte"
    return "info"


def compare_field(field: str, left: Any, right: Any, rule: str):
    if left is None and right is None:
        return "missing_both", "Neither side parsed a value.", 0.0, False
    if left is None or right is None:
        return "missing_one", "One side is missing a parsed value.", -0.1, False

    if rule == "exact":
        if left == right:
            return "match", "Exact match.", 1.0, False
        return "mismatch", "Exact-value mismatch.", -1.0, True

    if rule == "candidate_gte":
        if right >= left:
            bonus = 0.75 if right == left else 0.5
            return "compatible", "Right side meets or exceeds left side requirement.", bonus, False
        return "mismatch", "Right side is below left side requirement.", -1.0, True

    if rule == "candidate_lte":
        if right <= left:
            bonus = 0.75 if right == left else 0.5
            return "compatible", "Right side tolerance is equal or tighter.", bonus, False
        return "mismatch", "Right side tolerance is looser.", -1.0, True

    return "info", "No rule applied.", 0.0, False


# ==========================================================
# DISPLAY HELPERS
# ==========================================================
def print_parse_result(result: Dict[str, Any]) -> None:
    print("\n=== INPUT ===")
    print(result["input"])
    print("\n=== NORMALIZED ===")
    print(result["normalized"])

    print("\n=== TOKENS ===")
    if result["tokens"]:
        for token in result["tokens"]:
            print(token)
    else:
        print("<none>")

    print("\n=== VALUES ===")
    for key, value in result["values"].items():
        print(f"{key}: {value}")

    if "full_parse" in result:
        print("\n=== FULL PARSE ===")
        print(json.dumps(result["full_parse"], indent=2, default=str))


def print_compare_result(result: Dict[str, Any]) -> None:
    print("\n=== LEFT VALUES ===")
    for key, value in result["left"]["values"].items():
        print(f"{key}: {value}")

    print("\n=== RIGHT VALUES ===")
    for key, value in result["right"]["values"].items():
        print(f"{key}: {value}")

    print("\n=== FIELD COMPARISON ===")
    for row in result["comparisons"]:
        print(
            f"{row['field']}: left={row['left']} | right={row['right']} | "
            f"rule={row['rule']} | status={row['status']} | {row['note']}"
        )

    print("\n=== RESULT ===")
    print(f"hard_fail: {result['hard_fail']}")
    print(f"score: {result['score']}")


# ==========================================================
# INTERNALS
# ==========================================================
def _first_token(tokens: List[Dict[str, Any]], kind: str) -> Any:
    for token in tokens:
        if token.get("kind") == kind:
            return token.get("value")
    return None


# ==========================================================
# SIMPLE CLI
# ==========================================================
def main() -> None:
    config = None
    if DEFAULT_CONFIG_PATH.exists():
        try:
            config = load_parser_config(DEFAULT_CONFIG_PATH)
        except Exception as exc:
            print(f"Warning: could not load config: {exc}")

    while True:
        print("\nChoose mode: [1] parse one line  [2] compare two lines  [q] quit")
        choice = input("> ").strip().lower()

        if choice == "q":
            break

        if choice == "1":
            line = input("Enter one component line: ").strip()
            result = parse_line(line, config=config)
            print_parse_result(result)
            continue

        if choice == "2":
            left = input("Enter left/source component line: ").strip()
            right = input("Enter right/candidate component line: ").strip()
            result = compare_lines(left, right, config=config)
            print_compare_result(result)
            continue

        print("Invalid choice.")


if __name__ == "__main__":
    main()
