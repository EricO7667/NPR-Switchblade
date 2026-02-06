import re
from typing import Dict, Callable

class ParsingRegistry:
    def __init__(self):
        self.detectors: Dict[str, Callable[[str, str], bool]] = {}
        self.extractors: Dict[str, Callable[[str], Dict[str, str]]] = {}

    def register_detector(self, name):
        def decorator(fn):
            self.detectors[name] = fn
            return fn
        return decorator

    def register_extractor(self, name):
        def decorator(fn):
            self.extractors[name] = fn
            return fn
        return decorator

registry = ParsingRegistry()

# -------------------
# DETECTORS
# -------------------
@registry.register_detector("contains")
def contains_detector(desc: str, value: str) -> bool:
    return value.lower() in desc.lower()

@registry.register_detector("prefix")
def prefix_detector(desc: str, value: str) -> bool:
    return desc.strip().startswith(value)

# -------------------
# EXTRACTORS
# -------------------
@registry.register_extractor("ohmic_value")
def extract_ohmic_value(desc: str) -> Dict[str, str]:
    match = re.search(r"(\d+(?:\.\d+)?k?)", desc, re.IGNORECASE)
    return {"value": match.group(1)} if match else {}

@registry.register_extractor("tolerance")
def extract_tolerance(desc: str) -> Dict[str, str]:
    match = re.search(r"(\d+%)", desc)
    return {"tolerance": match.group(1)} if match else {}

@registry.register_extractor("cap_value")
def extract_cap_value(desc: str) -> Dict[str, str]:
    match = re.search(r"(\d+(?:\.\d+)?[munp]?F)", desc, re.IGNORECASE)
    return {"value": match.group(1)} if match else {}

@registry.register_extractor("voltage")
def extract_voltage(desc: str) -> Dict[str, str]:
    match = re.search(r"(\d+V)", desc)
    return {"voltage": match.group(1)} if match else {}
