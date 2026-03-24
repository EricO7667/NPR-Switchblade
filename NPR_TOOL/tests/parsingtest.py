from parsing_engine import parse_description, normalize, parse_quantity_tokens
from config_loader import load_config
import json

cfg = load_config("config/components.yaml")

texts = [
    "CAP CER 0402 4.7NF 16V X7R 10%",]

for i, text in enumerate(texts, start=1):
    norm = normalize(text)
    toks = parse_quantity_tokens(norm)
    parsed = parse_description(text, cfg)

    rec = {
        "raw_text": text,
        "normalized_text": norm,
        "tokens": toks,
        "parsed": parsed,
        "index": i,
    }

    print(json.dumps(rec, ensure_ascii=False))