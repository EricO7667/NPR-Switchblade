import os
import pandas as pd
from core.payload import Payload

def load_bom(path: str) -> Payload:
    """
    Load a Bill of Materials (BOM) from Excel, CSV, or JSON.
    Returns a Payload with standardized lowercase column names.
    """
    ext = os.path.splitext(path)[1].lower()
    if ext in [".xlsx", ".xls"]:
        df = pd.read_excel(path, dtype=str)
    elif ext == ".csv":
        df = pd.read_csv(path, dtype=str)
    elif ext == ".json":
        df = pd.read_json(path)
    else:
        raise ValueError(f"Unsupported file type: {ext}")

    # Clean headers and fill NaN
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    df = df.fillna("")

    print(f"📦 Loaded {len(df)} rows from {path}")
    return Payload(data=df.to_dict(orient="records"), schema="raw", metadata={"source": path})
