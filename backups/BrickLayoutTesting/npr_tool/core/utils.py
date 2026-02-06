"""
====================================================================
 UTILS — General Helper Functions for Cleaning and Normalizing
====================================================================

PURPOSE:
Small, stateless helpers used across bricks for string cleanup and
data safety. Keeps logic DRY and predictable.

Data Flow: Imported wherever data cleaning occurs.
====================================================================
"""

import re
import pandas as pd


def safe_str(x):
    """Convert to clean string, avoiding NaNs and None."""
    if pd.isna(x):
        return ""
    return str(x).strip()


def clean_excel_str(s: str):
    """
    Remove hidden Excel encodings like `_x000D_` that appear
    when exporting spreadsheets. Keeps strings pipeline-safe.
    """
    if not s:
        return ""
    s = str(s)
    return re.sub(r"_x[0-9a-fA-F]{4}_", "", s).strip()
