# engine_pyodide.py  — v2025-10-05
# Drop-in Pyodide engine for PXT Attendance Dashboard
# - Adds robust MyTime parser that reads real headers on row 2 (header=1)
# - Keeps lightweight classifiers and normalizers
# - Returns tidy dicts ready for JS consumption via Pyodide

from __future__ import annotations

import io
import json
import re
from datetime import datetime
from typing import Dict, Any, List, Tuple

import pandas as pd


# =========================
# Constants / Config
# =========================

# Presence markers to treat as "present"
PRESENT_MARKERS = {
    "X","Y","YES","TRUE","1",
    "ON PREMISE","ON-PREMISE","ONPREMISE","On Premise",
    "PRESENT","YELLOW","GREEN"
}

# Columns we often see for ID mapping
ID_HINTS = [
    "person id", "employee id", "person number", "employee number", "badge id", "associate id", "id"
]

# =========================
# Utilities
# =========================

def _norm(s: Any) -> str:
    return str(s).strip().replace("\u200b", "").replace("\ufeff", "")

def _lower(s: Any) -> str:
    return _norm(s).lower()

def _read_csv_any(blob: bytes, **kwargs) -> pd.DataFrame:
    try:
        return pd.read_csv(io.BytesIO(blob), encoding_errors="ignore", **kwargs)
    except Exception:
        return pd.DataFrame()

def _read_mytime_with_header2(blob: bytes) -> pd.DataFrame:
    """
    MyTime exports: banner row on line 1, real headers on line 2 (1-indexed).
    Read with header=1 and drop empty/Unnamed columns.
    """
    try:
        df = pd.read_csv(io.BytesIO(blob), header=1, encoding_errors="ignore")
        # drop empty/Unnamed columns
        df = df.loc[:, [c for c in df.columns if str(c).strip() and "unnamed" not in str(c).lower()]]
        return df
    except Exception:
        return pd.DataFrame()

def _looks_like_mytime_banner(df: pd.DataFrame) -> bool:
    if df is None or df.empty:
        return False
    cols = [ _lower(c) for c in df.columns ]
    return any("hyperfind" in c or "timeframe" in c for c in cols)

def _classify(df: pd.DataFrame) -> str:
    """Heuristic file classifier used by the dashboard build."""
    if df is None or df.empty:
        return "unknown"
    cols = [_lower(c) for c in df.columns]

    # MyTime attendance (CAN Daily Attendance)
    if any("on premise" in c for c in cols) or any("present" == c for c in cols):
        if any("hyperfind" in c or "timeframe" in c for c in cols) or any("person" in c or "employee" in c for c in cols):
            return "mytime_attendance"

    # Daily Hours Summary (often used to detect vacation/banked)
    if any("pay code" in c for c in cols) and any("amount" in c for c in cols):
        # Often also has Hyperfind/Timeframe banner
        return "daily_hours_summary"

    # VET/VTO (Scheduling export, columns like opportunity.acceptedCount, opportunity.type, employeeId, departmentId)
    if any("opportunity.acceptedcount" in c for c in cols) or any("opportunity.type" in c for c in cols):
        return "vetvto"

    # Swaps (Shift Swap Explorer: Status, Date to Skip, Date to Work)
    if any("date to skip" in c for c in cols) or any("date to work" in c for c in cols) or any("swap status" in c for c in cols):
        return "swap"

    # Roster (FCLM: Employment Type, Department ID, Corner, On Premise etc.)
    if any("employment type" in c for c in cols) and any("department id" in c for c in cols):
        return "roster"

    return "unknown"

def _pick_id_column(df: pd.DataFrame) -> str | None:
    cols = list(df.columns)
    for c in cols:
        lc = _lower(c)
        for hint in ID_HINTS:
            if hint in lc:
                return c
    return None

def _presence_series(df: pd.DataFrame) -> pd.Series:
    # Try common presence-like columns
    candidates = [c for c in df.columns if _lower(c) in ("on premise","present","status","attendance status")]
    if not candidates:
        return pd.Series([False] * len(df))
    s = df[candidates[0]].astype(str).map(_norm)
    return s.apply(lambda v: _lower(v) in {m.lower() for m in PRESENT_MARKERS})

def _df_preview(df: pd.DataFrame, n: int = 5) -> List[Dict[str, Any]]:
    try:
        return df.head(n).to_dict(orient="records")
    except Exception:
        return []

# =========================
# Public API
# =========================

def build_all(files: List[Tuple[str, bytes]]) -> Dict[str, Any]:
    """
    Build step run inside Pyodide.
    :param files: list of (name, blob_bytes)
    :return: dict with typed tables + diagnostics
    """
    tables: Dict[str, pd.DataFrame] = {}
    diags: Dict[str, Any] = {"picked_columns": {}, "classifications": {}, "previews": {}}

    for name, blob in files:
        # --- initial read ---
        df = _read_csv_any(blob)

        # --- MyTime patch: if banner row (Hyperfind/Timeframe) detected, re-read with header row 2 ---
        if _looks_like_mytime_banner(df):
            re = _read_mytime_with_header2(blob)
            if not re.empty:
                df = re

        kind = _classify(df)
        tables[name] = df
        diags["classifications"][name] = kind
        diags["previews"][name] = _df_preview(df, 5)

        # Useful picked columns
        id_col = _pick_id_column(df)
        if id_col:
            diags.setdefault("picked_columns", {})[name] = {"id_col": id_col}

        # Presence sample only for likely attendance files
        if kind in ("mytime_attendance","roster","swap","vetvto","daily_hours_summary"):
            try:
                pres = _presence_series(df)
                diags["previews"][name + "::presence_sample"] = pres.head(10).tolist()
            except Exception:
                pass

    # Convert dataframes to records for JS
    out_tables: Dict[str, Any] = {}
    for name, df in tables.items():
        try:
            out_tables[name] = {
                "columns": list(map(str, df.columns)),
                "rows": df.to_dict(orient="records")
            }
        except Exception:
            out_tables[name] = {"columns": [], "rows": []}

    return {
        "tables": out_tables,
        "diagnostics": diags,
        "engine_version": "2025-10-05.mytime-header2"
    }

# Convenience to run locally inside Pyodide with a single file
def build_single(name: str, blob: bytes) -> Dict[str, Any]:
    return build_all([(name, blob)])
