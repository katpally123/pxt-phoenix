# engine_pyodide.py  — v2025-10-05b (MyTime header=1 + compat)
from __future__ import annotations

import io
from typing import Dict, Any, List, Tuple
import pandas as pd

# ===== Config =====
PRESENT_MARKERS = {
    "X","Y","YES","TRUE","1",
    "ON PREMISE","ON-PREMISE","ONPREMISE","On Premise",
    "PRESENT","YELLOW","GREEN"
}
ID_HINTS = ["person id","employee id","person number","employee number","badge id","associate id","id"]

# ===== Utils =====
def _norm(s: Any) -> str: return str(s).strip().replace("\u200b","").replace("\ufeff","")
def _lower(s: Any) -> str: return _norm(s).lower()
def _read_csv_any(blob: bytes, **kw) -> pd.DataFrame:
    try: return pd.read_csv(io.BytesIO(blob), encoding_errors="ignore", **kw)
    except Exception: return pd.DataFrame()
def _read_mytime_with_header2(blob: bytes) -> pd.DataFrame:
    try:
        df = pd.read_csv(io.BytesIO(blob), header=1, encoding_errors="ignore")
        df = df.loc[:, [c for c in df.columns if str(c).strip() and "unnamed" not in str(c).lower()]]
        return df
    except Exception:
        return pd.DataFrame()
def _looks_like_mytime_banner(df: pd.DataFrame) -> bool:
    if df is None or df.empty: return False
    cols = [_lower(c) for c in df.columns]
    return any("hyperfind" in c or "timeframe" in c for c in cols)
def _classify(df: pd.DataFrame) -> str:
    if df is None or df.empty: return "unknown"
    cols = [_lower(c) for c in df.columns]
    if any("on premise" in c for c in cols) or any(c == "present" for c in cols):
        if any(("hyperfind" in c or "timeframe" in c) for c in cols) or any(("person" in c or "employee" in c) for c in cols):
            return "mytime_attendance"
    if any("pay code" in c for c in cols) and any("amount" in c for c in cols):
        return "daily_hours_summary"
    if any("opportunity.acceptedcount" in c for c in cols) or any("opportunity.type" in c for c in cols):
        return "vetvto"
    if any("date to skip" in c for c in cols) or any("date to work" in c for c in cols) or any("swap status" in c for c in cols):
        return "swap"
    if any("employment type" in c for c in cols) and any("department id" in c for c in cols):
        return "roster"
    return "unknown"
def _pick_id_column(df: pd.DataFrame) -> str | None:
    for c in df.columns:
        lc = _lower(c)
        if any(h in lc for h in ID_HINTS): return c
    return None
def _presence_series(df: pd.DataFrame) -> pd.Series:
    candidates = [c for c in df.columns if _lower(c) in ("on premise","present","status","attendance status")]
    if not candidates: return pd.Series([False]*len(df))
    s = df[candidates[0]].astype(str).map(_norm)
    S = {m.lower() for m in PRESENT_MARKERS}
    return s.apply(lambda v: _lower(v) in S)
def _df_preview(df: pd.DataFrame, n: int = 5) -> List[Dict[str, Any]]:
    try: return df.head(n).to_dict(orient="records")
    except Exception: return []

# ===== Public API =====
def build_all(files: List[Tuple[str, bytes]], *_, **__) -> Dict[str, Any]:
    """
    Compatible with calls like: build_all(files), build_all(files, target_date), build_all(files, target_date, settings)
    Extra args are ignored by the engine (JS can still use them separately).
    """
    tables: Dict[str, pd.DataFrame] = {}
    diags: Dict[str, Any] = {"picked_columns": {}, "classifications": {}, "previews": {}}

    for name, blob in files:
        df = _read_csv_any(blob)
        if _looks_like_mytime_banner(df):
            re = _read_mytime_with_header2(blob)
            if not re.empty: df = re

        kind = _classify(df)
        tables[name] = df
        diags["classifications"][name] = kind
        diags["previews"][name] = _df_preview(df, 5)

        id_col = _pick_id_column(df)
        if id_col:
            diags.setdefault("picked_columns", {})[name] = {"id_col": id_col}

        if kind in ("mytime_attendance","roster","swap","vetvto","daily_hours_summary"):
            try:
                pres = _presence_series(df)
                diags["previews"][name + "::presence_sample"] = pres.head(10).tolist()
            except Exception:
                pass

    out_tables: Dict[str, Any] = {}
    for name, df in tables.items():
        try:
            out_tables[name] = {"columns": list(map(str, df.columns)), "rows": df.to_dict(orient="records")}
        except Exception:
            out_tables[name] = {"columns": [], "rows": []}

    return {"tables": out_tables, "diagnostics": diags, "engine_version": "2025-10-05b.mytime-header2-compat"}

def build_single(name: str, blob: bytes) -> Dict[str, Any]:
    return build_all([(name, blob)])
