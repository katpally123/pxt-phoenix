# Runs entirely in the browser via Pyodide. No storage, no network.
# Paste your production logic into build_all(). This starter only wires inputs→outputs.
import io, json, pandas as pd
from datetime import datetime

PRESENT_MARKERS = {"X","Y","YES","TRUE","1","ON PREMISE","On Premise"}

def _now_stamp():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def read_csv_bytes(name: str, blob: bytes) -> pd.DataFrame:
    try:
        return pd.read_csv(io.BytesIO(blob))
    except Exception:
        try:
            return pd.read_csv(io.BytesIO(blob), skiprows=1)
        except Exception:
            return pd.DataFrame()

def build_all(file_map: dict, settings: dict) -> dict:
    roster = read_csv_bytes("roster", next((b for n,b in file_map.items() if "employee" in n.lower() or "roster" in n.lower()), b""))
    mytime = read_csv_bytes("attendance", next((b for n,b in file_map.items() if "attendance" in n.lower() or "daily" in n.lower()), b""))
    vet    = read_csv_bytes("vetvto", next((b for n,b in file_map.items() if "posting" in n.lower() or "vet" in n.lower() or "vto" in n.lower()), b""))
    swaps  = read_csv_bytes("swaps", next((b for n,b in file_map.items() if "swap" in n.lower()), b""))

    def norm_id(x):
        s = str(x).strip().replace("\u200b","").replace(" ","")
        if s.endswith(".0"): s = s[:-2]
        return s

    pres = []
    if not roster.empty:
        eid_col = next((c for c in roster.columns if "person" in c.lower() or "employee" in c.lower() or c.lower()=="id"), None)
        dept_col= next((c for c in roster.columns if "department" in c.lower() and "name" not in c.lower()), None)
        type_col= next((c for c in roster.columns if "employment" in c.lower() and "type" in c.lower()), None)
        on_col  = next((c for c in roster.columns if "premise" in c.lower() or "present" in c.lower()), None)
        ma_col  = next((c for c in roster.columns if "management" in c.lower() and "area" in c.lower()), None)
        name_col= next((c for c in roster.columns if "name" in c.lower() and "first" in c.lower()), None)

        mytime_eid_col = next((c for c in mytime.columns if "person" in c.lower() or "employee" in c.lower() or c.lower()=="id"), None)
        mytime_on_col  = next((c for c in mytime.columns if "premise" in c.lower() or "present" in c.lower()), None)

        mt_map = {}
        if mytime_eid_col:
            for _, r in mytime.iterrows():
                e = norm_id(r.get(mytime_eid_col, ""))
                if not e: continue
                on = str(r.get(mytime_on_col, "")).upper() if mytime_on_col else ""
                mt_map[e] = on

        for _, r in roster.iterrows():
            e = norm_id(r.get(eid_col, "")) if eid_col else ""
            if not e: continue
            on_roster = str(r.get(on_col, "")).upper() if on_col else ""
            on = mt_map.get(e, on_roster)
            pres.append({
                "eid": e,
                "name": str(r.get(name_col, "")),
                "dept_id": str(r.get(dept_col, "")),
                "employment_type": str(r.get(type_col, "")),
                "management_area_id": str(r.get(ma_col, "")),
                "present": on in PRESENT_MARKERS
            })

    summary = {
        "generated_at": _now_stamp(),
        "by_department": {}
    }
    for k in ["Inbound","DA","ICQA","CRETs"]:
        summary["by_department"][k] = {
            "regular_expected_AMZN": 0, "regular_present_AMZN": 0,
            "regular_expected_TEMP": 0, "regular_present_TEMP": 0,
            "swap_out": 0, "swap_in_expected": 0, "swap_in_present": 0,
            "vet_accept": 0, "vet_present": 0, "vto_accept": 0
        }

    for p in pres:
        et = str(p.get("employment_type","")).upper()
        is_amzn = "AMZN" in et
        dept = None
        raw = str(p.get("dept_id",""))
        for d, conf in settings.get("departments",{}).items():
            if raw in [str(x) for x in conf.get("dept_ids",[])]:
                dept = d; break
        if not dept: continue
        key_exp = "regular_expected_AMZN" if is_amzn else "regular_expected_TEMP"
        key_pre = "regular_present_AMZN" if is_amzn else "regular_present_TEMP"
        summary["by_department"][dept][key_exp] += 1
        if p.get("present"): summary["by_department"][dept][key_pre] += 1

    result = {
        "generated_at": _now_stamp(),
        "dept_summary": summary,
        "presence_map": {"generated_at": _now_stamp(), "presence": pres},
        "vet_vto": {"generated_at": _now_stamp(), "records": []},
        "swaps": {"generated_at": _now_stamp(), "swap_out": [], "swap_in_expected": [], "swap_in_present": []}
    }
    return result
