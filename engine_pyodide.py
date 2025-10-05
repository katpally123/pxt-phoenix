# Runs entirely in the browser via Pyodide. No storage, no network.
# This version includes target-date filtering and ICQA/CRETs MA gating.

import io, json, pandas as pd
from datetime import datetime
from dateutil import parser as dparser

PRESENT_MARKERS = {"X","Y","YES","TRUE","1","ON PREMISE","On Premise"}

def _now_stamp():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def _norm_id(x):
    s = str(x).strip().replace("\u200b","").replace(" ","")
    return s[:-2] if s.endswith(".0") else s

def _read_csv_bytes(blob: bytes) -> pd.DataFrame:
    if not blob:
        return pd.DataFrame()
    for kw in ({}, {"skiprows":1}):
        try:
            return pd.read_csv(io.BytesIO(blob), **kw)
        except Exception:
            pass
    return pd.DataFrame()

def _parse_date(x):
    if pd.isna(x): return None
    try:
        return dparser.parse(str(x), dayfirst=False, fuzzy=True).date()
    except Exception:
        return None

def _dept_bucket(dept_id, ma_id, settings):
    if not dept_id:
        return None
    for k, v in settings.get("departments", {}).items():
        ids = set(map(str, v.get("dept_ids", [])))
        need_ma = str(v.get("management_area_id", "")) if v.get("management_area_id") else None
        if str(dept_id) in ids:
            if need_ma:
                return k if str(ma_id) == need_ma else None
            return k
    return None

def build_all(file_map: dict, settings: dict, target_date_str: str = "") -> dict:
    # ---- Detect & read inputs by filename hints ----
    roster = _read_csv_bytes(next((b for n,b in file_map.items() if "employee" in n.lower() or "roster" in n.lower()), b""))
    mytime = _read_csv_bytes(next((b for n,b in file_map.items() if "attendance" in n.lower() or "daily" in n.lower()), b""))
    vetvto = _read_csv_bytes(next((b for n,b in file_map.items() if "posting" in n.lower() or "vet" in n.lower() or "vto" in n.lower()), b""))
    swaps  = _read_csv_bytes(next((b for n,b in file_map.items() if "swap" in n.lower()), b""))

    target_date = None
    if target_date_str:
        try:
            target_date = dparser.parse(target_date_str).date()
        except Exception:
            target_date = None

    # ---- Column pick helper (exact then fuzzy) ----
    def pick(df, want):
        cols = list(df.columns)
        for w in want:
            for c in cols:
                if w.lower() == c.lower(): return c
        for w in want:
            for c in cols:
                if w.lower() in c.lower(): return c
        return None

    # ---- Normalize roster ----
    eid_col   = pick(roster, ["Employee ID","Person ID","Associate ID","ID"])
    dept_col  = pick(roster, ["Department ID","Department"])
    et_col    = pick(roster, ["Employment Type","EmploymentType","Emp Type"])
    on_col    = pick(roster, ["On Premise","OnPremise","Present","Status"])
    ma_col    = pick(roster, ["Management Area ID","ManagementAreaId","MA ID","Corner","Management Area"])
    name_col  = pick(roster, ["First Name","Given Name","Name"])
    lname_col = pick(roster, ["Last Name","Surname"])

    roster_norm = []
    if not roster.empty and eid_col:
        for _, r in roster.iterrows():
            eid = _norm_id(r.get(eid_col, ""))
            if not eid: continue
            roster_norm.append({
                "eid": eid,
                "dept_id": str(r.get(dept_col, "")),
                "employment_type": str(r.get(et_col, "")),
                "management_area_id": str(r.get(ma_col, "")),
                "on_roster": str(r.get(on_col, "")).upper(),
                "name": (str(r.get(name_col, "")) + " " + str(r.get(lname_col, ""))).strip()
            })
    roster_df = pd.DataFrame(roster_norm)

    # ---- Normalize MyTime presence ----
    mt_eid_col = pick(mytime, ["Person ID","Employee ID","ID"])
    mt_on_col  = pick(mytime, ["On Premise","OnPremise","Present","Status"])
    mt_map = {}
    if not mytime.empty and mt_eid_col:
        for _, r in mytime.iterrows():
            e = _norm_id(r.get(mt_eid_col,""))
            if not e: continue
            mt_map[e] = str(r.get(mt_on_col,"")).upper() if mt_on_col else ""

    # ---- Presence map (MyTime preferred; roster fallback) ----
    pres = []
    for _, r in roster_df.iterrows():
        on = mt_map.get(r["eid"], r["on_roster"])
        pres.append({
            "eid": r["eid"],
            "name": r["name"],
            "dept_id": r["dept_id"],
            "employment_type": r["employment_type"],
            "management_area_id": r["management_area_id"],
            "present": on in PRESENT_MARKERS
        })

    # ---- VET/VTO records (filtered to target_date if provided) ----
    vet_out = []
    if not vetvto.empty:
        eid_v   = pick(vetvto, ["employeeId","Employee ID","Person ID","Associate ID","ID"])
        typ_v   = pick(vetvto, ["opportunity.type","Type","Opportunity Type"])
        stat_v  = pick(vetvto, ["Status","opportunity.status","Swap Status"])
        acc_v   = pick(vetvto, ["opportunity.acceptedCount","Accepted Count","acceptedCount"])
        work_v  = pick(vetvto, ["Date to Work","Work Date","Work"])
        approved_words = {"APPROVED","ACCEPTED","COMPLETED"}
        for _, row in vetvto.iterrows():
            eid = _norm_id(row.get(eid_v,"")) if eid_v else ""
            if not eid: continue
            accepted = str(row.get(acc_v,"")).strip() in {"1","1.0","TRUE","True"}
            if not accepted: continue
            st = str(row.get(stat_v,"")).upper()
            if not (st in approved_words or "APPROV" in st or "ACCEPT" in st): continue
            wdt = _parse_date(row.get(work_v))
            if target_date and wdt and wdt != target_date: 
                continue
            typ = str(row.get(typ_v,"")).upper()
            typ = "VET" if "VET" in typ else ("VTO" if "VTO" in typ else typ)
            p = next((p for p in pres if p["eid"] == eid), None)
            vet_out.append({
                "eid": eid,
                "type": typ,
                "work_date": str(wdt) if wdt else None,
                "present": bool(p["present"]) if p else False,
                "dept_id": p["dept_id"] if p else None,
                "employment_type": p["employment_type"] if p else None,
                "management_area_id": p["management_area_id"] if p else None
            })

    # ---- Swaps (filter by Skip/Work == target_date if provided) ----
    sw_out, sw_in_exp, sw_in_pres = [], [], []
    if not swaps.empty:
        id_s   = pick(swaps, ["Employee 1 ID","Employee ID","Person ID","Associate ID","ID"])
        stat_s = pick(swaps, ["Status","Swap Status"])
        skip_s = pick(swaps, ["Date to Skip","Skip Date","Skip"])
        work_s = pick(swaps, ["Date to Work","Work Date","Work"])
        appr_words = {"APPROVED","COMPLETED","ACCEPTED"}
        for _, row in swaps.iterrows():
            eid = _norm_id(row.get(id_s,"")) if id_s else ""
            if not eid: continue
            st = str(row.get(stat_s,"")).upper()
            if not (st in appr_words or "APPROV" in st or "ACCEPT" in st): continue
            sdt = _parse_date(row.get(skip_s))
            wdt = _parse_date(row.get(work_s))
            if target_date:
                if sdt and sdt != target_date and wdt and wdt != target_date:
                    continue
            p = next((p for p in pres if p["eid"] == eid), None)
            rec = {
                "eid": eid,
                "skip_date": str(sdt) if sdt else None,
                "work_date": str(wdt) if wdt else None,
                "present": bool(p["present"]) if p else False,
                "dept_id": p["dept_id"] if p else None,
                "employment_type": p["employment_type"] if p else None,
                "management_area_id": p["management_area_id"] if p else None
            }
            if sdt and (not target_date or sdt == target_date):
                sw_out.append({**rec, "kind":"Swap OUT"})
            if wdt and (not target_date or wdt == target_date):
                sw_in_exp.append({**rec, "kind":"Swap IN (expected)"})
                if rec["present"]:
                    sw_in_pres.append({**rec, "kind":"Swap IN (present)"})

    # ---- Department summary ----
    summary = {
        "generated_at": _now_stamp(),
        "by_department": {k:{
            "regular_expected_AMZN":0, "regular_present_AMZN":0,
            "regular_expected_TEMP":0, "regular_present_TEMP":0,
            "swap_out":0, "swap_in_expected":0, "swap_in_present":0,
            "vet_accept":0, "vet_present":0,
            "vto_accept":0
        } for k in ["Inbound","DA","ICQA","CRETs"]}
    }

    for p in pres:
        dept = _dept_bucket(p["dept_id"], p["management_area_id"], settings)
        if not dept: continue
        is_amzn = "AMZN" in str(p["employment_type"]).upper()
        ke = "regular_expected_AMZN" if is_amzn else "regular_expected_TEMP"
        kp = "regular_present_AMZN" if is_amzn else "regular_present_TEMP"
        summary["by_department"][dept][ke] += 1
        if p["present"]: summary["by_department"][dept][kp] += 1

    for r in sw_out:
        dept = _dept_bucket(r["dept_id"], r["management_area_id"], settings)
        if dept: summary["by_department"][dept]["swap_out"] += 1
    for r in sw_in_exp:
        dept = _dept_bucket(r["dept_id"], r["management_area_id"], settings)
        if dept: summary["by_department"][dept]["swap_in_expected"] += 1
    for r in sw_in_pres:
        dept = _dept_bucket(r["dept_id"], r["management_area_id"], settings)
        if dept: summary["by_department"][dept]["swap_in_present"] += 1

    for r in vet_out:
        dept = _dept_bucket(r["dept_id"], r["management_area_id"], settings)
        if not dept: continue
        if r["type"] == "VET":
            summary["by_department"][dept]["vet_accept"] += 1
            if r["present"]: summary["by_department"][dept]["vet_present"] += 1
        elif r["type"] == "VTO":
            summary["by_department"][dept]["vto_accept"] += 1

    return {
        "generated_at": _now_stamp(),
        "dept_summary": summary,
        "presence_map": {"generated_at": _now_stamp(), "presence": pres},
        "vet_vto": {"generated_at": _now_stamp(), "records": vet_out},
        "swaps": {"generated_at": _now_stamp(), "swap_out": sw_out,
                  "swap_in_expected": sw_in_exp, "swap_in_present": sw_in_pres}
    }
