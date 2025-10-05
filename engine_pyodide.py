# Runs entirely in the browser via Pyodide. No storage, no network.

import io, json, pandas as pd
from datetime import datetime
from dateutil import parser as dparser

PRESENT_MARKERS = {"X","Y","YES","TRUE","1","ON PREMISE","On Premise","PRESENT","YELLOW","GREEN"}

def _now_stamp(): return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def _norm_id(x):
    s = str(x).strip().replace("\u200b","").replace(" ","")
    return s[:-2] if s.endswith(".0") else s

def _read_csv_any(blob: bytes, **kwargs) -> pd.DataFrame:
    if not blob: return pd.DataFrame()
    for kw in ({}, {"skiprows":1}, {"header":None}, {"header":None,"encoding_errors":"ignore"}):
        try: return pd.read_csv(io.BytesIO(blob), **{**kw, **kwargs})
        except Exception: pass
    return pd.DataFrame()

def _parse_date(x):
    if pd.isna(x): return None
    try: return dparser.parse(str(x), dayfirst=False, fuzzy=True).date()
    except Exception: return None

def _pick(df, want):
    if df is None or df.empty: return None
    cols = list(df.columns)
    # exact (case-insensitive)
    for w in want:
        for c in cols:
            if w.lower() == c.lower(): return c
    # fuzzy contains
    for w in want:
        lw = w.lower()
        for c in cols:
            if lw in c.lower(): return c
    return None

# ---------- MyTime “weird header” fixer ----------
def _fix_mytime_layout(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Many UKG/Kronos extracts start with banner rows like 'Hyperfind: Ad Hoc', 'Timeframe: Today'
    and then the real header row appears a few lines later. This scans the first 30 rows to
    find a row that contains Person/Employee and On Premise/Present, then uses it as header.
    """
    if df_raw.empty: return df_raw
    # If it already looks good, return as-is
    cols_low = [str(c).lower() for c in df_raw.columns]
    if any("person id" in c or "employee id" in c for c in cols_low) and any("premise" in c or "present" in c for c in cols_low):
        return df_raw

    # Try header=None read to access raw rows
    df0 = _read_csv_any(df_raw.to_csv(index=False).encode(), header=None)
    # If that failed, try reading original bytes with header=None again
    if df0.empty:
        return df_raw

    # Search top 30 rows for potential header row
    header_row = None
    for i in range(min(30, len(df0))):
        row_vals = [str(x) for x in list(df0.iloc[i].values)]
        joined = "|".join(row_vals).lower()
        if ("person id" in joined or "employee id" in joined) and ("premise" in joined or "present" in joined):
            header_row = i
            break

    if header_row is None:
        # As a fallback, if we see a row containing 'Person' or 'Employee' broadly, use it
        for i in range(min(30, len(df0))):
            joined = "|".join([str(x) for x in list(df0.iloc[i].values)]).lower()
            if ("person" in joined or "employee" in joined):
                header_row = i
                break

    if header_row is None:
        return df_raw

    # Rebuild with that row as header
    new_header = [str(x) for x in list(df0.iloc[header_row].values)]
    body = df0.iloc[header_row+1:].reset_index(drop=True)
    body.columns = new_header
    # Drop empty/Unnamed columns
    body = body.loc[:, [c for c in body.columns if str(c).strip() and "unnamed" not in str(c).lower()]]
    return body

# ---- File classifier by headers (not filenames) ----
def _classify(df: pd.DataFrame):
    cols = set([c.lower() for c in df.columns]) if not df.empty else set()
    if any("opportunity." in c or "acceptedcount" in c for c in cols):
        return "vetvto"
    if any("swap" in c for c in cols) or ("date to skip" in " ".join(cols)):
        return "swaps"
    if any("on premise" in c or "onprem" in c or "present" in c for c in cols):
        deptish = sum(1 for c in cols if "dept" in c or "department" in c)
        return "roster" if deptish >= 2 else "mytime"
    if any("department" in c for c in cols): return "roster"
    return "unknown"

def _dept_bucket(dept_id, ma_id, settings):
    if not dept_id: return None
    for k, v in settings.get("departments", {}).items():
        ids = set(map(str, v.get("dept_ids", [])))
        need_ma = str(v.get("management_area_id", "")) if v.get("management_area_id") else None
        if str(dept_id) in ids:
            if need_ma:
                return k if str(ma_id) == need_ma else None
            return k
    return None

def build_all(file_map: dict, settings: dict, target_date_str: str = "") -> dict:
    # Load all CSVs first (rough read)
    loaded = []
    for name, blob in file_map.items():
        df = _read_csv_any(blob)
        # Try to fix MyTime banner layout if it looks like the 'Hyperfind/Timeframe' export
        if not df.empty and any("hyperfind" in str(c).lower() for c in df.columns):
            # re-read raw bytes header=None and try to detect the true header row
            df_bytes = _read_csv_any(blob, header=None)
            if not df_bytes.empty:
                # heuristic: rebuild DataFrame from df_bytes by finding header row
                # reuse the same function by passing through a temp roundtrip
                fixed = _fix_mytime_layout(df_bytes)
                if not fixed.empty:
                    df = fixed
        kind = _classify(df)
        loaded.append({"name": name, "kind": kind, "cols": list(df.columns), "rows": int(df.shape[0]), "df": df})

    # Choose by kind
    roster_df = next((x["df"] for x in loaded if x["kind"] == "roster"), pd.DataFrame())
    mytime_df = next((x["df"] for x in loaded if x["kind"] == "mytime"), pd.DataFrame())
    vetvto_df = next((x["df"] for x in loaded if x["kind"] == "vetvto"), pd.DataFrame())
    swaps_df  = next((x["df"] for x in loaded if x["kind"] == "swaps"), pd.DataFrame())

    # If still missing MyTime and we have exactly one “unknown” with Hyperfind/Timeframe, try to fix it
    if mytime_df.empty:
        for x in loaded:
            if x["kind"] == "unknown" and any("hyperfind" in str(c).lower() for c in x["cols"]):
                fixed = _fix_mytime_layout(x["df"])
                if not fixed.empty:
                    mytime_df = fixed
                    x["kind"] = "mytime"
                    break

    # Target date
    target_date = None
    if target_date_str:
        try: target_date = dparser.parse(target_date_str).date()
        except Exception: target_date = None

    # ---- Normalize roster ----
    eid_col   = _pick(roster_df, ["Employee ID","Person ID","Associate ID","ID"])
    dept_col  = _pick(roster_df, ["Department ID","Department"])
    et_col    = _pick(roster_df, ["Employment Type","EmploymentType","Emp Type"])
    on_col    = _pick(roster_df, ["On Premise","OnPremise","Present","Status"])
    ma_col    = _pick(roster_df, ["Management Area ID","ManagementAreaId","MA ID","Corner","Management Area"])
    fn_col    = _pick(roster_df, ["First Name","Given Name","First"])
    ln_col    = _pick(roster_df, ["Last Name","Surname","Last"])

    roster_norm = []
    if not roster_df.empty and eid_col:
        for _, r in roster_df.iterrows():
            eid = _norm_id(r.get(eid_col, ""))
            if not eid: continue
            roster_norm.append({
                "eid": eid,
                "dept_id": str(r.get(dept_col, "")),
                "employment_type": str(r.get(et_col, "")),
                "management_area_id": str(r.get(ma_col, "")),
                "on_roster": str(r.get(on_col, "")).upper(),
                "name": (str(r.get(fn_col, "")) + " " + str(r.get(ln_col, ""))).strip()
            })
    rosterN = pd.DataFrame(roster_norm)

    # ---- Normalize MyTime presence ----
    mt_eid_col = _pick(mytime_df, ["Person ID","Employee ID","ID"])
    mt_on_col  = _pick(mytime_df, ["On Premise","OnPremise","Present","Status"])
    mt_map = {}
    if not mytime_df.empty and mt_eid_col:
        for _, r in mytime_df.iterrows():
            e = _norm_id(r.get(mt_eid_col,""))
            if not e: continue
            mt_map[e] = str(r.get(mt_on_col,"")).upper() if mt_on_col else ""

    # ---- Presence map (MyTime preferred; roster fallback) ----
    pres = []
    for _, r in rosterN.iterrows():
        on = mt_map.get(r["eid"], r["on_roster"])
        pres.append({
            "eid": r["eid"],
            "name": r["name"],
            "dept_id": r["dept_id"],
            "employment_type": r["employment_type"],
            "management_area_id": r["management_area_id"],
            "present": on in PRESENT_MARKERS
        })

    # ---- VET/VTO (acceptedCount > 0; work date from shiftStart/shiftEnd) ----
    vet_out = []
    if not vetvto_df.empty:
        eid_v   = _pick(vetvto_df, ["employeeId","Employee ID","Person ID","Associate ID","ID"])
        typ_v   = _pick(vetvto_df, ["opportunity.type","Type","Opportunity Type"])
        acc_v   = _pick(vetvto_df, ["opportunity.acceptedCount","Accepted Count","acceptedCount"])
        work_v1 = _pick(vetvto_df, ["opportunity.shiftStart","shiftStart","Shift Start"])
        work_v2 = _pick(vetvto_df, ["opportunity.shiftEnd","shiftEnd","Shift End"])
        for _, row in vetvto_df.iterrows():
            eid = _norm_id(row.get(eid_v,"")) if eid_v else ""
            if not eid: continue
            accepted = str(row.get(acc_v,"")).strip() not in {"","0","0.0","FALSE","False","NaN","nan"}
            if not accepted: continue
            wdt = _parse_date(row.get(work_v1)) or _parse_date(row.get(work_v2))
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

    # ---- Swaps (as before) ----
    sw_out, sw_in_exp, sw_in_pres = [], [], []
    if not swaps_df.empty:
        id_s   = _pick(swaps_df, ["Employee 1 ID","Employee ID","Person ID","Associate ID","ID"])
        stat_s = _pick(swaps_df, ["Status","Swap Status"])
        skip_s = _pick(swaps_df, ["Date to Skip","Skip Date","Skip"])
        work_s = _pick(swaps_df, ["Date to Work","Work Date","Work"])
        appr_words = {"APPROVED","COMPLETED","ACCEPTED"}
        for _, row in swaps_df.iterrows():
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
            "vet_accept":0, "vet_present":0, "vto_accept":0
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

    # ---- Diagnostics (keep this visible until numbers look right) ----
    diag = {
        "target_date": str(target_date) if target_date else None,
        "loaded_files": [
            {"name": x["name"], "kind": x["kind"], "rows": x["rows"], "cols": x["cols"][:25]} for x in loaded
        ],
        "picked_columns": {
            "roster": {"eid": eid_col, "dept": dept_col, "employment_type": et_col, "on_prem": on_col, "ma": ma_col},
            "mytime": {"eid": mt_eid_col, "on_prem": mt_on_col},
        },
        "presence_count": len(pres),
    }

    return {
        "generated_at": _now_stamp(),
        "dept_summary": summary,
        "presence_map": {"generated_at": _now_stamp(), "presence": pres},
        "vet_vto": {"generated_at": _now_stamp(), "records": vet_out},
        "swaps": {"generated_at": _now_stamp(), "swap_out": sw_out,
                  "swap_in_expected": sw_in_exp, "swap_in_present": sw_in_pres},
        "diagnostics": diag
    }
