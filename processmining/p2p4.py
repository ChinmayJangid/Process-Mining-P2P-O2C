import io
import os
import json
import pandas as pd
from fastapi import APIRouter, Query, HTTPException, UploadFile, File, Form
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from typing import Optional
import warnings
import re
from datetime import datetime

warnings.filterwarnings("ignore")

router = APIRouter(prefix="/p2p", tags=["Procure-to-Pay"])

# ─── Server-side CSV Output Directory ───────────────────────────────────────
# All processed CSVs are saved here on the server.  No Windows hard-coded path.
P2P_OUTPUT_DIR = os.path.join("user_data", "p2p_outputs")
os.makedirs(P2P_OUTPUT_DIR, exist_ok=True)


def _save_output_csv(df: pd.DataFrame, filename: str) -> str:
    """
    Save a clean wide-format CSV to the server-side P2P output directory.
    Returns the saved path.  Always succeeds (raises on real errors).
    """
    os.makedirs(P2P_OUTPUT_DIR, exist_ok=True)
    export_df = df[[c for c in df.columns if not c.startswith("_")]].copy()
    for c in export_df.columns:
        if pd.api.types.is_datetime64_any_dtype(export_df[c]):
            export_df[c] = export_df[c].dt.strftime("%Y-%m-%d")
    out_path = os.path.join(P2P_OUTPUT_DIR, filename)
    export_df.to_csv(out_path, index=False)
    print(f"[P2P OUTPUT] CSV saved → {out_path}  ({len(export_df)} rows)")
    return out_path


# ─── Format Detection & Conversion ──────────────────────────────────────────
#
# There are TWO possible CSV formats that can be uploaded:
#
# 1. WIDE FORMAT (transformer output, one row per UniqueID_PO):
#       UniqueID_PO | PO Creation | GR Posting | Invoice Posting | BUKRS | …
#    → Already correct for all chart endpoints.  Use as-is.
#
# 2. LONG FORMAT (KNIME Unpivot #86 → Renamer #87 output, one row per event):
#       UniqueID_PO | Activity     | Timestamp  | BUKRS | Invoice Posting | …
#       4500…01     | PO Creation  | 2023-01-05 | 1000  | 2023-03-01      | …
#       4500…01     | GR Posting   | 2023-02-01 | 1000  | 2023-03-01      | …
#    → Must be pivoted to wide before storage.
#
#    KNIME Unpivot #86 value_columns (these become Activity rows):
#       PO Creation, PO Date, PO Reversal Date,
#       PR Creation, PR Release Date, PR Reversal Date,
#       GR Posting, GR Reversal          ← NOTE: "GR Reversal" not "GR Reversal Date"
#
#    KNIME Unpivot #86 id_columns (these stay as columns on every row):
#       UniqueID_PO, BUKRS, LIFNR, BSART, EKGRP, MATNR, MATKL, WERKS, ERNAM,
#       Invoice Posting, Invoice Reversal Date, GR Reversal Date,   ← kept as cols!
#       GR Creation User, Invoice Creation User, etc.
#
#    Pre-Renamer exports use 'ColumnNames'/'ColumnValues' instead of 'Activity'/'Timestamp'.
#
# ACTIVITY_RENAME_MAP normalises KNIME activity names → our internal names.

ACTIVITY_RENAME_MAP = {
    # KNIME Unpivot output names       → our ACTIVITY_COLUMNS names
    "GR Reversal":                      "GR Reversal Date",
    "Invoice Reversal":                 "Invoice Reversal Date",
    "PR Reversal":                      "PR Reversal Date",
    "PO Reversal":                      "PO Reversal Date",
}

# All activity names we recognise — both KNIME names and our internal names
_ALL_KNOWN_ACTIVITIES = {
    "PR Creation", "PR Release Date", "PR Reversal Date", "PR Reversal",
    "PO Creation", "PO Date", "PO Reversal Date", "PO Reversal",
    "GR Posting", "GR Reversal Date", "GR Reversal",
    "Invoice Posting", "Invoice Reversal Date", "Invoice Reversal",
}


def _is_long_format(df: pd.DataFrame) -> bool:
    """
    Returns True when the dataframe is a KNIME long-format event log.

    Detection rules:
     - Has an activity-name column: 'Activity' OR 'ColumnNames'
     - Has a timestamp column: 'Timestamp' OR 'ColumnValues'
     - The activity column values contain known P2P activity names (≥10 %)

    NOTE: We intentionally do NOT bail out based on whether wide-style activity
    columns (e.g. 'PO Creation', 'GR Posting') also exist as separate columns.
    KNIME's Unpivot node keeps some activities as id-columns (e.g. Invoice Posting,
    GR Reversal Date) that remain as real columns alongside the Activity/Timestamp
    long-format rows. Checking for those would cause a false-negative and leave
    the df in long format — resulting in incorrect row counts and missing KPIs.

    The ONLY reliable signal is: does the Activity column contain known activity names?
    If yes → this is long format, always convert.
    """
    # Find activity and timestamp column names (handle pre-renamer exports)
    act_col = next((c for c in df.columns if c in ("Activity", "ColumnNames")), None)
    ts_col  = next((c for c in df.columns if c in ("Timestamp", "ColumnValues")), None)

    if act_col is None or ts_col is None:
        return False

    # Confirm the activity column contains recognisable P2P activity names
    sample    = df[act_col].dropna().astype(str).head(1000)
    match_pct = sample.isin(_ALL_KNOWN_ACTIVITIES).mean()
    return match_pct >= 0.10


def _long_to_wide(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert KNIME long-format event log → wide format (one row per UniqueID_PO).

    Handles:
     - 'Activity'/'Timestamp' OR 'ColumnNames'/'ColumnValues' column names
     - KNIME activity name variants (GR Reversal → GR Reversal Date, etc.)
     - Activities that KNIME kept as id-columns (Invoice Posting, GR Reversal Date)
       — these already exist as real columns on every row; we deduplicate them
     - Activity cols that exist BOTH as rows (in Activity column) AND as id-cols
       → the pivot value wins (earliest date from the event rows); id-col is dropped
     - Duplicate (UniqueID_PO, Activity) pairs → keep earliest date (min)
    """
    df = df.copy()

    # Normalise activity / timestamp column names
    act_col = next((c for c in df.columns if c in ("Activity", "ColumnNames")), None)
    ts_col  = next((c for c in df.columns if c in ("Timestamp", "ColumnValues")), None)

    print(f"[P2P FORMAT] LONG format detected  "
          f"(act_col='{act_col}', ts_col='{ts_col}', rows={len(df)})")

    if act_col != "Activity":
        df = df.rename(columns={act_col: "Activity"})
    if ts_col != "Timestamp":
        df = df.rename(columns={ts_col: "Timestamp"})

    # Rename KNIME activity name variants to our internal names
    df["Activity"] = df["Activity"].map(
        lambda x: ACTIVITY_RENAME_MAP.get(str(x), x) if pd.notna(x) else x
    )

    df["Timestamp"] = pd.to_datetime(df["Timestamp"], errors="coerce")

    # Find the case-ID column (UniqueID_PO)
    case_col = "UniqueID_PO" if "UniqueID_PO" in df.columns else None
    if case_col is None:
        raise ValueError("Cannot convert long→wide: 'UniqueID_PO' column not found.")

    # Columns that are ALREADY wide (kept as id-cols by KNIME Unpivot).
    # e.g. Invoice Posting, GR Reversal Date, GR Creation User, Invoice Creation User.
    # We'll merge these back AFTER the pivot, only for cols the pivot didn't produce.
    already_wide_date_cols = [
        c for c in df.columns
        if c in set(ACTIVITY_COLUMNS) and c not in ("Activity", "Timestamp")
    ]

    # Columns to skip when building dim_agg (not dimension data)
    skip_in_dim = {"Activity", "Timestamp", "_ts", "Month", "Year", "Quarter"} | set(ACTIVITY_COLUMNS)
    dim_cols    = [c for c in df.columns if c not in skip_in_dim and c != case_col]

    # ── Step 1: Pivot the Activity rows → columns ──────────────────────────
    activity_df = (
        df[[case_col, "Activity", "Timestamp"]]
        .dropna(subset=["Timestamp"])
        .sort_values("Timestamp")
        .drop_duplicates(subset=[case_col, "Activity"], keep="first")   # earliest date
    )

    if activity_df.empty:
        raise ValueError("No valid Activity/Timestamp rows found after filtering.")

    pivoted = activity_df.pivot(
        index=case_col, columns="Activity", values="Timestamp"
    ).reset_index()
    pivoted.columns.name = None

    # Rename any remaining KNIME variants that survived the map above
    pivoted = pivoted.rename(columns=ACTIVITY_RENAME_MAP)

    # Keep only recognised activity columns
    known_acts = [c for c in ACTIVITY_COLUMNS if c in pivoted.columns]
    pivoted    = pivoted[[case_col] + known_acts]

    # ── Step 2: Re-attach dimension columns (one value per UniqueID_PO) ───
    if dim_cols:
        dim_agg = (
            df[[case_col] + dim_cols]
            .groupby(case_col, sort=False)
            .first()
            .reset_index()
        )
        wide = pivoted.merge(dim_agg, on=case_col, how="left")
    else:
        wide = pivoted

    # ── Step 3: Merge in already-wide date columns with coalesce ─────────────
    # KNIME id-cols (e.g. Invoice Posting, GR Reversal Date) appear as real columns
    # on every long-format row alongside the Activity/Timestamp rows.
    #
    # Coalesce strategy:
    #  - If pivot produced the column: fill its NaN gaps with the id-col value
    #  - If pivot did NOT produce the column: use the id-col value directly
    # This ensures Invoice Posting is never lost even when few Activity rows exist.
    if already_wide_date_cols:
        # Convert to datetime before aggregating
        for _c in already_wide_date_cols:
            df[_c] = pd.to_datetime(df[_c], errors="coerce")

        aw_agg = (
            df[[case_col] + already_wide_date_cols]
            .groupby(case_col, sort=False)
            .first()
            .reset_index()
        )
        # Use temp prefix to avoid merge collision with existing pivot columns
        aw_renamed = {c: f"__aw_{c}" for c in already_wide_date_cols}
        aw_agg = aw_agg.rename(columns=aw_renamed)
        wide = wide.merge(aw_agg, on=case_col, how="left")

        for _c in already_wide_date_cols:
            aw_col = f"__aw_{_c}"
            if _c in wide.columns:
                # Column exists from pivot — fill gaps with id-col value
                wide[_c] = wide[_c].fillna(wide[aw_col])
            else:
                # Column only in id-col — rename to final name
                wide = wide.rename(columns={aw_col: _c})
            # Always drop the temp column
            if aw_col in wide.columns:
                wide = wide.drop(columns=[aw_col])

    # ── Step 4: Drop the raw Timestamp column if it leaked into wide ───────
    # This prevents process_df from using the per-row Timestamp as _ts
    for drop_col in ("Timestamp", "Activity", "ColumnNames", "ColumnValues"):
        if drop_col in wide.columns:
            wide = wide.drop(columns=[drop_col])

    print(f"[P2P FORMAT] Wide: {len(wide)} rows × {len(wide.columns)} cols  "
          f"| activities: {[c for c in ACTIVITY_COLUMNS if c in wide.columns]}")
    return wide


# ─── Audit Logging ─────────────────────────────────────────────
AUDIT_FILE = "p2p_audit_logs.json"

def log_audit(username: str, action: str, details: str):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[P2P AUDIT] {timestamp} | {username} | {action} | {details}")
    logs = []
    if os.path.exists(AUDIT_FILE):
        try:
            with open(AUDIT_FILE, "r") as f:
                logs = json.load(f)
        except Exception:
            logs = []
    logs.append({"timestamp": timestamp, "username": username, "action": action, "details": details})
    try:
        with open(AUDIT_FILE, "w") as f:
            json.dump(logs, f, indent=4)
    except Exception as e:
        print(f"[P2P ERROR] Failed to write audit log to file: {e}")

class AuditAction(BaseModel):
    username: str
    action: str
    details: str

@router.post("/log")
def log_action(data: AuditAction):
    log_audit(data.username, data.action, data.details)
    return {"status": "logged"}

@router.get("/audit_logs")
def get_audit_logs():
    if os.path.exists(AUDIT_FILE):
        try:
            with open(AUDIT_FILE, "r") as f:
                logs = json.load(f)
                return list(reversed(logs))[:200]
        except Exception:
            pass
    return []

# ─── Column config ───────────────────────────────────────────────────────────
COL_CASE    = "UniqueID_PO"
COL_VENDOR  = "NAME1"
COL_COMPANY = "BUKRS"
COL_BSART   = "BSART"
COL_MATKL   = "MATKL"
COL_LIFNR   = "LIFNR"
COL_EKGRP   = "EKGRP"
COL_WERKS   = "WERKS"
COL_ERNAM   = "ERNAM"  

ACTIVITY_COLUMNS = [
    "PR Creation", "PR Release Date", "PR Reversal Date",
    "PO Creation", "PO Date", "PO Reversal Date",
    "GR Posting", "GR Reversal Date", "Invoice Posting", "Invoice Reversal Date",
]

MAIN_NODES = {"PR Creation", "PR Release Date", "PO Creation", "PO Date", "GR Posting", "Invoice Posting"}

def process_df(df: pd.DataFrame) -> pd.DataFrame:
    print(f"[P2P PROCESS] Parsing dates for {len(df)} rows. Outputting to terminal.")
    all_date_cols = list(dict.fromkeys(ACTIVITY_COLUMNS + ["Timestamp", "PO Date"]))
    for c in all_date_cols:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce")

    act_present = [c for c in ACTIVITY_COLUMNS if c in df.columns]

    # Priority for _ts (earliest activity date per case):
    # 1. Use ACTIVITY_COLUMNS if any are present — these are the correct wide-format dates
    # 2. Only fall back to the raw 'Timestamp' column if NO activity columns exist
    #    (raw Timestamp is a long-format artifact; if it leaked through, it holds only
    #    one activity's date per row which would give wrong Month/Year/Quarter values)
    if act_present:
        ts = df[act_present].apply(
            lambda row: row.dropna().min() if row.notna().any() else pd.NaT, axis=1)
    elif "Timestamp" in df.columns:
        ts = df["Timestamp"]
    else:
        ts = pd.Series([pd.NaT] * len(df))

    df["_ts"]     = ts
    df["Month"]   = df["_ts"].dt.to_period("M").astype(str).replace("NaT", pd.NA)
    df["Year"]    = df["_ts"].dt.year.astype("Int64").astype(str).replace("<NA>", pd.NA)
    df["Quarter"] = df["_ts"].dt.to_period("Q").astype(str).replace("NaT", pd.NA)
    return df

# ─── MULTI-USER FILE SYSTEM ──────────────────────────────────────────────────
USER_DFS = {} 
FILE_REGISTRY = "file_registry.json"
UPLOAD_DIR = "user_data"
os.makedirs(UPLOAD_DIR, exist_ok=True)

def load_registry():
    if os.path.exists(FILE_REGISTRY):
        try:
            with open(FILE_REGISTRY, "r") as f: return json.load(f)
        except Exception: pass
    return {}

def save_registry(reg):
    with open(FILE_REGISTRY, "w") as f: json.dump(reg, f, indent=4)

if os.path.exists("event_log.json"):
    try:
        USER_DFS["Unknown"] = process_df(pd.read_json("event_log.json", orient="records"))
        print("[P2P INFO] Legacy event_log.json loaded into memory.")
    except Exception: pass

def get_user_df(username: str) -> pd.DataFrame:
    if username in USER_DFS: return USER_DFS[username]
    if "Unknown" in USER_DFS: return USER_DFS["Unknown"]
    return pd.DataFrame()

@router.post("/clear")
def clear_data(username: str = Query("Unknown")):
    if username in USER_DFS: del USER_DFS[username]
    if "Unknown" in USER_DFS: del USER_DFS["Unknown"]
    print(f"[P2P INFO] Cleared active data for user: {username}. Terminal output confirmed.")
    return {"status": "cleared"}

# ─── MULTI-FILE ENDPOINTS ────────────────────────────────────────────────────
@router.get("/my_files")
def get_my_files(username: str = Query(...)):
    """Return all saved sessions — CSV uploads AND transformer builds."""
    reg = load_registry()
    files = reg.get(username, [])
    return list(reversed(files))


class LoadFileReq(BaseModel):
    username: str
    file_id: str


@router.post("/load_file")
def load_specific_file(req: LoadFileReq):
    if not os.path.exists(req.file_id):
        print(f"[P2P ERROR] File not found: {req.file_id}")
        raise HTTPException(404, "File not found on server")
    try:
        df = pd.read_json(req.file_id, orient="records")
        processed_df = process_df(df)
        USER_DFS[req.username] = processed_df
        USER_DFS["Unknown"] = processed_df
        print(f"[P2P INFO] {req.username} loaded file: {req.file_id}")
        log_audit(req.username, "LOAD_FILE", f"Loaded file: {req.file_id}")
        return {"status": "ok"}
    except Exception as e:
        print(f"[P2P ERROR] Failed to load file: {e}")
        raise HTTPException(500, f"Failed to load file: {str(e)}")


def register_transform_build(username: str, processed: pd.DataFrame,
                              save_path: str, csv_path: str = ""):
    """
    Register a transformer build in the shared file registry so it appears
    in the Previous Uploads list alongside CSV uploads.
    Called from p2ptransformer.py after a successful build.
    """
    cases = int(processed[COL_CASE].nunique()) if COL_CASE in processed.columns else 0
    reg = load_registry()
    if username not in reg:
        reg[username] = []
    reg[username].append({
        "file_id":     save_path,
        "filename":    f"Built Event Log ({datetime.now().strftime('%Y-%m-%d %H:%M')})",
        "upload_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "rows":        len(processed),
        "cases":       cases,
        "format":      "wide",
        "source":      "table_build",
        "csv_path":    csv_path,
    })
    save_registry(reg)
    print(f"[P2P REGISTRY] Registered transform build for '{username}': {cases} cases")


# ─── Download Output CSV ─────────────────────────────────────────────────────
@router.get("/download_output")
def download_output_csv(username: str = Query("Unknown")):
    """
    Stream the current in-memory wide-format dataframe as a downloadable CSV.
    Works for BOTH upload paths:
      - Pre-built CSV upload  → wide-format df stored in USER_DFS
      - Table upload + build  → transformer output stored in USER_DFS

    The CSV is also saved server-side to user_data/p2p_outputs/.
    """
    df = get_user_df(username)
    if df.empty:
        raise HTTPException(404, "No data loaded. Upload a file or build the event log first.")

    ts       = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_name = f"P2P_Output_{username}_{ts}.csv"

    # Save to server output directory
    try:
        _save_output_csv(df, csv_name)
    except Exception as e:
        print(f"[P2P DOWNLOAD] Server save failed (non-fatal): {e}")

    # Prepare export (drop internal cols, format dates)
    export_cols = [c for c in df.columns if not c.startswith("_")]
    export_df   = df[export_cols].copy()
    for c in export_df.columns:
        if pd.api.types.is_datetime64_any_dtype(export_df[c]):
            export_df[c] = export_df[c].dt.strftime("%Y-%m-%d")

    buf = io.StringIO()
    export_df.to_csv(buf, index=False)
    buf.seek(0)

    log_audit(username, "DOWNLOAD", f"Downloaded output CSV: {csv_name} ({len(export_df)} rows)")
    return StreamingResponse(
        iter([buf.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{csv_name}"'},
    )


# ─── Data Filtering ──────────────────────────────────────────────────────────
def filter_raw(df, company=None, bsart=None, matkl=None, vendor=None, plant=None, purch_group=None, 
               case_id=None, activity=None, month=None, year=None, quarter=None, lifnr=None, 
               lead_time=None, status=None, ernam=None, sod=None):
    
    if company     and company     != "ALL" and COL_COMPANY in df.columns: df = df[df[COL_COMPANY].astype(str) == company]
    if bsart       and bsart       != "ALL" and COL_BSART   in df.columns: df = df[df[COL_BSART].astype(str) == bsart]
    if matkl       and matkl       != "ALL" and COL_MATKL   in df.columns: df = df[df[COL_MATKL].astype(str) == matkl]
    if vendor      and vendor      != "ALL":
        # Filter on NAME1 when it has data, otherwise fall back to LIFNR
        if COL_VENDOR in df.columns and df[COL_VENDOR].notna().any():
            df = df[df[COL_VENDOR].astype(str) == vendor]
        elif COL_LIFNR in df.columns:
            df = df[df[COL_LIFNR].astype(str) == vendor]
    if plant       and plant       != "ALL" and COL_WERKS   in df.columns: df = df[df[COL_WERKS].astype(str) == plant]
    if purch_group and purch_group != "ALL" and COL_EKGRP   in df.columns: df = df[df[COL_EKGRP].astype(str) == purch_group]
    if case_id     and case_id     != "ALL" and COL_CASE    in df.columns: df = df[df[COL_CASE].astype(str) == case_id]
    if lifnr       and lifnr       != "ALL" and COL_LIFNR   in df.columns: df = df[df[COL_LIFNR].astype(str) == lifnr]
    if ernam       and ernam       != "ALL" and COL_ERNAM   in df.columns: df = df[df[COL_ERNAM].astype(str) == ernam]
    if activity    and activity    != "ALL" and activity in df.columns:    df = df[df[activity].notna()]
    if month       and month       != "ALL" and "Month"     in df.columns: df = df[df["Month"].astype(str) == month]
    if year        and year        != "ALL" and "Year"      in df.columns: df = df[df["Year"].astype(str) == year]
    if quarter     and quarter     != "ALL" and "Quarter"   in df.columns: df = df[df["Quarter"].astype(str) == quarter]

    if lead_time and "PO Creation" in df.columns and "GR Posting" in df.columns:
        try:
            df["_lt"] = (df["GR Posting"] - df["PO Creation"]).dt.days
            nums = re.findall(r"\d+", str(lead_time))
            if len(nums) == 2: df = df[df["_lt"].between(int(nums[0]), int(nums[1]), inclusive='left')] 
            elif len(nums) == 1: df = df[df["_lt"] >= int(nums[0])]
        except Exception: pass 

    if status and status != "ALL":
        req = ["PR Creation", "PO Creation", "GR Posting", "Invoice Posting"]
        rev = ["PR Reversal Date", "PO Reversal Date", "GR Reversal Date", "Invoice Reversal Date"]
        has_all = pd.Series([True]*len(df), index=df.index)
        is_seq = pd.Series([True]*len(df), index=df.index)
        prev_col = None
        for c in req:
            if c in df.columns:
                has_all = has_all & df[c].notna()
                if prev_col:
                    is_seq = is_seq & (df[c] >= df[prev_col]).fillna(False)
                prev_col = c
            else:
                has_all = pd.Series([False]*len(df), index=df.index)
                is_seq = pd.Series([False]*len(df), index=df.index)
                break
        has_no_rev = pd.Series([True]*len(df), index=df.index)
        for c in rev:
            if c in df.columns: has_no_rev = has_no_rev & df[c].isna()
        happy_mask = has_all & is_seq & has_no_rev
        if status == "Happy Path": df = df[happy_mask]
        elif status == "Deviations": df = df[~happy_mask]

    SOD_MAP = {
        "PO Maker = GR Checker":       ("PO Creation", "ERNAM", "GR Posting", "GR Creation User"),
        "PO Maker = Invoice Checker":  ("PO Creation", "ERNAM", "Invoice Posting", "Invoice Creation User"),
        "GR Checker = Invoice Checker":("GR Posting", "GR Creation User", "Invoice Posting", "Invoice Creation User"),
        "PR Maker = PO Maker":         ("PR Creation", "ERNAM (EBAN)", "PO Creation", "ERNAM"),
        "PR Maker = GR Checker":       ("PR Creation", "ERNAM (EBAN)", "GR Posting", "GR Creation User"),
    }
    if sod and sod != "ALL" and sod in SOD_MAP and COL_CASE in df.columns:
        date_a, user_a, date_b, user_b = SOD_MAP[sod]
        needed = [date_a, user_a, date_b, user_b]
        if all(c in df.columns for c in needed):
            sub = df.dropna(subset=needed).copy()
            sub["_ua"] = sub[user_a].astype(str).str.strip().str.upper()
            sub["_ub"] = sub[user_b].astype(str).str.strip().str.upper()
            violation_cases = sub[sub["_ua"] == sub["_ub"]][COL_CASE].unique()
            df = df[df[COL_CASE].isin(violation_cases)]

    return df

def col_unique_cases(df, col):
    if col not in df.columns or COL_CASE not in df.columns: return 0
    return int(df.loc[df[col].notna(), COL_CASE].nunique())

def unique_cases(df):
    return int(df[COL_CASE].nunique()) if COL_CASE in df.columns else len(df)

def fp(company, bsart, matkl, vendor, plant, purch_group, case_id,
       activity, month, year=None, quarter=None, lifnr=None, lead_time=None, status=None, ernam=None, sod=None):
    return dict(company=company, bsart=bsart, matkl=matkl, vendor=vendor, plant=plant, purch_group=purch_group, 
                case_id=case_id, activity=activity, month=month, year=year, quarter=quarter, 
                lifnr=lifnr, lead_time=lead_time, status=status, ernam=ernam, sod=sod)

@router.get("/")
def p2p_root():
    df = get_user_df("Unknown")
    return {"status": "P2P Sub-module active", "rows": len(df), "data_loaded": not df.empty, "columns": list(df.columns) if not df.empty else []}

@router.post("/upload")
async def upload_csv(file: UploadFile = File(...), username: str = Form("Unknown"), column_mapping: str = Form("{}")):
    if not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV files are supported")
    content = await file.read()
    try:
        # ── Parse CSV (try UTF-8 first, then latin-1) ──────────────────────
        for enc in ("utf-8", "latin-1", "windows-1252"):
            try:
                df = pd.read_csv(io.BytesIO(content), encoding=enc, low_memory=False)
                break
            except (UnicodeDecodeError, Exception):
                continue
        else:
            raise ValueError("Could not decode CSV with any supported encoding.")

        try:
            import json
            mapping = json.loads(column_mapping)
            if mapping:
                df = df.rename(columns=mapping)
                print(f"Applied column mapping for prebuilt CSV: {mapping}")
        except Exception as e:
            print(f"Error applying column mapping: {e}")

        # ── Detect format and convert long → wide if needed ─────────────────
        input_format = "long" if _is_long_format(df) else "wide"
        print(f"[P2P UPLOAD] Detected format: {input_format.upper()} | rows: {len(df)} | cols: {len(df.columns)}")

        if input_format == "long":
            df = _long_to_wide(df)
            # Safety check: if rows still > unique POs, the pivot produced duplicates
            # (can happen if df had duplicate (UniqueID_PO, Activity) with different dates)
            if COL_CASE in df.columns:
                unique_po = df[COL_CASE].nunique()
                if len(df) > unique_po:
                    print(f"[P2P FORMAT] Safety dedup: {len(df)} rows → {unique_po} unique POs")
                    act_cols_present = [c for c in ACTIVITY_COLUMNS if c in df.columns]
                    dim_cols_present = [c for c in df.columns if c not in act_cols_present
                                        and not c.startswith("_") and c != COL_CASE]
                    agg = {c: "first" for c in dim_cols_present}
                    for c in act_cols_present:
                        agg[c] = "first"
                    df = df.groupby(COL_CASE, sort=False).agg(agg).reset_index()
                    print(f"[P2P FORMAT] After dedup: {len(df)} rows")

        # ── Parse dates / add Month, Year, Quarter ───────────────────────────
        df = process_df(df)

        # ── Save JSON session file ───────────────────────────────────────────
        user_dir  = os.path.join(UPLOAD_DIR, username)
        os.makedirs(user_dir, exist_ok=True)
        file_id   = datetime.now().strftime("%Y%m%d_%H%M%S")
        safe_name = "".join(c for c in file.filename if c.isalnum() or c in "._- ")
        save_path = os.path.join(user_dir, f"{file_id}_{safe_name}.json")
        try:
            df.to_json(save_path, orient="records", date_format="iso")
        except Exception as e:
            print(f"[P2P ERROR] JSON save failed: {e}")

        # ── Save processed wide CSV to server output directory ───────────────
        csv_name = f"P2P_Upload_{username}_{file_id}.csv"
        csv_path = ""
        try:
            csv_path = _save_output_csv(df, csv_name)
        except Exception as e:
            print(f"[P2P ERROR] CSV output save failed: {e}")

        # ── Register file ────────────────────────────────────────────────────
        cases = int(df[COL_CASE].nunique()) if COL_CASE in df.columns else 0
        reg = load_registry()
        if username not in reg: reg[username] = []
        reg[username].append({
            "file_id":     save_path,
            "filename":    file.filename,
            "upload_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "rows":        len(df),
            "cases":       cases,
            "format":      input_format,
            "source":      "csv_upload",
            "csv_path":    csv_path,
        })
        save_registry(reg)

        USER_DFS[username] = df
        USER_DFS["Unknown"] = df

        print(f"[P2P INFO] Saved to {save_path} | user={username} | format={input_format} | cases={cases}")
        log_audit(username, "UPLOAD",
                  f"Uploaded {file.filename} ({input_format} format, {cases} unique cases)")
        return {
            "status":       "ok",
            "rows":         len(df),
            "columns":      list(df.columns),
            "unique_cases": cases,
            "input_format": input_format,
        }
    except Exception as e:
        import traceback
        print(f"[P2P ERROR] Upload failed: {e}\n{traceback.format_exc()}")
        log_audit(username, "ERROR", f"Failed to upload {file.filename}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to parse CSV: {e}")

@router.get("/filters")
def get_filters(
    username: str = Query("Unknown"), company: Optional[str]=Query(None), bsart: Optional[str]=Query(None), matkl: Optional[str]=Query(None), 
    vendor: Optional[str]=Query(None), plant: Optional[str]=Query(None), purch_group: Optional[str]=Query(None),
    case_id: Optional[str]=Query(None), activity: Optional[str]=Query(None), month: Optional[str]=Query(None), 
    year: Optional[str]=Query(None), quarter: Optional[str]=Query(None), lifnr: Optional[str]=Query(None),
    lead_time: Optional[str]=Query(None), status: Optional[str]=Query(None), ernam: Optional[str]=Query(None), sod: Optional[str]=Query(None)
):
    df_raw = get_user_df(username)
    if df_raw.empty: raise HTTPException(500, "Data not loaded")
    d = filter_raw(df_raw.copy(), **fp(company,bsart,matkl,vendor,plant,purch_group,case_id,activity,month,year,quarter,lifnr,lead_time,status,ernam,sod=sod))
    def uniq(col): return ["ALL"] + sorted(d[col].dropna().astype(str).unique().tolist()) if col in d.columns else ["ALL"]
    # Vendor filter: prefer NAME1, fall back to LIFNR if NAME1 is all-null
    vendor_col = COL_VENDOR if (COL_VENDOR in d.columns and d[COL_VENDOR].notna().any()) else COL_LIFNR
    return {
        "companies": uniq(COL_COMPANY), "bsarts": uniq(COL_BSART), "matkls": uniq(COL_MATKL),
        "vendors": ["ALL"] + d[vendor_col].dropna().value_counts().index.tolist() if vendor_col in d.columns else ["ALL"],
        "plants": uniq(COL_WERKS), "purch_groups": uniq(COL_EKGRP), "lifnrs": uniq(COL_LIFNR),
        "case_ids": ["ALL"] + sorted(d[COL_CASE].dropna().astype(str).unique().tolist()) if COL_CASE in d.columns else ["ALL"],
        "months": ["ALL"] + sorted(d["Month"].dropna().astype(str).unique().tolist()) if "Month" in d.columns else ["ALL"],
        "years": ["ALL"] + sorted(d["Year"].dropna().astype(str).unique().tolist()) if "Year" in d.columns else ["ALL"]
    }

@router.get("/kpis")
def get_kpis(
    username: str = Query("Unknown"), company: Optional[str]=Query(None), bsart: Optional[str]=Query(None), matkl: Optional[str]=Query(None), 
    vendor: Optional[str]=Query(None), plant: Optional[str]=Query(None), purch_group: Optional[str]=Query(None),
    case_id: Optional[str]=Query(None), activity: Optional[str]=Query(None), month: Optional[str]=Query(None), 
    year: Optional[str]=Query(None), quarter: Optional[str]=Query(None), lifnr: Optional[str]=Query(None),
    lead_time: Optional[str]=Query(None), status: Optional[str]=Query(None), ernam: Optional[str]=Query(None), sod: Optional[str]=Query(None)
):
    df_raw = get_user_df(username)
    if df_raw.empty: raise HTTPException(500, "Data not loaded")
    d = filter_raw(df_raw.copy(), **fp(company,bsart,matkl,vendor,plant,purch_group,case_id,activity,month,year,quarter,lifnr,lead_time,status,ernam,sod=sod))

    po_without_pr, pr_rev_after_po, po_rev_after_gr, gr_no_invoice, inv_no_gr = 0,0,0,0,0
    if "PO Creation" in d.columns and "PR Creation" in d.columns: po_without_pr = int((d["PO Creation"].notna() & d["PR Creation"].isna()).sum())
    if "PR Reversal Date" in d.columns and "PO Date" in d.columns: pr_rev_after_po = int((d.dropna(subset=["PR Reversal Date","PO Date"])["PR Reversal Date"] > d.dropna(subset=["PR Reversal Date","PO Date"])["PO Date"]).sum())
    if "PO Reversal Date" in d.columns and "GR Posting" in d.columns: po_rev_after_gr = int((d.dropna(subset=["PO Reversal Date","GR Posting"])["PO Reversal Date"] > d.dropna(subset=["PO Reversal Date","GR Posting"])["GR Posting"]).sum())
    if "GR Posting" in d.columns and "Invoice Posting" in d.columns:
        gr_no_invoice = int((d["GR Posting"].notna() & d["Invoice Posting"].isna()).sum())
        inv_no_gr     = int((d["Invoice Posting"].notna() & d["GR Posting"].isna()).sum())

    avg_days = 0.0
    if COL_CASE in d.columns:
        time_cols = [c for c in ACTIVITY_COLUMNS if c in d.columns]
        if time_cols:
            d_times = d[time_cols]
            durations = (d_times.max(axis=1) - d_times.min(axis=1)).dt.total_seconds() / 86400
            if not durations.empty: avg_days = round(durations.mean(), 1)

    return {
        "total_cases": unique_cases(d), "po_created": col_unique_cases(d, "PO Creation"),
        "gr_postings": col_unique_cases(d, "GR Posting"), "invoices_posted": col_unique_cases(d, "Invoice Posting"),
        "reversals": sum(col_unique_cases(d, c) for c in ["PO Reversal Date","PR Reversal Date","GR Reversal Date","Invoice Reversal Date"]),
        "unique_vendors": int(d[COL_VENDOR].dropna().nunique()) if (COL_VENDOR in d.columns and d[COL_VENDOR].notna().any()) else (int(d[COL_LIFNR].dropna().nunique()) if COL_LIFNR in d.columns else 0),
        "po_without_pr": po_without_pr, "pr_rev_after_po": pr_rev_after_po, "po_rev_after_gr": po_rev_after_gr,
        "gr_no_invoice": gr_no_invoice, "inv_no_gr": inv_no_gr, "avg_completion_days": avg_days,
    }

@router.get("/cases")
def get_cases(
    username: str = Query("Unknown"), company: Optional[str]=Query(None), bsart: Optional[str]=Query(None), matkl: Optional[str]=Query(None), 
    vendor: Optional[str]=Query(None), plant: Optional[str]=Query(None), purch_group: Optional[str]=Query(None),
    case_id: Optional[str]=Query(None), activity: Optional[str]=Query(None), month: Optional[str]=Query(None), 
    year: Optional[str]=Query(None), quarter: Optional[str]=Query(None), lifnr: Optional[str]=Query(None),
    lead_time: Optional[str]=Query(None), status: Optional[str]=Query(None), ernam: Optional[str]=Query(None), sod: Optional[str]=Query(None)
):
    df_raw = get_user_df(username)
    if df_raw.empty: raise HTTPException(500, "Data not loaded")
    d = filter_raw(df_raw.copy(), **fp(company,bsart,matkl,vendor,plant,purch_group,case_id,activity,month,year,quarter,lifnr,lead_time,status,ernam,sod=sod))
    if COL_CASE not in d.columns: return []
    time_cols = [c for c in ACTIVITY_COLUMNS if c in d.columns]
    if not time_cols: return []
    d["start_date"] = d[time_cols].min(axis=1).dt.strftime("%Y-%m-%d")
    d["end_date"]   = d[time_cols].max(axis=1).dt.strftime("%Y-%m-%d")
    res = d[[COL_CASE, "start_date", "end_date"]].dropna(subset=[COL_CASE]).rename(columns={COL_CASE: "case_id"})
    return res.sort_values("start_date", ascending=False).head(200).to_dict("records")

@router.get("/case_events")
def get_case_events(case_id: str = Query(...), username: str = Query("Unknown")):
    df_raw = get_user_df(username)
    if df_raw.empty or case_id == "ALL": return []
    d = df_raw[df_raw[COL_CASE].astype(str) == case_id].copy()
    if d.empty: return []
    
    cols_to_melt = [c for c in ACTIVITY_COLUMNS if c in d.columns]
    if not cols_to_melt: return []
    
    has_ernam = COL_ERNAM in d.columns
    id_vars = [COL_CASE]
    if has_ernam: id_vars.append(COL_ERNAM)
    
    melted = d.melt(id_vars=id_vars, value_vars=cols_to_melt, var_name="Activity", value_name="Timestamp")
    melted = melted.dropna(subset=["Timestamp"]).drop_duplicates(subset=["Activity", "Timestamp"]).sort_values("Timestamp")
    
    res = melted[["Activity", "Timestamp"]].copy()
    res["User"] = melted[COL_ERNAM] if has_ernam else "Unknown"
    res["Timestamp"] = res["Timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")
    return res.to_dict("records")

@router.get("/charts/activity")
def chart_activity(
    username: str = Query("Unknown"), company: Optional[str]=Query(None), bsart: Optional[str]=Query(None), matkl: Optional[str]=Query(None), 
    vendor: Optional[str]=Query(None), plant: Optional[str]=Query(None), purch_group: Optional[str]=Query(None),
    case_id: Optional[str]=Query(None), activity: Optional[str]=Query(None), month: Optional[str]=Query(None), 
    year: Optional[str]=Query(None), quarter: Optional[str]=Query(None), lifnr: Optional[str]=Query(None),
    lead_time: Optional[str]=Query(None), status: Optional[str]=Query(None), ernam: Optional[str]=Query(None), sod: Optional[str]=Query(None)
):
    df_raw = get_user_df(username)
    if df_raw.empty: raise HTTPException(500, "Data not loaded")
    d = filter_raw(df_raw.copy(), **fp(company,bsart,matkl,vendor,plant,purch_group,case_id,activity,month,year,quarter,lifnr,lead_time,status,ernam,sod=sod))
    results = []
    for col in ACTIVITY_COLUMNS:
        if col not in d.columns: continue
        occ = int(d[col].notna().sum())
        if occ > 0: results.append({"activity": col, "count": occ, "unique_cases": col_unique_cases(d, col)})
    return sorted(results, key=lambda x: x["count"], reverse=True)

@router.get("/charts/monthly")
def chart_monthly(
    username: str = Query("Unknown"), company: Optional[str]=Query(None), bsart: Optional[str]=Query(None), matkl: Optional[str]=Query(None), 
    vendor: Optional[str]=Query(None), plant: Optional[str]=Query(None), purch_group: Optional[str]=Query(None),
    case_id: Optional[str]=Query(None), activity: Optional[str]=Query(None), month: Optional[str]=Query(None), 
    year: Optional[str]=Query(None), quarter: Optional[str]=Query(None), lifnr: Optional[str]=Query(None),
    lead_time: Optional[str]=Query(None), status: Optional[str]=Query(None), ernam: Optional[str]=Query(None), sod: Optional[str]=Query(None)
):
    df_raw = get_user_df(username)
    if df_raw.empty: raise HTTPException(500, "Data not loaded")
    d = filter_raw(df_raw.copy(), **fp(company,bsart,matkl,vendor,plant,purch_group,case_id,activity,month,year,quarter,lifnr,lead_time,status,ernam,sod=sod))
    cols_to_melt = [c for c in ACTIVITY_COLUMNS if c in d.columns]
    if not cols_to_melt: return []
    melted = d.melt(id_vars=[COL_CASE], value_vars=cols_to_melt, value_name="Date").dropna(subset=["Date"])
    melted["Month"] = melted["Date"].dt.to_period("M").astype(str)
    res = melted.groupby("Month")[COL_CASE].nunique().reset_index()
    res.columns = ["Month", "count"]
    return res.sort_values("Month").to_dict("records")

@router.get("/charts/ernam")
def chart_ernam(
    username: str = Query("Unknown"), company: Optional[str]=Query(None), bsart: Optional[str]=Query(None), matkl: Optional[str]=Query(None), 
    vendor: Optional[str]=Query(None), plant: Optional[str]=Query(None), purch_group: Optional[str]=Query(None),
    case_id: Optional[str]=Query(None), activity: Optional[str]=Query(None), month: Optional[str]=Query(None), 
    year: Optional[str]=Query(None), quarter: Optional[str]=Query(None), lifnr: Optional[str]=Query(None),
    lead_time: Optional[str]=Query(None), status: Optional[str]=Query(None), ernam: Optional[str]=Query(None), sod: Optional[str]=Query(None)
):
    df_raw = get_user_df(username)
    if df_raw.empty: raise HTTPException(500, "Data not loaded")
    d = filter_raw(df_raw.copy(), **fp(company,bsart,matkl,vendor,plant,purch_group,case_id,activity,month,year,quarter,lifnr,lead_time,status,ernam,sod=sod))
    if COL_ERNAM not in d.columns: return []
    vc = d.dropna(subset=[COL_ERNAM]).groupby(COL_ERNAM)[COL_CASE].nunique().reset_index()
    vc.columns = ["ernam","count"]
    return vc.sort_values("count", ascending=False).head(20).to_dict("records")

@router.get("/charts/company")
def chart_company(
    username: str = Query("Unknown"), company: Optional[str]=Query(None), bsart: Optional[str]=Query(None), matkl: Optional[str]=Query(None), 
    vendor: Optional[str]=Query(None), plant: Optional[str]=Query(None), purch_group: Optional[str]=Query(None),
    case_id: Optional[str]=Query(None), activity: Optional[str]=Query(None), month: Optional[str]=Query(None), 
    year: Optional[str]=Query(None), quarter: Optional[str]=Query(None), lifnr: Optional[str]=Query(None),
    lead_time: Optional[str]=Query(None), status: Optional[str]=Query(None), ernam: Optional[str]=Query(None), sod: Optional[str]=Query(None)
):
    df_raw = get_user_df(username)
    if df_raw.empty: raise HTTPException(500, "Data not loaded")
    d = filter_raw(df_raw.copy(), **fp(company,bsart,matkl,vendor,plant,purch_group,case_id,activity,month,year,quarter,lifnr,lead_time,status,ernam,sod=sod))
    if COL_COMPANY not in d.columns: return []
    vc = d.dropna(subset=[COL_COMPANY]).groupby(COL_COMPANY)[COL_CASE].nunique().reset_index()
    vc.columns = ["company","count"]
    return vc.sort_values("count", ascending=False).to_dict("records")

@router.get("/charts/bsart")
def chart_bsart(
    username: str = Query("Unknown"), company: Optional[str]=Query(None), matkl: Optional[str]=Query(None), vendor: Optional[str]=Query(None),  
    plant: Optional[str]=Query(None), purch_group: Optional[str]=Query(None), case_id: Optional[str]=Query(None),
    activity: Optional[str]=Query(None), month: Optional[str]=Query(None), year: Optional[str]=Query(None),    
    quarter: Optional[str]=Query(None), lifnr: Optional[str]=Query(None), lead_time: Optional[str]=Query(None), status: Optional[str]=Query(None), ernam: Optional[str]=Query(None), sod: Optional[str]=Query(None)
):
    df_raw = get_user_df(username)
    if df_raw.empty: raise HTTPException(500, "Data not loaded")
    d = filter_raw(df_raw.copy(), **fp(company,None,matkl,vendor,plant,purch_group,None,activity,month,year,quarter,lifnr,lead_time,status,ernam,sod=sod))
    if COL_BSART not in d.columns: return []
    vc = d.dropna(subset=[COL_BSART]).groupby(COL_BSART)[COL_CASE].nunique().reset_index()
    vc.columns = ["bsart","count"]
    return vc.sort_values("count", ascending=False).to_dict("records")

@router.get("/charts/matkl")
def chart_matkl(
    username: str = Query("Unknown"), company: Optional[str]=Query(None), bsart: Optional[str]=Query(None), vendor: Optional[str]=Query(None),  
    plant: Optional[str]=Query(None), purch_group: Optional[str]=Query(None), case_id: Optional[str]=Query(None),
    activity: Optional[str]=Query(None), month: Optional[str]=Query(None), year: Optional[str]=Query(None),    
    quarter: Optional[str]=Query(None), lifnr: Optional[str]=Query(None), lead_time: Optional[str]=Query(None), status: Optional[str]=Query(None), ernam: Optional[str]=Query(None), sod: Optional[str]=Query(None)
):
    df_raw = get_user_df(username)
    if df_raw.empty: raise HTTPException(500, "Data not loaded")
    d = filter_raw(df_raw.copy(), **fp(company,bsart,None,vendor,plant,purch_group,None,activity,month,year,quarter,lifnr,lead_time,status,ernam,sod=sod))
    if COL_MATKL not in d.columns: return []
    vc = d.dropna(subset=[COL_MATKL]).groupby(COL_MATKL)[COL_CASE].nunique().reset_index()
    vc.columns = ["matkl","count"]
    return vc.sort_values("count", ascending=False).to_dict("records")

@router.get("/charts/vendors")
def chart_vendors(
    username: str = Query("Unknown"), company: Optional[str]=Query(None), bsart: Optional[str]=Query(None), matkl: Optional[str]=Query(None),   
    plant: Optional[str]=Query(None), purch_group: Optional[str]=Query(None), case_id: Optional[str]=Query(None),
    activity: Optional[str]=Query(None), month: Optional[str]=Query(None), year: Optional[str]=Query(None),    
    quarter: Optional[str]=Query(None), lifnr: Optional[str]=Query(None), lead_time: Optional[str]=Query(None), status: Optional[str]=Query(None), ernam: Optional[str]=Query(None), sod: Optional[str]=Query(None)
):
    df_raw = get_user_df(username)
    if df_raw.empty: raise HTTPException(500, "Data not loaded")
    d = filter_raw(df_raw.copy(), **fp(company,bsart,matkl,None,plant,purch_group,None,activity,month,year,quarter,lifnr,lead_time,status,ernam,sod=sod))
    # Use NAME1 (vendor name) when available and populated; fall back to LIFNR (vendor ID)
    name_col = None
    if COL_VENDOR in d.columns and d[COL_VENDOR].notna().any():
        name_col = COL_VENDOR
    elif COL_LIFNR in d.columns:
        name_col = COL_LIFNR
    if name_col is None: return []
    vc = d.dropna(subset=[name_col]).groupby(name_col)[COL_CASE].nunique().reset_index()
    vc.columns = ["vendor", "count"]
    return vc.sort_values("count", ascending=False).head(30).to_dict("records")

@router.get("/charts/leadtime")
def chart_leadtime(
    username: str = Query("Unknown"), company: Optional[str]=Query(None), bsart: Optional[str]=Query(None), matkl: Optional[str]=Query(None), 
    vendor: Optional[str]=Query(None), plant: Optional[str]=Query(None), purch_group: Optional[str]=Query(None),
    case_id: Optional[str]=Query(None), activity: Optional[str]=Query(None), month: Optional[str]=Query(None), 
    year: Optional[str]=Query(None), quarter: Optional[str]=Query(None), lifnr: Optional[str]=Query(None),
    lead_time: Optional[str]=Query(None), status: Optional[str]=Query(None), ernam: Optional[str]=Query(None), sod: Optional[str]=Query(None)
):
    df_raw = get_user_df(username)
    if df_raw.empty: raise HTTPException(500, "Data not loaded")
    d = filter_raw(df_raw.copy(), **fp(company,bsart,matkl,vendor,plant,purch_group,case_id,activity,month,year,quarter,lifnr,lead_time,status,ernam,sod=sod))
    if "PO Creation" not in d.columns or "GR Posting" not in d.columns: return []
    lt = d.dropna(subset=["PO Creation","GR Posting"]).copy()
    lt["days"] = (lt["GR Posting"] - lt["PO Creation"]).dt.days
    lt = lt[lt["days"].between(0, 365)]
    if lt.empty: return []
    lt["bucket"] = pd.cut(lt["days"], bins=list(range(0,370,10)), right=False)
    hist = lt.groupby("bucket", observed=True).size().reset_index(name="count")
    hist["label"] = hist["bucket"].astype(str)
    return hist[hist["count"]>0][["label","count"]].to_dict("records")

@router.get("/charts/po_rev_ernam")
def chart_po_rev_ernam(
    username: str = Query("Unknown"), company: Optional[str]=Query(None), bsart: Optional[str]=Query(None), matkl: Optional[str]=Query(None), 
    vendor: Optional[str]=Query(None), plant: Optional[str]=Query(None), purch_group: Optional[str]=Query(None),
    case_id: Optional[str]=Query(None), activity: Optional[str]=Query(None), month: Optional[str]=Query(None), 
    year: Optional[str]=Query(None), quarter: Optional[str]=Query(None), lifnr: Optional[str]=Query(None),
    lead_time: Optional[str]=Query(None), status: Optional[str]=Query(None), ernam: Optional[str]=Query(None), sod: Optional[str]=Query(None)
):
    df_raw = get_user_df(username)
    if df_raw.empty: raise HTTPException(500, "Data not loaded")
    d = filter_raw(df_raw.copy(), **fp(company,bsart,matkl,vendor,plant,purch_group,case_id,activity,month,year,quarter,lifnr,lead_time,status,ernam,sod=sod))
    if "PO Reversal Date" not in d.columns or COL_ERNAM not in d.columns: return []
    b = d.dropna(subset=["PO Reversal Date", COL_ERNAM])
    vc = b.groupby(COL_ERNAM)[COL_CASE].nunique().reset_index(name="count")
    if not vc.empty: vc.columns = ["ernam", "count"]
    return vc.sort_values("count", ascending=False).head(20).to_dict("records")

@router.get("/charts/po_rev_timeline")
def chart_po_rev_timeline(
    username: str = Query("Unknown"), company: Optional[str]=Query(None), bsart: Optional[str]=Query(None), matkl: Optional[str]=Query(None), 
    vendor: Optional[str]=Query(None), plant: Optional[str]=Query(None), purch_group: Optional[str]=Query(None),
    case_id: Optional[str]=Query(None), activity: Optional[str]=Query(None), month: Optional[str]=Query(None), 
    year: Optional[str]=Query(None), quarter: Optional[str]=Query(None), lifnr: Optional[str]=Query(None),
    lead_time: Optional[str]=Query(None), status: Optional[str]=Query(None), ernam: Optional[str]=Query(None), sod: Optional[str]=Query(None)
):
    df_raw = get_user_df(username)
    if df_raw.empty: raise HTTPException(500, "Data not loaded")
    d = filter_raw(df_raw.copy(), **fp(company,bsart,matkl,vendor,plant,purch_group,case_id,activity,month,year,quarter,lifnr,lead_time,status,ernam,sod=sod))
    if "PO Reversal Date" not in d.columns: return []
    b = d.dropna(subset=["PO Reversal Date"]).copy()
    b["Month"] = b["PO Reversal Date"].dt.to_period("M").astype(str)
    res = b.groupby("Month")[COL_CASE].nunique().reset_index(name="count")
    return res.sort_values("Month").to_dict("records")

@router.get("/charts/pr_rev_after_po_ernam")
def chart_pr_rev_after_po_ernam(
    username: str = Query("Unknown"), company: Optional[str]=Query(None), bsart: Optional[str]=Query(None), matkl: Optional[str]=Query(None), 
    vendor: Optional[str]=Query(None), plant: Optional[str]=Query(None), purch_group: Optional[str]=Query(None),
    case_id: Optional[str]=Query(None), activity: Optional[str]=Query(None), month: Optional[str]=Query(None), 
    year: Optional[str]=Query(None), quarter: Optional[str]=Query(None), lifnr: Optional[str]=Query(None),
    lead_time: Optional[str]=Query(None), status: Optional[str]=Query(None), ernam: Optional[str]=Query(None), sod: Optional[str]=Query(None)
):
    df_raw = get_user_df(username)
    if df_raw.empty: raise HTTPException(500, "Data not loaded")
    d = filter_raw(df_raw.copy(), **fp(company,bsart,matkl,vendor,plant,purch_group,case_id,activity,month,year,quarter,lifnr,lead_time,status,ernam,sod=sod))
    if "PR Reversal Date" not in d.columns or "PO Date" not in d.columns or COL_ERNAM not in d.columns: return []
    b = d.dropna(subset=["PR Reversal Date", "PO Date", COL_ERNAM])
    b = b[b["PR Reversal Date"] > b["PO Date"]]
    vc = b.groupby(COL_ERNAM)[COL_CASE].nunique().reset_index(name="count")
    if not vc.empty: vc.columns = ["ernam", "count"]
    return vc.sort_values("count", ascending=False).head(20).to_dict("records")

@router.get("/charts/seq_violation_ernam")
def chart_seq_violation_ernam(
    username: str = Query("Unknown"), company: Optional[str]=Query(None), bsart: Optional[str]=Query(None), matkl: Optional[str]=Query(None), 
    vendor: Optional[str]=Query(None), plant: Optional[str]=Query(None), purch_group: Optional[str]=Query(None),
    case_id: Optional[str]=Query(None), activity: Optional[str]=Query(None), month: Optional[str]=Query(None), 
    year: Optional[str]=Query(None), quarter: Optional[str]=Query(None), lifnr: Optional[str]=Query(None),
    lead_time: Optional[str]=Query(None), status: Optional[str]=Query(None), ernam: Optional[str]=Query(None), sod: Optional[str]=Query(None)
):
    df_raw = get_user_df(username)
    if df_raw.empty: raise HTTPException(500, "Data not loaded")
    d = filter_raw(df_raw.copy(), **fp(company,bsart,matkl,vendor,plant,purch_group,case_id,activity,month,year,quarter,lifnr,lead_time,status,ernam,sod=sod))
    if "PO Date" not in d.columns or COL_ERNAM not in d.columns: return []
    
    mask_gr = (d["GR Posting"].notna()) & (d["PO Date"] > d["GR Posting"]) if "GR Posting" in d.columns else False
    mask_inv = (d["Invoice Posting"].notna()) & (d["PO Date"] > d["Invoice Posting"]) if "Invoice Posting" in d.columns else False
    
    b = d[mask_gr | mask_inv].dropna(subset=[COL_ERNAM])
    vc = b.groupby(COL_ERNAM)[COL_CASE].nunique().reset_index(name="count")
    if not vc.empty: vc.columns = ["ernam", "count"]
    return vc.sort_values("count", ascending=False).head(20).to_dict("records")

@router.get("/charts/happy_path")
def chart_happy_path(
    username: str = Query("Unknown"), company: Optional[str]=Query(None), bsart: Optional[str]=Query(None), matkl: Optional[str]=Query(None), 
    vendor: Optional[str]=Query(None), plant: Optional[str]=Query(None), purch_group: Optional[str]=Query(None),
    case_id: Optional[str]=Query(None), activity: Optional[str]=Query(None), month: Optional[str]=Query(None), 
    year: Optional[str]=Query(None), quarter: Optional[str]=Query(None), lifnr: Optional[str]=Query(None),
    lead_time: Optional[str]=Query(None), status: Optional[str]=Query(None), ernam: Optional[str]=Query(None), sod: Optional[str]=Query(None)
):
    df_raw = get_user_df(username)
    if df_raw.empty: raise HTTPException(500, "Data not loaded")
    d = filter_raw(df_raw.copy(), **fp(company,bsart,matkl,vendor,plant,purch_group,case_id,activity,month,year,quarter,lifnr,lead_time,None,ernam,sod=sod))
    
    req = ["PR Creation", "PO Creation", "GR Posting", "Invoice Posting"]
    rev = ["PR Reversal Date", "PO Reversal Date", "GR Reversal Date", "Invoice Reversal Date"]
    
    has_all = pd.Series([True]*len(d), index=d.index)
    is_seq = pd.Series([True]*len(d), index=d.index)
    prev_col = None
    
    for c in req:
        if c in d.columns:
            has_all = has_all & d[c].notna()
            if prev_col:
                is_seq = is_seq & (d[c] >= d[prev_col]).fillna(False)
            prev_col = c
        else:
            has_all = pd.Series([False]*len(d), index=d.index)
            is_seq = pd.Series([False]*len(d), index=d.index)
            break
            
    has_no_rev = pd.Series([True]*len(d), index=d.index)
    for c in rev:
        if c in d.columns: has_no_rev = has_no_rev & d[c].isna()
            
    happy_mask = has_all & is_seq & has_no_rev
    
    happy_cases = d[happy_mask][COL_CASE].nunique() if COL_CASE in d.columns else 0
    total_cases = unique_cases(d)
    deviations = total_cases - happy_cases
    
    return [{"status": "Happy Path", "count": happy_cases}, {"status": "Deviations", "count": deviations}]

@router.get("/charts/sod_violations")
def chart_sod_violations(
    username: str = Query("Unknown"), company: Optional[str]=Query(None), bsart: Optional[str]=Query(None), matkl: Optional[str]=Query(None),
    vendor: Optional[str]=Query(None), plant: Optional[str]=Query(None), purch_group: Optional[str]=Query(None),
    case_id: Optional[str]=Query(None), activity: Optional[str]=Query(None), month: Optional[str]=Query(None),
    year: Optional[str]=Query(None), quarter: Optional[str]=Query(None), lifnr: Optional[str]=Query(None),
    lead_time: Optional[str]=Query(None), status: Optional[str]=Query(None), ernam: Optional[str]=Query(None), sod: Optional[str]=Query(None)
):
    df_raw = get_user_df(username)
    if df_raw.empty: raise HTTPException(500, "Data not loaded")
    d = filter_raw(df_raw.copy(), **fp(company,bsart,matkl,vendor,plant,purch_group,case_id,activity,month,year,quarter,lifnr,lead_time,status,ernam,sod=sod))

    COL_PO_USER  = "ERNAM"
    COL_PR_USER  = "ERNAM (EBAN)"
    COL_GR_USER  = "GR Creation User"
    COL_INV_USER = "Invoice Creation User"

    COL_PO_DATE  = "PO Creation"
    COL_PR_DATE  = "PR Creation"
    COL_GR_DATE  = "GR Posting"
    COL_INV_DATE = "Invoice Posting"

    def sod_check(date_a, user_a, date_b, user_b):
        needed = [date_a, user_a, date_b, user_b, COL_CASE]
        if not all(c in d.columns for c in needed): return 0
        sub = d.dropna(subset=[date_a, user_a, date_b, user_b]).copy()
        if sub.empty: return 0
        sub["_ua"] = sub[user_a].astype(str).str.strip().str.upper()
        sub["_ub"] = sub[user_b].astype(str).str.strip().str.upper()
        return int(sub[sub["_ua"] == sub["_ub"]][COL_CASE].nunique())

    checks = [
        (COL_PO_DATE,  COL_PO_USER,  COL_GR_DATE,   COL_GR_USER,   "PO Maker = GR Checker"),
        (COL_PO_DATE,  COL_PO_USER,  COL_INV_DATE,  COL_INV_USER,  "PO Maker = Invoice Checker"),
        (COL_GR_DATE,  COL_GR_USER,  COL_INV_DATE,  COL_INV_USER,  "GR Checker = Invoice Checker"),
        (COL_PR_DATE,  COL_PR_USER,  COL_PO_DATE,   COL_PO_USER,   "PR Maker = PO Maker"),
        (COL_PR_DATE,  COL_PR_USER,  COL_GR_DATE,   COL_GR_USER,   "PR Maker = GR Checker"),
    ]

    results = []
    for date_a, user_a, date_b, user_b, display in checks:
        count = sod_check(date_a, user_a, date_b, user_b)
        if count > 0: results.append({"violation": display, "count": count})
    return sorted(results, key=lambda x: x["count"], reverse=True)

@router.get("/charts/bottleneck")
def chart_bottleneck(
    username: str = Query("Unknown"), company: Optional[str]=Query(None), bsart: Optional[str]=Query(None), matkl: Optional[str]=Query(None),
    vendor: Optional[str]=Query(None), plant: Optional[str]=Query(None), purch_group: Optional[str]=Query(None),
    case_id: Optional[str]=Query(None), activity: Optional[str]=Query(None), month: Optional[str]=Query(None),
    year: Optional[str]=Query(None), quarter: Optional[str]=Query(None), lifnr: Optional[str]=Query(None),
    lead_time: Optional[str]=Query(None), status: Optional[str]=Query(None), ernam: Optional[str]=Query(None), sod: Optional[str]=Query(None)
):
    df_raw = get_user_df(username)
    if df_raw.empty: raise HTTPException(500, "Data not loaded")
    d = filter_raw(df_raw.copy(), **fp(company,bsart,matkl,vendor,plant,purch_group,case_id,activity,month,year,quarter,lifnr,lead_time,status,ernam,sod=sod))
    steps = [
        ("PR Creation", "PO Creation", "PR → PO"),
        ("PO Creation", "GR Posting", "PO → GR"),
        ("GR Posting", "Invoice Posting", "GR → Invoice"),
        ("PR Creation", "GR Posting", "PR → GR (total)"),
    ]
    results = []
    for from_col, to_col, label in steps:
        if from_col in d.columns and to_col in d.columns:
            sub = d.dropna(subset=[from_col, to_col]).copy()
            sub["days"] = (sub[to_col] - sub[from_col]).dt.days
            sub = sub[sub["days"] >= 0]
            if not sub.empty:
                results.append({
                    "step": label,
                    "avg_days": round(float(sub["days"].mean()), 1),
                    "median_days": round(float(sub["days"].median()), 1),
                    "count": int(len(sub))
                })
    return results

@router.get("/charts/rev_by_purch_group")
def chart_rev_by_purch_group(
    username: str = Query("Unknown"), company: Optional[str]=Query(None), bsart: Optional[str]=Query(None), matkl: Optional[str]=Query(None),
    vendor: Optional[str]=Query(None), plant: Optional[str]=Query(None), purch_group: Optional[str]=Query(None),
    case_id: Optional[str]=Query(None), activity: Optional[str]=Query(None), month: Optional[str]=Query(None),
    year: Optional[str]=Query(None), quarter: Optional[str]=Query(None), lifnr: Optional[str]=Query(None),
    lead_time: Optional[str]=Query(None), status: Optional[str]=Query(None), ernam: Optional[str]=Query(None), sod: Optional[str]=Query(None)
):
    df_raw = get_user_df(username)
    if df_raw.empty: raise HTTPException(500, "Data not loaded")
    d = filter_raw(df_raw.copy(), **fp(company,bsart,matkl,vendor,plant,purch_group,case_id,activity,month,year,quarter,lifnr,lead_time,status,ernam,sod=sod))
    if "PO Reversal Date" not in d.columns or COL_EKGRP not in d.columns: return []
    b = d.dropna(subset=["PO Reversal Date", COL_EKGRP])
    vc = b.groupby(COL_EKGRP)[COL_CASE].nunique().reset_index(name="count")
    vc.columns = ["purch_group", "count"]
    return vc.sort_values("count", ascending=False).head(20).to_dict("records")

@router.get("/charts/purch_group_workload")
def chart_purch_group_workload(
    username: str = Query("Unknown"), company: Optional[str]=Query(None), bsart: Optional[str]=Query(None), matkl: Optional[str]=Query(None),
    vendor: Optional[str]=Query(None), plant: Optional[str]=Query(None), purch_group: Optional[str]=Query(None),
    case_id: Optional[str]=Query(None), activity: Optional[str]=Query(None), month: Optional[str]=Query(None),
    year: Optional[str]=Query(None), quarter: Optional[str]=Query(None), lifnr: Optional[str]=Query(None),
    lead_time: Optional[str]=Query(None), status: Optional[str]=Query(None), ernam: Optional[str]=Query(None), sod: Optional[str]=Query(None)
):
    df_raw = get_user_df(username)
    if df_raw.empty: raise HTTPException(500, "Data not loaded")
    d = filter_raw(df_raw.copy(), **fp(company,bsart,matkl,vendor,plant,purch_group,case_id,activity,month,year,quarter,lifnr,lead_time,status,ernam,sod=sod))
    if COL_EKGRP not in d.columns: return []
    vc = d.dropna(subset=[COL_EKGRP]).groupby(COL_EKGRP)[COL_CASE].nunique().reset_index()
    vc.columns = ["purch_group", "count"]
    return vc.sort_values("count", ascending=False).head(25).to_dict("records")

@router.get("/charts/vendor_lead_time")
def chart_vendor_lead_time(
    username: str = Query("Unknown"), company: Optional[str]=Query(None), bsart: Optional[str]=Query(None), matkl: Optional[str]=Query(None),
    vendor: Optional[str]=Query(None), plant: Optional[str]=Query(None), purch_group: Optional[str]=Query(None),
    case_id: Optional[str]=Query(None), activity: Optional[str]=Query(None), month: Optional[str]=Query(None),
    year: Optional[str]=Query(None), quarter: Optional[str]=Query(None), lifnr: Optional[str]=Query(None),
    lead_time: Optional[str]=Query(None), status: Optional[str]=Query(None), ernam: Optional[str]=Query(None), sod: Optional[str]=Query(None)
):
    df_raw = get_user_df(username)
    if df_raw.empty: raise HTTPException(500, "Data not loaded")
    d = filter_raw(df_raw.copy(), **fp(company,bsart,matkl,vendor,plant,purch_group,case_id,activity,month,year,quarter,lifnr,lead_time,status,ernam,sod=sod))
    if "PO Creation" not in d.columns or "GR Posting" not in d.columns: return []
    # Use NAME1 when populated, fall back to LIFNR (vendor ID) so chart always renders
    name_col = None
    if "NAME1" in d.columns and d["NAME1"].notna().any():
        name_col = "NAME1"
    elif COL_LIFNR in d.columns:
        name_col = COL_LIFNR
    if name_col is None: return []
    sub = d.dropna(subset=[name_col, "PO Creation", "GR Posting"]).copy()
    sub["days"] = (sub["GR Posting"] - sub["PO Creation"]).dt.days
    sub = sub[sub["days"] >= 0]
    if sub.empty: return []
    agg = sub.groupby(name_col).agg(avg_days=("days", "mean"), case_count=(COL_CASE, "nunique")).reset_index()
    agg.columns = ["vendor", "avg_days", "case_count"]
    agg["avg_days"] = agg["avg_days"].round(1)
    return agg.sort_values("avg_days", ascending=False).head(30).to_dict("records")

@router.get("/process-map")
def get_process_map(
    username: str = Query("Unknown"), company: Optional[str]=Query(None), bsart: Optional[str]=Query(None), matkl: Optional[str]=Query(None), 
    vendor: Optional[str]=Query(None), plant: Optional[str]=Query(None), purch_group: Optional[str]=Query(None),
    case_id: Optional[str]=Query(None), activity: Optional[str]=Query(None), month: Optional[str]=Query(None), 
    year: Optional[str]=Query(None), quarter: Optional[str]=Query(None), lifnr: Optional[str]=Query(None),
    lead_time: Optional[str]=Query(None), status: Optional[str]=Query(None), ernam: Optional[str]=Query(None), sod: Optional[str]=Query(None)
):
    df_raw = get_user_df(username)
    if df_raw.empty: raise HTTPException(500, "Data not loaded")
    d = filter_raw(df_raw.copy(), **fp(company,bsart,matkl,vendor,plant,purch_group,case_id,activity,month,year,quarter,lifnr,lead_time,status,ernam,sod=sod))

    LAYOUT_H = {
        "PR Creation":           {"x":100,  "y":800},
        "PR Release Date":       {"x":900,  "y":800},
        "PO Creation":           {"x":1700, "y":800},
        "PO Date":               {"x":2500, "y":800},
        "GR Posting":            {"x":3300, "y":800},
        "Invoice Posting":       {"x":4100, "y":800},
        "PR Reversal":           {"x":500,  "y":100},
        "PR Reversal (Post-PO)": {"x":2100, "y":1500},
        "PO Reversal":           {"x":2100, "y":100},
        "PO Reversal (Post-GR)": {"x":3700, "y":1500},
        "GR Reversal":           {"x":3700, "y":100},
        "GR Reversal (Post-Inv)":{"x":4500, "y":1500},
        "Invoice Reversal Date": {"x":4500, "y":100},
    }

    LAYOUT_V = {
        "PR Creation":           {"x":900,  "y":100},
        "PR Release Date":       {"x":900,  "y":750},
        "PO Creation":           {"x":900,  "y":1400},
        "PO Date":               {"x":900,  "y":2050},
        "GR Posting":            {"x":900,  "y":2700},
        "Invoice Posting":       {"x":900,  "y":3350},
        "PR Reversal":           {"x":-200, "y":425},
        "PR Reversal (Post-PO)": {"x":2000, "y":1075},
        "PO Reversal":           {"x":-200, "y":1075},
        "PO Reversal (Post-GR)": {"x":-200, "y":2375},
        "GR Reversal":           {"x":2000, "y":2375},
        "GR Reversal (Post-Inv)":{"x":-200, "y":3025},
        "Invoice Reversal Date": {"x":2000, "y":3025},
    }

    cols_to_melt = [c for c in ACTIVITY_COLUMNS if c in d.columns]
    melted = d.melt(id_vars=[COL_CASE], value_vars=cols_to_melt, var_name="Activity", value_name="Activitytime")
    melted = melted.dropna(subset=["Activitytime"])

    # Build ref_times only from columns that actually exist in d
    # (guards against KeyError when optional activities are absent)
    ref_cols = [COL_CASE] + [c for c in ["PO Creation", "GR Posting", "Invoice Posting"] if c in d.columns]
    ref_times = d[ref_cols].copy()
    melted = melted.merge(ref_times, on=COL_CASE, how="left")

    def rename_activity(row):
        act = row["Activity"]
        ts  = row["Activitytime"]
        # Guard all comparisons against NaT to prevent TypeError
        if act == "PR Reversal Date":
            po_c = row.get("PO Creation")
            if pd.notna(po_c) and pd.notna(ts) and ts > po_c: return "PR Reversal (Post-PO)"
            return "PR Reversal"
        if act == "PO Reversal Date":
            gr_p = row.get("GR Posting")
            if pd.notna(gr_p) and pd.notna(ts) and ts > gr_p: return "PO Reversal (Post-GR)"
            return "PO Reversal"
        if act == "GR Reversal Date":
            inv_p = row.get("Invoice Posting")
            if pd.notna(inv_p) and pd.notna(ts) and ts > inv_p: return "GR Reversal (Post-Inv)"
            return "GR Reversal"
        return act

    melted["Activity"] = melted.apply(rename_activity, axis=1)
    
    base_order = [
        "PR Creation", "PR Release Date", "PR Reversal", "PR Reversal (Post-PO)",
        "PO Creation", "PO Date", "PO Reversal", "PO Reversal (Post-GR)",
        "GR Posting", "GR Reversal", "GR Reversal (Post-Inv)",
        "Invoice Posting", "Invoice Reversal Date"
    ]
    act_order = {col: i for i, col in enumerate(base_order)}
    for c in ACTIVITY_COLUMNS:
        if c not in act_order: act_order[c] = 99

    melted["Act_Idx"] = melted["Activity"].map(act_order).fillna(99)
    melted = melted.sort_values(by=[COL_CASE,"Activitytime","Act_Idx"])
    
    melted["Next_Activity"]     = melted.groupby(COL_CASE)["Activity"].shift(-1)
    melted["Next_Activitytime"] = melted.groupby(COL_CASE)["Activitytime"].shift(-1)

    transitions = melted.dropna(subset=["Next_Activity"]).copy()

    edge_freqs = transitions.groupby(["Activity","Next_Activity"])[COL_CASE].nunique().reset_index(name="frequency")

    transitions["duration_days"] = ((transitions["Next_Activitytime"] - transitions["Activitytime"]).dt.total_seconds() / 86400)
    avg_dur = transitions[transitions["duration_days"]>=0].groupby(["Activity","Next_Activity"])["duration_days"].mean().reset_index()
    avg_dur.columns = ["Activity","Next_Activity","avg_days"]

    edge_freqs = edge_freqs.merge(avg_dur, on=["Activity","Next_Activity"], how="left")

    present_acts = set(melted["Activity"].unique()) | MAIN_NODES
    
    nodes_out = []
    for name in present_acts:
        pos_h = LAYOUT_H.get(name, {"x":2800, "y":500})
        pos_v = LAYOUT_V.get(name, {"x":600, "y":2300})
        count = melted[melted["Activity"]==name][COL_CASE].nunique()
        nodes_out.append({
            "id": name, "label": name, "position_h": pos_h, "position_v": pos_v, "is_main": name in MAIN_NODES, "frequency": count,
        })

    edges_out = []
    for _, row in edge_freqs.iterrows():
        avg_d = round(float(row["avg_days"]), 1) if pd.notna(row.get("avg_days")) else None
        edges_out.append({
            "id": f"{row['Activity']}--{row['Next_Activity']}", "source": row["Activity"], "target": row["Next_Activity"],
            "frequency": int(row["frequency"]), "avg_days": avg_d,
        })

    return {"nodes": nodes_out, "edges": edges_out}