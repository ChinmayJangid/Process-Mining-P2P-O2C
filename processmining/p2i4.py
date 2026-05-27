"""
p2i4.py  —  Plan-to-Inventory (P2I) Process Mining Analytics Backend
=======================================================================

CASE ID:    UniqueID_P2I  (AUFNR + POSNR — Production Order + Item Number)
PROCESS:    Order Confirmation → Execution → GI → FG Receipt → GR → Invoice
            (upload AFKO to populate production order date columns)

ACTIVITY COLUMNS (no quantity KPIs per requirement):
  Operation Confirmations (AFRU):
    Order Confirmation, Order Posting, Start of Execution,
    Processing Start, Execution Finish, Operation Reversal

  Material Movements (MSEG/MKPF by BWART):
    Goods Issued to Order (261), Finished Goods Receipts (101),
    Goods transfer to Sub Contractor (541), Goods Issue Reversed (262),
    Finished Goods Reversal (102), Finished Goods to Quality Inpection (131),
    QI to FG (132)

  PR Branch (EBAN):
    PR Creation, PR Release Date, PR Reversal Date

  PO Branch (EKKO+EKPO):
    PO Creation, PO Date, PO Reversal Date

  GR Branch (EKBE VGABE=1):
    GR Posting, GR Reversal Date

  Invoice Branch (EKBE VGABE=2):
    Invoice Posting, Invoice Reversal Date
"""

import io
import os
import json
import pandas as pd
from fastapi import APIRouter, Query, HTTPException, UploadFile, File, Form, Depends
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from typing import Optional
import warnings
import re
from datetime import datetime

warnings.filterwarnings("ignore")

router = APIRouter(prefix="/p2i", tags=["Plan-to-Inventory"])

# ─── Server-side CSV Output Directory ───────────────────────────────────────
P2I_OUTPUT_DIR = os.path.join("user_data", "p2i_outputs")
os.makedirs(P2I_OUTPUT_DIR, exist_ok=True)


def _save_output_csv(df: pd.DataFrame, filename: str) -> str:
    os.makedirs(P2I_OUTPUT_DIR, exist_ok=True)
    export_df = df[[c for c in df.columns if not c.startswith("_")]].copy()
    for c in export_df.columns:
        if pd.api.types.is_datetime64_any_dtype(export_df[c]):
            export_df[c] = export_df[c].dt.strftime("%Y-%m-%d")
    out_path = os.path.join(P2I_OUTPUT_DIR, filename)
    export_df.to_csv(out_path, index=False)
    print(f"[P2I OUTPUT] CSV saved → {out_path}  ({len(export_df)} rows)")
    return out_path


# ─── Column Config ────────────────────────────────────────────────────────────
COL_CASE    = "UniqueID_P2I"    # Case ID = AUFNR + POSNR (item-level granularity)
COL_AUFNR   = "AUFNR"           # Production Order Number (dimension, not case ID)
COL_POSNR   = "POSNR"           # Order Item Number (dimension)
COL_PLANT   = "WERKS"           # Plant
COL_MATNR   = "MATNR"           # Material Number
COL_BUKRS   = "BUKRS"           # Company Code
COL_BSART   = "BSART"           # Order Type
COL_EKGRP   = "EKGRP"           # Purchasing Group
COL_LIFNR   = "LIFNR"           # Vendor ID
COL_VENDOR  = "NAME1"           # Vendor Name
COL_ERNAM   = "ERNAM"           # Created By (PO)

# ─── AUFK Header Columns (from Production Order Header table) ────────────────
COL_AUART   = "AUART"           # SAP Order Type  (PP01, PM01, PI01 etc.)
COL_GAMNG   = "GAMNG"           # Order Quantity  (total planned qty from AUFK)
COL_GMEIN   = "GMEIN"           # Unit of Measure (base UoM from AUFK)
COL_GLTRP   = "Planned Finish Date"   # AUFK.GLTRP — planned finish (schedule adherence)
COL_LTRMI   = "Actual Finish Date"    # AUFK.LTRMI — actual finish  (schedule adherence)
COL_KOSTL   = "KOSTL"           # Cost Center           (from AUFK)
COL_GSBER   = "GSBER"           # Business Area         (from AUFK)
COL_FEVOR   = "FEVOR"           # Production Supervisor (from AUFK)
COL_MAKTX   = "MAKTX"           # Material Description  (from AUFK)
COL_IGMNG   = "IGMNG"           # Confirmed Yield qty   (from AUFK)
COL_RMNGA   = "RMNGA"           # Scrap Quantity        (from AUFK)
COL_GASMG   = "GASMG"           # Delivered Quantity    (from AUFK)
COL_GSTRP   = "GSTRP"           # Scheduled Start Date  (from AUFK)
COL_SYSST   = "SYSST"           # System Status         (from AUFK)
COL_LOEKZ   = "LOEKZ"           # Deletion Flag         (from AUFK)

# All columns to read when a standalone AUFK file is uploaded
AUFK_COLS = [
    "AUFNR",   # Production Order  — join key
    "AUART",   # Order Type        (PP01, PP02, PI01 etc.)
    "WERKS",   # Plant
    "MATNR",   # Material Number
    "MAKTX",   # Material Description
    "GAMNG",   # Order Quantity    (planned target)
    "GMEIN",   # Base Unit of Measure
    "GASMG",   # Delivered Quantity
    "GISMG",   # Goods Issue Quantity
    "IGMNG",   # Confirmed Yield   (actual good qty)
    "RMNGA",   # Scrap Quantity
    "ERDAT",   # Order Creation Date
    "GSTRP",   # Scheduled Start Date
    "GLTRP",   # Scheduled Finish Date  → COL_GLTRP "Planned Finish Date"
    "FTRMS",   # Scheduled Release Date
    "LTRMI",   # Actual Finish Date     → COL_LTRMI "Actual Finish Date"
    "FEVOR",   # Production Supervisor
    "KOSTL",   # Cost Center
    "GSBER",   # Business Area
    "SYSST",   # System Status
    "ANWST",   # User Status
    "LOEKZ",   # Deletion Flag
]

# AUFK date columns that need to be renamed on ingest
AUFK_DATE_RENAME = {
    "GLTRP": "Planned Finish Date",
    "LTRMI": "Actual Finish Date",
}

# ─── All P2I Activity Columns (ordered for process flow) ─────────────────────
ACTIVITY_COLUMNS = [
    # ── Happy Path — 8 P2I steps (in order) ─────────────────────────────────
    "Production Order Creation",             # step 1
    "Order Confirmation",                    # step 2
    "Basic Start",                           # step 3
    "Scheduled Start",                       # step 4
    "Actual Start",                          # step 5
    "Goods Issued to Order",                 # step 6  BWART 261
    "Finished Goods Receipt",                # step 7  BWART 101
    "Finished Goods to Quality Inspection",  # step 8  BWART 131
    # ── Deviations — 5 P2I deviation nodes ───────────────────────────────────
    "Operation Reversed",                    # dev 1
    "Goods Issued Reversed",                 # dev 2  BWART 262
    "Goods transferred to subcontractor",    # dev 3  BWART 541
    "Finished Goods Reversed",               # dev 4  BWART 102
    "Quality Inspection to Finished Goods",  # dev 5  BWART 132
    # ── P2P Branch (shown as-is via gateway) ─────────────────────────────────
    "PR Creation",
    "PR Release Date",
    "PR Reversal Date",
    "PO Creation",
    "PO Date",
    "PO Reversal Date",
    "GR Posting",
    "GR Reversal Date",
    "Invoice Posting",
    "Invoice Reversal Date",
]

# Main visible nodes — all 8 happy path steps always shown
MAIN_NODES = {
    "Production Order Creation", "Order Confirmation", "Basic Start",
    "Scheduled Start", "Actual Start", "Goods Issued to Order",
    "Finished Goods Receipt", "Finished Goods to Quality Inspection",
}

# All known activity names (including KNIME variants for long→wide detection)
_ALL_KNOWN_ACTIVITIES = set(ACTIVITY_COLUMNS) | {
    "GR Reversal", "Invoice Reversal", "PR Reversal", "PO Reversal",
    "Order Creation", "Finished Goods Receipts", "Finished Goods to Quality Inpection",
    "Operation Reversal", "Goods transfer to Sub Contractor", "Goods Issue Reversed",
    "Finished Goods Reversal", "QI to FG", "Actual start date",
}

# Rename KNIME activity variants to our internal names
ACTIVITY_RENAME_MAP = {
    # Old P2I names -> New P2I names
    "Order Creation":                      "Production Order Creation",
    "Finished Goods Receipts":             "Finished Goods Receipt",
    "Finished Goods to Quality Inpection": "Finished Goods to Quality Inspection",
    "Operation Reversal":                  "Operation Reversed",
    "Goods transfer to Sub Contractor":    "Goods transferred to subcontractor",
    "Goods Issue Reversed":                "Goods Issued Reversed",
    "Finished Goods Reversal":             "Finished Goods Reversed",
    "QI to FG":                            "Quality Inspection to Finished Goods",
    "Actual start date":                   "Actual Start",
    # P2P reversals
    "GR Reversal":                         "GR Reversal Date",
    "Invoice Reversal":                    "Invoice Reversal Date",
    "PR Reversal":                         "PR Reversal Date",
    "PO Reversal":                         "PO Reversal Date",
}


# ─── Format Detection & Long→Wide Conversion ─────────────────────────────────

def _is_long_format(df: pd.DataFrame) -> bool:
    """True when the dataframe is a KNIME long-format event log."""
    act_col = next((c for c in df.columns if c in ("Activity", "ColumnNames")), None)
    ts_col  = next((c for c in df.columns if c in ("Timestamp", "ColumnValues")), None)
    if act_col is None or ts_col is None:
        return False
    sample    = df[act_col].dropna().astype(str).head(1000)
    match_pct = sample.isin(_ALL_KNOWN_ACTIVITIES).mean()
    return match_pct >= 0.10


def _long_to_wide(df: pd.DataFrame) -> pd.DataFrame:
    """Convert KNIME long-format event log → wide format (one row per AUFNR)."""
    df = df.copy()

    act_col = next((c for c in df.columns if c in ("Activity", "ColumnNames")), None)
    ts_col  = next((c for c in df.columns if c in ("Timestamp", "ColumnValues")), None)

    print(f"[P2I FORMAT] LONG format detected "
          f"(act_col='{act_col}', ts_col='{ts_col}', rows={len(df)})")

    if act_col != "Activity":
        df = df.rename(columns={act_col: "Activity"})
    if ts_col != "Timestamp":
        df = df.rename(columns={ts_col: "Timestamp"})

    df["Activity"] = df["Activity"].map(
        lambda x: ACTIVITY_RENAME_MAP.get(str(x), x) if pd.notna(x) else x
    )
    df["Timestamp"] = pd.to_datetime(df["Timestamp"], errors="coerce")

    case_col = None
    # Prefer UniqueID_P2I if already present (transformer output)
    if COL_CASE in df.columns:
        case_col = COL_CASE
    # Fall back: build UniqueID_P2I from AUFNR + POSNR
    elif COL_AUFNR in df.columns:
        if COL_POSNR in df.columns:
            df[COL_CASE] = (
                df[COL_AUFNR].astype(str).str.strip().str.zfill(12)
                + df[COL_POSNR].astype(str).str.strip().str.zfill(4)
            )
            print(f"[P2I FORMAT] Built UniqueID_P2I from AUFNR+POSNR: {df[COL_CASE].nunique()} unique items")
        else:
            # Single-item orders — use AUFNR padded + "0001"
            df[COL_CASE] = df[COL_AUFNR].astype(str).str.strip().str.zfill(12) + "0001"
            print(f"[P2I FORMAT] Built UniqueID_P2I from AUFNR only (POSNR missing): {df[COL_CASE].nunique()} items")
        case_col = COL_CASE
    if case_col is None:
        raise ValueError("Cannot convert long→wide: neither 'UniqueID_P2I' nor 'AUFNR' column found.")

    already_wide_date_cols = [
        c for c in df.columns
        if c in set(ACTIVITY_COLUMNS) and c not in ("Activity", "Timestamp")
    ]

    skip_in_dim = {"Activity", "Timestamp", "_ts", "Month", "Year", "Quarter"} | set(ACTIVITY_COLUMNS)
    dim_cols    = [c for c in df.columns if c not in skip_in_dim and c != case_col]

    activity_df = (
        df[[case_col, "Activity", "Timestamp"]]
        .dropna(subset=["Timestamp"])
        .sort_values("Timestamp")
        .drop_duplicates(subset=[case_col, "Activity"], keep="first")
    )

    if activity_df.empty:
        raise ValueError("No valid Activity/Timestamp rows found after filtering.")

    pivoted = activity_df.pivot(
        index=case_col, columns="Activity", values="Timestamp"
    ).reset_index()
    pivoted.columns.name = None
    pivoted = pivoted.rename(columns=ACTIVITY_RENAME_MAP)
    known_acts = [c for c in ACTIVITY_COLUMNS if c in pivoted.columns]
    pivoted    = pivoted[[case_col] + known_acts]

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

    if already_wide_date_cols:
        for _c in already_wide_date_cols:
            df[_c] = pd.to_datetime(df[_c], errors="coerce")
        aw_agg = (
            df[[case_col] + already_wide_date_cols]
            .groupby(case_col, sort=False)
            .first()
            .reset_index()
        )
        aw_renamed = {c: f"__aw_{c}" for c in already_wide_date_cols}
        aw_agg = aw_agg.rename(columns=aw_renamed)
        wide = wide.merge(aw_agg, on=case_col, how="left")

        for _c in already_wide_date_cols:
            aw_col = f"__aw_{_c}"
            if _c in wide.columns:
                wide[_c] = wide[_c].fillna(wide[aw_col])
            else:
                wide = wide.rename(columns={aw_col: _c})
            if aw_col in wide.columns:
                wide = wide.drop(columns=[aw_col])

    for drop_col in ("Timestamp", "Activity", "ColumnNames", "ColumnValues"):
        if drop_col in wide.columns:
            wide = wide.drop(columns=[drop_col])

    print(f"[P2I FORMAT] Wide: {len(wide)} rows × {len(wide.columns)} cols "
          f"| activities: {[c for c in ACTIVITY_COLUMNS if c in wide.columns]}")
    return wide


# ─── Audit Logging ────────────────────────────────────────────────────────────
AUDIT_FILE = "p2i_audit_logs.json"


def log_audit(username: str, action: str, details: str):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[P2I AUDIT] {timestamp} | {username} | {action} | {details}")
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
        print(f"[P2I ERROR] Failed to write audit log: {e}")


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


# ─── Data Processing ─────────────────────────────────────────────────────────

def process_df(df: pd.DataFrame) -> pd.DataFrame:
    print(f"[P2I PROCESS] Parsing dates for {len(df)} rows.")

    # ── Ensure UniqueID_P2I exists ────────────────────────────────────────────
    if COL_CASE not in df.columns:
        if COL_AUFNR in df.columns and COL_POSNR in df.columns:
            df[COL_CASE] = (
                df[COL_AUFNR].astype(str).str.strip().str.zfill(12)
                + df[COL_POSNR].astype(str).str.strip().str.zfill(4)
            )
            print(f"[P2I PROCESS] Built UniqueID_P2I from AUFNR+POSNR: {df[COL_CASE].nunique()} items")
        elif COL_AUFNR in df.columns:
            df[COL_CASE] = df[COL_AUFNR].astype(str).str.strip().str.zfill(12) + "0001"
            print(f"[P2I PROCESS] Built UniqueID_P2I from AUFNR only: {df[COL_CASE].nunique()} items")
        else:
            print("[P2I PROCESS] WARNING: Neither UniqueID_P2I nor AUFNR found — case ID will be missing")
    all_date_cols = list(dict.fromkeys(ACTIVITY_COLUMNS))
    for c in all_date_cols:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce")

    act_present = [c for c in ACTIVITY_COLUMNS if c in df.columns]

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


# ─── Multi-User File System ───────────────────────────────────────────────────
USER_DFS: dict = {}
FILE_REGISTRY = "p2i_file_registry.json"
UPLOAD_DIR = "user_data"
os.makedirs(UPLOAD_DIR, exist_ok=True)


def load_registry():
    if os.path.exists(FILE_REGISTRY):
        try:
            with open(FILE_REGISTRY, "r") as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def save_registry(reg):
    with open(FILE_REGISTRY, "w") as f:
        json.dump(reg, f, indent=4)


def get_user_df(username: str) -> pd.DataFrame:
    if username in USER_DFS:
        return USER_DFS[username]
    if "Unknown" in USER_DFS:
        return USER_DFS["Unknown"]
    return pd.DataFrame()


@router.post("/clear")
def clear_data(username: str = Query("Unknown")):
    if username in USER_DFS:
        del USER_DFS[username]
    if "Unknown" in USER_DFS:
        del USER_DFS["Unknown"]
    print(f"[P2I INFO] Cleared active data for user: {username}")
    return {"status": "cleared"}


# ─── Multi-File Endpoints ─────────────────────────────────────────────────────

@router.get("/my_files")
def get_my_files(username: str = Query(...)):
    reg = load_registry()
    files = reg.get(username, [])
    return list(reversed(files))


class LoadFileReq(BaseModel):
    username: str
    file_id: str


@router.post("/load_file")
def load_specific_file(req: LoadFileReq):
    if not os.path.exists(req.file_id):
        print(f"[P2I ERROR] File not found: {req.file_id}")
        raise HTTPException(404, "File not found on server")
    try:
        df = pd.read_json(req.file_id, orient="records")
        processed_df = process_df(df)
        USER_DFS[req.username] = processed_df
        USER_DFS["Unknown"] = processed_df
        print(f"[P2I INFO] {req.username} loaded file: {req.file_id}")
        log_audit(req.username, "LOAD_FILE", f"Loaded file: {req.file_id}")
        return {"status": "ok"}
    except Exception as e:
        print(f"[P2I ERROR] Failed to load file: {e}")
        raise HTTPException(500, f"Failed to load file: {str(e)}")


def register_transform_build(username: str, processed: pd.DataFrame,
                              save_path: str, csv_path: str = ""):
    """Register a transformer build in the shared file registry."""
    cases = int(processed["UniqueID_P2I"].nunique()) if "UniqueID_P2I" in processed.columns else (
            int(processed["AUFNR"].nunique()) if "AUFNR" in processed.columns else 0)
    reg = load_registry()
    if username not in reg:
        reg[username] = []
    reg[username].append({
        "file_id":     save_path,
        "filename":    f"Built P2I Event Log ({datetime.now().strftime('%Y-%m-%d %H:%M')})",
        "upload_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "rows":        len(processed),
        "cases":       cases,
        "format":      "wide",
        "source":      "table_build",
        "csv_path":    csv_path,
    })
    save_registry(reg)
    print(f"[P2I REGISTRY] Registered transform build for '{username}': {cases} cases")


# ─── Download Output CSV ──────────────────────────────────────────────────────

@router.get("/download_output")
def download_output_csv(username: str = Query("Unknown")):
    df = get_user_df(username)
    if df.empty:
        raise HTTPException(404, "No data loaded. Upload a file or build the event log first.")

    ts       = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_name = f"P2I_Output_{username}_{ts}.csv"

    try:
        _save_output_csv(df, csv_name)
    except Exception as e:
        print(f"[P2I DOWNLOAD] Server save failed (non-fatal): {e}")

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


# ─── Filtering ────────────────────────────────────────────────────────────────

def filter_raw(df, company=None, plant=None, matnr=None, bsart=None,
               ekgrp=None, lifnr=None, vendor=None, case_id=None,
               activity=None, month=None, year=None, quarter=None,
               lead_time=None, status=None, ernam=None,
               auart=None, kostl=None, fevor=None):

    if company  and company  != "ALL" and COL_BUKRS  in df.columns: df = df[df[COL_BUKRS].astype(str) == company]
    if plant    and plant    != "ALL" and COL_PLANT  in df.columns: df = df[df[COL_PLANT].astype(str) == plant]
    if matnr    and matnr    != "ALL" and COL_MATNR  in df.columns: df = df[df[COL_MATNR].astype(str) == matnr]
    if bsart    and bsart    != "ALL" and COL_BSART  in df.columns: df = df[df[COL_BSART].astype(str) == bsart]
    if ekgrp    and ekgrp    != "ALL" and COL_EKGRP  in df.columns: df = df[df[COL_EKGRP].astype(str) == ekgrp]
    if lifnr    and lifnr    != "ALL" and COL_LIFNR  in df.columns: df = df[df[COL_LIFNR].astype(str) == lifnr]
    if ernam    and ernam    != "ALL" and COL_ERNAM  in df.columns: df = df[df[COL_ERNAM].astype(str) == ernam]
    if auart    and auart    != "ALL" and COL_AUART  in df.columns: df = df[df[COL_AUART].astype(str) == auart]
    if kostl    and kostl    != "ALL" and COL_KOSTL  in df.columns: df = df[df[COL_KOSTL].astype(str) == kostl]
    if fevor    and fevor    != "ALL" and COL_FEVOR  in df.columns: df = df[df[COL_FEVOR].astype(str) == fevor]
    if vendor   and vendor   != "ALL":
        if COL_VENDOR in df.columns and df[COL_VENDOR].notna().any():
            df = df[df[COL_VENDOR].astype(str) == vendor]
        elif COL_LIFNR in df.columns:
            df = df[df[COL_LIFNR].astype(str) == vendor]
    if case_id  and case_id  != "ALL" and COL_CASE   in df.columns: df = df[df[COL_CASE].astype(str) == case_id]
    if activity and activity  != "ALL" and activity in df.columns:   df = df[df[activity].notna()]
    if month    and month    != "ALL" and "Month"    in df.columns:  df = df[df["Month"].astype(str) == month]
    if year     and year     != "ALL" and "Year"     in df.columns:  df = df[df["Year"].astype(str) == year]
    if quarter  and quarter  != "ALL" and "Quarter"  in df.columns:  df = df[df["Quarter"].astype(str) == quarter]

    # Lead time filter: Production Order Creation → FG→QI (fallback to Basic Start / Order Confirmation)
    _lt_start = "Production Order Creation" if "Production Order Creation" in df.columns and df["Production Order Creation"].notna().any() else (
        "Basic Start" if "Basic Start" in df.columns and df["Basic Start"].notna().any() else "Order Confirmation"
    )
    _lt_end = "Finished Goods to Quality Inspection" if (
        "Finished Goods to Quality Inspection" in df.columns and
        df["Finished Goods to Quality Inspection"].notna().any()
    ) else "Finished Goods Receipt"
    if lead_time and _lt_start in df.columns and _lt_end in df.columns:
        try:
            df["_lt"] = (df[_lt_end] - df[_lt_start]).dt.days
            nums = re.findall(r"\d+", str(lead_time))
            if len(nums) == 2:
                df = df[df["_lt"].between(int(nums[0]), int(nums[1]), inclusive="left")]
            elif len(nums) == 1:
                df = df[df["_lt"] >= int(nums[0])]
        except Exception:
            pass

    # Happy / Deviation filter
    if status and status != "ALL":
        possible_req = [
            "Production Order Creation",
            "Order Confirmation",
            "Basic Start",
            "Scheduled Start",
            "Actual Start",
            "Goods Issued to Order",
            "Finished Goods Receipt",
            "Finished Goods to Quality Inspection"
        ]
        req = [c for c in possible_req if c in df.columns and df[c].notna().any()]
        rev = ["Operation Reversed", "Goods Issued Reversed",
               "Goods transferred to subcontractor", "Finished Goods Reversed",
               "Quality Inspection to Finished Goods",
               "PR Reversal Date", "PO Reversal Date"]
        has_all = pd.Series([True] * len(df), index=df.index)
        is_seq  = pd.Series([True] * len(df), index=df.index)
        prev_col = None
        for c in req:
            if c in df.columns:
                has_all = has_all & df[c].notna()
                if prev_col:
                    is_seq = is_seq & (df[c] >= df[prev_col]).fillna(False)
                prev_col = c
            else:
                has_all = pd.Series([False] * len(df), index=df.index)
                break
        has_no_rev = pd.Series([True] * len(df), index=df.index)
        for c in rev:
            if c in df.columns:
                has_no_rev = has_no_rev & df[c].isna()
        happy_mask = has_all & is_seq & has_no_rev
        if status == "Happy Path":
            df = df[happy_mask]
        elif status == "Deviations":
            df = df[~happy_mask]

    return df


def fp(company, plant, matnr, bsart, ekgrp, lifnr, vendor, case_id,
       activity, month, year=None, quarter=None, lead_time=None, status=None,
       ernam=None, auart=None, kostl=None, fevor=None):
    return dict(company=company, plant=plant, matnr=matnr, bsart=bsart,
                ekgrp=ekgrp, lifnr=lifnr, vendor=vendor, case_id=case_id,
                activity=activity, month=month, year=year, quarter=quarter,
                lead_time=lead_time, status=status, ernam=ernam,
                auart=auart, kostl=kostl, fevor=fevor)


def get_filter_params(
    username: str = Query("Unknown"),
    company: Optional[str] = Query(None),
    plant: Optional[str] = Query(None),
    matnr: Optional[str] = Query(None),
    bsart: Optional[str] = Query(None),
    ekgrp: Optional[str] = Query(None),
    lifnr: Optional[str] = Query(None),
    vendor: Optional[str] = Query(None),
    case_id: Optional[str] = Query(None),
    activity: Optional[str] = Query(None),
    month: Optional[str] = Query(None),
    year: Optional[str] = Query(None),
    quarter: Optional[str] = Query(None),
    lead_time: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    ernam: Optional[str] = Query(None),
    auart: Optional[str] = Query(None),
    kostl: Optional[str] = Query(None),
    fevor: Optional[str] = Query(None),
):
    return {
        "username": username,
        "company": company,
        "plant": plant,
        "matnr": matnr,
        "bsart": bsart,
        "ekgrp": ekgrp,
        "lifnr": lifnr,
        "vendor": vendor,
        "case_id": case_id,
        "activity": activity,
        "month": month,
        "year": year,
        "quarter": quarter,
        "lead_time": lead_time,
        "status": status,
        "ernam": ernam,
        "auart": auart,
        "kostl": kostl,
        "fevor": fevor,
    }



def col_unique_cases(df, col):
    if col not in df.columns or COL_CASE not in df.columns:
        return 0
    return int(df.loc[df[col].notna(), COL_CASE].nunique())


def unique_cases(df):
    return int(df[COL_CASE].nunique()) if COL_CASE in df.columns else len(df)


# ─── Root ─────────────────────────────────────────────────────────────────────

@router.get("/")
def p2i_root():
    df = get_user_df("Unknown")
    return {
        "status": "P2I Sub-module active",
        "rows": len(df),
        "data_loaded": not df.empty,
        "columns": list(df.columns) if not df.empty else [],
    }


# ─── Upload ───────────────────────────────────────────────────────────────────

@router.post("/upload")
async def upload_csv(
    file: UploadFile = File(...),
    username: str = Form("Unknown"),
    column_mapping: str = Form("{}"),
):
    if not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV files are supported")
    content = await file.read()
    try:
        for enc in ("utf-8", "latin-1", "windows-1252"):
            try:
                df = pd.read_csv(io.BytesIO(content), encoding=enc, low_memory=False)
                break
            except (UnicodeDecodeError, Exception):
                continue
        else:
            raise ValueError("Could not decode CSV with any supported encoding.")

        try:
            mapping = json.loads(column_mapping)
            if mapping:
                df = df.rename(columns=mapping)
        except Exception as e:
            print(f"Error applying column mapping: {e}")

        input_format = "long" if _is_long_format(df) else "wide"
        print(f"[P2I UPLOAD] Detected format: {input_format.upper()} | rows: {len(df)} | cols: {len(df.columns)}")

        if input_format == "long":
            df = _long_to_wide(df)
            if COL_CASE in df.columns:
                unique_items = df[COL_CASE].nunique()
                if len(df) > unique_items:
                    print(f"[P2I FORMAT] Safety dedup: {len(df)} rows → {unique_items} unique order items")
                    act_cols_present = [c for c in ACTIVITY_COLUMNS if c in df.columns]
                    dim_cols_present = [c for c in df.columns if c not in act_cols_present
                                        and not c.startswith("_") and c != COL_CASE]
                    agg = {c: "first" for c in dim_cols_present}
                    for c in act_cols_present:
                        agg[c] = "first"
                    df = df.groupby(COL_CASE, sort=False).agg(agg).reset_index()
                    print(f"[P2I FORMAT] After dedup: {len(df)} rows")

        df = process_df(df)

        user_dir  = os.path.join(UPLOAD_DIR, username)
        os.makedirs(user_dir, exist_ok=True)
        file_id   = datetime.now().strftime("%Y%m%d_%H%M%S")
        safe_name = "".join(c for c in file.filename if c.isalnum() or c in "._- ")
        save_path = os.path.join(user_dir, f"{file_id}_{safe_name}.json")
        try:
            df.to_json(save_path, orient="records", date_format="iso")
        except Exception as e:
            print(f"[P2I ERROR] JSON save failed: {e}")

        csv_name = f"P2I_Upload_{username}_{file_id}.csv"
        csv_path = ""
        try:
            csv_path = _save_output_csv(df, csv_name)
        except Exception as e:
            print(f"[P2I ERROR] CSV output save failed: {e}")

        cases = int(df[COL_CASE].nunique()) if COL_CASE in df.columns else 0
        reg = load_registry()
        if username not in reg:
            reg[username] = []
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

        log_audit(username, "UPLOAD",
                  f"Uploaded {file.filename} ({input_format} format, {cases} unique order items)")
        return {
            "status":             "ok",
            "rows":               len(df),
            "columns":            list(df.columns),
            "unique_cases":       cases,
            "input_format":       input_format,
        }
    except Exception as e:
        import traceback
        print(f"[P2I ERROR] Upload failed: {e}\n{traceback.format_exc()}")
        log_audit(username, "ERROR", f"Failed to upload {file.filename}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to parse CSV: {e}")


# ─── Filters ──────────────────────────────────────────────────────────────────

@router.get("/filters")
def get_filters(params: dict = Depends(get_filter_params)):
    df_raw = get_user_df(params.get("username", "Unknown"))
    if df_raw.empty:
        raise HTTPException(500, "Data not loaded")
    filter_args = {k: v for k, v in params.items() if k != "username"}
    d = filter_raw(df_raw.copy(), **fp(**filter_args))

    def uniq(col):
        return ["ALL"] + sorted(d[col].dropna().astype(str).unique().tolist()) if col in d.columns else ["ALL"]

    vendor_col = COL_VENDOR if (COL_VENDOR in d.columns and d[COL_VENDOR].notna().any()) else COL_LIFNR
    return {
        "companies":  uniq(COL_BUKRS),
        "plants":     uniq(COL_PLANT),
        "matnrs":     uniq(COL_MATNR),
        "bsarts":     uniq(COL_BSART),
        "ekgrps":     uniq(COL_EKGRP),
        "lifnrs":     uniq(COL_LIFNR),
        "vendors":    ["ALL"] + d[vendor_col].dropna().value_counts().index.tolist() if vendor_col in d.columns else ["ALL"],
        "ernams":     uniq(COL_ERNAM),
        "case_ids":   ["ALL"] + sorted(d[COL_CASE].dropna().astype(str).unique().tolist()) if COL_CASE in d.columns else ["ALL"],
        "months":     ["ALL"] + sorted(d["Month"].dropna().astype(str).unique().tolist()) if "Month" in d.columns else ["ALL"],
        "years":      ["ALL"] + sorted(d["Year"].dropna().astype(str).unique().tolist()) if "Year" in d.columns else ["ALL"],
        # ── AUFK-sourced filters ──────────────────────────────────────────────
        "auarts":     uniq(COL_AUART),   # Order Types (PP01, PM01 etc.)
        "kostls":     uniq(COL_KOSTL),   # Cost Centers
        "fevors":     uniq(COL_FEVOR),   # Production Supervisors
        "matnrs_aufk": uniq(COL_MATNR), # Materials (same col, explicit alias for UI)
        "sysstats":   uniq(COL_SYSST),   # System Statuses
    }


# ─── KPIs ────────────────────────────────────────────────────────────────────
# NOTE: Quantity KPIs (GAMNG, MAX_GAMNG etc.) are excluded per requirement.

@router.get("/kpis")
def get_kpis(params: dict = Depends(get_filter_params)):
    df_raw = get_user_df(params.get("username", "Unknown"))
    if df_raw.empty:
        raise HTTPException(500, "Data not loaded")
    filter_args = {k: v for k, v in params.items() if k != "username"}
    d = filter_raw(df_raw.copy(), **fp(**filter_args))

    # ── Core counts ──
    total_orders      = unique_cases(d)           # unique AUFNR+POSNR items
    orders_created    = col_unique_cases(d, "Production Order Creation")
    orders_planned    = col_unique_cases(d, "Basic Start")   # 0 when AFKO not uploaded
    orders_confirmed  = col_unique_cases(d, "Order Confirmation")
    goods_issued      = col_unique_cases(d, "Goods Issued to Order")
    finished_goods    = col_unique_cases(d, "Finished Goods Receipt")
    pr_created        = col_unique_cases(d, "PR Creation")
    po_created        = col_unique_cases(d, "PO Creation")
    gr_postings       = col_unique_cases(d, "GR Posting")
    invoices_posted   = col_unique_cases(d, "Invoice Posting")

    # ── Reversals ──
    reversal_cols = [
        "Operation Reversed", "Goods Issued Reversed",
        "Finished Goods Reversed", "PR Reversal Date",
        "PO Reversal Date", "GR Reversal Date", "Invoice Reversal Date",
    ]
    total_reversals = sum(col_unique_cases(d, c) for c in reversal_cols)

    # ── Sequence / quality violations ──
    # 1. GI before order confirmation (incorrect sequence)
    gi_before_confirm = 0
    if "Order Confirmation" in d.columns and "Goods Issued to Order" in d.columns:
        sub = d.dropna(subset=["Order Confirmation", "Goods Issued to Order"])
        gi_before_confirm = int((sub["Goods Issued to Order"] < sub["Order Confirmation"]).sum())

    # 2. FG receipt without GI (Finished Goods received but no Goods Issue to Order)
    fg_without_gi = 0
    if "Finished Goods Receipt" in d.columns and "Goods Issued to Order" in d.columns:
        fg_without_gi = int((d["Finished Goods Receipt"].notna() & d["Goods Issued to Order"].isna()).sum())

    # 3. GR without Invoice
    gr_no_invoice = 0
    inv_no_gr     = 0
    if "GR Posting" in d.columns and "Invoice Posting" in d.columns:
        gr_no_invoice = int((d["GR Posting"].notna() & d["Invoice Posting"].isna()).sum())
        inv_no_gr     = int((d["Invoice Posting"].notna() & d["GR Posting"].isna()).sum())

    # 4. PO without PR
    po_without_pr = 0
    if "PO Creation" in d.columns and "PR Creation" in d.columns:
        po_without_pr = int((d["PO Creation"].notna() & d["PR Creation"].isna()).sum())

    # ── Average completion time: Production Order Creation→FG→QI (with fallback to Basic Start / Confirm→FG→QI) ──
    avg_days = 0.0
    start_col = "Production Order Creation" if "Production Order Creation" in d.columns and d["Production Order Creation"].notna().any() else (
        "Basic Start" if "Basic Start" in d.columns and d["Basic Start"].notna().any() else "Order Confirmation"
    )
    end_col = "Finished Goods to Quality Inspection" if (
        "Finished Goods to Quality Inspection" in d.columns and
        d["Finished Goods to Quality Inspection"].notna().any()
    ) else "Finished Goods Receipt"
    if start_col in d.columns and end_col in d.columns:
        sub = d.dropna(subset=[start_col, end_col]).copy()
        sub["_dur"] = (sub[end_col] - sub[start_col]).dt.days
        valid = sub[sub["_dur"] >= 0]["_dur"]
        if not valid.empty:
            avg_days = round(float(valid.mean()), 1)

    # ── AUFK-based KPIs: Schedule adherence ──────────────────────────────────
    # Planned finish: AUFK.GLTRP  |  Actual finish: AUFK.LTRMI or FG→QI
    planned_col = COL_GLTRP   # "Planned Finish Date"
    actual_col  = COL_LTRMI   # "Actual Finish Date"
    # Fall back to process event if LTRMI absent
    if actual_col not in d.columns or d[actual_col].isna().all():
        actual_col = "Finished Goods to Quality Inspection" if (
            "Finished Goods to Quality Inspection" in d.columns and
            d["Finished Goods to Quality Inspection"].notna().any()
        ) else "Finished Goods Receipt"

    avg_schedule_slip   = None
    on_time_rate        = None
    total_planned_qty   = None
    unique_order_types  = None

    if planned_col in d.columns and actual_col in d.columns:
        sch = d.dropna(subset=[planned_col, actual_col]).copy()
        sch["slip_days"] = (sch[actual_col] - sch[planned_col]).dt.days
        if not sch.empty:
            avg_schedule_slip = round(float(sch["slip_days"].mean()), 1)
            on_time_count     = int((sch["slip_days"] <= 0).sum())
            on_time_rate      = round(on_time_count / len(sch) * 100, 1)

    if COL_GAMNG in d.columns:
        total_planned_qty = round(float(d[COL_GAMNG].dropna().sum()), 0)

    if COL_AUART in d.columns:
        unique_order_types = int(d[COL_AUART].dropna().nunique())

    return {
        "total_orders":        total_orders,
        "orders_created":      orders_created,
        "orders_planned":      orders_planned,
        "orders_confirmed":    orders_confirmed,
        "goods_issued":        goods_issued,
        "finished_goods":      finished_goods,
        "pr_created":          pr_created,
        "po_created":          po_created,
        "gr_postings":         gr_postings,
        "invoices_posted":     invoices_posted,
        "total_reversals":     total_reversals,
        "gi_before_confirm":   gi_before_confirm,
        "fg_without_gi":       fg_without_gi,
        "gr_no_invoice":       gr_no_invoice,
        "inv_no_gr":           inv_no_gr,
        "po_without_pr":       po_without_pr,
        "avg_completion_days": avg_days,
        # ── AUFK KPIs ────────────────────────────────────────────────────────
        "avg_schedule_slip_days": avg_schedule_slip,  # None when AUFK absent
        "on_time_rate_pct":       on_time_rate,        # None when AUFK absent
        "total_planned_qty":      total_planned_qty,   # None when AUFK absent
        "unique_order_types":     unique_order_types,  # None when AUFK absent
    }


# ─── Cases ────────────────────────────────────────────────────────────────────

@router.get("/cases")
def get_cases(params: dict = Depends(get_filter_params)):
    df_raw = get_user_df(params.get("username", "Unknown"))
    if df_raw.empty:
        raise HTTPException(500, "Data not loaded")
    filter_args = {k: v for k, v in params.items() if k != "username"}
    d = filter_raw(df_raw.copy(), **fp(**filter_args))
    if COL_CASE not in d.columns:
        return []
    time_cols = [c for c in ACTIVITY_COLUMNS if c in d.columns]
    if not time_cols:
        return []
    d["start_date"] = d[time_cols].min(axis=1).dt.strftime("%Y-%m-%d")
    d["end_date"]   = d[time_cols].max(axis=1).dt.strftime("%Y-%m-%d")

    # ── Core identifier columns ───────────────────────────────────────────────
    out_cols = [COL_CASE, "start_date", "end_date"]
    for col in [COL_AUFNR, COL_POSNR]:
        if col in d.columns:
            out_cols.insert(out_cols.index("start_date"), col)

    # ── AUFK enrichment columns (shown when AUFK was uploaded/joined) ─────────
    aufk_display = [
        (COL_AUART,  "order_type"),   # PP01 / PP02 etc.
        (COL_PLANT,  "plant"),
        (COL_MATNR,  "material"),
        (COL_MAKTX,  "material_desc"),
        (COL_GAMNG,  "order_qty"),
        (COL_GMEIN,  "uom"),
        (COL_IGMNG,  "yield_qty"),
        (COL_RMNGA,  "scrap_qty"),
        (COL_GASMG,  "delivered_qty"),
        ("Planned Finish Date", "planned_finish"),
        ("Actual Finish Date",  "actual_finish"),
        (COL_FEVOR,  "supervisor"),
        (COL_KOSTL,  "cost_center"),
        (COL_SYSST,  "sys_status"),
    ]
    rename_map = {COL_CASE: "case_id"}
    if COL_AUFNR in d.columns: rename_map[COL_AUFNR] = "aufnr"
    if COL_POSNR in d.columns: rename_map[COL_POSNR] = "posnr"

    for src_col, alias in aufk_display:
        if src_col in d.columns and d[src_col].notna().any():
            out_cols.append(src_col)
            rename_map[src_col] = alias

    res = (
        d[out_cols]
        .dropna(subset=[COL_CASE])
        .rename(columns=rename_map)
    )
    # Format date columns
    for dc in ["planned_finish", "actual_finish"]:
        if dc in res.columns:
            res[dc] = pd.to_datetime(res[dc], errors="coerce").dt.strftime("%Y-%m-%d")

    return res.sort_values("start_date", ascending=False).head(200).to_dict("records")


@router.get("/case_events")
def get_case_events(case_id: str = Query(...), username: str = Query("Unknown")):
    df_raw = get_user_df(username)
    if df_raw.empty or case_id == "ALL":
        return []
    d = df_raw[df_raw[COL_CASE].astype(str) == case_id].copy()
    if d.empty:
        return []

    cols_to_melt = [c for c in ACTIVITY_COLUMNS if c in d.columns]
    if not cols_to_melt:
        return []

    has_ernam = COL_ERNAM in d.columns
    id_vars = [COL_CASE]
    if has_ernam:
        id_vars.append(COL_ERNAM)

    melted = d.melt(id_vars=id_vars, value_vars=cols_to_melt, var_name="Activity", value_name="Timestamp")
    melted = melted.dropna(subset=["Timestamp"]).drop_duplicates(subset=["Activity", "Timestamp"]).sort_values("Timestamp")

    res = melted[["Activity", "Timestamp"]].copy()
    res["User"] = melted[COL_ERNAM] if has_ernam else "Unknown"
    res["Timestamp"] = res["Timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")
    return res.to_dict("records")


# ─── Chart Endpoints ─────────────────────────────────────────────────────────

def _get_filtered(params: dict):
    username = params.get("username", "Unknown")
    df_raw = get_user_df(username)
    if df_raw.empty:
        raise HTTPException(500, "Data not loaded")
    filter_args = {k: v for k, v in params.items() if k != "username"}
    return filter_raw(df_raw.copy(), **fp(**filter_args))



COMMON_PARAMS = dict(
    username=Query("Unknown"), company=Query(None), plant=Query(None),
    matnr=Query(None), bsart=Query(None), ekgrp=Query(None),
    lifnr=Query(None), vendor=Query(None), case_id=Query(None),
    activity=Query(None), month=Query(None), year=Query(None),
    quarter=Query(None), lead_time=Query(None), status=Query(None), ernam=Query(None),
)


@router.get("/charts/activity")
def chart_activity(params: dict = Depends(get_filter_params)):
    d = _get_filtered(params)
    results = []
    for col in ACTIVITY_COLUMNS:
        if col not in d.columns:
            continue
        occ = int(d[col].notna().sum())
        if occ > 0:
            results.append({"activity": col, "count": occ, "unique_cases": col_unique_cases(d, col)})
    return sorted(results, key=lambda x: x["count"], reverse=True)


@router.get("/charts/monthly")
def chart_monthly(params: dict = Depends(get_filter_params)):
    d = _get_filtered(params)
    cols_to_melt = [c for c in ACTIVITY_COLUMNS if c in d.columns]
    if not cols_to_melt:
        return []
    melted = d.melt(id_vars=[COL_CASE], value_vars=cols_to_melt, value_name="Date").dropna(subset=["Date"])
    melted["Month"] = melted["Date"].dt.to_period("M").astype(str)
    res = melted.groupby("Month")[COL_CASE].nunique().reset_index()
    res.columns = ["Month", "count"]
    return res.sort_values("Month").to_dict("records")


@router.get("/charts/company")
def chart_company(params: dict = Depends(get_filter_params)):
    d = _get_filtered(params)
    if COL_BUKRS not in d.columns:
        return []
    vc = d.dropna(subset=[COL_BUKRS]).groupby(COL_BUKRS)[COL_CASE].nunique().reset_index()
    vc.columns = ["company", "count"]
    return vc.sort_values("count", ascending=False).to_dict("records")


@router.get("/charts/plant")
def chart_plant(params: dict = Depends(get_filter_params)):
    d = _get_filtered(params)
    if COL_PLANT not in d.columns:
        return []
    vc = d.dropna(subset=[COL_PLANT]).groupby(COL_PLANT)[COL_CASE].nunique().reset_index()
    vc.columns = ["plant", "count"]
    return vc.sort_values("count", ascending=False).to_dict("records")


@router.get("/charts/material")
def chart_material(params: dict = Depends(get_filter_params)):
    d = _get_filtered(params)
    if COL_MATNR not in d.columns:
        return []
    vc = d.dropna(subset=[COL_MATNR]).groupby(COL_MATNR)[COL_CASE].nunique().reset_index()
    vc.columns = ["matnr", "count"]
    return vc.sort_values("count", ascending=False).head(30).to_dict("records")


@router.get("/charts/ernam")
def chart_ernam(params: dict = Depends(get_filter_params)):
    d = _get_filtered(params)
    if COL_ERNAM not in d.columns:
        return []
    vc = d.dropna(subset=[COL_ERNAM]).groupby(COL_ERNAM)[COL_CASE].nunique().reset_index()
    vc.columns = ["ernam", "count"]
    return vc.sort_values("count", ascending=False).head(20).to_dict("records")


@router.get("/charts/vendors")
def chart_vendors(params: dict = Depends(get_filter_params)):
    d = _get_filtered(params)
    name_col = None
    if COL_VENDOR in d.columns and d[COL_VENDOR].notna().any():
        name_col = COL_VENDOR
    elif COL_LIFNR in d.columns:
        name_col = COL_LIFNR
    if name_col is None:
        return []
    vc = d.dropna(subset=[name_col]).groupby(name_col)[COL_CASE].nunique().reset_index()
    vc.columns = ["vendor", "count"]
    return vc.sort_values("count", ascending=False).head(30).to_dict("records")


@router.get("/charts/leadtime")
def chart_leadtime(params: dict = Depends(get_filter_params)):
    """Histogram: Basic Start → FG→QI duration (fallback: Order Confirmation → FG→QI)."""
    d = _get_filtered(params)
    _end_col = "Finished Goods to Quality Inpection" if (
        "Finished Goods to Quality Inpection" in d.columns and
        d["Finished Goods to Quality Inpection"].notna().any()
    ) else "Finished Goods Receipts"
    _lt_col = "Basic Start" if "Basic Start" in d.columns and d["Basic Start"].notna().any() else "Order Confirmation"
    if _lt_col not in d.columns or _end_col not in d.columns:
        return []
    lt = d.dropna(subset=[_lt_col, _end_col]).copy()
    lt["days"] = (lt[_end_col] - lt[_lt_col]).dt.days
    lt = lt[lt["days"].between(0, 730)]
    if lt.empty:
        return []
    lt["bucket"] = pd.cut(lt["days"], bins=list(range(0, 740, 15)), right=False)
    hist = lt.groupby("bucket", observed=True).size().reset_index(name="count")
    hist["label"] = hist["bucket"].astype(str)
    return hist[hist["count"] > 0][["label", "count"]].to_dict("records")


@router.get("/charts/happy_path")
def chart_happy_path(params: dict = Depends(get_filter_params)):
    """
    Happy Path: Production Order Creation → Order Confirmation → Basic Start
                → Scheduled Start → Actual Start → Goods Issued to Order
                → Finished Goods Receipt → Finished Goods to Quality Inspection.
    """
    params_copy = params.copy()
    params_copy["status"] = None
    d = _get_filtered(params_copy)

    possible_req = [
        "Production Order Creation",
        "Order Confirmation",
        "Basic Start",
        "Scheduled Start",
        "Actual Start",
        "Goods Issued to Order",
        "Finished Goods Receipt",
        "Finished Goods to Quality Inspection"
    ]
    req = [c for c in possible_req if c in d.columns and d[c].notna().any()]
    rev = [
        "Operation Reversed", "Goods Issued Reversed",
        "Goods transferred to subcontractor", "Finished Goods Reversed",
        "Quality Inspection to Finished Goods",
        "PR Reversal Date", "PO Reversal Date",
    ]

    has_all = pd.Series([True] * len(d), index=d.index)
    is_seq  = pd.Series([True] * len(d), index=d.index)
    prev_col = None

    for c in req:
        if c in d.columns:
            has_all = has_all & d[c].notna()
            if prev_col:
                is_seq = is_seq & (d[c] >= d[prev_col]).fillna(False)
            prev_col = c
        else:
            has_all = pd.Series([False] * len(d), index=d.index)
            is_seq  = pd.Series([False] * len(d), index=d.index)
            break

    has_no_rev = pd.Series([True] * len(d), index=d.index)
    for c in rev:
        if c in d.columns:
            has_no_rev = has_no_rev & d[c].isna()

    happy_mask = has_all & is_seq & has_no_rev
    happy_cases = d[happy_mask][COL_CASE].nunique() if COL_CASE in d.columns else 0
    total_cases = unique_cases(d)

    return [
        {"status": "Happy Path",  "count": happy_cases},
        {"status": "Deviations", "count": total_cases - happy_cases},
    ]


@router.get("/charts/bottleneck")
def chart_bottleneck(params: dict = Depends(get_filter_params)):
    """Avg days between key P2I process steps."""
    d = _get_filtered(params)
    steps = [
        ("Production Order Creation", "Order Confirmation",                 "Creation → Confirm"),
        ("Order Confirmation",        "Basic Start",                        "Confirm → Basic Start"),
        ("Basic Start",               "Scheduled Start",                    "Basic Start → Scheduled Start"),
        ("Scheduled Start",           "Actual Start",                       "Scheduled Start → Actual Start"),
        ("Actual Start",              "Goods Issued to Order",              "Actual Start → GI"),
        ("Goods Issued to Order",     "Finished Goods Receipt",             "GI → FG Receipt"),
        ("Finished Goods Receipt",    "Finished Goods to Quality Inspection", "FG → Quality Inspection"),
        ("Production Order Creation", "Finished Goods to Quality Inspection", "Creation → QI (total)"),
    ]
    results = []
    for from_col, to_col, label in steps:
        if from_col in d.columns and to_col in d.columns:
            sub = d.dropna(subset=[from_col, to_col]).copy()
            sub["days"] = (sub[to_col] - sub[from_col]).dt.days
            sub = sub[sub["days"] >= 0]
            if not sub.empty:
                results.append({
                    "step":        label,
                    "avg_days":    round(float(sub["days"].mean()), 1),
                    "median_days": round(float(sub["days"].median()), 1),
                    "count":       int(len(sub)),
                })
    return results


@router.get("/charts/operation_reversals")
def chart_operation_reversals(params: dict = Depends(get_filter_params)):
    """Operation reversals by month (AFRU: Document Reverse = X)."""
    d = _get_filtered(params)
    if "Operation Reversed" not in d.columns:
        return []
    b = d.dropna(subset=["Operation Reversed"]).copy()
    b["Month"] = b["Operation Reversed"].dt.to_period("M").astype(str)
    res = b.groupby("Month")[COL_CASE].nunique().reset_index(name="count")
    return res.sort_values("Month").to_dict("records")


@router.get("/charts/gi_before_confirm_ernam")
def chart_gi_before_confirm_ernam(params: dict = Depends(get_filter_params)):
    """GI before Order Confirmation sequence violations grouped by ERNAM."""
    d = _get_filtered(params)
    if "Order Confirmation" not in d.columns or "Goods Issued to Order" not in d.columns:
        return []
    sub = d.dropna(subset=["Order Confirmation", "Goods Issued to Order", COL_ERNAM])
    violations = sub[sub["Goods Issued to Order"] < sub["Order Confirmation"]]
    if violations.empty:
        return []
    vc = violations.groupby(COL_ERNAM)[COL_CASE].nunique().reset_index(name="count")
    vc.columns = ["ernam", "count"]
    return vc.sort_values("count", ascending=False).head(20).to_dict("records")


@router.get("/charts/plant_lead_time")
def chart_plant_lead_time(params: dict = Depends(get_filter_params)):
    """Average production lead time per plant (Production Order Creation → FG→QI, fallback Confirm → FG→QI)."""
    d = _get_filtered(params)
    _end_col = "Finished Goods to Quality Inspection" if (
        "Finished Goods to Quality Inspection" in d.columns and
        d["Finished Goods to Quality Inspection"].notna().any()
    ) else "Finished Goods Receipt"
    if _end_col not in d.columns or COL_PLANT not in d.columns:
        return []
    _plt_start = "Production Order Creation" if "Production Order Creation" in d.columns and d["Production Order Creation"].notna().any() else (
        "Basic Start" if "Basic Start" in d.columns and d["Basic Start"].notna().any() else "Order Confirmation"
    )
    if _plt_start not in d.columns:
        return []
    sub = d.dropna(subset=[COL_PLANT, _plt_start, _end_col]).copy()
    sub["days"] = (sub[_end_col] - sub[_plt_start]).dt.days
    sub = sub[sub["days"] >= 0]
    if sub.empty:
        return []
    agg = sub.groupby(COL_PLANT).agg(avg_days=("days", "mean"), case_count=(COL_CASE, "nunique")).reset_index()
    agg.columns = ["plant", "avg_days", "case_count"]
    agg["avg_days"] = agg["avg_days"].round(1)
    return agg.sort_values("avg_days", ascending=False).head(25).to_dict("records")


@router.get("/charts/po_rev_timeline")
def chart_po_rev_timeline(params: dict = Depends(get_filter_params)):
    d = _get_filtered(params)
    if "PO Reversal Date" not in d.columns:
        return []
    b = d.dropna(subset=["PO Reversal Date"]).copy()
    b["Month"] = b["PO Reversal Date"].dt.to_period("M").astype(str)
    res = b.groupby("Month")[COL_CASE].nunique().reset_index(name="count")
    return res.sort_values("Month").to_dict("records")



# ─── AUFK Chart Endpoints ─────────────────────────────────────────────────────

@router.get("/charts/order_type")
def chart_order_type(params: dict = Depends(get_filter_params)):
    """Production order count by SAP order type (AUART) — from AUFK."""
    d = _get_filtered(params)
    if COL_AUART not in d.columns or d[COL_AUART].isna().all():
        return []
    vc = d.dropna(subset=[COL_AUART]).groupby(COL_AUART)[COL_CASE].nunique().reset_index()
    vc.columns = ["order_type", "count"]
    return vc.sort_values("count", ascending=False).head(20).to_dict("records")


@router.get("/charts/schedule_adherence")
def chart_schedule_adherence(params: dict = Depends(get_filter_params)):
    """
    Schedule slip per month: avg(Actual Finish Date − Planned Finish Date) in days.
    Planned = AUFK.GLTRP  |  Actual = AUFK.LTRMI (falls back to FG→QI event date).
    Positive slip = late; negative = early.
    """
    d = _get_filtered(params)

    planned_col = COL_GLTRP  # "Planned Finish Date"
    actual_col  = COL_LTRMI  # "Actual Finish Date"
    if actual_col not in d.columns or d[actual_col].isna().all():
        actual_col = "Finished Goods to Quality Inspection" if (
            "Finished Goods to Quality Inspection" in d.columns and
            d["Finished Goods to Quality Inspection"].notna().any()
        ) else "Finished Goods Receipt"

    if planned_col not in d.columns or actual_col not in d.columns:
        return []

    s = d.dropna(subset=[planned_col, actual_col]).copy()
    if s.empty:
        return []
    s["slip_days"] = (s[actual_col] - s[planned_col]).dt.days
    s["Month"] = s[planned_col].dt.to_period("M").astype(str)

    res = (
        s.groupby("Month")
        .agg(
            avg_slip=("slip_days", "mean"),
            on_time=("slip_days", lambda x: (x <= 0).sum()),
            total=("slip_days", "count"),
        )
        .reset_index()
    )
    res["avg_slip"]       = res["avg_slip"].round(1)
    res["on_time_rate"]   = (res["on_time"] / res["total"] * 100).round(1)
    return res.sort_values("Month").to_dict("records")


@router.get("/charts/order_quantity")
def chart_order_quantity(params: dict = Depends(get_filter_params)):
    """Histogram of planned order quantity (GAMNG) from AUFK."""
    d = _get_filtered(params)

    if COL_GAMNG not in d.columns or d[COL_GAMNG].isna().all():
        return []

    qty = d[COL_GAMNG].dropna()
    if qty.empty:
        return []

    p95 = qty.quantile(0.95)
    bins = pd.cut(qty.clip(upper=p95), bins=20)
    hist = bins.value_counts(sort=False).reset_index()
    hist.columns = ["bucket", "count"]
    hist["label"] = hist["bucket"].astype(str)
    return hist[hist["count"] > 0][["label", "count"]].to_dict("records")


@router.get("/charts/cost_center")
def chart_cost_center(params: dict = Depends(get_filter_params)):
    """Production order count by Cost Center (KOSTL) — from AUFK."""
    d = _get_filtered(params)
    if COL_KOSTL not in d.columns or d[COL_KOSTL].isna().all():
        return []
    vc = d.dropna(subset=[COL_KOSTL]).groupby(COL_KOSTL)[COL_CASE].nunique().reset_index()
    vc.columns = ["cost_center", "count"]
    return vc.sort_values("count", ascending=False).head(20).to_dict("records")




# ─── AUFK Standalone Upload & Join ────────────────────────────────────────────

@router.post("/upload/aufk")
async def upload_aufk(
    file: UploadFile = File(...),
    username: str = Form("Unknown"),
):
    """
    Upload a standalone AUFK (Production Order Header) CSV or Excel file.
    The backend reads the required columns, renames GLTRP→'Planned Finish Date'
    and LTRMI→'Actual Finish Date', then LEFT-JOINs on AUFNR into the
    already-loaded event-log DataFrame for this user.

    Accepted AUFK columns (all optional except AUFNR):
      AUFNR, AUART, WERKS, MATNR, MAKTX, GAMNG, GMEIN, GASMG, GISMG, IGMNG,
      RMNGA, ERDAT, GSTRP, GLTRP, FTRMS, LTRMI, FEVOR, KOSTL, GSBER,
      SYSST, ANWST, LOEKZ
    """
    fname = file.filename.lower()
    content = await file.read()
    try:
        if fname.endswith(".csv"):
            for enc in ("utf-8", "latin-1", "windows-1252"):
                try:
                    aufk_raw = pd.read_csv(io.BytesIO(content), encoding=enc, low_memory=False)
                    break
                except UnicodeDecodeError:
                    continue
            else:
                raise ValueError("Could not decode AUFK CSV.")
        elif fname.endswith((".xlsx", ".xls")):
            aufk_raw = pd.read_excel(io.BytesIO(content))
        else:
            raise HTTPException(400, "Only CSV or Excel (.xlsx/.xls) files are supported for AUFK upload.")

        # Strip whitespace from column names
        aufk_raw.columns = aufk_raw.columns.str.strip()

        if "AUFNR" not in aufk_raw.columns:
            raise HTTPException(400, "AUFK file must contain an AUFNR column.")

        # Keep only known AUFK columns that exist in the file
        avail_cols = ["AUFNR"] + [c for c in AUFK_COLS if c in aufk_raw.columns and c != "AUFNR"]
        aufk = aufk_raw[avail_cols].copy()

        # Normalise AUFNR join key (strip, strip leading zeros to match event log)
        aufk["AUFNR"] = aufk["AUFNR"].astype(str).str.strip()

        # Rename GLTRP → "Planned Finish Date", LTRMI → "Actual Finish Date"
        aufk = aufk.rename(columns=AUFK_DATE_RENAME)

        # Parse date columns
        date_cols = [v for v in AUFK_DATE_RENAME.values()] + ["ERDAT", "GSTRP", "FTRMS"]
        for dc in date_cols:
            if dc in aufk.columns:
                aufk[dc] = pd.to_datetime(aufk[dc], errors="coerce")

        # Parse numeric columns
        num_cols = ["GAMNG", "GASMG", "GISMG", "IGMNG", "RMNGA"]
        for nc in num_cols:
            if nc in aufk.columns:
                aufk[nc] = pd.to_numeric(aufk[nc], errors="coerce")

        # Drop rows with no order number
        aufk = aufk.dropna(subset=["AUFNR"])
        aufk = aufk.drop_duplicates(subset=["AUFNR"])

        # Get the current loaded DataFrame for this user
        df_existing = get_user_df(username)
        if df_existing.empty:
            raise HTTPException(400, "No event-log data loaded. Upload your P2I event log first, then upload AUFK.")

        if COL_AUFNR not in df_existing.columns:
            raise HTTPException(400, "Loaded event log does not contain an AUFNR column — cannot join AUFK.")

        # Normalise the event-log AUFNR key too
        df_existing = df_existing.copy()
        df_existing["_aufnr_join"] = df_existing[COL_AUFNR].astype(str).str.strip()
        aufk["_aufnr_join"] = aufk["AUFNR"]

        # Drop AUFK columns that already exist in the event log (except join key)
        aufk_cols_to_join = [c for c in aufk.columns
                             if c not in df_existing.columns and c != "AUFNR" and c != "_aufnr_join"]
        aufk_slim = aufk[["_aufnr_join"] + aufk_cols_to_join].copy()

        df_joined = df_existing.merge(aufk_slim, on="_aufnr_join", how="left")
        df_joined = df_joined.drop(columns=["_aufnr_join"], errors="ignore")

        # Re-derive Month/Year/Quarter from _ts (process_df already ran, just preserve)
        USER_DFS[username] = df_joined
        USER_DFS["Unknown"] = df_joined

        joined_pct = round(df_joined[aufk_cols_to_join[0]].notna().mean() * 100, 1) if aufk_cols_to_join else 0
        new_cols = aufk_cols_to_join
        log_audit(username, "UPLOAD_AUFK",
                  f"AUFK joined: {len(aufk)} header rows, {len(new_cols)} new columns added, {joined_pct}% rows matched")

        return {
            "status":        "ok",
            "aufk_rows":     len(aufk),
            "event_log_rows": len(df_joined),
            "new_columns":   new_cols,
            "match_rate_pct": joined_pct,
        }

    except HTTPException:
        raise
    except Exception as e:
        import traceback
        print(f"[P2I AUFK ERROR] {e}\n{traceback.format_exc()}")
        raise HTTPException(500, f"AUFK upload failed: {str(e)}")


@router.get("/charts/supervisor")
def chart_supervisor(params: dict = Depends(get_filter_params)):
    """Production order count + avg lead time per Production Supervisor (FEVOR) — from AUFK."""
    d = _get_filtered(params)
    if COL_FEVOR not in d.columns or d[COL_FEVOR].isna().all():
        return []

    _end_col = "Finished Goods to Quality Inpection" if (
        "Finished Goods to Quality Inpection" in d.columns and
        d["Finished Goods to Quality Inpection"].notna().any()
    ) else "Finished Goods Receipts"
    _st_col = "Basic Start" if "Basic Start" in d.columns and d["Basic Start"].notna().any() else "Order Confirmation"

    base = d.dropna(subset=[COL_FEVOR])
    agg = base.groupby(COL_FEVOR)[COL_CASE].nunique().reset_index(name="count")

    if _st_col in d.columns and _end_col in d.columns:
        lt = base.dropna(subset=[_st_col, _end_col]).copy()
        lt["days"] = (lt[_end_col] - lt[_st_col]).dt.days
        lt = lt[lt["days"] >= 0]
        lt_agg = lt.groupby(COL_FEVOR)["days"].mean().reset_index()
        lt_agg.columns = [COL_FEVOR, "avg_lead_days"]
        lt_agg["avg_lead_days"] = lt_agg["avg_lead_days"].round(1)
        agg = agg.merge(lt_agg, on=COL_FEVOR, how="left")

    # Deviation rate per supervisor
    rev_cols = ["Operation Reversal", "Goods Issue Reversed", "Finished Goods Reversal",
                "Goods transfer to Sub Contractor", "QI to FG"]
    present_rev = [c for c in rev_cols if c in d.columns]
    if present_rev:
        base2 = base.copy()
        base2["_has_dev"] = base2[present_rev].notna().any(axis=1)
        dev_agg = base2.groupby(COL_FEVOR)["_has_dev"].mean().reset_index()
        dev_agg.columns = [COL_FEVOR, "deviation_rate_pct"]
        dev_agg["deviation_rate_pct"] = (dev_agg["deviation_rate_pct"] * 100).round(1)
        agg = agg.merge(dev_agg, on=COL_FEVOR, how="left")

    agg.columns = [c if c != COL_FEVOR else "supervisor" for c in agg.columns]
    return agg.sort_values("count", ascending=False).head(30).to_dict("records")


@router.get("/charts/quantity_trend")
def chart_quantity_trend(params: dict = Depends(get_filter_params)):
    """
    Monthly trend of:
      - GAMNG  (planned order qty)
      - IGMNG  (confirmed yield / actual good qty)
      - RMNGA  (scrap qty)
    Only returned when AUFK has been joined (GAMNG column present).
    """
    d = _get_filtered(params)

    if COL_GAMNG not in d.columns or d[COL_GAMNG].isna().all():
        return []

    # Use the order creation / basic start date for the month bucket
    date_col = None
    for dc in ["ERDAT", "GSTRP", "Basic Start", "Order Creation", "Order Confirmation"]:
        if dc in d.columns and d[dc].notna().any():
            date_col = dc
            break
    if date_col is None:
        return []

    tmp = d.dropna(subset=[date_col, COL_GAMNG]).copy()
    tmp["Month"] = pd.to_datetime(tmp[date_col], errors="coerce").dt.to_period("M").astype(str)
    tmp = tmp.dropna(subset=["Month"])

    agg_dict = {COL_GAMNG: "sum"}
    if COL_IGMNG in tmp.columns: agg_dict[COL_IGMNG] = "sum"
    if COL_RMNGA in tmp.columns: agg_dict[COL_RMNGA] = "sum"

    res = tmp.groupby("Month").agg(agg_dict).reset_index()
    rename = {"Month": "month", COL_GAMNG: "planned_qty"}
    if COL_IGMNG in res.columns: rename[COL_IGMNG] = "yield_qty"
    if COL_RMNGA in res.columns: rename[COL_RMNGA] = "scrap_qty"
    res = res.rename(columns=rename)

    for c in ["planned_qty", "yield_qty", "scrap_qty"]:
        if c in res.columns:
            res[c] = res[c].round(0).astype(int)

    return res.sort_values("month").to_dict("records")


@router.get("/charts/yield_analysis")
def chart_yield_analysis(params: dict = Depends(get_filter_params)):
    """
    Yield vs Scrap analysis: top materials by planned qty with yield_rate and scrap_rate.
    Only returned when AUFK has been joined.
    """
    d = _get_filtered(params)

    if COL_GAMNG not in d.columns or d[COL_GAMNG].isna().all():
        return []
    if COL_MATNR not in d.columns:
        return []

    tmp = d.dropna(subset=[COL_MATNR, COL_GAMNG]).copy()
    agg_dict = {COL_GAMNG: "sum", COL_CASE: "nunique"}
    if COL_IGMNG in tmp.columns: agg_dict[COL_IGMNG] = "sum"
    if COL_RMNGA in tmp.columns: agg_dict[COL_RMNGA] = "sum"

    res = tmp.groupby(COL_MATNR).agg(agg_dict).reset_index()
    res = res.rename(columns={COL_MATNR: "material", COL_GAMNG: "planned_qty", COL_CASE: "orders"})
    if COL_IGMNG in agg_dict: res = res.rename(columns={COL_IGMNG: "yield_qty"})
    if COL_RMNGA in agg_dict: res = res.rename(columns={COL_RMNGA: "scrap_qty"})

    res = res.sort_values("planned_qty", ascending=False).head(20)

    if "yield_qty" in res.columns and "planned_qty" in res.columns:
        res["yield_rate_pct"] = (res["yield_qty"] / res["planned_qty"].replace(0, float("nan")) * 100).round(1)
    if "scrap_qty" in res.columns and "planned_qty" in res.columns:
        res["scrap_rate_pct"] = (res["scrap_qty"] / res["planned_qty"].replace(0, float("nan")) * 100).round(1)

    return res.to_dict("records")


# ─── Process Map ──────────────────────────────────────────────────────────────

@router.get("/process-map")
def get_process_map(params: dict = Depends(get_filter_params)):
    df_raw = get_user_df(params.get("username", "Unknown"))
    if df_raw.empty:
        raise HTTPException(500, "Data not loaded")
    filter_args = {k: v for k, v in params.items() if k != "username"}
    d = filter_raw(df_raw.copy(), **fp(**filter_args))

    # ── LAYOUT_H: LR mode — happy path left→right, deviations above, P2P continuing right
    LAYOUT_H = {
        # ── Happy Path (y = 340) ─────────────────────────────────────────────
        "Production Order Creation":             {"x": 50,    "y": 340},
        "Order Confirmation":                    {"x": 800,   "y": 340},
        "Basic Start":                           {"x": 1550,  "y": 340},
        "Scheduled Start":                       {"x": 2300,  "y": 340},
        "Actual Start":                          {"x": 3050,  "y": 340},
        "Goods Issued to Order":                 {"x": 3800,  "y": 340},
        "Finished Goods Receipt":                {"x": 4550,  "y": 340},
        "Finished Goods to Quality Inspection":  {"x": 5300,  "y": 340},
        # ── Deviations (above, y=80 / y=-180 for stacked pair) ───────────────
        "Operation Reversed":                    {"x": 800,  "y": 80},   # above Order Confirmation
        "Goods Issued Reversed":                 {"x": 3800, "y": 80},   # above Goods Issued
        "Goods transferred to subcontractor":    {"x": 3800, "y": -180}, # stacked above GI Reversed
        "Finished Goods Reversed":               {"x": 4550, "y": 80},   # above FG Receipt
        "Quality Inspection to Finished Goods":  {"x": 5300, "y": 80},   # above FG→QI
        # ── P2P Branch (y=340, continuing right from FG→QI) ─────────────────
        "PR Creation":                           {"x": 6050,  "y": 340},
        "PR Release Date":                       {"x": 6800,  "y": 340},
        "PO Creation":                           {"x": 7550,  "y": 340},
        "PO Date":                               {"x": 8300,  "y": 340},
        "GR Posting":                            {"x": 9050,  "y": 340},
        "Invoice Posting":                       {"x": 9800,  "y": 340},
        "PR Reversal Date":                      {"x": 6800,  "y": 80},
        "PO Reversal Date":                      {"x": 8300,  "y": 80},
        "GR Reversal Date":                      {"x": 9050,  "y": 80},
        "Invoice Reversal Date":                 {"x": 9800,  "y": 80},
    }

    # ── LAYOUT_V: TB mode — centre = happy path, left = deviations, right = P2P
    # NOTE: frontend FIXED_POS_TB takes precedence for all known nodes.
    # These coords are used only for any future/unknown columns.
    NODE_H_PY = 310
    LAYOUT_V = {
        # ── Centre (x = 650) — happy path ────────────────────────────────────
        "Production Order Creation":             {"x": 650, "y": 50 + NODE_H_PY * 0},
        "Order Confirmation":                    {"x": 650, "y": 50 + NODE_H_PY * 1},
        "Basic Start":                           {"x": 650, "y": 50 + NODE_H_PY * 2},
        "Scheduled Start":                       {"x": 650, "y": 50 + NODE_H_PY * 3},
        "Actual Start":                          {"x": 650, "y": 50 + NODE_H_PY * 4},
        "Goods Issued to Order":                 {"x": 650, "y": 50 + NODE_H_PY * 5},
        "Finished Goods Receipt":                {"x": 650, "y": 50 + NODE_H_PY * 6},
        "Finished Goods to Quality Inspection":  {"x": 650, "y": 50 + NODE_H_PY * 7},
        # ── Left (x=60 / x=-580 for stacked pair) — deviations ───────────────
        "Operation Reversed":                    {"x": 60,   "y": 50 + NODE_H_PY * 1},
        "Goods Issued Reversed":                 {"x": 60,   "y": 50 + NODE_H_PY * 5},
        "Goods transferred to subcontractor":    {"x": -580, "y": 50 + NODE_H_PY * 5},
        "Finished Goods Reversed":               {"x": 60,   "y": 50 + NODE_H_PY * 6},
        "Quality Inspection to Finished Goods":  {"x": 60,   "y": 50 + NODE_H_PY * 7},
        # ── Right (x=1300 / x=1950 for reversals) — P2P branch ───────────────
        "PR Creation":           {"x": 1300, "y": 50 + NODE_H_PY * 0},
        "PR Release Date":       {"x": 1300, "y": 50 + NODE_H_PY * 1},
        "PR Reversal Date":      {"x": 1950, "y": 50 + NODE_H_PY * 1},
        "PO Creation":           {"x": 1300, "y": 50 + NODE_H_PY * 2},
        "PO Date":               {"x": 1300, "y": 50 + NODE_H_PY * 3},
        "PO Reversal Date":      {"x": 1950, "y": 50 + NODE_H_PY * 3},
        "GR Posting":            {"x": 1300, "y": 50 + NODE_H_PY * 4},
        "GR Reversal Date":      {"x": 1950, "y": 50 + NODE_H_PY * 4},
        "Invoice Posting":       {"x": 1300, "y": 50 + NODE_H_PY * 5},
        "Invoice Reversal Date": {"x": 1950, "y": 50 + NODE_H_PY * 5},
    }

    cols_to_melt = [c for c in ACTIVITY_COLUMNS if c in d.columns]
    melted = d.melt(id_vars=[COL_CASE], value_vars=cols_to_melt,
                    var_name="Activity", value_name="Activitytime")
    melted = melted.dropna(subset=["Activitytime"])

    act_order = {col: i for i, col in enumerate(ACTIVITY_COLUMNS)}
    melted["Act_Idx"] = melted["Activity"].map(act_order).fillna(99)
    melted = melted.sort_values(by=[COL_CASE, "Activitytime", "Act_Idx"])

    melted["Next_Activity"]     = melted.groupby(COL_CASE)["Activity"].shift(-1)
    melted["Next_Activitytime"] = melted.groupby(COL_CASE)["Activitytime"].shift(-1)

    transitions = melted.dropna(subset=["Next_Activity"]).copy()
    edge_freqs  = transitions.groupby(["Activity", "Next_Activity"])[COL_CASE].nunique().reset_index(name="frequency")

    transitions["duration_days"] = (
        (transitions["Next_Activitytime"] - transitions["Activitytime"]).dt.total_seconds() / 86400
    )
    avg_dur = (
        transitions[transitions["duration_days"] >= 0]
        .groupby(["Activity", "Next_Activity"])["duration_days"]
        .mean()
        .reset_index()
    )
    avg_dur.columns = ["Activity", "Next_Activity", "avg_days"]
    edge_freqs = edge_freqs.merge(avg_dur, on=["Activity", "Next_Activity"], how="left")

    present_acts = set(melted["Activity"].unique()) | MAIN_NODES
    nodes_out = []
    for name in present_acts:
        pos_h = LAYOUT_H.get(name, {"x": 5000, "y": 500})
        pos_v = LAYOUT_V.get(name, {"x": 900, "y": 2000})
        count = melted[melted["Activity"] == name][COL_CASE].nunique()
        nodes_out.append({
            "id": name, "label": name,
            "position_h": pos_h, "position_v": pos_v,
            "is_main": name in MAIN_NODES,
            "frequency": count,
        })

    edges_out = []
    for _, row in edge_freqs.iterrows():
        avg_d = round(float(row["avg_days"]), 1) if pd.notna(row.get("avg_days")) else None
        edges_out.append({
            "id":        f"{row['Activity']}--{row['Next_Activity']}",
            "source":    row["Activity"],
            "target":    row["Next_Activity"],
            "frequency": int(row["frequency"]),
            "avg_days":  avg_d,
        })

    return {"nodes": nodes_out, "edges": edges_out}
