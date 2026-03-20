import io
import os
import json
import pandas as pd
import numpy as np
from fastapi import APIRouter, Query, HTTPException, UploadFile, File, Form
from pydantic import BaseModel
from typing import Optional
import warnings
import re
from datetime import datetime

warnings.filterwarnings("ignore")

router = APIRouter(prefix="/o2c", tags=["Order-to-Cash"])

# ─── Server-side CSV Output Directory ────────────────────────────────────────
O2C_OUTPUT_DIR = os.path.join("o2c_user_data", "o2c_outputs")
os.makedirs(O2C_OUTPUT_DIR, exist_ok=True)

def _save_output_csv(df: pd.DataFrame, filename: str) -> str:
    os.makedirs(O2C_OUTPUT_DIR, exist_ok=True)
    export_df = df[[c for c in df.columns if not c.startswith("_")]].copy()
    for c in export_df.columns:
        if pd.api.types.is_datetime64_any_dtype(export_df[c]):
            export_df[c] = export_df[c].dt.strftime("%Y-%m-%d")
    out_path = os.path.join(O2C_OUTPUT_DIR, filename)
    export_df.to_csv(out_path, index=False)
    print(f"[O2C OUTPUT] CSV saved → {out_path}")
    return out_path

# ─── Audit Logging ────────────────────────────────────────────────────────────
AUDIT_FILE = "o2c_audit_logs.json"

def log_audit(username: str, action: str, details: str):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[O2C AUDIT] {timestamp} | {username} | {action} | {details}")
    logs = []
    if os.path.exists(AUDIT_FILE):
        try:
            with open(AUDIT_FILE, "r") as f:
                logs = json.load(f)
        except Exception:
            logs = []
    logs.append({"timestamp": timestamp, "username": username,
                 "action": action, "details": details})
    try:
        with open(AUDIT_FILE, "w") as f:
            json.dump(logs, f, indent=4)
    except Exception as e:
        print(f"[O2C ERROR] Failed to write audit log: {e}")

class AuditAction(BaseModel):
    username: str
    action: str
    details: str

@router.post("/log")
def log_action(data: AuditAction):
    log_audit(data.username, data.action, data.details)
    return {"status": "logged"}

# ─── Column Config ────────────────────────────────────────────────────────────
COL_CASE     = "Subsequent Document"
COL_CUSTOMER = "NAME1"
COL_VKORG    = "VKORG"
COL_AUART    = "Sales Document Type"
COL_MATKL    = "MATNR"
COL_WERKS    = "WERKS"
COL_ERNAM    = "Sales Document Maker"

HAPPY_PATH_ACTIVITIES = [
    "SO Created", "SO Approved", "Delivery Created", "Delivery Posted",
    "Goods Issued", "Invoice Created", "Invoice Posted", "Invoice Cleared",
]
DEVIATION_ACTIVITIES = [
    "SO Reversed", "SO Reversed After GI", "Delivery Returned", "GI Reversed",
    "Invoice Reversed", "Credit Memo", "Debit Memo",
]
ACTIVITY_COLUMNS = HAPPY_PATH_ACTIVITIES + DEVIATION_ACTIVITIES
MAIN_NODES = set(HAPPY_PATH_ACTIVITIES)


def parse_o2c_dates(df: pd.DataFrame) -> pd.DataFrame:
    """Parse all raw SAP date columns to datetime — fully vectorised."""
    date_hints = [
        "Sales Document Creation Date", "Delivery Blocked Date", "Billing Block Date",
        "Delivery Creation Date", "WADAT", "Goods Movement Date",
        "Invoice Creation Date", "Clearing Date",
        "Invoice Reversal Date", "Credit Memo Date", "Debit Memo Date",
        "Sales Order Rejected Date", "Delivery Return Oder Date", "Return Oder Date",
        "Delivery Document Creation Date", "Goods Issued", "GI Reversed",
        "Item Changed Date", "Header Changed Date",
    ]
    for c in date_hints:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce")
    return df


def build_activity_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Map raw SAP date columns → named activity columns.
    All operations are fully vectorised — NO row-wise apply/loops.
    """
    # ── Happy path ──────────────────────────────────────────────────────────
    df["SO Created"] = (
        df["Sales Document Creation Date"].copy()
        if "Sales Document Creation Date" in df.columns
        else pd.NaT
    )

    # SO Approved = SO Creation Date where neither delivery nor billing block exists
    no_del = (
        df["Delivery Block"].isna() | (df["Delivery Block"].astype(str).str.strip() == "")
        if "Delivery Block" in df.columns
        else pd.Series(True, index=df.index)
    )
    no_bil = (
        df["Billing Block"].isna() | (df["Billing Block"].astype(str).str.strip() == "")
        if "Billing Block" in df.columns
        else pd.Series(True, index=df.index)
    )
    so_created = (
        df["Sales Document Creation Date"]
        if "Sales Document Creation Date" in df.columns
        else pd.Series(pd.NaT, index=df.index)
    )
    df["SO Approved"] = so_created.where(no_del & no_bil)

    df["Delivery Created"] = df["Delivery Creation Date"].copy() if "Delivery Creation Date" in df.columns else pd.NaT
    df["Delivery Posted"]  = df["WADAT"].copy()                  if "WADAT"                  in df.columns else pd.NaT
    # Goods Issued: prefer Goods Movement Date (VBFA VBTYP_N=R), fallback to WADAT_IST from LIKP
    if "Goods Movement Date" in df.columns:
        gi_base = df["Goods Movement Date"].copy()
        if "Goods Issued" in df.columns:
            gi_base = gi_base.fillna(df["Goods Issued"])
        df["Goods Issued"] = gi_base
    elif "Goods Issued" in df.columns:
        pass  # already present from LIKP WADAT_IST
    else:
        df["Goods Issued"] = pd.NaT
    df["Invoice Created"]  = df["Invoice Creation Date"].copy()  if "Invoice Creation Date"  in df.columns else pd.NaT

    # Invoice Posted = Clearing Date (accounting posting)
    df["Invoice Posted"] = df["Clearing Date"].copy() if "Clearing Date" in df.columns else pd.NaT

    # Invoice Cleared = Clearing Date where a clearing document or amount exists
    if "Clearing Date" in df.columns and "Amount in Local Currency" in df.columns:
        df["Invoice Cleared"] = df["Clearing Date"].where(df["Amount in Local Currency"].notna())
    elif "Clearing Date" in df.columns and "Clearing Document Number" in df.columns:
        df["Invoice Cleared"] = df["Clearing Date"].where(df["Clearing Document Number"].notna())
    elif "Clearing Date" in df.columns:
        df["Invoice Cleared"] = df["Clearing Date"].copy()
    else:
        df["Invoice Cleared"] = pd.NaT

    # ── Deviations ──────────────────────────────────────────────────────────
    if "Sales Order Rejected Date" in df.columns:
        df["SO Reversed"] = df["Sales Order Rejected Date"].copy()
    elif "Reason for Rejection" in df.columns:
        rej = df["Reason for Rejection"].notna() & (df["Reason for Rejection"].astype(str).str.strip() != "")
        df["SO Reversed"] = so_created.where(rej)
    else:
        df["SO Reversed"] = pd.NaT

    # SO Reversed After GI = SO Reversed date where reversal happened after GI
    so_rev = df["SO Reversed"] if "SO Reversed" in df.columns else pd.Series(pd.NaT, index=df.index)
    gi     = df["Goods Issued"] if "Goods Issued" in df.columns else pd.Series(pd.NaT, index=df.index)
    df["SO Reversed After GI"] = so_rev.where(so_rev.notna() & gi.notna() & (so_rev >= gi))

    # Delivery Returned
    for src in ["Delivery Return Oder Date", "Return Oder Date"]:
        if src in df.columns:
            df["Delivery Returned"] = df[src].copy()
            break
    else:
        df["Delivery Returned"] = pd.NaT

    # GI Reversed — already a date from the transformer (VBTYP_N = 'h')
    if "GI Reversed" in df.columns:
        raw = df["GI Reversed"]
        if raw.dtype == object:
            # Could be a flag string in old CSV uploads
            flag = raw.astype(str).str.strip().str.upper().isin(["Y","X","1","TRUE","YES"])
            df["GI Reversed"] = gi.where(flag)
        else:
            df["GI Reversed"] = pd.to_datetime(raw, errors="coerce")
    else:
        df["GI Reversed"] = pd.NaT

    df["Invoice Reversed"] = df["Invoice Reversal Date"].copy() if "Invoice Reversal Date" in df.columns else pd.NaT
    df["Credit Memo"]      = df["Credit Memo Date"].copy()      if "Credit Memo Date"      in df.columns else pd.NaT
    df["Debit Memo"]       = df["Debit Memo Date"].copy()       if "Debit Memo Date"       in df.columns else pd.NaT

    return df


def process_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Processes the WIDE dataframe from the transformer (or a pre-built CSV upload).
    Fully vectorised — no row-wise apply loops.
    """
    print(f"[O2C PROCESS] Processing {len(df):,} rows …")

    # Ensure case key
    if "Subsequent Document" in df.columns:
        df[COL_CASE] = df["Subsequent Document"].astype(str)
    else:
        print("[O2C WARNING] 'Subsequent Document' not found — using index as fallback.")
        df[COL_CASE] = df.index.astype(str)

    df = parse_o2c_dates(df)
    df = build_activity_columns(df)

    # ── Month / Year / Quarter ── (vectorised, no apply)
    act_cols = [c for c in ACTIVITY_COLUMNS if c in df.columns]
    if act_cols:
        # Stack all activity timestamps and take the earliest per row — fully vectorised
        ts_matrix = df[act_cols].apply(pd.to_datetime, errors="coerce")
        earliest  = ts_matrix.min(axis=1)          # pandas uses C-level min across columns
    else:
        earliest = pd.Series(pd.NaT, index=df.index)

    df["_ts"]     = earliest
    df["Month"]   = earliest.dt.to_period("M").astype(str).replace("NaT", pd.NA)
    df["Year"]    = earliest.dt.year.astype("Int64").astype(str).replace("<NA>", pd.NA)
    df["Quarter"] = earliest.dt.to_period("Q").astype(str).replace("NaT", pd.NA)

    print(f"[O2C PROCESS] Done — {len(df):,} rows, {df[COL_CASE].nunique():,} unique cases.")
    return df


# ─── MULTI-USER FILE SYSTEM ──────────────────────────────────────────────────
USER_DFS     = {}
FILE_REGISTRY = "o2c_file_registry.json"
UPLOAD_DIR    = "o2c_user_data"
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
    if username in USER_DFS:   return USER_DFS[username]
    if "Unknown" in USER_DFS:  return USER_DFS["Unknown"]
    return pd.DataFrame()

@router.post("/clear")
def clear_data(username: str = Query("Unknown")):
    if username in USER_DFS:   del USER_DFS[username]
    if "Unknown" in USER_DFS:  del USER_DFS["Unknown"]
    return {"status": "cleared"}

def register_transform_build(username: str, processed, save_path: str, csv_path: str = ""):
    cases = int(processed[COL_CASE].nunique()) if COL_CASE in processed.columns else 0
    reg = load_registry()
    if username not in reg:
        reg[username] = []
    reg[username].append({
        "file_id":     save_path,
        "filename":    f"O2C Built Event Log ({datetime.now().strftime('%Y-%m-%d %H:%M')})",
        "upload_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "rows":        len(processed),
        "cases":       cases,
        "source":      "table_build",
        "csv_path":    csv_path,
    })
    save_registry(reg)

@router.get("/download_output")
def download_output_csv(username: str = Query("Unknown")):
    from fastapi.responses import StreamingResponse
    df = get_user_df(username)
    if df.empty:
        raise HTTPException(404, "No data loaded.")
    ts       = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_name = f"O2C_Output_{username}_{ts}.csv"
    try:
        _save_output_csv(df, csv_name)
    except Exception as e:
        print(f"[O2C DOWNLOAD] Server save failed: {e}")
    export_cols = [c for c in df.columns if not c.startswith("_")]
    export_df   = df[export_cols].copy()
    for c in export_df.columns:
        if pd.api.types.is_datetime64_any_dtype(export_df[c]):
            export_df[c] = export_df[c].dt.strftime("%Y-%m-%d")
    buf = io.StringIO()
    export_df.to_csv(buf, index=False)
    buf.seek(0)
    return StreamingResponse(
        iter([buf.getvalue()]), media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{csv_name}"'}
    )

@router.get("/my_files")
def get_my_files(username: str = Query(...)):
    reg = load_registry()
    return list(reversed(reg.get(username, [])))

class LoadFileReq(BaseModel):
    username: str
    file_id: str

@router.post("/load_file")
def load_specific_file(req: LoadFileReq):
    if not os.path.exists(req.file_id):
        print(f"[O2C ERROR] File not found: {req.file_id}")
        raise HTTPException(404, "File not found on server")
    try:
        if req.file_id.endswith(".json"):
            df = pd.read_json(req.file_id, orient="records")
        else:
            df = pd.read_csv(req.file_id, low_memory=False)
        processed = process_df(df)
        USER_DFS[req.username] = processed
        USER_DFS["Unknown"]    = processed
        log_audit(req.username, "LOAD_FILE", f"Loaded: {req.file_id}")
        return {"status": "ok"}
    except Exception as e:
        print(f"[O2C ERROR] Failed to load file: {e}")
        raise HTTPException(500, f"Failed to load file: {e}")

@router.post("/upload")
async def upload_csv(file: UploadFile = File(...), username: str = Form("Unknown")):
    if not file.filename.lower().endswith(".csv"):
        raise HTTPException(400, "Only CSV files are supported")
    content = await file.read()
    try:
        df = pd.read_csv(io.BytesIO(content), low_memory=False)
        print(f"[O2C INFO] {username} uploaded {file.filename}: {len(df):,} rows")
    except Exception as e:
        raise HTTPException(400, f"Failed to read CSV: {e}")

    df = process_df(df)
    USER_DFS[username]  = df
    USER_DFS["Unknown"] = df

    ts      = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_id = os.path.join(UPLOAD_DIR, f"{username}_{ts}.csv")
    df.to_csv(file_id, index=False)

    cases    = int(df[COL_CASE].nunique()) if COL_CASE in df.columns else 0
    csv_name = f"O2C_Upload_{username}_{ts}.csv"
    csv_path = ""
    try:
        csv_path = _save_output_csv(df, csv_name)
    except Exception as e:
        print(f"[O2C ERROR] CSV save failed: {e}")

    reg = load_registry()
    if username not in reg:
        reg[username] = []
    reg[username].append({
        "filename":    file.filename,
        "file_id":     file_id,
        "upload_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "rows":        len(df),
        "cases":       cases,
        "source":      "csv_upload",
        "csv_path":    csv_path,
    })
    save_registry(reg)
    log_audit(username, "UPLOAD", f"Uploaded {file.filename}: {len(df):,} rows, {cases:,} cases")
    return {"status": "ok", "rows": len(df), "unique_cases": cases}

@router.get("/")
def o2c_root(username: str = Query("Unknown")):
    df = get_user_df(username)
    return {"status": "O2C Sub-module active", "rows": len(df), "data_loaded": not df.empty}


# ─── Filtering helper ─────────────────────────────────────────────────────────
def filter_raw(df, customer=None, vkorg=None, auart=None, matkl=None, werks=None,
               case_id=None, month=None, year=None, quarter=None, ernam=None,
               status=None, lead_time=None):
    if customer and customer != "ALL" and COL_CUSTOMER in df.columns:
        df = df[df[COL_CUSTOMER].astype(str) == customer]
    if vkorg and vkorg != "ALL" and COL_VKORG in df.columns:
        df = df[df[COL_VKORG].astype(str) == vkorg]
    if auart and auart != "ALL" and COL_AUART in df.columns:
        df = df[df[COL_AUART].astype(str) == auart]
    if matkl and matkl != "ALL" and COL_MATKL in df.columns:
        df = df[df[COL_MATKL].astype(str) == matkl]
    if werks and werks != "ALL" and COL_WERKS in df.columns:
        df = df[df[COL_WERKS].astype(str) == werks]
    if case_id and case_id != "ALL" and COL_CASE in df.columns:
        df = df[df[COL_CASE].astype(str) == case_id]
    if ernam and ernam != "ALL" and COL_ERNAM in df.columns:
        df = df[df[COL_ERNAM].astype(str) == ernam]
    if month and month != "ALL" and "Month" in df.columns:
        df = df[df["Month"].astype(str) == month]
    if year and year != "ALL" and "Year" in df.columns:
        df = df[df["Year"].astype(str) == year]
    if quarter and quarter != "ALL" and "Quarter" in df.columns:
        df = df[df["Quarter"].astype(str) == quarter]

    if lead_time and lead_time != "ALL":
        if "SO Created" in df.columns and "Goods Issued" in df.columns:
            try:
                lt = (df["Goods Issued"] - df["SO Created"]).dt.total_seconds() / 86400
                nums = re.findall(r"\d+", str(lead_time))
                if len(nums) == 2:
                    df = df[lt.between(int(nums[0]), int(nums[1]), inclusive="left")]
                elif len(nums) == 1:
                    df = df[lt >= int(nums[0])]
            except Exception:
                pass

    if status and status != "ALL":
        has_all  = pd.Series(True, index=df.index)
        is_seq   = pd.Series(True, index=df.index)
        prev_col = None
        for c in HAPPY_PATH_ACTIVITIES:
            if c in df.columns:
                has_all = has_all & df[c].notna()
                if prev_col:
                    is_seq = is_seq & (df[c] >= df[prev_col]).fillna(False)
                prev_col = c
            else:
                has_all = pd.Series(False, index=df.index)
                is_seq  = pd.Series(False, index=df.index)
                break
        has_no_rev = pd.Series(True, index=df.index)
        for c in DEVIATION_ACTIVITIES:
            if c in df.columns:
                has_no_rev = has_no_rev & df[c].isna()
        happy = has_all & is_seq & has_no_rev
        if status == "Happy Path":   df = df[happy]
        elif status == "Deviations": df = df[~happy]

    return df


def cfp(customer, vkorg, auart, matkl, werks, case_id, month, year, quarter, ernam, status, lead_time):
    return dict(customer=customer, vkorg=vkorg, auart=auart, matkl=matkl, werks=werks,
                case_id=case_id, month=month, year=year, quarter=quarter,
                ernam=ernam, status=status, lead_time=lead_time)

def col_unique_cases(df, col):
    if col not in df.columns or COL_CASE not in df.columns:
        return 0
    return int(df.loc[df[col].notna(), COL_CASE].nunique())

def unique_cases(df):
    return int(df[COL_CASE].nunique()) if COL_CASE in df.columns else len(df)

def get_filtered(username, customer, vkorg, auart, matkl, werks, case_id,
                 month, year, quarter, ernam, status, lead_time):
    df_raw = get_user_df(username)
    if df_raw.empty:
        return pd.DataFrame()
    return filter_raw(df_raw.copy(),
                      **cfp(customer, vkorg, auart, matkl, werks, case_id,
                            month, year, quarter, ernam, status, lead_time))


# ─── Endpoints ────────────────────────────────────────────────────────────────
@router.get("/filters")
def get_filters(
    username: str = Query("Unknown"),
    customer: Optional[str] = Query(None), vkorg: Optional[str] = Query(None),
    auart: Optional[str] = Query(None),    matkl: Optional[str] = Query(None),
    werks: Optional[str] = Query(None),    case_id: Optional[str] = Query(None),
    month: Optional[str] = Query(None),    year: Optional[str] = Query(None),
    quarter: Optional[str] = Query(None),  ernam: Optional[str] = Query(None),
    status: Optional[str] = Query(None),   lead_time: Optional[str] = Query(None),
):
    df_raw = get_user_df(username)
    if df_raw.empty:
        return {}
    d = filter_raw(df_raw.copy(),
                   **cfp(customer, vkorg, auart, matkl, werks, case_id,
                         month, year, quarter, ernam, status, lead_time))

    def vals(col):
        return sorted([str(x) for x in d[col].dropna().unique()]) if col in d.columns else []

    return {
        "case_ids":  vals(COL_CASE),
        "customers": vals(COL_CUSTOMER),
        "vkorgs":    vals(COL_VKORG),
        "auarts":    vals(COL_AUART),
        "matkls":    vals(COL_MATKL),
        "werkss":    vals(COL_WERKS),
        "years":     sorted([str(x) for x in d["Year"].dropna().unique()]) if "Year" in d.columns else [],
        "statuses":  ["ALL", "Happy Path", "Deviations"],
    }


@router.get("/kpis")
def get_kpis(
    username: str = Query("Unknown"),
    customer: Optional[str] = Query(None), vkorg: Optional[str] = Query(None),
    auart: Optional[str] = Query(None),    matkl: Optional[str] = Query(None),
    werks: Optional[str] = Query(None),    case_id: Optional[str] = Query(None),
    month: Optional[str] = Query(None),    year: Optional[str] = Query(None),
    quarter: Optional[str] = Query(None),  ernam: Optional[str] = Query(None),
    status: Optional[str] = Query(None),   lead_time: Optional[str] = Query(None),
):
    _empty = {
        "total_cases": 0, "so_approved": 0, "deliveries_created": 0,
        "deliveries_posted": 0, "goods_issues": 0, "invoices_created": 0,
        "invoices_posted": 0, "invoices_cleared": 0, "unique_customers": 0,
        "avg_cycle_days": 0, "so_reversals": 0, "so_rev_after_gi": 0,
        "gi_reversals": 0, "invoice_reversals": 0, "credit_memos": 0,
        "debit_memos": 0, "inv_no_del": 0, "inv_no_gi": 0,
    }
    try:
        df_raw = get_user_df(username)
        if df_raw.empty:
            return _empty
        d = filter_raw(df_raw.copy(),
                       **cfp(customer, vkorg, auart, matkl, werks, case_id,
                             month, year, quarter, ernam, status, lead_time))

        inv_no_del = 0
        if "Invoice Created" in d.columns and "Delivery Created" in d.columns:
            inv_no_del = int((d["Invoice Created"].notna() & d["Delivery Created"].isna()).sum())

        inv_no_gi = 0
        if "Invoice Created" in d.columns and "Goods Issued" in d.columns:
            inv_no_gi = int((
                d["Invoice Created"].notna() &
                d["Goods Issued"].notna() &
                (d["Invoice Created"] < d["Goods Issued"])
            ).sum())

        avg_days = 0.0
        if "SO Created" in d.columns and "Invoice Cleared" in d.columns:
            sub = d.dropna(subset=["SO Created", "Invoice Cleared"])
            if not sub.empty:
                diff = (sub["Invoice Cleared"] - sub["SO Created"]).dt.total_seconds() / 86400
                avg_days = round(float(diff.mean()), 1)

        return {
            "total_cases":        unique_cases(d),
            "so_approved":        col_unique_cases(d, "SO Approved"),
            "deliveries_created": col_unique_cases(d, "Delivery Created"),
            "deliveries_posted":  col_unique_cases(d, "Delivery Posted"),
            "goods_issues":       col_unique_cases(d, "Goods Issued"),
            "invoices_created":   col_unique_cases(d, "Invoice Created"),
            "invoices_posted":    col_unique_cases(d, "Invoice Posted"),
            "invoices_cleared":   col_unique_cases(d, "Invoice Cleared"),
            "unique_customers":   int(d[COL_CUSTOMER].nunique()) if COL_CUSTOMER in d.columns else 0,
            "avg_cycle_days":     avg_days,
            "so_reversals":       col_unique_cases(d, "SO Reversed"),
            "so_rev_after_gi":    col_unique_cases(d, "SO Reversed After GI"),
            "gi_reversals":       col_unique_cases(d, "GI Reversed"),
            "invoice_reversals":  col_unique_cases(d, "Invoice Reversed"),
            "credit_memos":       col_unique_cases(d, "Credit Memo"),
            "debit_memos":        col_unique_cases(d, "Debit Memo"),
            "inv_no_del":         inv_no_del,
            "inv_no_gi":          inv_no_gi,
        }
    except Exception as e:
        print(f"[O2C ERROR] KPI failed: {e}")
        return _empty


@router.get("/cases")
def get_cases(
    username: str = Query("Unknown"),
    customer: Optional[str] = Query(None), vkorg: Optional[str] = Query(None),
    auart: Optional[str] = Query(None),    matkl: Optional[str] = Query(None),
    werks: Optional[str] = Query(None),    case_id: Optional[str] = Query(None),
    month: Optional[str] = Query(None),    year: Optional[str] = Query(None),
    quarter: Optional[str] = Query(None),  ernam: Optional[str] = Query(None),
    status: Optional[str] = Query(None),   lead_time: Optional[str] = Query(None),
):
    df_raw = get_user_df(username)
    if df_raw.empty: return []
    d = filter_raw(df_raw.copy(),
                   **cfp(customer, vkorg, auart, matkl, werks, case_id,
                         month, year, quarter, ernam, status, lead_time))
    if COL_CASE not in d.columns:
        return []
    time_cols = [c for c in ACTIVITY_COLUMNS if c in d.columns]
    if not time_cols:
        return []
    d["start_date"] = d[time_cols].min(axis=1).dt.strftime("%Y-%m-%d")
    d["end_date"]   = d[time_cols].max(axis=1).dt.strftime("%Y-%m-%d")
    res = (d[[COL_CASE, "start_date", "end_date"]]
.dropna(subset=[COL_CASE])
           .rename(columns={COL_CASE: "case_id"}))
    return res.sort_values("start_date", ascending=False).to_dict("records")


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
    has_user = COL_ERNAM in d.columns
    id_vars  = [COL_CASE] + ([COL_ERNAM] if has_user else [])
    melted   = d.melt(id_vars=id_vars, value_vars=cols_to_melt,
                      var_name="Activity", value_name="Timestamp")
    melted   = (melted.dropna(subset=["Timestamp"])
                .drop_duplicates(subset=["Activity"])
                .sort_values("Timestamp"))
    res            = melted[["Activity", "Timestamp"]].copy()
    res["User"]    = melted[COL_ERNAM] if has_user else "Unknown"
    res["Timestamp"] = res["Timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")
    return res.to_dict("records")


@router.get("/charts/{chart_name}")
def get_chart(
    chart_name: str,
    username: str = Query("Unknown"),
    customer: Optional[str] = Query(None), vkorg: Optional[str] = Query(None),
    auart: Optional[str] = Query(None),    matkl: Optional[str] = Query(None),
    werks: Optional[str] = Query(None),    case_id: Optional[str] = Query(None),
    month: Optional[str] = Query(None),    year: Optional[str] = Query(None),
    quarter: Optional[str] = Query(None),  ernam: Optional[str] = Query(None),
    status: Optional[str] = Query(None),   lead_time: Optional[str] = Query(None),
):
    d = get_filtered(username, customer, vkorg, auart, matkl, werks, case_id,
                     month, year, quarter, ernam, status, lead_time)

    if chart_name == "activity":
        return sorted(
            [{"activity": col,
              "count": int(d[col].notna().sum()),
              "unique_cases": col_unique_cases(d, col)}
             for col in ACTIVITY_COLUMNS
             if col in d.columns and d[col].notna().sum() > 0],
            key=lambda x: x["count"], reverse=True,
        )

    if chart_name == "monthly":
        cols = [c for c in ACTIVITY_COLUMNS if c in d.columns]
        if not cols or COL_CASE not in d.columns:
            return []
        melted = d.melt(id_vars=[COL_CASE], value_vars=cols,
                        value_name="Date").dropna(subset=["Date"])
        melted["Month"] = melted["Date"].dt.to_period("M").astype(str)
        res = melted.groupby("Month")[COL_CASE].nunique().reset_index()
        res.columns = ["Month", "count"]
        return res.sort_values("Month").to_dict("records")

    col_map = {
        "customer": COL_CUSTOMER, "auart": COL_AUART, "matkl": COL_MATKL,
        "ernam": COL_ERNAM, "vkorg": COL_VKORG,
    }
    if chart_name in col_map:
        c_name = col_map[chart_name]
        if c_name not in d.columns:
            return []
        vc = d.dropna(subset=[c_name]).groupby(c_name)[COL_CASE].nunique().reset_index()
        vc.columns = [chart_name, "count"]
        return vc.sort_values("count", ascending=False).head(20).to_dict("records")

    if chart_name == "leadtime":
        if "SO Created" not in d.columns or "Goods Issued" not in d.columns:
            return []
        lt = d.dropna(subset=["SO Created", "Goods Issued"]).copy()
        lt["days"] = (lt["Goods Issued"] - lt["SO Created"]).dt.total_seconds() / 86400
        lt_valid = lt[lt["days"].between(0, 365)].copy()
        if lt_valid.empty:
            return []
        lt_valid["bucket"] = pd.cut(lt_valid["days"], bins=list(range(0, 370, 10)), right=False)
        hist = lt_valid.groupby("bucket", observed=True).size().reset_index(name="count")
        hist["label"] = hist["bucket"].astype(str)
        return hist[hist["count"] > 0][["label", "count"]].to_dict("records")

    if chart_name == "bottleneck":
        steps = [
            ("SO Created",       "SO Approved",     "SO to Approval"),
            ("SO Approved",      "Delivery Created", "Approval to Delivery"),
            ("Delivery Created", "Delivery Posted",  "Delivery to Posted"),
            ("Delivery Posted",  "Goods Issued",     "Posted to GI"),
            ("Goods Issued",     "Invoice Created",  "GI to Invoice"),
            ("Invoice Created",  "Invoice Posted",   "Invoice to Accounting"),
            ("Invoice Posted",   "Invoice Cleared",  "Accounting to Cleared"),
        ]
        results = []
        for src, tgt, label in steps:
            if src in d.columns and tgt in d.columns:
                sub = d.dropna(subset=[src, tgt]).copy()
                sub["days"] = (sub[tgt] - sub[src]).dt.total_seconds() / 86400
                sub = sub[sub["days"] >= 0]
                if not sub.empty:
                    results.append({
                        "step":        label,
                        "avg_days":    round(float(sub["days"].mean()), 1),
                        "median_days": round(float(sub["days"].median()), 1),
                        "count":       int(sub[COL_CASE].nunique()) if COL_CASE in sub.columns else len(sub),
                    })
        return results

    if chart_name == "sod":
        res = []
        def count_sod(c1, c2):
            if c1 in d.columns and c2 in d.columns:
                mask = (d[c1].notna() & (d[c1].astype(str).str.strip() != "") &
                        d[c2].notna() & (d[c2].astype(str).str.strip() != "") &
                        (d[c1] == d[c2]))
                return int(d[mask][COL_CASE].nunique())
            return 0
        for pair, label in [
            (("Sales Document Maker", "Delivery Document Maker"), "SO & Delivery"),
            (("Sales Document Maker", "Invoice Maker"),           "SO & Invoice"),
            (("Delivery Document Maker", "Invoice Maker"),        "Delivery & Invoice"),
        ]:
            c = count_sod(*pair)
            if c > 0:
                res.append({"violation": label, "count": c})
        return sorted(res, key=lambda x: x["count"], reverse=True)

    if chart_name == "inv_rev_ernam":
        if "Invoice Reversed" not in d.columns or COL_ERNAM not in d.columns:
            return []
        b  = d.dropna(subset=["Invoice Reversed", COL_ERNAM])
        vc = b.groupby(COL_ERNAM)[COL_CASE].nunique().reset_index(name="count")
        vc.columns = ["ernam", "count"]
        return vc.sort_values("count", ascending=False).head(20).to_dict("records")

    if chart_name == "inv_rev_timeline":
        if "Invoice Reversed" not in d.columns:
            return []
        b = d.dropna(subset=["Invoice Reversed"]).copy()
        b["Month"] = b["Invoice Reversed"].dt.to_period("M").astype(str)
        res = b.groupby("Month")[COL_CASE].nunique().reset_index(name="count")
        return res.sort_values("Month").to_dict("records")

    if chart_name == "customer_lead_time":
        if COL_CUSTOMER not in d.columns or "SO Created" not in d.columns or "Invoice Cleared" not in d.columns:
            return []
        sub = d.dropna(subset=[COL_CUSTOMER, "SO Created", "Invoice Cleared"]).copy()
        sub["days"] = (sub["Invoice Cleared"] - sub["SO Created"]).dt.total_seconds() / 86400
        sub = sub[sub["days"] >= 0]
        if sub.empty:
            return []
        agg = sub.groupby(COL_CUSTOMER).agg(
            avg_days=("days", "mean"), case_count=(COL_CASE, "nunique")
        ).reset_index()
        agg.columns = ["customer", "avg_days", "case_count"]
        agg["avg_days"] = agg["avg_days"].round(1)
        return agg.sort_values("avg_days", ascending=False).to_dict("records")

    if chart_name == "seq_violation_ernam":
        if "Invoice Created" not in d.columns or COL_ERNAM not in d.columns:
            return []
        mask_del = (
            d["Delivery Created"].notna() & (d["Invoice Created"] < d["Delivery Created"])
            if "Delivery Created" in d.columns
            else pd.Series(False, index=d.index)
        )
        mask_gi = (
            d["Goods Issued"].notna() & (d["Invoice Created"] < d["Goods Issued"])
            if "Goods Issued" in d.columns
            else pd.Series(False, index=d.index)
        )
        b  = d[mask_del | mask_gi].dropna(subset=[COL_ERNAM])
        vc = b.groupby(COL_ERNAM)[COL_CASE].nunique().reset_index(name="count")
        vc.columns = ["ernam", "count"]
        return vc.sort_values("count", ascending=False).head(20).to_dict("records")

    if chart_name == "deviations_summary":
        dev_labels = {
            "SO Reversed":          "SO Reversed",
            "SO Reversed After GI": "SO Rev After GI",
            "Delivery Returned":    "Delivery Returned",
            "GI Reversed":          "GI Reversed",
            "Invoice Reversed":     "Invoice Reversed",
            "Credit Memo":          "Credit Memo",
            "Debit Memo":           "Debit Memo",
        }
        results = [
            {"deviation": label, "count": col_unique_cases(d, col)}
            for col, label in dev_labels.items()
            if col_unique_cases(d, col) > 0
        ]
        return sorted(results, key=lambda x: x["count"], reverse=True)

    if chart_name == "happy_path":
        has_all  = pd.Series(True, index=d.index)
        is_seq   = pd.Series(True, index=d.index)
        prev_col = None
        for c in HAPPY_PATH_ACTIVITIES:
            if c in d.columns:
                has_all = has_all & d[c].notna()
                if prev_col:
                    is_seq = is_seq & (d[c] >= d[prev_col]).fillna(False)
                prev_col = c
            else:
                has_all = pd.Series(False, index=d.index)
                is_seq  = pd.Series(False, index=d.index)
                break
        has_no_rev = pd.Series(True, index=d.index)
        for c in DEVIATION_ACTIVITIES:
            if c in d.columns:
                has_no_rev = has_no_rev & d[c].isna()
        happy      = has_all & is_seq & has_no_rev
        happy_cnt  = int(d[happy][COL_CASE].nunique()) if COL_CASE in d.columns else 0
        total_cnt  = unique_cases(d)
        return [
            {"status": "Happy Path",  "count": happy_cnt},
            {"status": "Deviations",  "count": total_cnt - happy_cnt},
        ]

    return []


@router.get("/process-map")
def get_process_map(
    username: str = Query("Unknown"),
    customer: Optional[str] = Query(None), vkorg: Optional[str] = Query(None),
    auart: Optional[str] = Query(None),    matkl: Optional[str] = Query(None),
    werks: Optional[str] = Query(None),    case_id: Optional[str] = Query(None),
    month: Optional[str] = Query(None),    year: Optional[str] = Query(None),
    quarter: Optional[str] = Query(None),  ernam: Optional[str] = Query(None),
    status: Optional[str] = Query(None),   lead_time: Optional[str] = Query(None),
):
    df_raw = get_user_df(username)
    if df_raw.empty:
        return {"nodes": [], "edges": []}
    d = filter_raw(df_raw.copy(),
                   **cfp(customer, vkorg, auart, matkl, werks, case_id,
                         month, year, quarter, ernam, status, lead_time))
    if d.empty:
        return {"nodes": [], "edges": []}

    # Sample up to 15,000 cases for performance — process map is statistical
    if COL_CASE in d.columns and d[COL_CASE].nunique() > 15000:
        sampled_cases = d[COL_CASE].drop_duplicates().sample(15000, random_state=42)
        d = d[d[COL_CASE].isin(sampled_cases)].copy()

    LAYOUT_H = {
        "SO Created":           {"x": 100,  "y": 800},
        "SO Approved":          {"x": 700,  "y": 800},
        "Delivery Created":     {"x": 1300, "y": 800},
        "Delivery Posted":      {"x": 1900, "y": 800},
        "Goods Issued":         {"x": 2500, "y": 800},
        "Invoice Created":      {"x": 3100, "y": 800},
        "Invoice Posted":       {"x": 3700, "y": 800},
        "Invoice Cleared":      {"x": 4300, "y": 800},
        "SO Reversed":          {"x": 400,  "y": 100},
        "SO Reversed After GI": {"x": 2200, "y": 100},
        "Delivery Returned":    {"x": 1900, "y": 1500},
        "GI Reversed":          {"x": 2500, "y": 1500},
        "Invoice Reversed":     {"x": 3400, "y": 100},
        "Credit Memo":          {"x": 3700, "y": 1500},
        "Debit Memo":           {"x": 4000, "y": 1500},
    }
    LAYOUT_V = {
        "SO Created":           {"x": 900,  "y": 100},
        "SO Approved":          {"x": 900,  "y": 650},
        "Delivery Created":     {"x": 900,  "y": 1200},
        "Delivery Posted":      {"x": 900,  "y": 1750},
        "Goods Issued":         {"x": 900,  "y": 2300},
        "Invoice Created":      {"x": 900,  "y": 2850},
        "Invoice Posted":       {"x": 900,  "y": 3400},
        "Invoice Cleared":      {"x": 900,  "y": 3950},
        "SO Reversed":          {"x": -300, "y": 375},
        "SO Reversed After GI": {"x": -300, "y": 2025},
        "Delivery Returned":    {"x": 2100, "y": 1750},
        "GI Reversed":          {"x": 2100, "y": 2300},
        "Invoice Reversed":     {"x": -300, "y": 3125},
        "Credit Memo":          {"x": 2100, "y": 3125},
        "Debit Memo":           {"x": 2100, "y": 3675},
    }

    if COL_CASE not in d.columns:
        return {"nodes": [], "edges": []}

    cols_to_melt = [c for c in ACTIVITY_COLUMNS if c in d.columns]
    melted = d.melt(id_vars=[COL_CASE], value_vars=cols_to_melt,
                    var_name="Activity", value_name="Activitytime")
    melted = melted.dropna(subset=["Activitytime"])

    act_order       = {col: i for i, col in enumerate(ACTIVITY_COLUMNS)}
    melted["Act_Idx"] = melted["Activity"].map(act_order).fillna(99)
    melted = melted.sort_values([COL_CASE, "Activitytime", "Act_Idx"])

    melted["Next_Activity"]     = melted.groupby(COL_CASE)["Activity"].shift(-1)
    melted["Next_Activitytime"] = melted.groupby(COL_CASE)["Activitytime"].shift(-1)
    transitions = melted.dropna(subset=["Next_Activity"]).copy()

    edge_freqs = (transitions
                  .groupby(["Activity", "Next_Activity"])[COL_CASE]
                  .nunique().reset_index(name="frequency"))
    transitions["duration_days"] = (
        (transitions["Next_Activitytime"] - transitions["Activitytime"])
        .dt.total_seconds() / 86400
    )
    avg_dur = (transitions[transitions["duration_days"] >= 0]
               .groupby(["Activity", "Next_Activity"])["duration_days"]
               .mean().reset_index())
    avg_dur.columns = ["Activity", "Next_Activity", "avg_days"]
    edge_freqs = edge_freqs.merge(avg_dur, on=["Activity", "Next_Activity"], how="left")

    present_acts = set(melted["Activity"].unique()) | MAIN_NODES
    nodes_out = [
        {
            "id": name, "label": name,
            "position_h": LAYOUT_H.get(name, {"x": 2800, "y": 500}),
            "position_v": LAYOUT_V.get(name, {"x": 600,  "y": 3600}),
            "is_main":    name in MAIN_NODES,
            "frequency":  int(melted[melted["Activity"] == name][COL_CASE].nunique()),
        }
        for name in present_acts
    ]
    edges_out = [
        {
            "id":        f"{row['Activity']}--{row['Next_Activity']}",
            "source":    row["Activity"],
            "target":    row["Next_Activity"],
            "frequency": int(row["frequency"]),
            "avg_days":  round(float(row["avg_days"]), 1) if pd.notna(row.get("avg_days")) else None,
        }
        for _, row in edge_freqs.iterrows()
    ]

    return {"nodes": nodes_out, "edges": edges_out}