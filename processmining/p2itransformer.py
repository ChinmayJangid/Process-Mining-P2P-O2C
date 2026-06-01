"""
p2itransformer.py  —  KNIME P2I_4 exact replication → wide-format output for p2i4.py
========================================================================================

KNIME WORKFLOW NODE MAP  (P2I_4.knwf)
---------------------------------------

SOURCE TABLES
  AUFK   → Production Order Header (optional — ERDAT: order creation date, ERNAM: creator)
  AFKO   → Production Order Header Scheduling (optional — dates: GSTRP, GLTRP, FTRMS etc.)
  AFPO   → Production Order Items  (PRIMARY — mandatory)
  AFRU   → Operation Confirmations (Routing/Task-list confirmations)
  RESB   → Production Reservations
  MKPF   → Material Document Headers
  MSEG   → Material Document Lines (GI/GR movements)
  EKKO   → Purchase Order Header  (same as P2P)
  EKPO   → Purchase Order Items   (same as P2P)
  EBAN   → Purchase Requisitions  (same as P2P)
  EKBE   → PO History / GR & Invoice lines  (same as P2P)
  LFA1   → Vendor Master          (same as P2P)

AUFK BRANCH  (optional — new addition)
  AUFK is the SAP Production Order Header table.
  Required columns: AUFNR (join key), ERDAT (creation date), ERNAM (creator).
  Join: AFPO LEFT JOIN AUFK on AUFNR (after UniqueID_P2I is built).
  Output columns added:
    AUFK.ERDAT → "Order Creation"  (activity step 2 of the P2I happy path)
    AUFK.ERNAM → "ERNAM"           (order-creator dimension — used in filters/charts)

PRODUCTION BRANCH  (AFKO optional, AFPO mandatory)
  AFPO is the primary table. UniqueID_P2I = join(AUFNR, POSNR) built directly.
  AFKO date columns (Basic Start, Basic Finish, Scheduled dates, Actual dates)
  are only available when AFKO is uploaded.

OPERATION CONFIRMATIONS BRANCH  (AFRU)
  Column Filter #12: keep RUECK, RMZHL, ERSDA, ERNAM, LAEDA, BUDAT, STOKZ,
                          ISDD, ISBD, IEDD, AUFNR, VORNR, SMENG, ERZET etc.
  Renamer #48: ERSDA→"Order Confirmation", BUDAT→"Order Posting",
               LAEDA→"Last Change", ERNAM→"Order Creation User",
               STOKZ→"Document Reverse", ISDD→"Start of Execution",
               IEDD→"Execution Finish", ISBD→"Processing Start"
  NOTE: AFRU.ERNAM → "Order Creation User" (confirmation user, NOT order creator)
        AUFK.ERNAM → "ERNAM" (order creator — more meaningful for filtering)
  Rule Engine #50: $Document Reverse$ = "X" => $Order Confirmation$
                   → new col "Operation Reversal"
  Collapse to one row per AUFNR:
    Order Confirmation = earliest (min)
    Order Posting      = earliest (min)
    Start of Execution = earliest (min)
    Processing Start   = earliest (min)
    Execution Finish   = latest   (max)
    Operation Reversal = latest   (max)
    Order Creation User = first non-null

  Joiner #49: (AFKO+AFPO) LEFT JOIN afru_wide on AUFNR

RESERVATIONS BRANCH  (RESB)
  Column Filter #103: keep AUFNR, RSNUM, RSPOS, MATNR, WERKS etc.
  Joiner #99:  production_op LEFT JOIN resb on AUFNR

MATERIAL DOCUMENTS  (MKPF + MSEG)
  CSV #15 = MKPF (header): MBLNR, MJAHR, BLART, BUDAT, USNAM, TCODE…
  MSEG (lines): MBLNR, MJAHR, AUFNR, BWART, BUDAT, EBELN, EBELP…
  String Manipulation #54: Unique Key = join(MBLNR, GJAHR)   ← replaces col in MKPF
  String Manipulation #55: Unique Key = join(MBLNR, MJAHR)   ← replaces col in MSEG
  Joiner #56: MKPF INNER JOIN MSEG on Unique Key
  String Manipulation #107: UniqueID_PO = join(EBELN, EBELP) ← for PO link
  Column Expressions #59: classify by BWART:
    261 → "Goods Issued to Order"
    101 → "Finished Goods Receipts"
    541 → "Goods transfer to Sub Contractor"
    262 → "Goods Issue Reversed"
    102 → "Finished Goods Reversal"
    131 → "Finished Goods to Quality Inpection"
    132 → "QI to FG"
  Collapse to one row per AUFNR (earliest date per movement type)
  Joiner #104: production_op_resb INNER JOIN mat_docs_wide on AUFNR

PO/GR/INVOICE BRANCH  (EKKO+EKPO+EBAN+EKBE)  — identical to P2P transformer:
  Nodes #8,#14,#15,#16,#17,#95: EKKO INNER JOIN EKPO → PO branch
  Nodes #98,#94,#19(PR),#34: EBAN → PR branch
  Node #20: PO LEFT JOIN PR on UniqueID_PR
  Nodes #21,#27,#79,#80: EKBE VGABE=1 → GR branch
  Nodes #22,#30,#82,#83: EKBE VGABE=2 → Invoice branch
  Nodes #81,#84: join po_pr LEFT JOIN gr LEFT JOIN invoice on UniqueID_PO
  Node #85: Value Lookup LIFNR → NAME1

FINAL JOIN
  Joiner #106: production_wide LEFT JOIN po_pr_gr_inv_wide on UniqueID_PO
               (links production orders to their procurement chain)

FINAL OUTPUT:  one row per AUFNR+POSNR (production order item), wide format
  Activity columns (in order):
    Basic Start | Order Creation | Order Confirmation | Goods Issued to Order |
    Finished Goods Receipts | Finished Goods to Quality Inpection |
    Operation Reversal | Goods transfer to Sub Contractor | Goods Issue Reversed |
    Finished Goods Reversal | QI to FG |
    PR Creation | PR Release Date | PO Creation | PO Date | GR Posting | Invoice Posting
"""

import io
import os
import traceback
import warnings
from datetime import datetime

import pandas as pd
from fastapi import APIRouter, File, Form, HTTPException, Query, UploadFile

warnings.filterwarnings("ignore")

p2i_transformer_router = APIRouter(prefix="/p2i/transform", tags=["P2I Transformer"])

RAW_TABLES: dict[str, dict[str, pd.DataFrame]] = {}

EXPECTED_TABLES = {"AUFK", "AFKO", "AFPO", "AFRU", "RESB", "MKPF", "MSEG", "EKKO", "EKPO", "EBAN", "EKBE", "LFA1"}
REQUIRED_TABLES = {"AFPO"}

# Minimum required columns per SAP table
REQUIRED_COLS = {
    "AUFK": {
        "AUFNR",   # Production Order Number
        "ERDAT",   # Creation Date -> "Production Order Creation"
        "ERNAM",   # Created By
    },
    "AFKO": {
        # OPTIONAL — same scheduling columns as AUFK but from AFKO table
        "AUFNR",   # Production Order Number — join key to AFPO
        "GSTRP",   # Basic Start date → "Basic Start"
        "GLTRP",   # Basic Finish date → "Basic Finish"
    },
    "AFPO": {
        "AUFNR",   # Production Order Number — join key
        "POSNR",   # Order Item Number
        "MATNR",   # Material Number
        "WERKS",   # Plant
        "MEINS",   # Base Unit of Measure
    },
    "AFRU": {
        "AUFNR",   # Production Order Number — join key
        "ERSDA",   # Confirmation Date → renamed "Order Confirmation"
        "BUDAT",   # Posting Date → renamed "Order Posting"
        "ERNAM",   # User who confirmed → renamed "Order Creation User"
        "STOKZ",   # Cancellation indicator → "Document Reverse"
        "ISDD",    # Execution Start → renamed "Start of Execution"
        "IEDD",    # Execution Finish → renamed "Execution Finish"
        "ISBD",    # Processing Start → renamed "Processing Start"
    },
    "RESB": {
        "AUFNR",   # Production Order Number — join key
        "RSNUM",   # Reservation Number
        "MATNR",   # Material Number
        "WERKS",   # Plant
    },
    "MKPF": {
        "MBLNR",   # Material Document Number
        "MJAHR",   # Material Document Year
        "BUDAT",   # Posting Date
    },
    "MSEG": {
        "MBLNR",   # Material Document Number — join key with MKPF
        "MJAHR",   # Material Document Year — join key
        "AUFNR",   # Production Order Number — link to production
        "BWART",   # Movement Type (261=GI, 101=FG GR, etc.)
        "BUDAT",   # Posting Date
    },
    # PO/GR/Invoice tables (identical requirements to P2P)
    "EKKO": {
        "EBELN", "AEDAT", "BEDAT", "BSART", "LIFNR",
        "BUKRS", "EKGRP", "ERNAM", "LOEKZ",
    },
    "EKPO": {
        "EBELN", "EBELP", "MATNR", "WERKS", "MATKL",
        "BANFN", "BNFPO", "LOEKZ", "AEDAT",
    },
    "EBAN": {
        "BANFN", "BNFPO", "BADAT", "FRGDT",
        "ERNAM", "ERDAT", "LOEKZ",
    },
    "EKBE": {
        "EBELN", "EBELP", "VGABE", "BUDAT",
        "SHKZG", "ERNAM", "BELNR", "GJAHR",
    },
    "LFA1": {
        "LIFNR", "NAME1",
    },
}

# BWART → Activity Name mapping
BWART_MAP = {
    "261": "Goods Issued to Order",
    "101": "Finished Goods Receipt",
    "541": "Goods transferred to subcontractor",
    "262": "Goods Issued Reversed",
    "102": "Finished Goods Reversed",
    "131": "Finished Goods to Quality Inspection",
    "132": "Quality Inspection to Finished Goods",
}


def _log(msg: str):
    print(f"[P2I TRANSFORM] {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | {msg}")


def _read_csv(content: bytes, filename: str) -> pd.DataFrame:
    fn = filename.lower()
    if fn.endswith(".xlsx") or fn.endswith(".xls"):
        df = pd.read_excel(io.BytesIO(content))
        df = df.dropna(how="all")
        return df
    for enc in ("utf-8", "latin-1", "windows-1252"):
        try:
            return pd.read_csv(io.BytesIO(content), encoding=enc, low_memory=False)
        except (UnicodeDecodeError, pd.errors.ParserError, Exception):
            continue
    raise ValueError(f"Cannot decode {filename}")


def _str(series: pd.Series) -> pd.Series:
    s = series.astype(str).str.strip()
    return s.str.replace(r'\.0$', '', regex=True)


def _safe_join_key(s1: pd.Series, s2: pd.Series,
                   pad1: int = 0, pad2: int = 0) -> pd.Series:
    """
    Build a composite join key from two columns with optional zero-padding.
    Returns NaN when either component is null/empty.
    """
    def _norm(s: pd.Series, width: int) -> pd.Series:
        s = s.astype(str).str.strip()
        s = s.str.replace(r'\.0$', '', regex=True)
        if width:
            is_numeric = s.str.match(r'^\d+$')
            s = s.where(~is_numeric, s.str.zfill(width))
        return s

    a = _norm(s1, pad1)
    b = _norm(s2, pad2)
    null_a = s1.isna() | (a == '') | (a == 'nan') | (a == 'None')
    null_b = s2.isna() | (b == '') | (b == 'nan') | (b == 'None')
    key = a + b
    key[null_a | null_b] = None
    return key


def _keep(df: pd.DataFrame, cols: list) -> pd.DataFrame:
    present = [c for c in cols if c in df.columns]
    missing = [c for c in cols if c not in df.columns]
    if missing:
        _log(f"  WARNING — columns not found (skipped): {missing}")
    return df[present].copy()


# ── API endpoints ─────────────────────────────────────────────────────────────

@p2i_transformer_router.get("/status")
def transform_status(username: str = Query("Unknown")):
    tables = RAW_TABLES.get(username, {})
    loaded  = list(tables.keys())
    missing = [t for t in EXPECTED_TABLES if t not in tables]
    missing_req = [t for t in REQUIRED_TABLES if t not in tables]
    return {"loaded": loaded, "missing": missing, "ready": len(missing_req) == 0}


@p2i_transformer_router.post("/preview_columns")
async def preview_columns(file: UploadFile = File(...)):
    raw = await file.read()
    try:
        df = _read_csv(raw, file.filename)
        return {"status": "ok", "columns": list(df.columns)}
    except Exception as e:
        raise HTTPException(500, f"Failed to parse CSV: {e}")


@p2i_transformer_router.post("/upload_table")
async def upload_raw_table(
    file: UploadFile = File(...),
    table_name: str = Form(...),
    username: str = Form("Unknown"),
    column_mapping: str = Form("{}"),
):
    table_name = table_name.strip().upper()

    if table_name not in EXPECTED_TABLES:
        raise HTTPException(400, f"Unknown table '{table_name}'. Expected: {sorted(EXPECTED_TABLES)}")

    fn = file.filename.lower()
    if not (fn.endswith(".csv") or fn.endswith(".xlsx") or fn.endswith(".xls")):
        raise HTTPException(400, "Only CSV and Excel files are supported.")
    raw = await file.read()
    try:
        df = _read_csv(raw, file.filename)
    except Exception as e:
        raise HTTPException(500, f"Failed to parse {file.filename}: {e}")

    df.columns = [str(c).strip() for c in df.columns]

    # Apply column mapping
    try:
        import json
        mapping = json.loads(column_mapping)
        if mapping:
            actual_cols = {c.lower(): c for c in df.columns}
            clean_mapping = {}
            for k, v in mapping.items():
                if k.lower() in actual_cols:
                    clean_mapping[actual_cols[k.lower()]] = v
                else:
                    clean_mapping[k] = v
            df = df.rename(columns=clean_mapping)
            _log(f"Applied column mapping for {table_name}: {clean_mapping}")
    except Exception as e:
        _log(f"Error applying column mapping: {e}")

    # Validate required columns
    required = REQUIRED_COLS.get(table_name, set())
    uploaded_cols = {c.upper() for c in df.columns}
    missing = {c for c in required if c.upper() not in uploaded_cols}
    if missing:
        msg = f"Table '{table_name}' is missing required columns: {sorted(missing)}"
        _log(f"Error: {msg}")
        raise HTTPException(status_code=400, detail=msg)

    RAW_TABLES.setdefault(username, {})[table_name] = df
    _log(f"User '{username}' uploaded {table_name}: {len(df)} rows, {len(df.columns)} cols.")
    return {"status": "ok", "table": table_name, "rows": len(df), "columns": list(df.columns)}


@p2i_transformer_router.delete("/clear_table")
def clear_table(table_name: str, username: str = Query("Unknown")):
    table_name = table_name.strip().upper()
    if username in RAW_TABLES and table_name in RAW_TABLES[username]:
        del RAW_TABLES[username][table_name]
    return {"status": "ok", "message": f"{table_name} cleared"}


@p2i_transformer_router.post("/build")
def build_event_log(username: str = Query("Unknown")):
    from p2i4 import USER_DFS, process_df, log_audit, _save_output_csv, register_transform_build, UPLOAD_DIR

    # Clear stale data
    if username in USER_DFS:
        del USER_DFS[username]
    if "Unknown" in USER_DFS:
        del USER_DFS["Unknown"]

    tables = RAW_TABLES.get(username, {})
    missing_req = [t for t in REQUIRED_TABLES if t not in tables]
    if missing_req:
        raise HTTPException(400, f"Missing required tables: {missing_req}.")

    try:
        for t in REQUIRED_TABLES:
            if t not in tables:
                raise ValueError(f"Required table '{t}' is missing.")
            df = tables[t]
            required = REQUIRED_COLS.get(t, set())
            uploaded_cols = {c.upper() for c in df.columns}
            missing = {c for c in required if c.upper() not in uploaded_cols}
            if missing:
                raise ValueError(f"Column mapping is incorrect for {t}: missing {sorted(missing)}")

        result_df = _run_pipeline(tables, username)
    except Exception as e:
        _log(f"Pipeline failed for '{username}': {e}\n{traceback.format_exc()}")
        err_msg = str(e)
        if "KeyError" in err_msg:
            err_msg = f"Missing column in data: {err_msg}"
        raise HTTPException(status_code=400, detail=f"Build failed: {err_msg}")

    processed = process_df(result_df)
    USER_DFS[username] = processed
    USER_DFS["Unknown"] = processed
    log_audit(username, "TRANSFORM", f"Built P2I event log: {len(processed)} rows")
    _log(f"Done for '{username}': {len(processed)} rows, {len(processed.columns)} cols.")

    ts        = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path  = ""
    save_path = ""

    # Save JSON session file
    try:
        import os as _os
        user_dir  = _os.path.join(UPLOAD_DIR, username)
        _os.makedirs(user_dir, exist_ok=True)
        save_path = _os.path.join(user_dir, f"{ts}_p2i_transform_build.json")
        processed.to_json(save_path, orient="records", date_format="iso")
        _log(f"Session JSON saved → {save_path}")
    except Exception as e:
        _log(f"Session JSON save failed (non-fatal): {e}")

    # Save wide CSV to output directory
    try:
        csv_name = f"P2I_Transform_{username}_{ts}.csv"
        csv_path = _save_output_csv(processed, csv_name)
        _log(f"Output CSV saved → {csv_path}")
    except Exception as e:
        _log(f"CSV output save failed (non-fatal): {e}")

    # Register in file registry
    try:
        register_transform_build(username, processed, save_path, csv_path)
    except Exception as e:
        _log(f"Registry update failed (non-fatal): {e}")

    return {
        "status":   "ok",
        "rows":     len(processed),
        "columns":  list(processed.columns),
        "csv_path": csv_path,
    }


# ── Core Pipeline ──────────────────────────────────────────────────────────────

def _run_pipeline(tables: dict[str, pd.DataFrame], username: str) -> pd.DataFrame:

    # ── Load tables ────────────────────────────────────────────────────────────
    # AUFK = Production Order Header (optional) — provides ERDAT + ERNAM
    # AFKO = Production Order Header Scheduling (optional) — provides scheduling dates
    # AFPO = Production Order Items (MANDATORY)
    afpo = tables["AFPO"].copy()
    afru = tables["AFRU"].copy() if "AFRU" in tables else None
    resb = tables["RESB"].copy() if "RESB" in tables else None
    mkpf = tables["MKPF"].copy() if "MKPF" in tables else None
    mseg = tables["MSEG"].copy() if "MSEG" in tables else None
    ekko = tables["EKKO"].copy() if "EKKO" in tables else None
    ekpo = tables["EKPO"].copy() if "EKPO" in tables else None
    eban = tables["EBAN"].copy() if "EBAN" in tables else None
    ekbe = tables["EKBE"].copy() if "EKBE" in tables else None
    lfa1 = tables["LFA1"].copy() if "LFA1" in tables else None

    # AUFK — Production Order Header: ERDAT (creation date) + ERNAM (creator)
    aufk_hdr    = tables.get("AUFK")   # ← separate from AFKO
    # AFKO — Scheduling header: Basic Start, Basic Finish, scheduled dates etc.
    aufk_loaded = tables.get("AFKO")
    counts = (
        f"AUFK:{len(aufk_hdr) if aufk_hdr is not None else 'not uploaded'} "
        f"AFKO:{len(aufk_loaded) if aufk_loaded is not None else 'not uploaded'} "
        f"AFPO:{len(afpo)} "
        f"AFRU:{len(afru) if afru is not None else 0} "
        f"RESB:{len(resb) if resb is not None else 0} "
        f"MKPF:{len(mkpf) if mkpf is not None else 0} "
        f"MSEG:{len(mseg) if mseg is not None else 0} "
        f"EKKO:{len(ekko) if ekko is not None else 0} "
        f"EKPO:{len(ekpo) if ekpo is not None else 0}"
    )
    _log(f"Input — {counts}")

    # ══════════════════════════════════════════════════════════════════════════
    # PRIMARY TABLE: AFPO (Production Order Items) — MANDATORY
    # AUFK (Production Order Header) — OPTIONAL; adds date columns when present.
    # ══════════════════════════════════════════════════════════════════════════
    afpo["AUFNR"] = _str(afpo["AUFNR"])

    # Rename XLOEK in AFPO per Renamer #44
    if "XLOEK" in afpo.columns:
        afpo = afpo.rename(columns={"XLOEK": "Deletion Indicator"})

    aufk = tables.get("AFKO")
    if aufk is not None:
        aufk = aufk.copy()
        aufk["AUFNR"] = _str(aufk["AUFNR"])
        # Column Renamer #43 — KNIME P2I_4 exact mapping
        aufk = aufk.rename(columns={
            "GSTRP": "Basic Start",
            "GSTRS": "Scheduled Start",
            "GSTRI": "Actual Start",
            "GLTRI": "Actual finish date",
        })
        aufk_date_cols = [
            "Basic Start",
            "Scheduled Start", "Actual Start", "Actual finish date",
        ]
        for c in aufk_date_cols:
            if c in aufk.columns:
                aufk[c] = pd.to_datetime(aufk[c], errors="coerce")
        # AUFK LEFT JOIN AFPO on AUFNR
        afpo_r = afpo.rename(columns={"AUFNR": "AUFNR(AFPO)"})
        prod = aufk.merge(afpo_r, left_on="AUFNR", right_on="AUFNR(AFPO)",
                          how="left", suffixes=("", "_AFPO_dup"))
        prod = prod[[c for c in prod.columns if not c.endswith("_AFPO_dup")]]
        # Restore POSNR from AFPO
        if "POSNR" not in prod.columns and "POSNR" in afpo.columns:
            afpo_pos = afpo[["AUFNR", "POSNR"]].rename(columns={"AUFNR": "_aufnr_pos"})
            prod = prod.merge(afpo_pos, left_on="AUFNR", right_on="_aufnr_pos",
                              how="left").drop(columns=["_aufnr_pos"], errors="ignore")
        _log(f"Joiner #45 AFKO LEFT JOIN AFPO: {len(prod)} rows | AFKO dates present ✓")
    else:
        # AUFK not uploaded — AFPO is the starting point; date columns will be NaN
        prod = afpo.copy()
        _log(f"AFPO as primary table (AFKO not uploaded — production dates (Basic Start, Basic Finish etc.) will be empty): "
             f"{len(prod)} rows")

    # ── Build UniqueID_P2I = join(AUFNR, POSNR) ──────────────────────────────
    def _norm_posnr(s: pd.Series) -> pd.Series:
        s = s.astype(str).str.strip().str.replace(r"\.0$", "", regex=True)
        is_num = s.str.match(r"^\d+$")
        return s.where(~is_num, s.str.zfill(4))

    def _norm_aufnr_key(s: pd.Series) -> pd.Series:
        s = s.astype(str).str.strip().str.replace(r"\.0$", "", regex=True)
        is_num = s.str.match(r"^\d+$")
        return s.where(~is_num, s.str.zfill(12))

    if "POSNR" in prod.columns:
        prod["POSNR"] = prod["POSNR"].fillna("0001")
        prod["UniqueID_P2I"] = _norm_aufnr_key(prod["AUFNR"]) + _norm_posnr(prod["POSNR"])
    else:
        prod["POSNR"] = "0001"
        prod["UniqueID_P2I"] = prod["AUFNR"].astype(str).str.strip().str.zfill(12) + "0001"

    _log(f"UniqueID_P2I built: {prod['UniqueID_P2I'].nunique()} unique order items "
         f"from {prod['AUFNR'].nunique()} unique orders")

    # ══════════════════════════════════════════════════════════════════════════
    # AUFK JOIN: Production Order Header
    # Brings in ERDAT → "Order Creation" (activity step 2 of the happy path)
    #           ERNAM → "ERNAM"          (order creator dimension column)
    #
    # This is a separate join from the AFKO (scheduling) branch above.
    # AUFK.ERDAT = the date SAP created the production order (transaction CO01/CO40)
    # AUFK.ERNAM = the SAP user who created the order
    # ══════════════════════════════════════════════════════════════════════════
    if aufk_hdr is not None:
        _aufk = aufk_hdr.copy()
        _aufk["AUFNR"] = _str(_aufk["AUFNR"])

        # Build a slim table with only the columns we need from AUFK
        aufk_slim_cols = ["AUFNR"]

        if "ERDAT" in _aufk.columns:
            _aufk["Production Order Creation"] = pd.to_datetime(_aufk["ERDAT"], errors="coerce")
            aufk_slim_cols.append("Production Order Creation")
            _log(f"AUFK: ERDAT → 'Production Order Creation' parsed for "
                 f"{_aufk['Production Order Creation'].notna().sum()}/{len(_aufk)} rows")
        else:
            _log("AUFK WARNING: 'ERDAT' column not found — 'Production Order Creation' will be NaT")

        if "ERNAM" in _aufk.columns:
            _aufk["ERNAM"] = _aufk["ERNAM"].astype(str).str.strip().replace("nan", None)
            aufk_slim_cols.append("ERNAM")
            _log(f"AUFK: ERNAM (order creator) found for "
                 f"{_aufk['ERNAM'].notna().sum()}/{len(_aufk)} rows")
        else:
            _log("AUFK WARNING: 'ERNAM' column not found — order creator will be None")

        # One row per order (AUFNR) — take the first if duplicates exist
        _aufk_slim = (
            _aufk[aufk_slim_cols]
            .drop_duplicates(subset=["AUFNR"])
            .reset_index(drop=True)
        )

        # If ERNAM already exists in prod (e.g. from AFRU via "Order Creation User"),
        # keep the existing column and only add AUFK's ERNAM if the prod column is absent.
        _aufk_merge_cols = ["AUFNR"]
        if "Production Order Creation" in _aufk_slim.columns:
            _aufk_merge_cols.append("Production Order Creation")
        if "ERNAM" in _aufk_slim.columns:
            # Only include ERNAM in the merge if prod doesn't already have it
            if "ERNAM" not in prod.columns:
                _aufk_merge_cols.append("ERNAM")
            else:
                # prod already has ERNAM from another source — rename to avoid clash
                _aufk_slim = _aufk_slim.rename(columns={"ERNAM": "ERNAM_AUFK"})
                _aufk_merge_cols.append("ERNAM_AUFK")

        _aufk_slim = _aufk_slim[_aufk_merge_cols]

        prod = prod.merge(_aufk_slim, on="AUFNR", how="left", suffixes=("", "_AUFK_dup"))
        prod = prod[[c for c in prod.columns if not c.endswith("_AUFK_dup")]]

        # If ERNAM came in as ERNAM_AUFK (because prod had ERNAM already),
        # use it to fill blanks in the existing ERNAM column then drop the temp column.
        if "ERNAM_AUFK" in prod.columns:
            if "ERNAM" in prod.columns:
                prod["ERNAM"] = prod["ERNAM"].fillna(prod["ERNAM_AUFK"])
            else:
                prod = prod.rename(columns={"ERNAM_AUFK": "ERNAM"})
            prod = prod.drop(columns=["ERNAM_AUFK"], errors="ignore")

        _log(f"AUFK joined: {len(_aufk_slim)} unique orders → "
             f"Production Order Creation: {prod['Production Order Creation'].notna().sum() if 'Production Order Creation' in prod.columns else 0} rows filled | "
             f"ERNAM: {prod['ERNAM'].notna().sum() if 'ERNAM' in prod.columns else 0} rows filled")
    else:
        # AUFK not uploaded — add empty columns so downstream code doesn't break
        if "Production Order Creation" not in prod.columns:
            prod["Production Order Creation"] = pd.NaT
        if "ERNAM" not in prod.columns:
            prod["ERNAM"] = None
        _log("AUFK not uploaded — 'Order Creation' = NaT, 'ERNAM' = None")

    # ══════════════════════════════════════════════════════════════════════════
    # OPERATION CONFIRMATIONS BRANCH: AFRU
    # Column Filter #12 → Renamer #48 → Rule Engine #50 → collapse to wide
    # ══════════════════════════════════════════════════════════════════════════
    if afru is not None:
        afru["AUFNR"] = _str(afru["AUFNR"])

        # Column Filter #12 — keep relevant AFRU cols
        col12 = [
            "AUFNR", "RUECK", "RMZHL", "ERSDA", "ERNAM", "LAEDA", "AENAM",
            "BUDAT", "WERKS", "STOKZ", "ISDD", "ISBD", "IEDD", "IEDZ",
            "VORNR", "SMENG", "BELNR_IST", "BELNR_UMB", "RMNGA",
            "CATSBELNR", "ERZET", "AUFPL", "APLZL",
        ]
        afru = _keep(afru, col12)
        _log(f"Column Filter #12 AFRU: {len(afru.columns)} cols")

        # Renamer #48: rename key AFRU columns
        afru = afru.rename(columns={
            "ERSDA": "Order Confirmation",
            "BUDAT": "Order Posting",
            "LAEDA": "Last Change",
            "ERNAM": "Order Creation User",
            "STOKZ": "Document Reverse",
            "ISDD":  "Start of Execution",
            "IEDD":  "Execution Finish",
            "ISBD":  "Processing Start",
        })
        _log(f"Renamer #48: AFRU columns renamed")

        # Rule Engine #50: $Document Reverse$ = "X" => $Order Confirmation$
        # → new col "Operation Reversal"
        if "Document Reverse" in afru.columns and "Order Confirmation" in afru.columns:
            afru["Order Confirmation"] = pd.to_datetime(afru["Order Confirmation"], errors="coerce")
            afru["Operation Reversal"] = afru["Order Confirmation"].where(
                _str(afru["Document Reverse"]) == "X"
            )
            _log(f"Rule Engine #50: Operation Reversal = {afru['Operation Reversal'].notna().sum()} rows")
        else:
            afru["Operation Reversal"] = pd.NaT

        # Parse remaining date cols
        # Convert ALL potential date cols to datetime before any groupby
        afru_date_cols = [
            "Order Confirmation", "Order Posting", "Start of Execution",
            "Processing Start", "Execution Finish", "Operation Reversal",
            "Last Change",   # LAEDA — must be converted or max() fails on object dtype
        ]
        for c in afru_date_cols:
            if c in afru.columns:
                afru[c] = pd.to_datetime(afru[c], errors="coerce")

        # Safe groupby collapse — only apply min/max to confirmed-datetime cols
        afru_date_agg = {
            "Order Confirmation":  "min",
            "Order Posting":       "min",
            "Start of Execution":  "min",
            "Processing Start":    "min",
            "Execution Finish":    "max",
            "Operation Reversal":  "max",
            "Last Change":         "max",
        }
        afru_str_agg = {
            "Order Creation User": "first",
        }

        def _safe_afru_agg(df: pd.DataFrame, key: str, how: str):
            """Apply agg only when dtype is datetime; fall back to first."""
            col = df[key]
            if pd.api.types.is_datetime64_any_dtype(col):
                return col.agg(how)
            return col.iloc[0] if len(col) else pd.NaT

        afru_agg_dict = {}
        for col, agg in {**afru_date_agg, **afru_str_agg}.items():
            if col in afru.columns:
                afru_agg_dict[col] = agg

        if afru_agg_dict:
            # Build per-column agg ensuring only datetime cols use min/max
            safe_agg = {}
            for col, agg in afru_agg_dict.items():
                if col in afru.columns:
                    if agg in ("min", "max"):
                        if pd.api.types.is_datetime64_any_dtype(afru[col]):
                            safe_agg[col] = agg
                        else:
                            safe_agg[col] = "first"  # fallback for unconverted cols
                    else:
                        safe_agg[col] = agg

            afru_wide = (
                afru.groupby("AUFNR", sort=False)
                .agg(**{k: (k, v) for k, v in safe_agg.items()})
                .reset_index()
            )
        else:
            afru_wide = afru[["AUFNR"]].drop_duplicates()
        _log(f"AFRU collapsed: {len(afru_wide)} unique AUFNR values")

        # Joiner #49: prod LEFT JOIN afru_wide on AUFNR
        prod = prod.merge(afru_wide, on="AUFNR", how="left", suffixes=("", " (AFRU)"))
        _log(f"Joiner #49 +AFRU: {len(prod)} rows, {len(prod.columns)} cols")
    else:
        # Add empty operation confirmation columns
        for col in ["Order Confirmation", "Order Posting", "Start of Execution",
                    "Processing Start", "Execution Finish", "Operation Reversal",
                    "Order Creation User"]:
            prod[col] = pd.NaT
        _log("AFRU not provided — empty operation columns added")

    # ══════════════════════════════════════════════════════════════════════════
    # RESERVATIONS BRANCH: RESB
    # Column Filter #103 → Joiner #99: prod LEFT JOIN resb on AUFNR
    # ══════════════════════════════════════════════════════════════════════════
    if resb is not None:
        resb["AUFNR"] = _str(resb["AUFNR"])

        col103 = [
            "AUFNR", "RSNUM", "RSPOS", "BDART", "XLOEK", "MATNR",
            "WERKS", "LGORT", "CHARG", "BDTER", "BDMNG", "WAERS",
            "SAKNR", "POSNR", "STLTY", "STLNR", "AUFPL", "APLZL",
            "RSART", "RSSTA", "MEINS", "SHKZG", "BANFN", "BNFPO",
        ]
        resb_f = _keep(resb, col103)
        _log(f"Column Filter #103 RESB: {len(resb_f.columns)} cols")

        # Collapse RESB to one row per AUFNR (keep first non-null per column)
        resb_agg = {c: "first" for c in resb_f.columns if c != "AUFNR"}
        resb_wide = resb_f.groupby("AUFNR", sort=False).agg(resb_agg).reset_index()

        # Add suffix to RESB cols that may clash with prod cols
        resb_rename = {}
        for c in resb_wide.columns:
            if c != "AUFNR" and c in prod.columns:
                resb_rename[c] = f"{c} (RESB)"
        if resb_rename:
            resb_wide = resb_wide.rename(columns=resb_rename)

        prod = prod.merge(resb_wide, on="AUFNR", how="left", suffixes=("", " (RESB_dup)"))
        prod = prod[[c for c in prod.columns if not c.endswith("_RESB_dup")]]
        _log(f"Joiner #99 +RESB: {len(prod)} rows, {len(prod.columns)} cols")
    else:
        _log("RESB not provided — skipped")

    # ══════════════════════════════════════════════════════════════════════════
    # MATERIAL DOCUMENTS BRANCH: MKPF + MSEG
    # String Manip #54/#55: Unique Key = join(MBLNR, MJAHR/GJAHR)
    # Joiner #56: MKPF INNER JOIN MSEG on Unique Key
    # String Manip #107: UniqueID_PO = join(EBELN, EBELP)
    # Column Filter #57: keep relevant cols
    # Column Expressions #59: classify by BWART → activity date columns
    # Collapse to wide by AUFNR (earliest date per movement type)
    # Joiner #104: prod INNER JOIN mat_docs_wide on AUFNR
    # ══════════════════════════════════════════════════════════════════════════
    mat_wide = None
    if mkpf is not None and mseg is not None:
        mkpf = mkpf.copy()
        mseg = mseg.copy()

        # String Manipulation #54: Unique Key = join(MBLNR, GJAHR) in MKPF
        # String Manipulation #55: Unique Key = join(MBLNR, MJAHR) in MSEG
        # NOTE: MKPF uses GJAHR (fiscal year), MSEG uses MJAHR (material doc year)
        # In practice these are often identical; we try GJAHR first in MKPF, else MJAHR
        mkpf["MBLNR"] = _str(mkpf["MBLNR"])
        mseg["MBLNR"] = _str(mseg["MBLNR"])

        mkpf_year = "GJAHR" if "GJAHR" in mkpf.columns else ("MJAHR" if "MJAHR" in mkpf.columns else None)
        mseg_year = "MJAHR" if "MJAHR" in mseg.columns else ("GJAHR" if "GJAHR" in mseg.columns else None)

        if mkpf_year and mseg_year:
            mkpf["_uk"] = _str(mkpf["MBLNR"]) + _str(mkpf[mkpf_year])
            mseg["_uk"] = _str(mseg["MBLNR"]) + _str(mseg[mseg_year])

            # Joiner #56: MKPF INNER JOIN MSEG on Unique Key
            col_filter_57 = [
                "MBLNR", "MJAHR", "BLART", "BLDAT", "BUDAT", "CPUDT", "AEDAT",
                "USNAM", "TCODE", "XBLNR", "AUFNR", "BELNR",  # AUFPS excluded — does not match AFPO.POSNR
                "BUDAT_MKPF", "BUKRS", "BUZEI", "BWART", "CHARG", "DMBTR",
                "EBELN", "EBELP", "GJAHR", "LGORT", "LIFNR", "MATNR",
                "SGTXT", "SHKZG", "WERKS", "_uk",
            ]
            # Select available cols from MSEG (has the detailed line data)
            mseg_avail = _keep(mseg, col_filter_57 + ["_uk"])
            mkpf_avail = _keep(mkpf, ["_uk", "BUDAT", "USNAM", "TCODE", "BLART"])

            # INNER JOIN on Unique Key
            mat_docs = mkpf_avail.merge(
                mseg_avail, on="_uk", how="inner", suffixes=("_MKPF", "")
            )
            # Resolve BUDAT: prefer MSEG BUDAT (line-level posting date)
            if "BUDAT_MKPF" in mat_docs.columns and "BUDAT" in mat_docs.columns:
                mat_docs["BUDAT"] = mat_docs["BUDAT"].fillna(mat_docs["BUDAT_MKPF"])
            _log(f"Joiner #56 MKPF INNER JOIN MSEG: {len(mat_docs)} rows")

            # String Manipulation #107: UniqueID_PO = join(EBELN, EBELP)
            if "EBELN" in mat_docs.columns and "EBELP" in mat_docs.columns:
                mat_docs["UniqueID_PO"] = _safe_join_key(
                    mat_docs["EBELN"], mat_docs["EBELP"], pad1=10, pad2=5
                )

            # Normalise AUFNR from material documents
            if "AUFNR" in mat_docs.columns:
                mat_docs["AUFNR"] = _str(mat_docs["AUFNR"])

            # Column Expressions #59: classify by BWART → activity date columns
            mat_docs["BUDAT"] = pd.to_datetime(mat_docs["BUDAT"], errors="coerce")
            if "BWART" in mat_docs.columns:
                bwart_str = _str(mat_docs["BWART"])
                for bwart_code, col_name in BWART_MAP.items():
                    mat_docs[col_name] = mat_docs["BUDAT"].where(bwart_str == bwart_code)
                _log(f"Column Expressions #59: classified {len(BWART_MAP)} BWART movement types")

            # ─────────────────────────────────────────────────────────────────
            # KNIME Joiner #104: material docs join to production on AUFNR ONLY.
            #
            # WHY NOT AUFPS?
            #   MSEG.AUFPS is an internal operation reference — it does NOT match
            #   AFPO.POSNR (the order item number). The KNIME P2I_4 workflow
            #   Joiner #104 joins material documents on AUFNR only.
            #
            # RESULT: Movement dates are at order (AUFNR) level. All AFPO items
            #   (POSNR) for the same AUFNR share the same movement dates.
            #   UniqueID_P2I = AUFNR+POSNR granularity comes only from AFPO.
            # ─────────────────────────────────────────────────────────────────
            if "AUFNR" in mat_docs.columns:
                mat_docs["AUFNR"] = _str(mat_docs["AUFNR"])

            # Collapse to wide: one row per AUFNR (earliest date per BWART type)
            bwart_cols = list(BWART_MAP.values())
            agg_dict = {col: "min" for col in bwart_cols if col in mat_docs.columns}
            if "UniqueID_PO" in mat_docs.columns:
                agg_dict["UniqueID_PO"] = "first"

            if "AUFNR" in mat_docs.columns and agg_dict:
                mat_wide = (
                    mat_docs[mat_docs["AUFNR"].notna()]
                    .groupby("AUFNR", sort=False)
                    .agg(agg_dict)
                    .reset_index()
                )
                for c in bwart_cols:
                    if c in mat_wide.columns:
                        mat_wide[c] = pd.to_datetime(mat_wide[c], errors="coerce")
                _log(
                    f"Mat docs wide (AUFNR level — KNIME Joiner #104): "
                    f"{len(mat_wide)} unique orders | "
                    f"cols: {[c for c in bwart_cols if c in mat_wide.columns]}"
                )

                # Joiner #104: prod LEFT JOIN mat_wide on AUFNR (KNIME exact)
                # Each AFPO item (UniqueID_P2I = AUFNR+POSNR) gets the same
                # order-level movement dates — correct per KNIME P2I_4 workflow.
                prod = prod.merge(mat_wide, on="AUFNR", how="left", suffixes=("", "_MATDOC"))
                prod = prod[[c for c in prod.columns if not c.endswith("_MATDOC")]]
                _log(f"Joiner #104 +MatDocs on AUFNR: {len(prod)} rows, {len(prod.columns)} cols")
            else:
                _log("Material docs: AUFNR missing or no BWART cols — skipping")
        else:
            _log("MKPF/MSEG: missing year columns (GJAHR/MJAHR) — skipping material docs join")
    else:
        # Add empty movement type columns
        for col_name in BWART_MAP.values():
            prod[col_name] = pd.NaT
        _log("MKPF/MSEG not provided — empty material movement columns added")

    # ══════════════════════════════════════════════════════════════════════════
    # PO/GR/INVOICE BRANCH: identical to P2P transformer
    # ══════════════════════════════════════════════════════════════════════════
    po_wide = None  # Will hold the final po_pr_gr_inv result
    po_uid_map = {}  # AUFNR → UniqueID_PO (for linking)

    if ekko is not None and ekpo is not None:
        # ── PO Branch (Node #8, #14, #15, #16, #17, #95) ──────────────────────
        ekko["EBELN"] = _str(ekko["EBELN"])
        ekpo["EBELN"] = _str(ekpo["EBELN"])

        # Pre-rename EKPO clashing cols before join
        ekpo["EBELN (EKPO)"] = ekpo["EBELN"]
        ekpo_rename = {}
        for col in ["LOEKZ", "AEDAT"]:
            if col in ekpo.columns:
                ekpo_rename[col] = f"{col} (EKPO)"
        if ekpo_rename:
            ekpo = ekpo.rename(columns=ekpo_rename)

        # Node #8: EKKO INNER JOIN EKPO on EBELN
        po = ekko.merge(ekpo, on="EBELN", how="inner", suffixes=("", "_EKPO_dup"))
        po = po[[c for c in po.columns if not c.endswith("_EKPO_dup")]]
        _log(f"PO #8 EKKO INNER JOIN EKPO: {len(po)} rows")

        # Node #15: UniqueID_PO = join(EBELN(EKPO), EBELP)
        po["UniqueID_PO"] = _safe_join_key(
            po["EBELN (EKPO)"], po["EBELP"], pad1=10, pad2=5
        )

        # Node #16: UniqueID_PR = join(BANFN, BNFPO)
        po["UniqueID_PR"] = _safe_join_key(
            po["BANFN"], po["BNFPO"], pad1=10, pad2=5
        )

        # Node #17: PO Reversal Date = AEDAT(EKPO) if LOEKZ(EKPO) IN ("L","C")
        loekz_ekpo = po.get("LOEKZ (EKPO)", pd.Series(dtype=str, index=po.index))
        aedat_ekpo  = po.get("AEDAT (EKPO)", pd.Series(dtype=str, index=po.index))
        po["PO Reversal Date"] = aedat_ekpo.where(_str(loekz_ekpo).isin(["L", "C"]))

        # Node #95: AEDAT → "PO Creation", BEDAT → "PO Date"
        po = po.rename(columns={"AEDAT": "PO Creation", "BEDAT": "PO Date"})
        _log(f"PO branch done: {len(po)} rows")

        # ── PR Branch (Node #98, #94, PR #19, #34) ────────────────────────────
        if eban is not None:
            col_eban = [
                "BANFN", "BNFPO", "LOEKZ", "ERNAM", "ERDAT",
                "BADAT", "FRGDT", "CREATIONDATE", "CREATIONTIME",
                "BSART", "BSTYP", "EKGRP", "MATNR", "WERKS",
                "MATKL", "RSNUM", "ARSNR",
            ]
            pr = _keep(eban, col_eban)

            # Node #94: PR Reversal Date = ERDAT if LOEKZ == "X"
            pr["PR Reversal Date"] = pr["ERDAT"].where(
                _str(pr.get("LOEKZ", pd.Series(dtype=str, index=pr.index))) == "X"
            )

            # PR Node #19: UniqueID_PR = join(BANFN, BNFPO)
            pr["UniqueID_PR"] = _safe_join_key(
                pr["BANFN"], pr["BNFPO"], pad1=10, pad2=5
            )

            # Node #34: BADAT → "PR Creation", FRGDT → "PR Release Date"
            pr = pr.rename(columns={"BADAT": "PR Creation", "FRGDT": "PR Release Date"})

            # Node #20: PO LEFT JOIN PR on UniqueID_PR
            po_pr = po.merge(pr, on="UniqueID_PR", how="left", suffixes=("", " (EBAN)"))
        else:
            po_pr = po.copy()
            po_pr["PR Creation"]    = pd.NaT
            po_pr["PR Release Date"] = pd.NaT
            po_pr["PR Reversal Date"] = pd.NaT
            po_pr["ERNAM (EBAN)"]  = None
        _log(f"PO+PR branch: {len(po_pr)} rows")

        # ── GR & Invoice Branches (EKBE) ───────────────────────────────────────
        if ekbe is not None:
            ekbe["EBELN"] = _str(ekbe["EBELN"])
            if "EBELP" in ekbe.columns:
                ekbe["EBELP"] = _str(ekbe["EBELP"])

            col_ekbe = [
                "VGABE", "GJAHR", "BELNR", "BUDAT",
                "MENGE", "BPMNG", "DMBTR", "SHKZG",
                "XBLNR", "WERKS", "ERNAM",
            ]

            # GR Branch (VGABE=1)
            gr = ekbe[ekbe["VGABE"].astype(str).str.strip() == "1"].copy()
            _log(f"GR (VGABE=1): {len(gr)} rows")
            gr["UniqueID_PO"] = _safe_join_key(gr["EBELN"], gr["EBELP"], pad1=10, pad2=5)
            gr = _keep(gr, col_ekbe + ["UniqueID_PO"])
            s_mask = _str(gr["SHKZG"]) == "S"
            h_mask = _str(gr["SHKZG"]) == "H"
            gr["GR Posting"]                = gr["BUDAT"].where(s_mask)
            gr["GR Reversal"]               = gr["BUDAT"].where(h_mask)
            gr["GR Creation User"]          = gr["ERNAM"].where(s_mask)
            gr["GR Reversal Creation User"] = gr["ERNAM"].where(h_mask)
            gr_wide = _collapse_ekbe_branch(
                gr,
                posting_col="GR Posting", reversal_col="GR Reversal",
                posting_user_col="GR Creation User", reversal_user_col="GR Reversal Creation User",
                out_posting="GR Posting", out_reversal="GR Reversal Date",
                out_posting_user="GR Creation User", out_reversal_user="GR Reversal Creation User",
            )
            _log(f"GR wide: {len(gr_wide)} unique UniqueID_PO")

            # Invoice Branch (VGABE=2)
            inv = ekbe[ekbe["VGABE"].astype(str).str.strip() == "2"].copy()
            _log(f"Invoice (VGABE=2): {len(inv)} rows")
            inv["UniqueID_PO"] = _safe_join_key(inv["EBELN"], inv["EBELP"], pad1=10, pad2=5)
            inv = _keep(inv, col_ekbe + ["UniqueID_PO"])
            s_inv = _str(inv["SHKZG"]) == "S"
            h_inv = _str(inv["SHKZG"]) == "H"
            inv["Invoice Posting"]                  = inv["BUDAT"].where(s_inv)
            inv["Invoice Reversal"]                 = inv["BUDAT"].where(h_inv)
            inv["Invoice Creation User"]            = inv["ERNAM"].where(s_inv)
            inv["Invoice Reversal Creation User"]   = inv["ERNAM"].where(h_inv)
            inv_wide = _collapse_ekbe_branch(
                inv,
                posting_col="Invoice Posting", reversal_col="Invoice Reversal",
                posting_user_col="Invoice Creation User", reversal_user_col="Invoice Reversal Creation User",
                out_posting="Invoice Posting", out_reversal="Invoice Reversal Date",
                out_posting_user="Invoice Creation User", out_reversal_user="Invoice Reversal Creation User",
            )
            _log(f"Invoice wide: {len(inv_wide)} unique UniqueID_PO")
        else:
            gr_wide = pd.DataFrame(columns=[
                "UniqueID_PO", "GR Posting", "GR Reversal Date",
                "GR Creation User", "GR Reversal Creation User",
            ])
            inv_wide = pd.DataFrame(columns=[
                "UniqueID_PO", "Invoice Posting", "Invoice Reversal Date",
                "Invoice Creation User", "Invoice Reversal Creation User",
            ])

        # Node #81: po_pr LEFT JOIN gr_wide on UniqueID_PO
        po_pr_gr = po_pr.merge(
            gr_wide, on="UniqueID_PO", how="left", suffixes=("", " (GRN)")
        )
        # Node #84: po_pr_gr LEFT JOIN inv_wide on UniqueID_PO
        po_all = po_pr_gr.merge(
            inv_wide, on="UniqueID_PO", how="left", suffixes=("", " (Right)")
        )
        _log(f"PO+PR+GR+Invoice: {len(po_all)} rows, {len(po_all.columns)} cols")

        # Node #85: Value Lookup LIFNR → NAME1
        def _norm_lifnr(s: pd.Series) -> pd.Series:
            s = s.astype(str).str.strip().str.replace(r'\.0$', '', regex=True)
            is_numeric = s.str.match(r'^\d+$')
            return s.where(~is_numeric, s.str.zfill(10))

        if lfa1 is not None and "LIFNR" in lfa1.columns and "NAME1" in lfa1.columns:
            lfa1_map = (
                lfa1[["LIFNR", "NAME1"]]
                .drop_duplicates(subset="LIFNR")
                .assign(LIFNR=lambda d: _norm_lifnr(d["LIFNR"]))
            )
            if "LIFNR" in po_all.columns:
                po_all["LIFNR"] = _norm_lifnr(po_all["LIFNR"])
            if "NAME1" in po_all.columns:
                po_all = po_all.drop(columns=["NAME1"])
            po_all = po_all.merge(lfa1_map, on="LIFNR", how="left")
            _log(f"VALUE Lookup NAME1: {po_all['NAME1'].notna().sum()}/{len(po_all)} matched")
        else:
            po_all["NAME1"] = None
            _log("LFA1 not provided — NAME1 set to None")

        # Collapse po_all to one row per UniqueID_PO
        if "UniqueID_PO" in po_all.columns:
            _po_date_cols = ["PO Creation", "PO Date", "PO Reversal Date",
                              "PR Creation", "PR Release Date", "PR Reversal Date",
                              "GR Posting", "GR Reversal Date",
                              "Invoice Posting", "Invoice Reversal Date"]
            for _pdc in _po_date_cols:
                if _pdc in po_all.columns:
                    po_all[_pdc] = pd.to_datetime(po_all[_pdc], errors="coerce")
            agg_dict_po = {c: "first" for c in po_all.columns if c != "UniqueID_PO"}
            po_wide = (
                po_all.groupby("UniqueID_PO", sort=False)
                .agg(agg_dict_po)
                .reset_index()
            )
            _log(f"PO side collapsed: {len(po_wide)} unique UniqueID_PO")
    else:
        _log("EKKO/EKPO not provided — PO/GR/Invoice branch skipped")

    # ══════════════════════════════════════════════════════════════════════════
    # Collapse production side to one row per UniqueID_P2I (AUFNR + POSNR)
    # This gives ITEM-LEVEL granularity — one case per production order line
    # ══════════════════════════════════════════════════════════════════════════
    _log(f"Before prod collapse: {len(prod)} rows, "
         f"{prod['UniqueID_P2I'].nunique() if 'UniqueID_P2I' in prod.columns else '?'} unique UniqueID_P2I, "
         f"{prod['AUFNR'].nunique()} unique AUFNR")

    if "UniqueID_P2I" not in prod.columns:
        # Safety fallback — should not happen
        prod["UniqueID_P2I"] = prod["AUFNR"].astype(str).str.zfill(12) + "0001"

    # Convert all KNIME-renamed date cols to datetime before final collapse
    _knime_date_cols = [
        # ── P2I happy-path activity columns (steps 1–8) ──────────────────────
        "Production Order Creation",
        "Order Confirmation",
        "Basic Start",
        "Scheduled Start",
        "Actual Start",
        "Goods Issued to Order",
        "Finished Goods Receipt",
        "Finished Goods to Quality Inspection",
        # ── AFKO scheduling dates ─────────────────────────────────────────────
        "Basic Finish", "Scheduled Release",
        "Scheduled Finish", "Planned release date",
        "Confirmed Order Finish Date", "Actual finish date", "Actual release",
        # ── AFRU operation dates ──────────────────────────────────────────────
        "Order Posting", "Start of Execution",
        "Processing Start", "Execution Finish",
        # ── Production Deviations ─────────────────────────────────────────────
        "Operation Reversed",
        "Goods Issued Reversed",
        "Goods transferred to subcontractor",
        "Finished Goods Reversed",
        "Quality Inspection to Finished Goods",
        # ── P2P / procurement dates ───────────────────────────────────────────
        "PR Creation", "PR Release Date", "PR Reversal Date",
        "PO Creation", "PO Date", "PO Reversal Date",
        "GR Posting", "GR Reversal Date", "Invoice Posting", "Invoice Reversal Date",
    ]
    for _dc in _knime_date_cols:
        if _dc in prod.columns:
            prod[_dc] = pd.to_datetime(prod[_dc], errors="coerce")

    agg_dict_prod = {c: "first" for c in prod.columns if c != "UniqueID_P2I"}
    prod_collapsed = prod.groupby("UniqueID_P2I", sort=False).agg(agg_dict_prod).reset_index()
    _log(f"Prod side collapsed: {len(prod_collapsed)} rows "
         f"(item-level: AUFNR+POSNR)")

    # ══════════════════════════════════════════════════════════════════════════
    # FINAL JOIN: Joiner #106
    # prod_collapsed LEFT JOIN po_wide on UniqueID_PO
    # (links production orders to their procurement/goods receipt chain)
    # ══════════════════════════════════════════════════════════════════════════
    if po_wide is not None and "UniqueID_PO" in prod_collapsed.columns:
        result = prod_collapsed.merge(
            po_wide, on="UniqueID_PO", how="left", suffixes=("", "_PO_dup")
        )
        result = result[[c for c in result.columns if not c.endswith("_PO_dup")]]
        _log(f"Joiner #106 prod LEFT JOIN PO: {len(result)} rows, {len(result.columns)} cols")
    elif po_wide is not None:
        # No UniqueID_PO in prod side — try linking via AUFNR from mat docs
        _log("No UniqueID_PO in prod side; PO branch columns added without join")
        result = prod_collapsed.copy()
        for po_col in ["PO Creation", "PO Date", "PO Reversal Date", "PR Creation",
                       "PR Release Date", "PR Reversal Date", "GR Posting",
                       "GR Reversal Date", "Invoice Posting", "Invoice Reversal Date",
                       "GR Creation User", "Invoice Creation User", "NAME1", "LIFNR"]:
            if po_col not in result.columns:
                result[po_col] = pd.NaT if "Date" in po_col or po_col in [
                    "PO Creation", "PO Date", "GR Posting", "Invoice Posting",
                    "PR Creation", "PR Release Date"] else None
    else:
        result = prod_collapsed.copy()
        _log("No PO branch available — production-only result")

    # ── Final sanity log ──────────────────────────────────────────────────────
    from p2i4 import ACTIVITY_COLUMNS
    activity_cols = [c for c in ACTIVITY_COLUMNS if c in result.columns]
    missing_act   = [c for c in ACTIVITY_COLUMNS if c not in result.columns]
    dim_cols_check = ["UniqueID_P2I", "AUFNR", "POSNR", "BUKRS", "LIFNR", "NAME1",
                      "BSART", "MATNR", "WERKS", "EKGRP", "ERNAM"]
    present_dim   = [c for c in dim_cols_check if c in result.columns]

    _log(f"Activity cols present : {activity_cols}")
    _log(f"Activity cols MISSING : {missing_act}")
    _log(f"Dimension cols present: {present_dim}")
    _log(f"Final output          : {len(result)} rows × {len(result.columns)} cols")
    _log(f"Case ID granularity   : {result['UniqueID_P2I'].nunique() if 'UniqueID_P2I' in result.columns else '?'} "
         f"unique order items from "
         f"{result['AUFNR'].nunique() if 'AUFNR' in result.columns else '?'} unique orders")

    return result


# ── Helper: collapse EKBE branch ──────────────────────────────────────────────

def _collapse_ekbe_branch(
    df: pd.DataFrame,
    posting_col: str,
    reversal_col: str,
    posting_user_col: str,
    reversal_user_col: str,
    out_posting: str,
    out_reversal: str,
    out_posting_user: str,
    out_reversal_user: str,
) -> pd.DataFrame:
    """
    Collapse multiple EKBE rows per UniqueID_PO into one wide row.
    Posting date = earliest (MIN); Reversal date = latest (MAX).
    """
    if df.empty:
        return pd.DataFrame(columns=[
            "UniqueID_PO", out_posting, out_reversal,
            out_posting_user, out_reversal_user,
        ])

    for col in [posting_col, reversal_col, posting_user_col, reversal_user_col]:
        if col in df.columns and col in (posting_col, reversal_col):
            df[col] = pd.to_datetime(df[col], errors="coerce")
    # Verify datetime before agg
    for col in [posting_col, reversal_col]:
        if col in df.columns and not pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = pd.to_datetime(df[col], errors="coerce")

    result = (
        df.groupby("UniqueID_PO", sort=False)
        .agg(
            **{out_posting:       (posting_col,       "min")},
            **{out_reversal:      (reversal_col,      "max")},
            **{out_posting_user:  (posting_user_col,  "first")},
            **{out_reversal_user: (reversal_user_col, "first")},
        )
        .reset_index()
    )

    for c in [out_posting, out_reversal]:
        result[c] = pd.to_datetime(result[c], errors="coerce")

    return result
