"""
p2ptransformer.py  —  KNIME P2P_4 exact replication → wide-format output for p2p4.py
========================================================================================

KNIME WORKFLOW NODE MAP  (P2P_4.knwf)
---------------------------------------

SOURCE TABLES
  CSV #1  → EKKO   (Purchase Order Header)
  CSV #2  → EKPO   (Purchase Order Item)
  CSV #3  → EBAN   (Purchase Requisition)
  CSV #4  → EKBE   (PO History / GR & Invoice lines)
  CSV #89 → LFA1   (Vendor Master)

PO BRANCH
  #8   Joiner        EKKO INNER JOIN EKPO on EBELN
                     Duplicate cols renamed: LOEKZ→"LOEKZ (EKPO)", AEDAT→"AEDAT (EKPO)"
                     EKPO EBELN copied as "EBELN (EKPO)" before join
  #14  Column Filter  keep 22 cols (incl. WERKS, EKGRP from EKPO side)
  #15  String Manip   UniqueID_PO = join(EBELN(EKPO), EBELP)      [replaces col]
  #16  String Manip   UniqueID_PR = join(BANFN, BNFPO)             [replaces col → new]
  #17  Rule Engine    PO Reversal Date = AEDAT(EKPO) if LOEKZ(EKPO) IN ("L","C") [append]
  #35  Col Renamer    AEDAT → "PO Creation",  BEDAT → "PO Date"

PR BRANCH
  #12  Column Filter  keep 9 cols from EBAN
  #13  Rule Engine    PR Reversal Date = ERDAT if LOEKZ == "X"     [append]
  #19  String Manip   UniqueID_PR = join(BANFN, BNFPO)             [replaces col]
  #34  Col Renamer    BADAT → "PR Creation",  FRGDT → "PR Release Date"

JOIN PO + PR
  #20  Joiner         PO LEFT JOIN PR on UniqueID_PR
                     Right cols brought in with suffix " (EBAN)" for clashes:
                       BANFN (EBAN), BNFPO (EBAN), LOEKZ (EBAN), ERNAM (EBAN),
                       CREATIONDATE (EBAN), CREATIONTIME (EBAN), UniqueID_PR (EBAN)
                     Right cols brought in as-is (no clash):
                       ERDAT, PR Creation, PR Release Date, PR Reversal Date

GR BRANCH  (EKBE where VGABE == "1")
  #21  Row Filter     VGABE == "1"
  #27  String Manip   UniqueID_PO = join(EBELN, EBELP)
  #79  Column Filter  keep 12 cols: VGABE,GJAHR,BELNR,BUDAT,MENGE,BPMNG,DMBTR,
                                    SHKZG,XBLNR,WERKS,ERNAM,UniqueID_PO
  #80  Col Expressions (legacy)  [4 new columns appended]:
         GR Posting              = BUDAT  if SHKZG == "S"  else null
         GR Reversal             = BUDAT  if SHKZG == "H"  else null
         GR Creation User        = ERNAM  if SHKZG == "S"  else null
         GR Reversal Creation User = ERNAM if SHKZG == "H" else null
  → Collapse to wide (one row per UniqueID_PO) replacing Unpivot for p2p4:
         GR Posting              = earliest non-null (min)
         GR Reversal Date        = latest  non-null (max)   ← KNIME #81 expects this name
         GR Creation User        = first non-null
         GR Reversal Creation User = first non-null

INVOICE BRANCH  (EKBE where VGABE == "2")
  #22  Row Filter     VGABE == "2"
  #30  String Manip   UniqueID_PO = join(EBELN, EBELP)
  #82  Column Filter  same 12 cols as #79
  #83  Col Expressions (legacy)  [4 new columns appended]:
         Invoice Posting              = BUDAT  if SHKZG == "S"  else null
         Invoice Reversal             = BUDAT  if SHKZG == "H"  else null
         Invoice Creation User        = ERNAM  if SHKZG == "S"  else null
         Invoice Reversal Creation User = ERNAM if SHKZG == "H" else null
  → Collapse to wide:
         Invoice Posting              = earliest non-null (min)
         Invoice Reversal Date        = latest  non-null (max)  ← KNIME #84 expects this name
         Invoice Creation User        = first non-null
         Invoice Reversal Creation User = first non-null

JOIN ALL
  #81  Joiner         (po_pr) LEFT JOIN gr_wide on UniqueID_PO
                     Right side brings: GR Reversal Date, VGABE, GJAHR, BELNR, BUDAT,
                       MENGE, BPMNG, DMBTR, SHKZG, XBLNR, WERKS, UniqueID_PO,
                       GR Posting, GR Reversal, GR Creation User, GR Reversal Creation User
                     ERNAM from right side is excluded
                     Suffix " (GRN)" for any name clash

  #84  Joiner         (po_pr_gr) LEFT JOIN inv_wide on UniqueID_PO
                     Left side keeps 36 named cols (incl. GR Reversal Date, GR Posting,
                       and drops GR Reversal/GR Creation cols from transient columns)
                     Right side brings: Invoice Reversal Date, VGABE, GJAHR, BELNR,
                       BUDAT, MENGE, BPMNG, DMBTR, SHKZG, XBLNR, WERKS, UniqueID_PO,
                       Invoice Posting, Invoice Reversal, Invoice Creation User,
                       Invoice Reversal Creation User
                     ERNAM from right side is excluded
                     Suffix " (Right)" for any name clash

  #85  Value Lookup   LIFNR → NAME1 (from LFA1 / CSV#89)

  #86  Unpivot        (NOT replicated — we produce wide format directly for p2p4.py)
  #87  Col Renamer    ColumnNames → "Activity",  ColumnValues → "Timestamp"
  #88  Rule-based Row Filter  keep rows where Timestamp IS NOT MISSING

FINAL OUTPUT COLUMNS (wide, one row per UniqueID_PO):
  Identifiers:
    UniqueID_PO, UniqueID_PR
  PO header dims:
    BSTYP, LOEKZ, EBELN, BUKRS, LIFNR, BSART, ERNAM, EKORG, EKGRP
    EBELN (EKPO), EBELP, LOEKZ (EKPO), AEDAT (EKPO), AGDAT
    BANFN, BNFPO, CREATIONDATE, CREATIONTIME, WERKS, MATKL, MATNR
  PO dates:
    PO Creation, PO Date, PO Reversal Date
  PR dims (from EBAN join):
    ERNAM (EBAN), ERDAT, BANFN (EBAN), BNFPO (EBAN), LOEKZ (EBAN)
    CREATIONDATE (EBAN), CREATIONTIME (EBAN), UniqueID_PR (EBAN)
  PR dates:
    PR Creation, PR Release Date, PR Reversal Date
  GR dates & users:
    GR Posting, GR Reversal Date, GR Creation User, GR Reversal Creation User
  Invoice dates & users:
    Invoice Posting, Invoice Reversal Date, Invoice Creation User, Invoice Reversal Creation User
  Vendor:
    NAME1
"""

import io
import traceback
import warnings
from datetime import datetime

import pandas as pd
from fastapi import APIRouter, File, Form, HTTPException, Query, UploadFile

warnings.filterwarnings("ignore")

transformer_router = APIRouter(prefix="/p2p/transform", tags=["P2P Transformer"])
RAW_TABLES: dict[str, dict[str, pd.DataFrame]] = {}
EXPECTED_TABLES = {"EKKO", "EKPO", "EBAN", "EKBE", "LFA1"}
REQUIRED_TABLES = {"EKKO", "EKPO"}

# Minimum required columns for each SAP table — used to validate uploads
REQUIRED_COLS = {
    # Every column the pipeline and charts actually read from each SAP table
    "EKKO": {
        "EBELN",    # PO number — join key
        "AEDAT",    # PO creation date → renamed PO Creation
        "BEDAT",    # PO document date → renamed PO Date
        "BSART",    # Document type  — filter/chart
        "LIFNR",    # Vendor ID       — filter/chart
        "BUKRS",    # Company code    — filter/chart
        "EKGRP",    # Purchasing group — filter/chart
        "ERNAM",    # PO creator (user) — SOD + chart
        "LOEKZ",    # Deletion flag   — PO Reversal rule
    },
    "EKPO": {
        "EBELN",    # PO number — join key
        "EBELP",    # PO item   — part of UniqueID_PO
        "MATNR",    # Material number
        "WERKS",    # Plant           — filter/chart
        "MATKL",    # Material group  — filter/chart
        "BANFN",    # PR number       — part of UniqueID_PR
        "BNFPO",    # PR item         — part of UniqueID_PR
        "LOEKZ",    # Deletion flag (EKPO) → PO Reversal Date
        "AEDAT",    # Item change date (EKPO)
    },
    "EBAN": {
        "BANFN",    # PR number — join key
        "BNFPO",    # PR item   — join key
        "BADAT",    # PR requirement date → renamed PR Creation
        "FRGDT",    # PR release date    → renamed PR Release Date
        "ERNAM",    # PR creator (user)  — SOD + chart
        "ERDAT",    # PR creation date   — PR Reversal rule
        "LOEKZ",    # Deletion flag      — PR Reversal rule
    },
    "EKBE": {
        "EBELN",    # PO number  — join key
        "EBELP",    # PO item    — join key
        "VGABE",    # Movement type: 1=GR, 2=Invoice
        "BUDAT",    # Posting date → GR Posting / Invoice Posting
        "SHKZG",    # Debit/Credit: S=normal, H=reversal
        "ERNAM",    # User who posted — GR/Invoice Creation User (SOD)
        "BELNR",    # Accounting document
        "GJAHR",    # Fiscal year
        "MENGE",    # Quantity
        "DMBTR",    # Amount in local currency
    },
    "LFA1": {
        "LIFNR",    # Vendor ID — join key
        "NAME1",    # Vendor name — vendor filter/chart
    },
}


def _log(msg: str):
    print(f"[P2P TRANSFORM] {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | {msg}")


def _read_csv(content: bytes, filename: str) -> pd.DataFrame:
    for enc in ("utf-8", "latin-1", "windows-1252"):
        try:
            return pd.read_csv(io.BytesIO(content), encoding=enc, low_memory=False)
        except (UnicodeDecodeError, pd.errors.ParserError):
            continue
    raise ValueError(f"Cannot decode {filename}")


# ── API endpoints ──────────────────────────────────────────────────────────────

@transformer_router.get("/status")
def transform_status(username: str = Query("Unknown")):
    tables = RAW_TABLES.get(username, {})
    loaded  = list(tables.keys())
    missing = [t for t in EXPECTED_TABLES if t not in tables]
    missing_req = [t for t in REQUIRED_TABLES if t not in tables]
    return {"loaded": loaded, "missing": missing, "ready": len(missing_req) == 0}


@transformer_router.post("/preview_columns")
async def preview_columns(file: UploadFile = File(...)):
    raw = await file.read()
    try:
        df = _read_csv(raw, file.filename)
        return {"status": "ok", "columns": list(df.columns)}
    except Exception as e:
        raise HTTPException(500, f"Failed to parse CSV: {e}")


@transformer_router.post("/upload_table")
async def upload_raw_table(
    file: UploadFile = File(...),
    table_name: str = Form(...),
    username: str = Form("Unknown"),
    column_mapping: str = Form("{}"),
):
    table_name = table_name.strip().upper()

    # ── 1. Validate table name ───────────────────────────────────────────────
    if table_name not in EXPECTED_TABLES:
        raise HTTPException(400, f"Unknown table '{table_name}'. Expected: {sorted(EXPECTED_TABLES)}")

    # ── 2. Check for duplicate — already uploaded in this session ────────────
    user_tables = RAW_TABLES.get(username, {})
    if table_name in user_tables:
        existing = user_tables[table_name]
        raise HTTPException(400,
            f"Table '{table_name}' has already been uploaded this session "
            f"({len(existing):,} rows). "
            f"Use the ✕ button to clear it first before uploading again."
        )

    # ── 3. Parse CSV ─────────────────────────────────────────────────────────
    if not file.filename.lower().endswith(".csv"):
        raise HTTPException(400, "Only CSV files are supported.")
    raw = await file.read()
    try:
        df = _read_csv(raw, file.filename)
    except Exception as e:
        raise HTTPException(500, f"Failed to parse {file.filename}: {e}")

    # Apply Column Mapping if provided
    try:
        import json
        mapping = json.loads(column_mapping)
        if mapping:
            df = df.rename(columns=mapping)
            _log(f"Applied column mapping for {table_name}: {mapping}")
    except Exception as e:
        _log(f"Error applying column mapping: {e}")

    # Normalise column names: strip whitespace
    df.columns = [str(c).strip() for c in df.columns]

    # ── 4. Validate required columns ─────────────────────────────────────────
    required = REQUIRED_COLS.get(table_name, set())
    uploaded_cols = {c.upper() for c in df.columns}
    missing = {c for c in required if c.upper() not in uploaded_cols}
    if missing:
        _log(f"Warning: Uploaded '{table_name}' is missing required columns: {sorted(missing)}.")

    # ── 5. Store ──────────────────────────────────────────────────────────────
    RAW_TABLES.setdefault(username, {})[table_name] = df
    _log(f"User '{username}' uploaded {table_name}: {len(df)} rows, {len(df.columns)} cols.")
    return {"status": "ok", "table": table_name, "rows": len(df), "columns": list(df.columns)}


@transformer_router.delete("/clear_table")
def clear_table(table_name: str, username: str = Query("Unknown")):
    table_name = table_name.strip().upper()
    if username in RAW_TABLES and table_name in RAW_TABLES[username]:
        del RAW_TABLES[username][table_name]
    return {"status": "ok", "message": f"{table_name} cleared"}


@transformer_router.post("/build")
def build_event_log(username: str = Query("Unknown")):
    tables = RAW_TABLES.get(username, {})
    missing_req = [t for t in REQUIRED_TABLES if t not in tables]
    if missing_req:
        raise HTTPException(400, f"Missing required tables: {missing_req}. Upload via /p2p/transform/upload_table first.")
    try:
        result_df = _run_pipeline(tables, username)
    except Exception as e:
        _log(f"Pipeline failed for '{username}': {e}\n{traceback.format_exc()}")
        raise HTTPException(status_code=400, detail="Column mapping is incorrect. Failed to process data.")

    from p2p4 import (
        USER_DFS, process_df, log_audit,
        _save_output_csv, register_transform_build,
        UPLOAD_DIR,
    )

    processed = process_df(result_df)
    USER_DFS[username] = processed
    USER_DFS["Unknown"] = processed
    log_audit(username, "TRANSFORM", f"Built event log: {len(processed)} rows")
    _log(f"Done for '{username}': {len(processed)} rows, {len(processed.columns)} cols.")

    ts       = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = ""
    save_path = ""

    # ── Save JSON session file (so it can be reloaded later) ──────────────
    try:
        import os as _os
        user_dir  = _os.path.join(UPLOAD_DIR, username)
        _os.makedirs(user_dir, exist_ok=True)
        save_path = _os.path.join(user_dir, f"{ts}_transform_build.json")
        processed.to_json(save_path, orient="records", date_format="iso")
        _log(f"Session JSON saved → {save_path}")
    except Exception as e:
        _log(f"Session JSON save failed (non-fatal): {e}")

    # ── Save wide CSV to server output directory ───────────────────────────
    try:
        csv_name = f"P2P_Transform_{username}_{ts}.csv"
        csv_path = _save_output_csv(processed, csv_name)
        _log(f"Output CSV saved → {csv_path}")
    except Exception as e:
        _log(f"CSV output save failed (non-fatal): {e}")

    # ── Register in file registry (shows up in Previous Uploads) ──────────
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


# ── Helpers ────────────────────────────────────────────────────────────────────

def _str(series: pd.Series) -> pd.Series:
    """Cast to string and strip whitespace (preserves NaN as 'nan' — handled by callers)."""
    return series.astype(str).str.strip()


def _safe_join_key(s1: pd.Series, s2: pd.Series,
                   pad1: int = 0, pad2: int = 0) -> pd.Series:
    """
    Build a composite join key from two columns, with optional zero-padding.

    SAP exports numeric key fields (EBELN, EBELP, BANFN, BNFPO) that some
    CSV readers load as integers, stripping leading zeros (e.g. BANFN stored
    as 10001234 instead of '0010001234').  When the same field in another table
    is stored as the padded string, the plain astype(str) join always misses.

    Rules
    -----
    - If either component is NaN / empty / 'nan', the whole key is NaN so that
      LEFT JOINs correctly leave those rows unmatched instead of grouping all
      null-BANFN PO lines together under the 'nannan' pseudo-key.
    - If pad1 > 0, the first component is zero-padded to that width (and vice
      versa for pad2).  The padded value is also tried as a plain integer first
      so that '10001234' → '0010001234' works regardless of how the CSV was read.
    """
    def _norm(s: pd.Series, width: int) -> pd.Series:
        s = s.astype(str).str.strip()
        # Convert float artifacts like '10001234.0' → '10001234'
        s = s.str.replace(r'\.0$', '', regex=True)
        if width:
            # Only pad rows that look like plain integers (all digits)
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
    """Keep only columns that exist; warn about missing ones."""
    present = [c for c in cols if c in df.columns]
    missing = [c for c in cols if c not in df.columns]
    if missing:
        _log(f"  WARNING — columns not found (skipped): {missing}")
    return df[present].copy()


# ── Core pipeline ──────────────────────────────────────────────────────────────

def _run_pipeline(tables: dict[str, pd.DataFrame], username: str) -> pd.DataFrame:

    # ── Load raw tables ────────────────────────────────────────────────────────
    ekko = tables["EKKO"].copy()
    ekpo = tables["EKPO"].copy()
    
    eban = tables["EBAN"].copy() if "EBAN" in tables else None
    ekbe = tables["EKBE"].copy() if "EKBE" in tables else None
    lfa1 = tables["LFA1"].copy() if "LFA1" in tables else None
    
    counts = f"EKKO:{len(ekko)} EKPO:{len(ekpo)} " + \
             f"EBAN:{len(eban) if eban is not None else 0} " + \
             f"EKBE:{len(ekbe) if ekbe is not None else 0} " + \
             f"LFA1:{len(lfa1) if lfa1 is not None else 0}"
    _log(f"Input — {counts}")

    # ══════════════════════════════════════════════════════════════════════════
    # PO BRANCH:  EKKO (CSV#1) + EKPO (CSV#2)
    #   → Node #8  Joiner (INNER JOIN on EBELN)
    #   → Node #14 Column Filter
    #   → Node #15 String Manipulation  (UniqueID_PO)
    #   → Node #16 String Manipulation  (UniqueID_PR)
    #   → Node #17 Rule Engine          (PO Reversal Date)
    #   → Node #35 Column Renamer       (AEDAT→PO Creation, BEDAT→PO Date)
    # ══════════════════════════════════════════════════════════════════════════

    # Cast join key to string on both sides
    ekko["EBELN"] = _str(ekko["EBELN"])
    ekpo["EBELN"] = _str(ekpo["EBELN"])

    # Pre-rename EKPO columns that clash with EKKO before the join.
    # KNIME Joiner #8 renames clashing EKPO cols with " (EKPO)" suffix.
    # Affected cols: LOEKZ, AEDAT (and EBELN which becomes "EBELN (EKPO)")
    ekpo["EBELN (EKPO)"] = ekpo["EBELN"]          # preserve EKPO's EBELN copy
    ekpo_rename = {}
    for col in ["LOEKZ", "AEDAT"]:
        if col in ekpo.columns:
            ekpo_rename[col] = f"{col} (EKPO)"
    if ekpo_rename:
        ekpo = ekpo.rename(columns=ekpo_rename)

    # ── Node #8: EKKO INNER JOIN EKPO on EBELN ────────────────────────────────
    po = ekko.merge(ekpo, on="EBELN", how="inner", suffixes=("", "_EKPO_dup"))
    po = po[[c for c in po.columns if not c.endswith("_EKPO_dup")]]
    _log(f"#8  Joiner EKKO INNER JOIN EKPO: {len(po)} rows, {len(po.columns)} cols")

    # ── Node #14: Column Filter ───────────────────────────────────────────────
    # 22 columns kept (including WERKS and EKGRP which come from EKPO side):
    col14 = [
        "BSTYP",            # EKKO
        "LOEKZ",            # EKKO
        "EBELN",            # join key (EKKO side)
        "AEDAT",            # EKKO → will be renamed to "PO Creation" in #35
        "BUKRS",            # EKKO
        "LIFNR",            # EKKO
        "BSART",            # EKKO
        "ERNAM",            # EKKO (PO creator)
        "EKORG",            # EKKO
        "EKGRP",            # EKKO
        "BEDAT",            # EKKO → will be renamed to "PO Date" in #35
        "EBELN (EKPO)",     # EKPO pre-renamed
        "EBELP",            # EKPO (PO item)
        "LOEKZ (EKPO)",     # EKPO pre-renamed
        "AEDAT (EKPO)",     # EKPO pre-renamed (PO item change date)
        "MATNR",            # EKPO
        "MATKL",            # EKPO (material group)
        "AGDAT",            # EKPO
        "BANFN",            # EKPO (PR number)
        "BNFPO",            # EKPO (PR item)
        "CREATIONDATE",     # EKPO
        "CREATIONTIME",     # EKPO
        "WERKS",            # EKPO (plant)
    ]
    po = _keep(po, col14)
    _log(f"#14 Column Filter: kept {len(po.columns)} cols → {list(po.columns)}")

    # ── Node #15: String Manipulation — UniqueID_PO ───────────────────────────
    # join(string($EBELN (EKPO)$), string($EBELP$))  → replaces/creates UniqueID_PO
    # SAP EBELN = 10-digit, EBELP = 5-digit — zero-pad to match across tables
    po["UniqueID_PO"] = _safe_join_key(po["EBELN (EKPO)"], po["EBELP"],
                                        pad1=10, pad2=5)
    _log(f"#15 UniqueID_PO sample: {po['UniqueID_PO'].head(3).tolist()}")

    # ── Node #16: String Manipulation — UniqueID_PR ───────────────────────────
    # join(string($BANFN$), string($BNFPO$))  → new column UniqueID_PR
    # SAP BANFN = 10-digit, BNFPO = 5-digit — zero-pad to handle integer CSV reads
    po["UniqueID_PR"] = _safe_join_key(po["BANFN"], po["BNFPO"],
                                        pad1=10, pad2=5)
    _log(f"#16 UniqueID_PR sample: {po['UniqueID_PR'].dropna().head(3).tolist()}")

    # ── Node #17: Rule Engine — PO Reversal Date ──────────────────────────────
    # Rule: $LOEKZ (EKPO)$ IN ("L", "C") => $AEDAT (EKPO)$
    # append-column = true → new column "PO Reversal Date" appended
    loekz_ekpo = po.get("LOEKZ (EKPO)", pd.Series(dtype=str, index=po.index))
    aedat_ekpo  = po.get("AEDAT (EKPO)",  pd.Series(dtype=str, index=po.index))
    po["PO Reversal Date"] = aedat_ekpo.where(_str(loekz_ekpo).isin(["L", "C"]))
    _log(f"#17 PO Reversal Date: {po['PO Reversal Date'].notna().sum()} non-null")

    # ── Node #35: Column Renamer ───────────────────────────────────────────────
    # AEDAT → "PO Creation",  BEDAT → "PO Date"
    po = po.rename(columns={"AEDAT": "PO Creation", "BEDAT": "PO Date"})
    _log(f"#35 PO branch done: {len(po)} rows, cols: {list(po.columns)}")

    # ══════════════════════════════════════════════════════════════════════════
    # PR BRANCH:  EBAN (CSV#3)
    #   → Node #12 Column Filter
    #   → Node #13 Rule Engine  (PR Reversal Date)
    #   → Node #19 String Manipulation (UniqueID_PR)
    #   → Node #34 Column Renamer (BADAT→PR Creation, FRGDT→PR Release Date)
    # ══════════════════════════════════════════════════════════════════════════

    if eban is not None:
        # ── Node #12: Column Filter ───────────────────────────────────────────────
        # 9 columns kept from EBAN:
        col12 = [
            "BANFN",            # PR number
            "BNFPO",            # PR item
            "LOEKZ",            # deletion flag
            "ERNAM",            # PR creator (will clash with PO ERNAM → gets suffix "(EBAN)")
            "ERDAT",            # PR creation date
            "BADAT",            # PR requirement date → renamed "PR Creation"
            "FRGDT",            # PR release date → renamed "PR Release Date"
            "CREATIONDATE",     # will get suffix "(EBAN)"
            "CREATIONTIME",     # will get suffix "(EBAN)"
        ]
        pr = _keep(eban, col12)
        _log(f"#12 Column Filter EBAN: {len(pr)} rows, {len(pr.columns)} cols")

        # ── Node #13: Rule Engine — PR Reversal Date ──────────────────────────────
        # Rule: $LOEKZ$ = "X" => $ERDAT$
        # Comment in KNIME: "If the PR is flagged as deleted, grab the overwritten ERDAT date"
        # append-column = true → new column "PR Reversal Date" appended
        pr["PR Reversal Date"] = pr["ERDAT"].where(
            _str(pr.get("LOEKZ", pd.Series(dtype=str, index=pr.index))) == "X"
        )
        _log(f"#13 PR Reversal Date: {pr['PR Reversal Date'].notna().sum()} non-null")

        # ── Node #19: String Manipulation — UniqueID_PR ───────────────────────────
        # join(string($BANFN$), string($BNFPO$))
        # Same zero-padding as PO branch so LEFT JOIN on UniqueID_PR matches correctly
        pr["UniqueID_PR"] = _safe_join_key(pr["BANFN"], pr["BNFPO"],
                                            pad1=10, pad2=5)
        _log(f"#19 UniqueID_PR sample: {pr['UniqueID_PR'].dropna().head(3).tolist()}")

        # ── Node #34: Column Renamer ───────────────────────────────────────────────
        # BADAT → "PR Creation",  FRGDT → "PR Release Date"
        pr = pr.rename(columns={"BADAT": "PR Creation", "FRGDT": "PR Release Date"})
        _log(f"#34 PR branch done: {len(pr)} rows, cols: {list(pr.columns)}")

        # ══════════════════════════════════════════════════════════════════════════
        # Node #20: PO LEFT JOIN PR on UniqueID_PR
        # ══════════════════════════════════════════════════════════════════════════
        po_pr = po.merge(pr, on="UniqueID_PR", how="left", suffixes=("", " (EBAN)"))
    else:
        # Create empty columns for missing EBAN table
        po_pr = po.copy()
        po_pr["PR Creation"] = pd.NaT
        po_pr["PR Release Date"] = pd.NaT
        po_pr["PR Reversal Date"] = pd.NaT
        po_pr["ERNAM (EBAN)"] = None
    _log(f"#20 Joiner PO LEFT JOIN PR: {len(po_pr)} rows, {len(po_pr.columns)} cols")
    _log(f"#20 Columns: {list(po_pr.columns)}")

    # ══════════════════════════════════════════════════════════════════════════
    # GR BRANCH:  EKBE (CSV#4) where VGABE == "1"
    #   → Node #21 Row Filter    (VGABE == "1")
    #   → Node #27 String Manip  (UniqueID_PO = join(EBELN, EBELP))
    #   → Node #79 Column Filter (12 cols)
    #   → Node #80 Col Expressions (4 new cols: GR Posting, GR Reversal,
    #                                GR Creation User, GR Reversal Creation User)
    if ekbe is not None:
        ekbe["EBELN"] = _str(ekbe["EBELN"])
        if "EBELP" in ekbe.columns:
            ekbe["EBELP"] = _str(ekbe["EBELP"])

        # ── Node #21: Row Filter — VGABE == "1" (Goods Receipt) ──────────────────
        gr = ekbe[ekbe["VGABE"].astype(str).str.strip() == "1"].copy()
        _log(f"#21 Row Filter GR (VGABE=1): {len(gr)} rows")

        # ── Node #27: UniqueID_PO = join(EBELN, EBELP) ────────────────────────────
        gr["UniqueID_PO"] = _safe_join_key(gr["EBELN"], gr["EBELP"], pad1=10, pad2=5)

        # ── Node #79: Column Filter — 12 cols ─────────────────────────────────────
        col79 = [
            "VGABE", "GJAHR", "BELNR", "BUDAT",
            "MENGE", "BPMNG", "DMBTR", "SHKZG",
            "XBLNR", "WERKS", "ERNAM", "UniqueID_PO",
        ]
        gr = _keep(gr, col79)
        _log(f"#79 Column Filter GR: {len(gr.columns)} cols")

        # ── Node #80: Column Expressions (legacy) — 4 new cols ────────────────────
        s_mask = _str(gr["SHKZG"]) == "S"
        h_mask = _str(gr["SHKZG"]) == "H"
        gr["GR Posting"]                  = gr["BUDAT"].where(s_mask)
        gr["GR Reversal"]                 = gr["BUDAT"].where(h_mask)
        gr["GR Creation User"]            = gr["ERNAM"].where(s_mask)
        gr["GR Reversal Creation User"]   = gr["ERNAM"].where(h_mask)
        _log(f"#80 GR Col Expressions: GR Posting={gr['GR Posting'].notna().sum()} "
             f"GR Reversal={gr['GR Reversal'].notna().sum()} rows")

        # ── Collapse GR to wide (one row per UniqueID_PO) ─────────────────────────
        gr_wide = _collapse_ekbe_branch(
            gr,
            posting_col="GR Posting",
            reversal_col="GR Reversal",
            posting_user_col="GR Creation User",
            reversal_user_col="GR Reversal Creation User",
            out_posting="GR Posting",
            out_reversal="GR Reversal Date",
            out_posting_user="GR Creation User",
            out_reversal_user="GR Reversal Creation User",
        )
        _log(f"GR wide: {len(gr_wide)} unique UniqueID_PO values")

    # ══════════════════════════════════════════════════════════════════════════
    # INVOICE BRANCH:  EKBE (CSV#4) where VGABE == "2"
    #   → Node #22 Row Filter    (VGABE == "2")
    #   → Node #30 String Manip  (UniqueID_PO = join(EBELN, EBELP))
    #   → Node #82 Column Filter (same 12 cols as #79)
    #   → Node #83 Col Expressions (4 new cols: Invoice Posting, Invoice Reversal,
    #                                Invoice Creation User, Invoice Reversal Creation User)
    # ══════════════════════════════════════════════════════════════════════════

    if ekbe is not None:
        # ── Node #22: Row Filter — VGABE == "2" (Invoice) ─────────────────────────
        inv = ekbe[ekbe["VGABE"].astype(str).str.strip() == "2"].copy()
        _log(f"#22 Row Filter Invoice (VGABE=2): {len(inv)} rows")

        # ── Node #30: UniqueID_PO = join(EBELN, EBELP) ────────────────────────────
        # Must use same zero-padding as PO branch so Invoice rows join correctly
        inv["UniqueID_PO"] = _safe_join_key(inv["EBELN"], inv["EBELP"],
                                             pad1=10, pad2=5)

        # ── Node #82: Column Filter — same 12 cols as #79 ─────────────────────────
        inv = _keep(inv, col79)
        _log(f"#82 Column Filter Invoice: {len(inv.columns)} cols")

        # ── Node #83: Column Expressions (legacy) — 4 new cols ────────────────────
        s_inv = _str(inv["SHKZG"]) == "S"
        h_inv = _str(inv["SHKZG"]) == "H"
        inv["Invoice Posting"]                  = inv["BUDAT"].where(s_inv)
        inv["Invoice Reversal"]                 = inv["BUDAT"].where(h_inv)
        inv["Invoice Creation User"]            = inv["ERNAM"].where(s_inv)
        inv["Invoice Reversal Creation User"]   = inv["ERNAM"].where(h_inv)
        _log(f"#83 Invoice Col Expressions: Invoice Posting={inv['Invoice Posting'].notna().sum()} "
             f"Invoice Reversal={inv['Invoice Reversal'].notna().sum()} rows")

        # ── Collapse Invoice to wide ───────────────────────────────────────────────
        # KNIME Joiner #84 expects "Invoice Reversal Date" from the right side.
        inv_wide = _collapse_ekbe_branch(
            inv,
            posting_col="Invoice Posting",
            reversal_col="Invoice Reversal",
            posting_user_col="Invoice Creation User",
            reversal_user_col="Invoice Reversal Creation User",
            out_posting="Invoice Posting",
            out_reversal="Invoice Reversal Date",        # KNIME #84 right-side col name
            out_posting_user="Invoice Creation User",
            out_reversal_user="Invoice Reversal Creation User",
        )
        _log(f"Invoice wide: {len(inv_wide)} unique UniqueID_PO values")
    else:
        # Create empty DataFrames for gr_wide and inv_wide
        gr_wide = pd.DataFrame(columns=[
            "UniqueID_PO", "GR Posting", "GR Reversal Date",
            "GR Creation User", "GR Reversal Creation User",
        ])
        inv_wide = pd.DataFrame(columns=[
            "UniqueID_PO", "Invoice Posting", "Invoice Reversal Date",
            "Invoice Creation User", "Invoice Reversal Creation User",
        ])

    # ══════════════════════════════════════════════════════════════════════════
    # Node #81: (po_pr) LEFT JOIN gr_wide on UniqueID_PO
    #   Right side brings (per KNIME config):
    #     GR Reversal Date, VGABE, GJAHR, BELNR, BUDAT, MENGE, BPMNG, DMBTR,
    #     SHKZG, XBLNR, WERKS, UniqueID_PO, GR Posting, GR Reversal,
    #     GR Creation User, GR Reversal Creation User
    #     (ERNAM from right side is EXCLUDED)
    #   Suffix " (GRN)" for any clashes
    # ══════════════════════════════════════════════════════════════════════════
    po_pr_gr = po_pr.merge(
        gr_wide, on="UniqueID_PO", how="left", suffixes=("", " (GRN)")
    )
    _log(f"#81 Joiner +GR: {len(po_pr_gr)} rows, {len(po_pr_gr.columns)} cols")

    # ══════════════════════════════════════════════════════════════════════════
    # Node #84: (po_pr_gr) LEFT JOIN inv_wide on UniqueID_PO
    #   Left side keeps 36 named cols (exact list from KNIME config):
    #     BSTYP, LOEKZ, EBELN, PO Creation, BUKRS, LIFNR, BSART, ERNAM, EKORG,
    #     PO Date, EBELN (EKPO), EBELP, LOEKZ (EKPO), AEDAT (EKPO), AGDAT,
    #     BANFN, BNFPO, CREATIONDATE, CREATIONTIME, UniqueID_PO, UniqueID_PR,
    #     PO Reversal Date, BANFN (EBAN), BNFPO (EBAN), LOEKZ (EBAN), ERNAM (EBAN),
    #     ERDAT, PR Creation, PR Release Date, CREATIONDATE (EBAN), CREATIONTIME (EBAN),
    #     PR Reversal Date, UniqueID_PR (EBAN), UniqueID_PO (GRN),
    #     GR Reversal Date, GR Posting
    #   Right side brings:
    #     Invoice Reversal Date, VGABE, GJAHR, BELNR, BUDAT, MENGE, BPMNG, DMBTR,
    #     SHKZG, XBLNR, WERKS, UniqueID_PO, Invoice Posting, Invoice Reversal,
    #     Invoice Creation User, Invoice Reversal Creation User
    #     (ERNAM from right side is EXCLUDED)
    #   Suffix " (Right)" for any clashes
    # ══════════════════════════════════════════════════════════════════════════

    # Apply the left-side column selection exactly as in KNIME node #84
    left_cols_84 = [
        "BSTYP", "LOEKZ", "EBELN", "PO Creation", "BUKRS", "LIFNR", "BSART",
        "ERNAM", "EKORG", "EKGRP", "PO Date", "EBELN (EKPO)", "EBELP",
        "LOEKZ (EKPO)", "AEDAT (EKPO)", "AGDAT", "BANFN", "BNFPO",
        "CREATIONDATE", "CREATIONTIME", "UniqueID_PO", "UniqueID_PR",
        "PO Reversal Date", "BANFN (EBAN)", "BNFPO (EBAN)", "LOEKZ (EBAN)",
        "ERNAM (EBAN)", "ERDAT", "PR Creation", "PR Release Date",
        "CREATIONDATE (EBAN)", "CREATIONTIME (EBAN)", "PR Reversal Date",
        "UniqueID_PR (EBAN)", "UniqueID_PO (GRN)",
        "GR Reversal Date", "GR Posting",
        # Additional cols not listed explicitly but kept via includeUnknownColumns=true:
        "MATNR", "MATKL", "WERKS", "GR Creation User", "GR Reversal Creation User",
    ]
    # Keep only those left-side cols that exist, preserving all others via unknowns
    # (KNIME includeUnknownColumns=true means all unlisted cols also pass through)
    left_present = [c for c in left_cols_84 if c in po_pr_gr.columns]
    # Also include any cols not in our explicit list (unknowns pass through in KNIME)
    all_left = list(dict.fromkeys(left_present + [c for c in po_pr_gr.columns if c not in left_present]))
    po_pr_gr_sel = po_pr_gr[all_left]

    wide = po_pr_gr_sel.merge(
        inv_wide, on="UniqueID_PO", how="left", suffixes=("", " (Right)")
    )
    _log(f"#84 Joiner +Invoice: {len(wide)} rows, {len(wide.columns)} cols")
    _log(f"#84 All columns: {list(wide.columns)}")

    # ══════════════════════════════════════════════════════════════════════════
    # Node #85: Value Lookup — LIFNR → NAME1  (from LFA1 / CSV#89)
    #   lookupCol="LIFNR", dictKeyCol="LIFNR", dictValueCols=["NAME1"]
    #
    #   FIX: SAP LIFNR is a 10-digit zero-padded string ('0000012345').
    #   Some CSV exports strip leading zeros when the column is read as integer.
    #   Normalise LIFNR on BOTH sides to zero-padded 10-char string so the
    #   lookup matches regardless of how each table was exported.
    # ══════════════════════════════════════════════════════════════════════════
    def _norm_lifnr(s: pd.Series) -> pd.Series:
        """Normalise SAP LIFNR: strip, remove float '.0' artifact, zero-pad to 10."""
        s = s.astype(str).str.strip().str.replace(r'\.0$', '', regex=True)
        is_numeric = s.str.match(r'^\d+$')
        return s.where(~is_numeric, s.str.zfill(10))

    if lfa1 is not None and "LIFNR" in lfa1.columns and "NAME1" in lfa1.columns:
        lfa1_map = (
            lfa1[["LIFNR", "NAME1"]]
            .drop_duplicates(subset="LIFNR")
            .assign(LIFNR=lambda d: _norm_lifnr(d["LIFNR"]))
        )
        wide["LIFNR"] = _norm_lifnr(wide["LIFNR"])
        if "NAME1" in wide.columns:
            wide = wide.drop(columns=["NAME1"])
        wide = wide.merge(lfa1_map, on="LIFNR", how="left")
        matched = int(wide["NAME1"].notna().sum())
        _log(f"#85 Value Lookup NAME1: {matched}/{len(wide)} rows matched")
        if matched == 0:
            _log("#85 WARNING — 0 NAME1 matches; LIFNR format may differ between EKKO and LFA1")
    else:
        wide["NAME1"] = None
        wide["LIFNR"] = _norm_lifnr(wide["LIFNR"]) # Keep LIFNR normalised even without LFA1
        _log("#85 Value Lookup skipped — LFA1 missing (or missing LIFNR / NAME1 column)")

    # ══════════════════════════════════════════════════════════════════════════
    # Final wide collapse — one row per UniqueID_PO
    # (Replaces Unpivot #86 + Renamer #87 + RowFilter #88 for p2p4.py's wide format)
    #
    # The KNIME long-format output (Unpivot → Activity/Timestamp) is NOT what
    # p2p4.py needs. p2p4 expects wide format with each activity as its own column.
    #
    # Strategy: group by UniqueID_PO, take first non-null for each column.
    # This resolves any fan-out from the LEFT JOINs with EBAN (multiple PR lines
    # per PO item) while faithfully preserving the earliest GR/Invoice posting dates
    # that were already collapsed in _collapse_ekbe_branch().
    # ══════════════════════════════════════════════════════════════════════════
    _log(f"Before final collapse: {len(wide)} rows, {wide['UniqueID_PO'].nunique()} unique UniqueID_PO")

    # All aggregation uses "first" (non-null first value per group).
    # Date columns were already min/max-collapsed in _collapse_ekbe_branch.
    agg_dict = {c: "first" for c in wide.columns if c != "UniqueID_PO"}
    result = wide.groupby("UniqueID_PO", sort=False).agg(agg_dict).reset_index()

    _log(f"Final wide table: {len(result)} rows, {len(result.columns)} cols")

    # Sanity-check: report which activity/date columns are present
    activity_cols = [
        "PR Creation", "PR Release Date", "PR Reversal Date",
        "PO Creation", "PO Date", "PO Reversal Date",
        "GR Posting", "GR Reversal Date",
        "Invoice Posting", "Invoice Reversal Date",
    ]
    user_cols = [
        "GR Creation User", "GR Reversal Creation User",
        "Invoice Creation User", "Invoice Reversal Creation User",
        "ERNAM", "ERNAM (EBAN)",
    ]
    dim_cols_check = ["BUKRS", "LIFNR", "NAME1", "BSART", "MATKL", "EKGRP", "WERKS", "UniqueID_PR"]

    present_act  = [c for c in activity_cols if c in result.columns]
    present_user = [c for c in user_cols if c in result.columns]
    present_dim  = [c for c in dim_cols_check if c in result.columns]
    missing_act  = [c for c in activity_cols if c not in result.columns]

    _log(f"Activity cols present  : {present_act}")
    _log(f"Activity cols MISSING  : {missing_act}")
    _log(f"User cols present      : {present_user}")
    _log(f"Dimension cols present : {present_dim}")
    _log(f"All output cols        : {list(result.columns)}")

    return result


# ══════════════════════════════════════════════════════════════════════════════
# HELPER: collapse EKBE branch to one wide row per UniqueID_PO
# ══════════════════════════════════════════════════════════════════════════════

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

    Aggregation strategy (mirrors KNIME Unpivot + GroupBy pattern):
      - Posting date  → earliest non-null (MIN) → first actual goods/invoice receipt
      - Reversal date → latest non-null  (MAX) → last reversal event
      - User cols     → first non-null (order of appearance)

    The output col names are the final names expected by the downstream joiners
    (#81 and #84) as declared in their rightColumnSelectionConfigV2.
    """
    if df.empty:
        return pd.DataFrame(columns=[
            "UniqueID_PO", out_posting, out_reversal,
            out_posting_user, out_reversal_user,
        ])

    # Convert date cols to datetime for proper min/max aggregation
    for col in [posting_col, reversal_col]:
        if col in df.columns:
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

    # Ensure date cols stay as datetime (NaT for missing)
    for c in [out_posting, out_reversal]:
        result[c] = pd.to_datetime(result[c], errors="coerce")

    return result
