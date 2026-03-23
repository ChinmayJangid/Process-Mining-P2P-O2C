"""
O2C Transformer — exactly replicates the O2C_4.knwf KNIME pipeline in Python.

OUTPUT FORMAT
─────────────
This transformer produces a WIDE dataframe (one row per case / Subsequent Document)
with all date columns populated. The o2c4.process_df() function then adds the named
activity columns (SO Created, Delivery Created, …) and computes Month/Year/Quarter.
The transformer does NOT unpivot — that is handled by process_df / the chart endpoints.

KEY POINTS matching KNIME exactly
───────────────────────────────────
• All document-number join keys are RAW string concatenations of the integer value
  (no zero-padding). KNIME's join(string($A$), string($B$)) strips the float .0
  suffix and concatenates directly.

• Joiner #39 (VBAK × VBAP) → LEFT join. We keep all VBAK rows; VBAP is aggregated
  to one row per Sales Order Number (first item) before the join.

• Joiner #49 → LEFT join on Preceding Document.
• Joiner #51 → LEFT join on raw VBELN (delivery header).
• Joiner #52 → LEFT join on Subsequent Document = Delivery Document (LIPS VBELN+POSNR).
• Joiner #55 → LEFT join on Subsequent Document = Preceding Document (VBFA-Goods).
• Joiner #56 → LEFT join on Subsequent Document = Preceding Document (VBFA-Invoice).
• Joiner #57 → LEFT join on VBELN_inv = Billing Document Number (VBRK).
• Joiner #59 → LEFT join on Subsequent Document (Invoice) = Billing Document (VBRP).
• Joiner #60 → LEFT join on Billing Document Number = Billing Document (BSAD).
• KNA1 ValueLookup uses KUNNR from VBAK.

• The pipeline returns a WIDE dataframe — NOT a melted event log.
  process_df() in o2c4.py handles activity mapping, Month/Year/Quarter.

SAP Tables required:
  VBAK  — Sales Document Header
  VBAP  — Sales Document Item
  VBFA  — Sales Document Flow
  LIKP  — Delivery Header
  LIPS  — Delivery Item
  VBRK  — Billing Document Header
  VBRP  — Billing Document Item
  BSAD  — Cleared Customer Items (FI)
  KNA1  — Customer Master
"""

import io
import os
import gc
import json
import traceback
import pandas as pd
from fastapi import APIRouter, Query, HTTPException, UploadFile, File, Form
from datetime import datetime

o2c_transformer_router = APIRouter(prefix="/o2c/transform", tags=["O2C-Transformer"])

O2C_RAW_TABLES: dict = {}
EXPECTED_TABLES = ["VBAK", "VBAP", "VBFA", "LIKP", "LIPS", "VBRK", "VBRP", "BSAD", "KNA1"]

# Minimum required columns for each SAP table — used to validate uploads
REQUIRED_COLS = {
    # Every column the pipeline and O2C charts actually read from each SAP table
    "VBAK": {
        "VBELN",   # Sales order number — join key
        "ERDAT",   # Sales document creation date → SO Created activity
        "ERNAM",   # Sales document creator — filter & chart
        "AUART",   # Sales document type — filter & chart
        "LIFSK",   # Delivery block — Delivery Blocked Date rule
        "FAKSK",   # Billing block  — Billing Block Date rule
        "VKORG",   # Sales organisation — filter & chart
        "KUNNR",   # Customer number — lookup key for KNA1
        "AEDAT",   # Header changed date — used by block date rules
        "VBTYP",   # Document category — RowFilter keeps only C (orders)
    },
    "VBAP": {
        "VBELN",   # Sales order number — join key
        "POSNR",   # Item number — part of Preceding Document key
        "MATNR",   # Material number — filter & chart
        "ABGRU",   # Reason for rejection — SO Rejected Date rule
        "AEDAT",   # Item changed date — SO Rejected Date rule
        "NETWR",   # Net value of order item — chart
    },
    "VBFA": {
        "VBELV",   # Preceding document number — Preceding Document key
        "POSNV",   # Preceding item number    — Preceding Document key
        "VBELN",   # Subsequent document       — Subsequent Document key
        "POSNN",   # Subsequent item number    — Subsequent Document key
        "VBTYP_N", # Subsequent document type — routes to Delivery/Goods/Invoice branch
        "ERDAT",   # Document date — all activity dates come from here
    },
    "LIKP": {
        "VBELN",      # Delivery document number — join key
        "ERDAT",      # Delivery creation date
        "WADAT_IST",  # Actual goods issue date → Goods Issued fallback
        "ERNAM",      # Delivery document maker
        "WADAT",      # Planned delivery date → Delivery Posted
    },
    "LIPS": {
        "VBELN",   # Delivery number — part of Delivery Document key
        "POSNR",   # Delivery item   — part of Delivery Document key
        "MATNR",   # Material number — filter & chart
        "WERKS",   # Plant           — filter & chart
        "LFIMG",   # Actual quantity delivered
    },
    "VBRK": {
        "VBELN",   # Billing document number — join key
        "ERDAT",   # Invoice creation date
        "ERNAM",   # Invoice maker — filter & chart
        "FKTYP",   # Billing type
        "FKART",   # Billing document type
    },
    "VBRP": {
        "VBELN",   # Billing document number — join key
        "POSNR",   # Billing item number     — part of Billing Document key
        "MATNR",   # Billing material
        "FKIMG",   # Actual billed quantity
        "NETWR",   # Net value of billing item
    },
    "BSAD": {
        "VBELN",   # Billing document number — join key
        "AUGDT",   # Clearing date → Invoice Cleared / Invoice Posted activity
        "DMBTR",   # Amount in local currency
        "AUGBL",   # Clearing document number
        "BUKRS",   # Company code
    },
    "KNA1": {
        "KUNNR",   # Customer number — join key
        "NAME1",   # Customer name   — customer filter & chart
    },
}

O2C_OUTPUT_DIR    = os.path.join("o2c_user_data", "o2c_outputs")
O2C_FILE_REGISTRY = "o2c_file_registry.json"
os.makedirs(O2C_OUTPUT_DIR, exist_ok=True)
os.makedirs("o2c_user_data", exist_ok=True)


def _log(msg: str):
    print(f"[O2C TRANSFORM] {msg}")


def _load_registry():
    if os.path.exists(O2C_FILE_REGISTRY):
        try:
            with open(O2C_FILE_REGISTRY) as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def _save_registry(reg):
    with open(O2C_FILE_REGISTRY, "w") as f:
        json.dump(reg, f, indent=4)


def _str(s: pd.Series) -> pd.Series:
    return s.astype(str).str.strip()


def _norm_num(series: pd.Series) -> pd.Series:
    """
    Normalise a numeric-like column to a clean integer string.
    Replicates KNIME string($VBELN$): 10000001.0 → "10000001".
    Empty / NaN → "" (will not participate in joins).
    """
    def _clean(v):
        v = str(v).strip()
        if v.lower() in ("nan", "none", "nat", ""):
            return ""
        try:
            f = float(v)
            if f == int(f):
                return str(int(f))
        except (ValueError, OverflowError):
            pass
        return v
    return series.map(_clean)


def _keep_cols(df: pd.DataFrame, cols: list) -> pd.DataFrame:
    return df[[c for c in cols if c in df.columns]].copy()


def _to_date(df: pd.DataFrame, cols: list) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce")
    return df


# ─────────────────────────────────────────────────────────────────────────────
def _run_o2c_pipeline(tables: dict, username: str) -> pd.DataFrame:
    """
    Returns a WIDE dataframe — one row per Subsequent Document (delivery case key).
    All date columns are populated; process_df() in o2c4.py maps them to activity names.
    """

    # ═══════════════════════════════════════════════════════════════════════
    # VBAK  — ColFilter#12 → ColRenamer#21 → RowFilter#11 → RuleEngine#35/#37
    # ═══════════════════════════════════════════════════════════════════════
    vbak = tables["VBAK"].copy()
    _log(f"VBAK raw: {len(vbak):,} rows")

    vbak = _keep_cols(vbak, ["VBELN","ERDAT","ERNAM","ANGDT","AUDAT","VBTYP",
                              "AUART","AUGRU","LIFSK","FAKSK","VKORG","VKGRP",
                              "KUNNR","AEDAT"])
    vbak = vbak.rename(columns={
        "VBELN": "Sales Order Number",
        "ERDAT": "Sales Document Creation Date",
        "ERNAM": "Sales Document Maker",
        "AUART": "Sales Document Type",
        "LIFSK": "Delivery Block",
        "FAKSK": "Billing Block",
        "AEDAT": "Header Changed Date",
    })

    # RowFilter#11 — VBTYP = 'C'
    if "VBTYP" in vbak.columns:
        vbak = vbak[_str(vbak["VBTYP"]).str.upper() == "C"].copy()
    _log(f"VBAK after VBTYP=C: {len(vbak):,}")

    vbak = _to_date(vbak, ["Sales Document Creation Date", "Header Changed Date"])
    vbak["Sales Order Number"] = _norm_num(vbak["Sales Order Number"])
    if "KUNNR" in vbak.columns:
        vbak["KUNNR"] = _norm_num(vbak["KUNNR"])

    # RuleEngine#35 — Delivery Blocked Date
    if "Delivery Block" in vbak.columns and "Header Changed Date" in vbak.columns:
        filled = vbak["Delivery Block"].notna() & (_str(vbak["Delivery Block"]) != "")
        vbak["Delivery Blocked Date"] = vbak["Header Changed Date"].where(filled)
    else:
        vbak["Delivery Blocked Date"] = pd.NaT

    # RuleEngine#37 — Billing Block Date
    if "Billing Block" in vbak.columns and "Header Changed Date" in vbak.columns:
        filled = vbak["Billing Block"].notna() & (_str(vbak["Billing Block"]) != "")
        vbak["Billing Block Date"] = vbak["Header Changed Date"].where(filled)
    else:
        vbak["Billing Block Date"] = pd.NaT

    vbak = vbak.drop_duplicates("Sales Order Number", keep="first").reset_index(drop=True)
    _log(f"VBAK deduplicated: {len(vbak):,}")

    # ═══════════════════════════════════════════════════════════════════════
    # VBAP  — ColFilter#13 → ColRenamer#23 → StringManip#22 → RuleEngine#33
    # ═══════════════════════════════════════════════════════════════════════
    vbap = tables["VBAP"].copy()
    _log(f"VBAP raw: {len(vbap):,}")

    vbap = _keep_cols(vbap, ["VBELN","POSNR","MATNR","ABGRU","NETWR","AEDAT"])
    vbap = vbap.rename(columns={
        "VBELN": "Sales Order Number",
        "ABGRU": "Reason for Rejection",
        "NETWR": "Net Value of the Order Item",
        "AEDAT": "Item Changed Date",
    })

    vbap["Sales Order Number"] = _norm_num(vbap["Sales Order Number"])
    if "POSNR" in vbap.columns:
        vbap["POSNR"] = _norm_num(vbap["POSNR"])

    # StringManip#22 — Preceding Document = VBELN + POSNR (raw concat)
    if "Sales Order Number" in vbap.columns and "POSNR" in vbap.columns:
        vbap["Preceding Document"] = vbap["Sales Order Number"] + vbap["POSNR"]
        bad = (vbap["Sales Order Number"] == "") | (vbap["POSNR"] == "")
        vbap.loc[bad, "Preceding Document"] = ""

    vbap = _to_date(vbap, ["Item Changed Date"])

    # RuleEngine#33 — Sales Order Rejected Date
    if "Reason for Rejection" in vbap.columns and "Item Changed Date" in vbap.columns:
        rej = vbap["Reason for Rejection"].notna() & (_str(vbap["Reason for Rejection"]) != "")
        vbap["Sales Order Rejected Date"] = vbap["Item Changed Date"].where(rej)
    else:
        vbap["Sales Order Rejected Date"] = pd.NaT

    # Aggregate VBAP to one row per Sales Order Number for the join
    # (take first item values — MATNR, rejection, net value)
    agg_d = {col: "first" for col in
             ["Reason for Rejection","Net Value of the Order Item",
              "Item Changed Date","Sales Order Rejected Date","MATNR","Preceding Document"]
             if col in vbap.columns}
    vbap_agg = vbap.groupby("Sales Order Number", sort=False).agg(agg_d).reset_index()
    _log(f"VBAP aggregated to {len(vbap_agg):,} SOs")
    del vbap; gc.collect()

    # ═══════════════════════════════════════════════════════════════════════
    # Joiner#39 — VBAK LEFT JOIN VBAP on Sales Order Number
    # ═══════════════════════════════════════════════════════════════════════
    combined = vbak.merge(vbap_agg, on="Sales Order Number", how="left", suffixes=("","_vbap"))
    _log(f"After VBAK+VBAP: {len(combined):,} rows")
    del vbak, vbap_agg; gc.collect()

    # ═══════════════════════════════════════════════════════════════════════
    # VBFA  — ColFilter#15 → StringManip#24/#25 → three branch splits
    # ═══════════════════════════════════════════════════════════════════════
    vbfa = tables["VBFA"].copy()
    _log(f"VBFA raw: {len(vbfa):,}")

    vbfa = _keep_cols(vbfa, ["VBELV","POSNV","VBELN","POSNN","VBTYP_N","ERDAT"])

    for col in ["VBELV","VBELN"]:
        if col in vbfa.columns:
            vbfa[col] = _norm_num(vbfa[col])
    for col in ["POSNV","POSNN"]:
        if col in vbfa.columns:
            vbfa[col] = _norm_num(vbfa[col])

    # StringManip#24 — Preceding Document = VBELV + POSNV
    if "VBELV" in vbfa.columns and "POSNV" in vbfa.columns:
        vbfa["Preceding Document"] = vbfa["VBELV"] + vbfa["POSNV"]
        bad = (vbfa["VBELV"] == "") | (vbfa["POSNV"] == "")
        vbfa.loc[bad, "Preceding Document"] = ""

    # StringManip#25 — Subsequent Document = VBELN + POSNN
    if "VBELN" in vbfa.columns and "POSNN" in vbfa.columns:
        vbfa["Subsequent Document"] = vbfa["VBELN"] + vbfa["POSNN"]
        bad = (vbfa["VBELN"] == "") | (vbfa["POSNN"] == "")
        vbfa.loc[bad, "Subsequent Document"] = ""

    vbfa = _to_date(vbfa, ["ERDAT"])
    if "VBTYP_N" in vbfa.columns:
        vbfa["VBTYP_N"] = vbfa["VBTYP_N"].astype(str).str.strip()  # preserve case: 'h' ≠ 'H'

    # ── Delivery branch: RowFilter#40 (J/T/H) → ColExpr#44 ──────────────
    vbfa_del = vbfa[vbfa["VBTYP_N"].isin(["J","T","H"])].copy()
    _log(f"VBFA-Delivery rows: {len(vbfa_del):,}")

    vbfa_del["Delivery Creation Date"]    = vbfa_del["ERDAT"].where(vbfa_del["VBTYP_N"] == "J")
    vbfa_del["Delivery Return Oder Date"] = vbfa_del["ERDAT"].where(vbfa_del["VBTYP_N"] == "T")
    vbfa_del["Return Oder Date"]          = vbfa_del["ERDAT"].where(vbfa_del["VBTYP_N"] == "H")

    vbfa_del_agg = (
        vbfa_del.sort_values("ERDAT")
        .groupby("Preceding Document", sort=False)
        .agg(
            VBELN_del              =("VBELN",                   "first"),
            Subsequent_Document    =("Subsequent Document",     "first"),
            Delivery_Creation_Date =("Delivery Creation Date",  "first"),
            Delivery_Return_Date   =("Delivery Return Oder Date","first"),
            Return_Oder_Date       =("Return Oder Date",        "first"),
        )
        .reset_index()
        .rename(columns={
            "VBELN_del":              "VBELN",
            "Subsequent_Document":    "Subsequent Document",
            "Delivery_Creation_Date": "Delivery Creation Date",
            "Delivery_Return_Date":   "Delivery Return Oder Date",
            "Return_Oder_Date":       "Return Oder Date",
        })
    )
    _log(f"VBFA-Delivery aggregated: {len(vbfa_del_agg):,}")
    del vbfa_del; gc.collect()

    # Joiner#49 — combined LEFT JOIN VBFA-Delivery on Preceding Document
    combined = combined.merge(vbfa_del_agg, on="Preceding Document", how="left", suffixes=("","_vdel"))
    _log(f"After +VBFA-Delivery: {len(combined):,}")
    del vbfa_del_agg; gc.collect()

    # ═══════════════════════════════════════════════════════════════════════
    # LIKP  — ColFilter#16 → ColRenamer#26
    # Joiner#51: left VBELN (delivery header from VBFA) = right Delivery Document number
    # ═══════════════════════════════════════════════════════════════════════
    likp = tables["LIKP"].copy()
    _log(f"LIKP raw: {len(likp):,}")

    likp = _keep_cols(likp, ["VBELN","ERDAT","WADAT_IST","ERNAM","WADAT"])
    likp = likp.rename(columns={
        "VBELN":     "Delivery Document number",
        "ERDAT":     "Delivery Document Creation Date",
        "WADAT_IST": "Goods Issued",
        "ERNAM":     "Delivery Document Maker",
    })
    likp["Delivery Document number"] = _norm_num(likp["Delivery Document number"])
    likp = likp.drop_duplicates("Delivery Document number", keep="first").reset_index(drop=True)
    likp = _to_date(likp, ["Delivery Document Creation Date","WADAT","Goods Issued"])
    _log(f"LIKP deduplicated: {len(likp):,}")

    # Joiner#51 — join on VBELN (raw delivery doc number carried from VBFA-Delivery agg)
    if "VBELN" in combined.columns:
        combined = combined.merge(
            likp.rename(columns={"Delivery Document number": "VBELN"}),
            on="VBELN", how="left", suffixes=("","_likp")
        )
    _log(f"After +LIKP: {len(combined):,}")
    del likp; gc.collect()

    # ═══════════════════════════════════════════════════════════════════════
    # LIPS  — ColFilter#17 → ColRenamer#27 → StringManip#28
    # Joiner#52: Subsequent Document = Delivery Document (VBELN+POSNR)
    # ═══════════════════════════════════════════════════════════════════════
    lips = tables["LIPS"].copy()
    _log(f"LIPS raw: {len(lips):,}")

    lips = _keep_cols(lips, ["VBELN","POSNR","MATNR","LFIMG","WERKS"])
    lips = lips.rename(columns={
        "VBELN": "Delivery Number",
        "POSNR": "Delivery Item Number",
        "MATNR": "Material Number",
        "LFIMG": "Actual quantity delivered",
    })
    lips["Delivery Number"]      = _norm_num(lips["Delivery Number"])
    lips["Delivery Item Number"] = _norm_num(lips["Delivery Item Number"])

    # StringManip#28 — Delivery Document = VBELN + POSNR
    lips["Delivery Document"] = lips["Delivery Number"] + lips["Delivery Item Number"]
    bad = (lips["Delivery Number"] == "") | (lips["Delivery Item Number"] == "")
    lips.loc[bad, "Delivery Document"] = ""
    lips = lips.drop_duplicates("Delivery Document", keep="first").reset_index(drop=True)
    _log(f"LIPS deduplicated: {len(lips):,}")

    # Joiner#52 — Subsequent Document (left) = Delivery Document (right)
    if "Subsequent Document" in combined.columns:
        combined = combined.merge(
            lips, left_on="Subsequent Document", right_on="Delivery Document",
            how="left", suffixes=("","_lips")
        )
        combined.drop(columns=["Delivery Document"], errors="ignore", inplace=True)
    _log(f"After +LIPS: {len(combined):,}")
    del lips; gc.collect()

    # ── Goods branch: RowFilter#41 (R/h) → ColExpr#54 ───────────────────
    vbfa_gds = vbfa[vbfa["VBTYP_N"].isin(["R","h"])].copy()
    _log(f"VBFA-Goods rows: {len(vbfa_gds):,}")

    vbfa_gds["Goods Movement Date"] = vbfa_gds["ERDAT"].where(vbfa_gds["VBTYP_N"] == "R")
    vbfa_gds["GI Reversed"]         = vbfa_gds["ERDAT"].where(vbfa_gds["VBTYP_N"] == "h")

    vbfa_gds_agg = (
        vbfa_gds.sort_values("ERDAT")
        .groupby("Preceding Document", sort=False)
        .agg(
            Goods_Movement_Date=("Goods Movement Date","first"),
            GI_Reversed        =("GI Reversed",        "first"),
        )
        .reset_index()
        .rename(columns={
            "Goods_Movement_Date": "Goods Movement Date",
            "GI_Reversed":         "GI Reversed",
        })
    )
    _log(f"VBFA-Goods aggregated: {len(vbfa_gds_agg):,}")
    del vbfa_gds; gc.collect()

    # Joiner#55 — Subsequent Document (left) = Preceding Document (right, VBFA-Goods)
    if "Subsequent Document" in combined.columns:
        combined = combined.merge(
            vbfa_gds_agg.rename(columns={"Preceding Document": "Subsequent Document"}),
            on="Subsequent Document", how="left", suffixes=("","_gds")
        )
    _log(f"After +VBFA-Goods: {len(combined):,}")
    del vbfa_gds_agg; gc.collect()

    # ── Invoice branch: RowFilter#42 (M/N/O/P) → ColExpr#48 ─────────────
    vbfa_inv = vbfa[vbfa["VBTYP_N"].isin(["M","N","O","P"])].copy()
    _log(f"VBFA-Invoice rows: {len(vbfa_inv):,}")

    vbfa_inv["Invoice Creation Date"] = vbfa_inv["ERDAT"].where(vbfa_inv["VBTYP_N"] == "M")
    vbfa_inv["Invoice Reversal Date"] = vbfa_inv["ERDAT"].where(vbfa_inv["VBTYP_N"] == "N")
    # SAP standard: P = Credit Note, O = Debit Memo
    vbfa_inv["Credit Memo Date"]      = vbfa_inv["ERDAT"].where(vbfa_inv["VBTYP_N"] == "P")
    vbfa_inv["Debit Memo Date"]       = vbfa_inv["ERDAT"].where(vbfa_inv["VBTYP_N"] == "O")

    vbfa_inv_agg = (
        vbfa_inv.sort_values("ERDAT")
        .groupby("Preceding Document", sort=False)
        .agg(
            VBELN_inv             =("VBELN",                 "first"),
            Subseq_Doc_Inv        =("Subsequent Document",   "first"),
            Invoice_Creation_Date =("Invoice Creation Date", "first"),
            Invoice_Reversal_Date =("Invoice Reversal Date", "first"),
            Credit_Memo_Date      =("Credit Memo Date",      "first"),
            Debit_Memo_Date       =("Debit Memo Date",       "first"),
        )
        .reset_index()
        .rename(columns={
            "Subseq_Doc_Inv":        "Subsequent Document (Invoice)",
            "Invoice_Creation_Date": "Invoice Creation Date",
            "Invoice_Reversal_Date": "Invoice Reversal Date",
            "Credit_Memo_Date":      "Credit Memo Date",
            "Debit_Memo_Date":       "Debit Memo Date",
        })
    )
    _log(f"VBFA-Invoice aggregated: {len(vbfa_inv_agg):,}")
    del vbfa_inv, vbfa; gc.collect()

    # Joiner#56 — Subsequent Document (left) = Preceding Document (right, VBFA-Invoice)
    if "Subsequent Document" in combined.columns:
        combined = combined.merge(
            vbfa_inv_agg.rename(columns={"Preceding Document": "Subsequent Document"}),
            on="Subsequent Document", how="left", suffixes=("","_inv")
        )
    _log(f"After +VBFA-Invoice: {len(combined):,}")
    del vbfa_inv_agg; gc.collect()

    # ═══════════════════════════════════════════════════════════════════════
    # VBRK  — ColFilter#19 → ColRenamer#29
    # Joiner#57: VBELN_inv (billing VBELN from VBFA-Invoice) = Billing Document Number
    # ═══════════════════════════════════════════════════════════════════════
    vbrk = tables["VBRK"].copy()
    _log(f"VBRK raw: {len(vbrk):,}")

    vbrk = _keep_cols(vbrk, ["VBELN","ERDAT","ERNAM","FKTYP","FKART"])
    vbrk = vbrk.rename(columns={
        "VBELN": "Billing Document Number",
        "ERDAT": "Invoice Record Creation Date",
        "ERNAM": "Invoice Maker",
        "FKTYP": "Billing Type",
    })
    vbrk["Billing Document Number"] = _norm_num(vbrk["Billing Document Number"])
    vbrk = vbrk.drop_duplicates("Billing Document Number", keep="first").reset_index(drop=True)
    _log(f"VBRK deduplicated: {len(vbrk):,}")

    # Joiner#57 — VBELN_inv (left) = Billing Document Number (right)
    if "VBELN_inv" in combined.columns:
        combined = combined.merge(
            vbrk.rename(columns={"Billing Document Number": "VBELN_inv"}),
            on="VBELN_inv", how="left", suffixes=("","_vbrk")
        )
        combined.rename(columns={"VBELN_inv": "Billing Document Number"}, inplace=True)
    _log(f"After +VBRK: {len(combined):,}")
    del vbrk; gc.collect()

    # ═══════════════════════════════════════════════════════════════════════
    # VBRP  — ColFilter#20 → ColRenamer#30 → StringManip#31
    # Joiner#59: Subsequent Document (Invoice) = Billing Document (VBELN+POSNR)
    # ═══════════════════════════════════════════════════════════════════════
    vbrp = tables["VBRP"].copy()
    _log(f"VBRP raw: {len(vbrp):,}")

    vbrp = _keep_cols(vbrp, ["VBELN","POSNR","MATNR","FKIMG","NETWR"])
    vbrp = vbrp.rename(columns={
        "VBELN": "Billing Document Number",
        "POSNR": "Billing Item Number",
        "MATNR": "Billing Material",
        "FKIMG": "Actual billed quantity",
        "NETWR": "Net value of the billing item",
    })
    vbrp["Billing Document Number"] = _norm_num(vbrp["Billing Document Number"])
    vbrp["Billing Item Number"]      = _norm_num(vbrp["Billing Item Number"])

    # StringManip#31 — Billing Document = VBELN + POSNR
    vbrp["Billing Document"] = vbrp["Billing Document Number"] + vbrp["Billing Item Number"]
    bad = (vbrp["Billing Document Number"] == "") | (vbrp["Billing Item Number"] == "")
    vbrp.loc[bad, "Billing Document"] = ""
    vbrp = vbrp.drop_duplicates("Billing Document", keep="first").reset_index(drop=True)
    _log(f"VBRP deduplicated: {len(vbrp):,}")

    # Joiner#59 — Subsequent Document (Invoice) (left) = Billing Document (right)
    if "Subsequent Document (Invoice)" in combined.columns:
        combined = combined.merge(
            vbrp[["Billing Document","Actual billed quantity","Net value of the billing item"]],
            left_on="Subsequent Document (Invoice)", right_on="Billing Document",
            how="left", suffixes=("","_vbrp")
        )
        combined.drop(columns=["Billing Document"], errors="ignore", inplace=True)
    _log(f"After +VBRP: {len(combined):,}")
    del vbrp; gc.collect()

    # ═══════════════════════════════════════════════════════════════════════
    # BSAD  — ColRenamer#32
    # Joiner#60: Billing Document Number (left) = Billing Document (BSAD.VBELN)
    # ═══════════════════════════════════════════════════════════════════════
    bsad = tables["BSAD"].copy()
    _log(f"BSAD raw: {len(bsad):,}")

    bsad = bsad.rename(columns={
        "VBELN": "Billing Document",
        "AUGDT": "Clearing Date",
        "DMBTR": "Amount in Local Currency",
        "AUGBL": "Clearing Document Number",
        "BUKRS": "Company Code",
    })
    bsad = _keep_cols(bsad, ["Billing Document","Clearing Date","Amount in Local Currency",
                              "Clearing Document Number","Company Code","KUNNR"])
    if "Billing Document" in bsad.columns:
        bsad["Billing Document"] = _norm_num(bsad["Billing Document"])
    bsad = _to_date(bsad, ["Clearing Date"])

    # Aggregate to one row per Billing Document
    agg_d = {}
    for col, func in [("Clearing Date","min"),("Clearing Document Number","first"),
                      ("Amount in Local Currency","sum"),("Company Code","first"),
                      ("KUNNR","first")]:
        if col in bsad.columns:
            agg_d[col] = func
    if "Billing Document" in bsad.columns and agg_d:
        bsad_agg = bsad.groupby("Billing Document", sort=False).agg(agg_d).reset_index()
    else:
        bsad_agg = bsad.drop_duplicates("Billing Document", keep="first").reset_index(drop=True)
    _log(f"BSAD aggregated: {len(bsad_agg):,} unique billing docs")
    del bsad; gc.collect()

    # Joiner#60 — Billing Document Number (left) = Billing Document (right)
    if "Billing Document Number" in combined.columns:
        combined = combined.merge(
            bsad_agg.rename(columns={"Billing Document": "Billing Document Number"}),
            on="Billing Document Number", how="left", suffixes=("","_bsad")
        )
    _log(f"After +BSAD: {len(combined):,}")
    del bsad_agg; gc.collect()

    # ═══════════════════════════════════════════════════════════════════════
    # KNA1  — ValueLookup#74: KUNNR → NAME1
    # ═══════════════════════════════════════════════════════════════════════
    kna1 = tables["KNA1"].copy()
    if "KUNNR" in kna1.columns and "NAME1" in kna1.columns:
        kna1["KUNNR"] = _norm_num(kna1["KUNNR"])
        kna1_lookup = (kna1[["KUNNR","NAME1"]]
                       .drop_duplicates("KUNNR", keep="first")
                       .rename(columns={"KUNNR": "_kunnr_key"}))
        if "KUNNR" in combined.columns:
            combined["_kunnr_key"] = _norm_num(combined["KUNNR"].fillna("").astype(str))
            combined = combined.merge(kna1_lookup, on="_kunnr_key", how="left")
            combined.drop(columns=["_kunnr_key"], errors="ignore", inplace=True)
            _log(f"KNA1 NAME1 filled: {combined['NAME1'].notna().sum():,} rows")
    del kna1; gc.collect()

    # ═══════════════════════════════════════════════════════════════════════
    # Finalise: ensure case key column, parse all date columns, deduplicate
    # ═══════════════════════════════════════════════════════════════════════
    if "Subsequent Document" not in combined.columns:
        if "Sales Order Number" in combined.columns:
            combined["Subsequent Document"] = combined["Sales Order Number"]
        else:
            combined["Subsequent Document"] = combined.index.astype(str)

    combined = _to_date(combined, [
        "Sales Document Creation Date", "Delivery Blocked Date", "Billing Block Date",
        "Sales Order Rejected Date", "Delivery Return Oder Date", "Return Oder Date",
        "Delivery Creation Date", "Goods Movement Date", "Invoice Creation Date",
        "Invoice Reversal Date", "Credit Memo Date", "Debit Memo Date",
        "Clearing Date", "Delivery Document Creation Date", "Goods Issued", "WADAT",
        "GI Reversed",
    ])

    # Final dedup on case key to ensure one row per case
    before = len(combined)
    combined = combined.drop_duplicates("Subsequent Document", keep="first").reset_index(drop=True)
    _log(
        f"Final: {before:,} → {len(combined):,} rows | "
        f"unique cases: {combined['Subsequent Document'].nunique():,}"
    )
    return combined


# ─── FastAPI Endpoints ────────────────────────────────────────────────────────

@o2c_transformer_router.post("/upload_table")
async def upload_o2c_table(
    file: UploadFile = File(...),
    table_name: str = Form(...),
    username: str = Form("Unknown"),
):
    table_name = table_name.strip().upper()

    # ── 1. Validate table name ───────────────────────────────────────────────
    if table_name not in EXPECTED_TABLES:
        raise HTTPException(400, f"Unknown table '{table_name}'. Expected one of: {EXPECTED_TABLES}")

    # ── 2. Check for duplicate — already uploaded in this session ────────────
    if username in O2C_RAW_TABLES and table_name in O2C_RAW_TABLES[username]:
        existing = O2C_RAW_TABLES[username][table_name]
        raise HTTPException(400,
            f"Table '{table_name}' has already been uploaded this session "
            f"({len(existing):,} rows). "
            f"Use the ✕ button to clear it first before uploading again."
        )

    # ── 3. Parse CSV ─────────────────────────────────────────────────────────
    raw = await file.read()
    df = None
    for enc in ("utf-8", "latin-1", "windows-1252"):
        try:
            df = pd.read_csv(io.BytesIO(raw), encoding=enc, low_memory=False)
            break
        except Exception:
            continue
    if df is None:
        raise HTTPException(400, "Could not decode CSV — try saving as UTF-8.")

    # Normalise column names: strip whitespace
    df.columns = [c.strip() for c in df.columns]

    # ── 4. Validate required columns ─────────────────────────────────────────
    required = REQUIRED_COLS.get(table_name, set())
    uploaded_cols = {c.upper() for c in df.columns}
    missing = {c for c in required if c.upper() not in uploaded_cols}
    if missing:
        raise HTTPException(400,
            f"Wrong file for '{table_name}'. "
            f"Missing required columns: {sorted(missing)}. "
            f"Please upload the correct SAP '{table_name}' table."
        )

    # ── 5. Store ──────────────────────────────────────────────────────────────
    if username not in O2C_RAW_TABLES:
        O2C_RAW_TABLES[username] = {}
    O2C_RAW_TABLES[username][table_name] = df
    _log(f"'{table_name}' uploaded by '{username}': {len(df):,} rows × {len(df.columns)} cols")
    return {"status": "ok", "table": table_name, "rows": len(df), "columns": list(df.columns)}


@o2c_transformer_router.get("/status")
def o2c_transform_status(username: str = Query("Unknown")):
    tables = O2C_RAW_TABLES.get(username, {})
    return {
        "uploaded": list(tables.keys()),
        "missing":  [t for t in EXPECTED_TABLES if t not in tables],
        "ready":    all(t in tables for t in EXPECTED_TABLES),
    }


@o2c_transformer_router.post("/build")
def build_o2c_event_log(username: str = Query("Unknown")):
    tables  = O2C_RAW_TABLES.get(username, {})
    missing = [t for t in EXPECTED_TABLES if t not in tables]
    if missing:
        raise HTTPException(400, f"Missing tables: {missing}")

    try:
        result_df = _run_o2c_pipeline(tables, username)
    except Exception as e:
        _log(f"Pipeline failed for '{username}': {e}\n{traceback.format_exc()}")
        raise HTTPException(500, f"O2C Transform pipeline error: {e}")

    from o2c4 import USER_DFS, process_df, log_audit, COL_CASE

    processed = process_df(result_df)
    USER_DFS[username]  = processed
    USER_DFS["Unknown"] = processed
    log_audit(username, "O2C_TRANSFORM", f"Built O2C event log: {len(processed):,} rows")
    _log(f"Done for '{username}': {len(processed):,} rows × {len(processed.columns)} cols")

    ts        = datetime.now().strftime("%Y%m%d_%H%M%S")
    save_path = ""
    csv_path  = ""

    # Save JSON session
    try:
        user_dir  = os.path.join("o2c_user_data", username)
        os.makedirs(user_dir, exist_ok=True)
        save_path = os.path.join(user_dir, f"{ts}_o2c_build.json")
        processed.to_json(save_path, orient="records", date_format="iso")
        _log(f"Session JSON → {save_path}")
    except Exception as e:
        _log(f"JSON save failed (non-fatal): {e}")

    # Save wide CSV
    try:
        export_df = processed[[c for c in processed.columns if not c.startswith("_")]].copy()
        for c in export_df.columns:
            if pd.api.types.is_datetime64_any_dtype(export_df[c]):
                export_df[c] = export_df[c].dt.strftime("%Y-%m-%d")
        csv_name = f"O2C_Build_{username}_{ts}.csv"
        csv_path = os.path.join(O2C_OUTPUT_DIR, csv_name)
        export_df.to_csv(csv_path, index=False)
        _log(f"Output CSV → {csv_path}")
    except Exception as e:
        _log(f"CSV save failed (non-fatal): {e}")

    # Register build
    try:
        cases = int(processed[COL_CASE].nunique()) if COL_CASE in processed.columns else 0
        reg   = _load_registry()
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
        _save_registry(reg)
        _log(f"Registry updated: {cases:,} cases")
    except Exception as e:
        _log(f"Registry update failed (non-fatal): {e}")

    return {
        "status":   "ok",
        "rows":     len(processed),
        "columns":  list(processed.columns),
        "csv_path": csv_path,
    }
