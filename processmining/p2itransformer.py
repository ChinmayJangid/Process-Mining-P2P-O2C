"""
p2itransformer.py — P2I Data Pipeline (FastAPI Router)
Handles raw SAP table uploads and builds the P2I event log.

Architecture:
  Phase 0: P2P Bridge  — EBAN, EKKO, EKPO, EKBE linked via AUFNR
  Phase 1: P2I Core    — AFKO, AFPO, RESB, MKPF+MSEG, AFRU, MARA
  Phase 2: Deviations  — Event-level (Scrap, Rework, Reversals)
  Phase 3: Case-level  — Over-production, Material Delay days
"""

import io
import os
import json
import traceback
import pandas as pd
import numpy as np
from datetime import datetime
from fastapi import APIRouter, HTTPException, UploadFile, File, Form, Query

p2i_transformer_router = APIRouter(prefix="/p2i/transform", tags=["P2I-Transformer"])

# ── In-memory raw table store (per user) ──────────────────────────────────────
RAW_TABLES: dict = {}

# Mandatory manufacturing tables; procurement bridge tables are optional
MFG_TABLES      = ["AFKO", "AFPO", "RESB", "MKPF", "MSEG", "AFRU", "MARA"]
BRIDGE_TABLES   = ["EBAN", "EKKO", "EKPO", "EKBE"]
REQUIRED_TABLES = MFG_TABLES + BRIDGE_TABLES

# Minimum required columns per table (uppercase)
REQUIRED_COLS = {
    "AFKO": {"AUFNR", "GSTRP"},
    "AFPO": {"AUFNR", "PSMNG", "MATNR"},
    "RESB": {"AUFNR", "BDTER", "XLOEK"},
    "MKPF": {"MBLNR", "BUDAT"},
    "MSEG": {"MBLNR", "AUFNR", "BWART", "MENGE"},
    "AFRU": {"AUFNR", "IEDD"},
    "MARA": {"MATNR", "MTART"},
    # Bridge (validated only if present)
    "EBAN": {"BANFN", "BADAT"},
    "EKKO": {"EBELN", "AEDAT"},
    "EKPO": {"EBELN", "EBELP", "BANFN", "AUFNR"},
    "EKBE": {"EBELN", "EBELP", "BUDAT", "BEWTP"},
}


def _log(msg: str):
    print(f"[P2I-TRANSFORM] {datetime.now():%Y-%m-%d %H:%M:%S} | {msg}")


# ── Upload individual table ───────────────────────────────────────────────────
@p2i_transformer_router.post("/upload_table")
async def upload_table(
    table_name:     str = Form(...),
    username:       str = Form("Unknown"),
    file:    UploadFile = File(...),
    column_mapping: str = Form("{}"),
):
    table_name = table_name.strip().upper()
    if table_name not in REQUIRED_TABLES:
        raise HTTPException(400, f"'{table_name}' is not a recognised P2I table. "
                                 f"Accepted: {REQUIRED_TABLES}")

    try:
        content = await file.read()
        
        # ── Try reading with UTF-8, fallback to latin1 for SAP CSVs ──────────
        try:
            df = pd.read_csv(io.BytesIO(content), low_memory=False, dtype=str, encoding='utf-8')
        except UnicodeDecodeError:
            _log(f"UTF-8 decode failed for {table_name}, retrying with latin1...")
            df = pd.read_csv(io.BytesIO(content), low_memory=False, dtype=str, encoding='latin1')

        # Strip whitespace from all column names
        df.columns = [str(c).strip() for c in df.columns]

        # ── Apply user column mapping ─────────────────────────────────────────
        try:
            mapping = json.loads(column_mapping) if column_mapping else {}
            if mapping:
                actual_lower = {c.lower(): c for c in df.columns}
                resolved = {}
                for src, tgt in mapping.items():
                    key = src.lower()
                    if key in actual_lower:
                        resolved[actual_lower[key]] = tgt
                    else:
                        resolved[src] = tgt          # best-effort fallback
                df = df.rename(columns=resolved)
                _log(f"Mapping applied for {table_name}: {resolved}")
        except Exception as e:
            _log(f"Mapping parse error for {table_name}: {e}")

        # Normalise column names to UNIQUE UPPERCASE
        seen = {}
        new_cols = []
        for c in df.columns:
            cu = str(c).strip().upper()
            if cu in seen:
                seen[cu] += 1
                new_cols.append(f"{cu}_{seen[cu]}")
            else:
                seen[cu] = 0
                new_cols.append(cu)
        df.columns = new_cols

        # Strip leading/trailing whitespace from all string cells
        for c in df.select_dtypes(include="object").columns:
            df[c] = df[c].astype(str).str.strip()

        # Normalise SAP keys (remove leading zeros for robust joins)
        for key_col in ["AUFNR", "MATNR", "BANFN", "EBELN"]:
            if key_col in df.columns:
                df[key_col] = df[key_col].astype(str).str.lstrip("0")

        # ── Validate minimum required columns ─────────────────────────────────
        required = REQUIRED_COLS.get(table_name, set())
        uploaded = set(df.columns)
        missing  = required - uploaded
        if missing:
            raise HTTPException(
                400, f"Table '{table_name}' is missing columns: {sorted(missing)}"
            )

        RAW_TABLES.setdefault(username, {})[table_name] = df
        _log(f"[{username}] SUCCESS: {table_name} stored. Rows: {len(df)}, Cols: {list(df.columns)}")
        return {"status": "ok", "table": table_name, "rows": len(df), "columns": list(df.columns)}

    except HTTPException:
        raise
    except Exception as e:
        _log(f"Upload error {table_name}: {e}\n{traceback.format_exc()}")
        raise HTTPException(400, f"Failed to process '{table_name}': {e}")


# ── Clear a single table ──────────────────────────────────────────────────────
@p2i_transformer_router.delete("/clear_table")
def clear_table(table_name: str, username: str = Query("Unknown")):
    table_name = table_name.strip().upper()
    user_tables = RAW_TABLES.get(username, {})
    if table_name in user_tables:
        del user_tables[table_name]
        _log(f"[{username}] {table_name} cleared.")
    return {"status": "ok", "message": f"{table_name} cleared"}


# ── Helper: safe date parser ──────────────────────────────────────────────────
def _to_date(series: pd.Series) -> pd.Series:
    # Handle SAP date formats (YYYYMMDD or DD.MM.YYYY)
    return pd.to_datetime(series, errors="coerce", dayfirst=True)


def _safe_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").fillna(0)


# ── Core pipeline ─────────────────────────────────────────────────────────────
def _run_pipeline(tables: dict, username: str) -> pd.DataFrame:
    # ── Normalize keys across all tables to ensure robust joins ───────────────
    for tname in tables:
        df = tables[tname]
        for key_col in ["AUFNR", "MATNR", "BANFN", "EBELN", "MBLNR", "MJAHR"]:
            if key_col in df.columns:
                df[key_col] = df[key_col].astype(str).str.strip().str.lstrip("0")

    # ── Load mandatory tables ─────────────────────────────────────────────────
    afko = tables["AFKO"].copy()
    afpo = tables["AFPO"].copy()
    
    # Optional manufacturing tables
    resb = tables.get("RESB", pd.DataFrame(columns=["AUFNR", "BDTER", "XLOEK"])).copy()
    mkpf = tables.get("MKPF", pd.DataFrame(columns=["MBLNR", "BUDAT"])).copy()
    mseg = tables.get("MSEG", pd.DataFrame(columns=["MBLNR", "AUFNR", "BWART", "MENGE"])).copy()
    afru = tables.get("AFRU", pd.DataFrame(columns=["AUFNR", "IEDD", "VORNR", "XMZMN", "RMNGA"])).copy()
    mara = tables.get("MARA", pd.DataFrame(columns=["MATNR", "MTART"])).copy()

    # ── Fix numeric / string typing before any filtering ─────────────────────
    # BWART must be a clean string (no ".0" suffix from float parsing)
    for col in ["BWART"]:
        if col in mseg.columns:
            mseg[col] = mseg[col].astype(str).str.strip().str.replace(r"\.0$", "", regex=True)

    # VORNR must be a string (no ".0")
    if "VORNR" in afru.columns:
        afru["VORNR"] = afru["VORNR"].astype(str).str.strip().str.replace(r"\.0$", "", regex=True)

    # Numeric fields
    for col in ["XMZMN", "RMNGA"]:
        if col in afru.columns:
            afru[col] = _safe_numeric(afru[col])

    if "PSMNG" in afpo.columns:
        afpo["PSMNG"] = _safe_numeric(afpo["PSMNG"])

    if "MENGE" in mseg.columns:
        mseg["MENGE"] = _safe_numeric(mseg["MENGE"])

    # ── Case Context: AFKO ⟕ AFPO ⟕ MARA ────────────────────────────────────
    afpo_cols = ["AUFNR", "PSMNG", "MATNR"]
    if "DWERK" in afpo.columns: afpo_cols.append("DWERK")
    elif "WERKS" in afpo.columns: afpo_cols.append("WERKS")
    
    case_ctx = pd.merge(afko, afpo[afpo_cols], on="AUFNR", how="left")
    case_ctx.rename(columns={"DWERK": "WERKS"}, inplace=True)
    case_ctx = pd.merge(case_ctx, mara[["MATNR", "MTART"]], on="MATNR", how="left")

    # ── Material Documents: MKPF ⟕ MSEG (join on BOTH MBLNR+MJAHR if present)
    join_keys = ["MBLNR"]
    if "MJAHR" in mkpf.columns and "MJAHR" in mseg.columns:
        join_keys.append("MJAHR")
    mat_docs = pd.merge(mkpf[join_keys + ["BUDAT"]], mseg, on=join_keys, how="inner")

    event_list: list[pd.DataFrame] = []

    def _evt(src_df, case_col, ts_col, activity: str, extra_cols=None) -> pd.DataFrame:
        """Helper that creates a minimal event DataFrame."""
        cols = [case_col, ts_col] + (extra_cols or [])
        cols = [c for c in cols if c in src_df.columns]
        e = src_df[cols].copy()
        e = e.rename(columns={ts_col: "Timestamp", case_col: "Case ID"})
        e["Activity"] = activity
        return e

    # ══════════════════════════════════════════════════════════════════════════
    # PHASE 0 — P2P BRIDGE (Procurement events linked via AUFNR)
    # ══════════════════════════════════════════════════════════════════════════
    has_bridge = all(t in tables for t in ["EBAN", "EKKO", "EKPO", "EKBE"])
    if has_bridge:
        _log("P2P Bridge tables found — building procurement linkage.")
        eban = tables["EBAN"].copy()
        ekko = tables["EKKO"].copy()
        ekpo = tables["EKPO"].copy()
        ekbe = tables["EKBE"].copy()

        # EKPO must have AUFNR to act as the bridge key
        if "AUFNR" in ekpo.columns:
            ekpo["AUFNR"] = ekpo["AUFNR"].astype(str).str.strip()
            valid_orders = set(afko["AUFNR"].astype(str).str.strip())
            ekpo_bridged = ekpo[ekpo["AUFNR"].isin(valid_orders)].copy()

            # ── PR Creation via EBAN ────────────────────────────────────────
            if "BANFN" in eban.columns and "BADAT" in eban.columns and "BANFN" in ekpo.columns:
                pr_events = pd.merge(
                    ekpo_bridged[["AUFNR", "BANFN"]].drop_duplicates(),
                    eban[["BANFN", "BADAT"]],
                    on="BANFN", how="inner"
                )
                if not pr_events.empty:
                    e = pr_events[["AUFNR", "BADAT"]].copy()
                    e.rename(columns={"AUFNR": "Case ID", "BADAT": "Timestamp"}, inplace=True)
                    e["Activity"] = "Create Purchase Requisition"
                    event_list.append(e)
                    _log(f"P2P Bridge: {len(e):,} PR Creation events.")

            # ── PO Creation via EKKO ────────────────────────────────────────
            if "EBELN" in ekko.columns and "AEDAT" in ekko.columns and "EBELN" in ekpo.columns:
                ekko_jk = ["EBELN"]
                ekpo_jk = ["EBELN"]
                if "EBELP" in ekko.columns and "EBELP" in ekpo.columns:
                    ekko_jk.append("EBELP"); ekpo_jk.append("EBELP")
                
                po_events = pd.merge(
                    ekpo_bridged[["AUFNR"] + ekpo_jk].drop_duplicates(),
                    ekko[ekko_jk + ["AEDAT"]],
                    on=ekko_jk, how="inner"
                )
                if not po_events.empty:
                    e = po_events[["AUFNR", "AEDAT"]].copy()
                    e.rename(columns={"AUFNR": "Case ID", "AEDAT": "Timestamp"}, inplace=True)
                    e["Activity"] = "Create Purchase Order"
                    event_list.append(e)
                    _log(f"P2P Bridge: {len(e):,} PO Creation events.")

            # ── Goods Receipt Raw Material via EKBE (BEWTP == 'E') ──────────
            if "EBELN" in ekbe.columns and "BUDAT" in ekbe.columns and "BEWTP" in ekbe.columns:
                ekbe["BEWTP"] = ekbe["BEWTP"].astype(str).str.strip()
                ekbe_jk = ["EBELN"]
                ekpo_jk = ["EBELN"]
                if "EBELP" in ekbe.columns and "EBELP" in ekpo.columns:
                    ekbe_jk.append("EBELP"); ekpo_jk.append("EBELP")

                gr_raw = pd.merge(
                    ekpo_bridged[["AUFNR"] + ekpo_jk].drop_duplicates(),
                    ekbe[ekbe["BEWTP"] == "E"][ekbe_jk + ["BUDAT"]],
                    on=ekbe_jk, how="inner"
                )
                if not gr_raw.empty:
                    e = gr_raw[["AUFNR", "BUDAT"]].copy()
                    e.rename(columns={"AUFNR": "Case ID", "BUDAT": "Timestamp"}, inplace=True)
                    e["Activity"] = "Goods Receipt (Raw Material)"
                    event_list.append(e)
                    _log(f"P2P Bridge: {len(e):,} GR Raw Material events.")
        else:
            _log("P2P Bridge: EKPO has no AUFNR column — bridge skipped.")
    else:
        _log("P2P Bridge tables not uploaded — skipping procurement linkage.")

    # ══════════════════════════════════════════════════════════════════════════
    # PHASE 1 — P2I CORE EVENTS
    # ══════════════════════════════════════════════════════════════════════════

    # 1a. Create Production Order
    event_list.append(_evt(afko, "AUFNR", "GSTRP", "Create Production Order"))

    # 1b. Release Production Order
    if "FTRMI" in afko.columns:
        rel = afko[afko["FTRMI"].notna()][["AUFNR", "FTRMI"]].copy()
        rel.rename(columns={"AUFNR": "Case ID", "FTRMI": "Timestamp"}, inplace=True)
        rel["Activity"] = "Release Production Order"
        event_list.append(rel)

    # 1c. Reserve Component (XLOEK != 'X')
    if "XLOEK" in resb.columns:
        res_active = resb[resb["XLOEK"].astype(str).str.strip() != "X"]
    else:
        res_active = resb
    event_list.append(_evt(res_active, "AUFNR", "BDTER", "Reserve Component"))

    # 1d. Goods Issue to WIP (BWART == '261')
    gi = mat_docs[mat_docs["BWART"] == "261"][["AUFNR", "BUDAT"]].copy()
    gi.rename(columns={"AUFNR": "Case ID", "BUDAT": "Timestamp"}, inplace=True)
    gi["Activity"] = "Goods Issue to WIP"
    event_list.append(gi)

    # 1e. Goods Receipt Finished Good (BWART == '101')
    gr = mat_docs[mat_docs["BWART"] == "101"][["AUFNR", "BUDAT"]].copy()
    gr.rename(columns={"AUFNR": "Case ID", "BUDAT": "Timestamp"}, inplace=True)
    gr["Activity"] = "Goods Receipt (Finished Good)"
    event_list.append(gr)

    # 1f. Release from QA (BWART == '321')
    qa = mat_docs[mat_docs["BWART"] == "321"][["AUFNR", "BUDAT"]].copy()
    qa.rename(columns={"AUFNR": "Case ID", "BUDAT": "Timestamp"}, inplace=True)
    qa["Activity"] = "Release from QA"
    event_list.append(qa)

    # NOTE: "Confirm Operation" (AFRU routing steps) is intentionally excluded from
    # the process map.  AFRU is still used for Scrap (XMZMN) and Rework (RMNGA)
    # deviation detection in Phase 2, but individual operation confirmations add
    # excessive noise and are not meaningful at the case / process-map level.

    # 1h. Technically Complete (TECO)
    if "GLTRI" in afko.columns:
        teco = afko[afko["GLTRI"].notna()][["AUFNR", "GLTRI"]].copy()
        teco.rename(columns={"AUFNR": "Case ID", "GLTRI": "Timestamp"}, inplace=True)
        teco["Activity"] = "Technically Complete (TECO)"
        event_list.append(teco)

    # ══════════════════════════════════════════════════════════════════════════
    # PHASE 2 — DEVIATION EVENTS
    # ══════════════════════════════════════════════════════════════════════════

    # 2a. Reverse Goods Issue (BWART == '262')
    rev_gi = mat_docs[mat_docs["BWART"] == "262"][["AUFNR", "BUDAT"]].copy()
    rev_gi.rename(columns={"AUFNR": "Case ID", "BUDAT": "Timestamp"}, inplace=True)
    rev_gi["Activity"] = "Reverse Goods Issue"
    event_list.append(rev_gi)

    # 2b. Reverse Goods Receipt (BWART == '102')
    rev_gr = mat_docs[mat_docs["BWART"] == "102"][["AUFNR", "BUDAT"]].copy()
    rev_gr.rename(columns={"AUFNR": "Case ID", "BUDAT": "Timestamp"}, inplace=True)
    rev_gr["Activity"] = "Reverse Goods Receipt"
    event_list.append(rev_gr)

    # 2c. Record Scrap (XMZMN > 0)
    if "XMZMN" in afru.columns and "IEDD" in afru.columns:
        scrap = afru[afru["XMZMN"] > 0][["AUFNR", "IEDD"]].copy()
        scrap.rename(columns={"AUFNR": "Case ID", "IEDD": "Timestamp"}, inplace=True)
        scrap["Activity"] = "Record Scrap"
        event_list.append(scrap)

    # 2d. Record Rework (RMNGA > 0)
    if "RMNGA" in afru.columns and "IEDD" in afru.columns:
        rework = afru[afru["RMNGA"] > 0][["AUFNR", "IEDD"]].copy()
        rework.rename(columns={"AUFNR": "Case ID", "IEDD": "Timestamp"}, inplace=True)
        rework["Activity"] = "Record Rework"
        event_list.append(rework)

    # ══════════════════════════════════════════════════════════════════════════
    # UNION all events
    # ══════════════════════════════════════════════════════════════════════════
    all_events = pd.concat(
        [e[["Case ID", "Timestamp", "Activity"]] for e in event_list if not e.empty],
        ignore_index=True
    )
    all_events["Timestamp"] = _to_date(all_events["Timestamp"])
    all_events.dropna(subset=["Timestamp", "Case ID"], inplace=True)
    all_events.sort_values(["Case ID", "Timestamp"], inplace=True)

    _log(f"Event union: {len(all_events):,} events across {all_events['Case ID'].nunique():,} orders.")

    # ══════════════════════════════════════════════════════════════════════════
    # PHASE 3 — CASE-LEVEL DEVIATIONS
    # ══════════════════════════════════════════════════════════════════════════

    # Deviation_Material_Delay_Days: GI date − Reservation date (days, ≥0)
    gi_min = (
        mat_docs[mat_docs["BWART"] == "261"]
        .assign(BUDAT=lambda d: _to_date(d["BUDAT"]))
        .groupby("AUFNR")["BUDAT"].min()
    )
    res_min = (
        res_active.assign(BDTER=lambda d: _to_date(d["BDTER"]))
        .groupby("AUFNR")["BDTER"].min()
    )
    delay_df = pd.DataFrame({"gi": gi_min, "res": res_min}).dropna()
    delay_df["Deviation_Material_Delay_Days"] = (
        (delay_df["gi"] - delay_df["res"]).dt.total_seconds() / 86400
    ).clip(lower=0).round(1)

    # Deviation_Over_Produced: sum(GR qty) > planned qty
    gr_sum = (
        mat_docs[mat_docs["BWART"] == "101"]
        .assign(MENGE=lambda d: _safe_numeric(d["MENGE"]))
        .groupby("AUFNR")["MENGE"].sum()
    )
    plan_qty = afpo.set_index("AUFNR")["PSMNG"] if "AUFNR" in afpo.columns else pd.Series(dtype=float)
    over_df = pd.DataFrame({"gr_qty": gr_sum, "plan_qty": plan_qty}).dropna()
    over_df["Deviation_Over_Produced"] = over_df["gr_qty"] > over_df["plan_qty"]

    # ── Build Case Context table ──────────────────────────────────────────────
    ctx_cols = ["AUFNR", "MATNR", "MTART", "PSMNG"]
    if "WERKS" in case_ctx.columns: ctx_cols.append("WERKS")
    ctx = case_ctx[ctx_cols].drop_duplicates("AUFNR").copy()

    ctx = ctx.merge(delay_df[["Deviation_Material_Delay_Days"]], left_on="AUFNR", right_index=True, how="left")
    ctx = ctx.merge(over_df[["Deviation_Over_Produced"]], left_on="AUFNR", right_index=True, how="left")
    ctx.fillna({"Deviation_Material_Delay_Days": 0, "Deviation_Over_Produced": False}, inplace=True)

    # ── Join events with case context ─────────────────────────────────────────
    final_df = pd.merge(
        all_events,
        ctx.rename(columns={"AUFNR": "Case ID"}),
        on="Case ID", how="left"
    )

    _log(f"Pipeline complete: {len(final_df):,} rows, {final_df['Case ID'].nunique():,} orders.")
    return final_df


# ── Build endpoint ────────────────────────────────────────────────────────────
@p2i_transformer_router.post("/build")
def build_event_log(username: str = Query("Unknown")):
    from p2i4 import USER_DFS, process_df, log_audit, UPLOAD_DIR

    # Clear stale in-memory data for this user
    for key in [username, "Unknown"]:
        if key in USER_DFS:
            del USER_DFS[key]

    tables = RAW_TABLES.get(username, {})

    # Only AFKO and AFPO are truly mandatory; rest are optional
    mandatory = {"AFKO", "AFPO"}
    uploaded  = set(tables.keys())
    missing   = mandatory - uploaded
    if missing:
        raise HTTPException(
            400, f"Missing mandatory tables: {sorted(missing)}. "
                 f"Uploaded so far: {sorted(uploaded)}"
        )

    try:
        # Column-level validation for uploaded tables
        for tname, tdf in tables.items():
            req = REQUIRED_COLS.get(tname, set())
            missing_cols = req - set(tdf.columns)
            if missing_cols:
                raise ValueError(
                    f"Column mapping is incorrect for {tname}: "
                    f"missing {sorted(missing_cols)}"
                )

        result_df  = _run_pipeline(tables, username)
        processed  = process_df(result_df)

        USER_DFS[username] = processed
        USER_DFS["Unknown"] = processed

        log_audit(username, "TRANSFORM", f"P2I event log built: {len(processed):,} rows")

        # ── Persist session to disk ───────────────────────────────────────────
        ts       = datetime.now().strftime("%Y%m%d_%H%M%S")
        user_dir = os.path.join(UPLOAD_DIR, username)
        os.makedirs(user_dir, exist_ok=True)
        save_path = os.path.join(user_dir, f"{ts}_transform_build.json")
        processed.to_json(save_path, orient="records", date_format="iso")
        _log(f"Session saved → {save_path}")

        return {
            "status": "ok",
            "rows":   len(processed),
            "orders": int(processed["Case ID"].nunique()) if "Case ID" in processed.columns else 0,
        }

    except HTTPException:
        raise
    except Exception as e:
        _log(f"Pipeline failed:\n{traceback.format_exc()}")
        msg = str(e)
        if "Column mapping is incorrect" not in msg:
            msg = f"Pipeline error: {msg}"
        raise HTTPException(400, f"Build failed: {msg}")