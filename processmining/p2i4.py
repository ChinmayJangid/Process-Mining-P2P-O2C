"""
p2i4.py — P2I Analytics Engine (FastAPI Router)
Serves all dashboard endpoints: KPIs, process map, charts, filters.

Prefix: /p2i
"""

import io
import os
import json
import math
import pandas as pd
from datetime import datetime
from fastapi import APIRouter, HTTPException, UploadFile, File, Form, Query

router = APIRouter(prefix="/p2i", tags=["Plan-to-Inventory"])

# ── In-memory data store (per user) ──────────────────────────────────────────
USER_DFS: dict = {}

COL_CASE = "Case ID"
COL_ACT  = "Activity"
COL_TS   = "Timestamp"

UPLOAD_DIR = os.path.join("user_data", "p2i")
os.makedirs(UPLOAD_DIR, exist_ok=True)


# ── Logging ───────────────────────────────────────────────────────────────────
def log_audit(username: str, action: str, msg: str):
    stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[P2I-AUDIT] {stamp} | {username} | {action} | {msg}")
    audit_file = "p2i_audit_logs.json"
    logs: list = []
    if os.path.exists(audit_file):
        try:
            with open(audit_file) as f:
                logs = json.load(f)
        except Exception:
            logs = []
    logs.append({"timestamp": datetime.now().isoformat(),
                 "username": username, "action": action, "message": msg})
    with open(audit_file, "w") as f:
        json.dump(logs[-500:], f, indent=2)


# ── DataFrame normaliser ──────────────────────────────────────────────────────
def process_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    # Canonicalise known column aliases
    rename = {}
    for col in df.columns:
        cl = col.lower()
        if cl in {"case id", "case_id", "aufnr"}:
            rename[col] = COL_CASE
        elif cl in {"activity", "event_name", "event"}:
            rename[col] = COL_ACT
        elif cl in {"timestamp", "date", "iedd", "budat", "ts"}:
            rename[col] = COL_TS
    df = df.rename(columns=rename)

    if COL_TS in df.columns:
        df[COL_TS] = pd.to_datetime(df[COL_TS], errors="coerce")
        df.sort_values([COL_CASE, COL_TS], inplace=True)

        if "Year" not in df.columns:
            df["Year"]  = df[COL_TS].dt.year.astype("Int64").astype(str).replace("<NA>", "")
        if "Month" not in df.columns:
            df["Month"] = df[COL_TS].dt.month_name()

    return df


def _safe_int(v) -> int:
    try:
        return int(v) if v is not None and not math.isnan(float(v)) else 0
    except Exception:
        return 0


def _safe_float(v, decimals=2) -> float:
    try:
        return round(float(v), decimals) if v is not None and not math.isnan(float(v)) else 0.0
    except Exception:
        return 0.0


# ── Data access ───────────────────────────────────────────────────────────────
def get_user_df(username: str) -> pd.DataFrame:
    return USER_DFS.get(username, pd.DataFrame())


def apply_filters(df: pd.DataFrame, order_id=None, mtart=None,
                  plant=None, year=None, month=None, deviation=None) -> pd.DataFrame:
    if df.empty:
        return df
    d = df.copy()
    if order_id and order_id != "ALL":
        d = d[d[COL_CASE].astype(str) == str(order_id)]
    if mtart and mtart != "ALL" and "MTART" in d.columns:
        d = d[d["MTART"].astype(str) == str(mtart)]
    if plant and plant != "ALL" and "WERKS" in d.columns:
        d = d[d["WERKS"].astype(str) == str(plant)]
    if year and year != "ALL" and "Year" in d.columns:
        d = d[d["Year"].astype(str) == str(year)]
    if month and month != "ALL" and "Month" in d.columns:
        d = d[d["Month"].astype(str) == str(month)]
    if deviation and deviation != "ALL":
        if deviation == "Over-Production" and "Deviation_Over_Produced" in d.columns:
            d = d[d["Deviation_Over_Produced"] == True]
        elif deviation == "Material Delay" and "Deviation_Material_Delay_Days" in d.columns:
            d = d[d["Deviation_Material_Delay_Days"] > 0]
        else:
            act_name = f"{deviation} (Deviation)"
            cases_with_dev = d[d[COL_ACT] == act_name][COL_CASE].unique()
            d = d[d[COL_CASE].isin(cases_with_dev)]
    return d


# ═════════════════════════════════════════════════════════════════════════════
# ROUTES
# ═════════════════════════════════════════════════════════════════════════════

@router.get("/")
def p2i_root():
    df = get_user_df("Unknown")
    return {
        "status": "P2I module active",
        "rows": len(df),
        "data_loaded": not df.empty,
    }


# ── Filters / Slicer options ──────────────────────────────────────────────────
@router.get("/filters")
def get_filters(username: str = Query("Unknown")):
    df = get_user_df(username)
    if df.empty:
        return {"order_ids": ["ALL"], "mtarts": ["ALL"],
                "plants": ["ALL"], "years": ["ALL"], "months": ["ALL"]}

    def _uniq(col) -> list:
        if col not in df.columns:
            return ["ALL"]
        vals = df[col].dropna().astype(str).unique().tolist()
        vals = sorted([v for v in vals if v not in {"", "nan", "NaT", "None"}])
        return ["ALL"] + vals

    months_raw = df["Month"].dropna().unique().tolist() if "Month" in df.columns else []
    month_order = ["January","February","March","April","May","June",
                   "July","August","September","October","November","December"]
    months_sorted = ["ALL"] + sorted(
        [m for m in months_raw if m in month_order],
        key=lambda m: month_order.index(m)
    )

    return {
        "order_ids": _uniq(COL_CASE),
        "mtarts":    _uniq("MTART"),
        "plants":    _uniq("WERKS"),
        "years":     _uniq("Year"),
        "months":    months_sorted,
    }


# ── Process Discovery Map (nodes + edges) ─────────────────────────────────────

# Canonical P2I happy-path order (Confirm Operation excluded from map)
P2I_HAPPY_PATH = [
    "Create Purchase Requisition",   # Bridge (optional)
    "Create Purchase Order",         # Bridge (optional)
    "Goods Receipt (Raw Material)",  # Bridge (optional)
    "Create Production Order",
    "Release Production Order",
    "Reserve Component",
    "Goods Issue to WIP",
    "Goods Receipt (Finished Good)",
    "Release from QA",
    "Technically Complete (TECO)",
]

# Deviation activities — shown as side nodes
P2I_DEVIATIONS = {
    "Reverse Goods Issue (Deviation)",
    "Reverse Goods Receipt (Deviation)",
    "Record Scrap (Deviation)",
    "Record Rework (Deviation)",
}

# Activities to exclude entirely from the process map
P2I_EXCLUDE = set()  # Confirm Operation filtered below

# Hierarchical node positions for Vertical (TB) layout
# Main path: centre column (x=400). Deviations: left (x=20) and right (x=780)
_TB_POSITIONS_MAIN = {
    "Create Purchase Requisition":    {"x": 400, "y":   0},
    "Create Purchase Order":          {"x": 400, "y": 160},
    "Goods Receipt (Raw Material)":   {"x": 400, "y": 320},
    "Create Production Order":        {"x": 400, "y": 480},
    "Release Production Order":       {"x": 400, "y": 640},
    "Reserve Component":              {"x": 400, "y": 800},
    "Goods Issue to WIP":             {"x": 400, "y": 960},
    "Goods Receipt (Finished Good)":  {"x": 400, "y":1120},
    "Release from QA":                {"x": 400, "y":1280},
    "Technically Complete (TECO)":    {"x": 400, "y":1440},
}
_TB_POSITIONS_DEV = {
    "Reverse Goods Issue (Deviation)":    {"x":  20, "y": 960},
    "Reverse Goods Receipt (Deviation)":  {"x": 780, "y":1120},
    "Record Scrap (Deviation)":           {"x":  20, "y":1120},
    "Record Rework (Deviation)":          {"x": 780, "y": 960},
}

# Horizontal (LR) positions
_LR_POSITIONS_MAIN = {
    "Create Purchase Requisition":    {"x":   0, "y": 240},
    "Create Purchase Order":          {"x": 220, "y": 240},
    "Goods Receipt (Raw Material)":   {"x": 440, "y": 240},
    "Create Production Order":        {"x": 660, "y": 240},
    "Release Production Order":       {"x": 880, "y": 240},
    "Reserve Component":              {"x":1100, "y": 240},
    "Goods Issue to WIP":             {"x":1320, "y": 240},
    "Goods Receipt (Finished Good)":  {"x":1540, "y": 240},
    "Release from QA":                {"x":1760, "y": 240},
    "Technically Complete (TECO)":    {"x":1980, "y": 240},
}
_LR_POSITIONS_DEV = {
    "Reverse Goods Issue (Deviation)":    {"x":1320, "y":   0},
    "Reverse Goods Receipt (Deviation)":  {"x":1540, "y":   0},
    "Record Scrap (Deviation)":           {"x":1320, "y": 480},
    "Record Rework (Deviation)":          {"x":1540, "y": 480},
}


@router.get("/nodes_edges")
def get_nodes_edges(
    username: str = Query("Unknown"),
    order_id: str = Query("ALL"),
    mtart:    str = Query("ALL"),
    plant:    str = Query("ALL"),
    year:     str = Query("ALL"),
    month:    str = Query("ALL"),
    deviation:str = Query("ALL"),
):
    df = apply_filters(get_user_df(username), order_id, mtart, plant, year, month, deviation)
    if df.empty or COL_ACT not in df.columns:
        return {"nodes": [], "edges": []}

    df = df.sort_values([COL_CASE, COL_TS])

    # ── Exclude "Confirm Operation" variants from the map ─────────────────────
    df = df[~df[COL_ACT].astype(str).str.startswith("Confirm Operation")]

    # Activity frequency (unique cases per activity)
    allowed = set(P2I_HAPPY_PATH) | P2I_DEVIATIONS
    act_freq = df[df[COL_ACT].isin(allowed)].groupby(COL_ACT)[COL_CASE].nunique().to_dict()

    # Transitions via shift(-1) within each case (unique cases per transition)
    df2 = df[df[COL_ACT].isin(allowed)].copy()
    
    # Sort by logical order to fix concurrent timestamps
    act_order = {act: i for i, act in enumerate(P2I_HAPPY_PATH)}
    df2["_order"] = df2[COL_ACT].map(lambda x: act_order.get(x, 999))
    df2 = df2.sort_values([COL_CASE, COL_TS, "_order"])
    
    df2["_next"] = df2.groupby(COL_CASE)[COL_ACT].shift(-1)
    trans = (
        df2.dropna(subset=["_next"])
        .query("_next in @allowed")
        .groupby([COL_ACT, "_next"])[COL_CASE]
        .nunique()
        .reset_index(name="freq")
    )

    nodes = []
    for act, freq in act_freq.items():
        is_deviation = act in P2I_DEVIATIONS
        is_bridge = act in {
            "Create Purchase Requisition",
            "Create Purchase Order",
            "Goods Receipt (Raw Material)",
        }
        if is_deviation:
            color = "#94a3b8"   # slate grey — deviations
        elif is_bridge:
            color = "#5C2D91"   # purple — P2P bridge events
        else:
            color = "#10b981"   # emerald green — core P2I

        # Determine position for both layouts
        pos_tb = (_TB_POSITIONS_MAIN.get(act) or _TB_POSITIONS_DEV.get(act) or {"x": 400, "y": 800})
        pos_lr = (_LR_POSITIONS_MAIN.get(act) or _LR_POSITIONS_DEV.get(act) or {"x": 900, "y": 240})

        nodes.append({
            "id":          act,
            "label":       act,
            "frequency":   int(freq),
            "color":       color,
            "is_main":     not is_deviation,
            "position_v":  pos_tb,
            "position_h":  pos_lr,
        })

    edges = [
        {
            "source": row[COL_ACT],
            "target": row["_next"],
            "value":  int(row["freq"]),
        }
        for _, row in trans.iterrows()
    ]

    return {"nodes": nodes, "edges": edges}


# ── KPIs ──────────────────────────────────────────────────────────────────────
@router.get("/kpis")
def get_kpis(
    username: str = Query("Unknown"),
    order_id: str = Query("ALL"),
    mtart:    str = Query("ALL"),
    plant:    str = Query("ALL"),
    year:     str = Query("ALL"),
    month:    str = Query("ALL"),
    deviation:str = Query("ALL"),
):
    df = apply_filters(get_user_df(username), order_id, mtart, plant, year, month, deviation)
    if df.empty:
        return {
            "total_cases": 0, "avg_lead_time": 0,
            "scrap_rate": 0, "rework_rate": 0,
            "teco_cases": 0, "over_prod_cases": 0, "material_delay_cases": 0,
        }

    total = df[COL_CASE].nunique()

    # Avg Production Lead Time (Order Created → FG Goods Receipt)
    created  = df[df[COL_ACT] == "Create Production Order"].groupby(COL_CASE)[COL_TS].min()
    received = df[df[COL_ACT] == "Goods Receipt (Finished Good)"].groupby(COL_CASE)[COL_TS].max()
    lt_df    = pd.DataFrame({"s": created, "e": received}).dropna()
    avg_lt   = 0.0
    if not lt_df.empty:
        avg_lt = (lt_df["e"] - lt_df["s"]).dt.total_seconds().mean() / 86400

    scrap_cases  = df[df[COL_ACT] == "Record Scrap (Deviation)"][COL_CASE].nunique()
    rework_cases = df[df[COL_ACT] == "Record Rework (Deviation)"][COL_CASE].nunique()
    teco_cases   = df[df[COL_ACT] == "Technically Complete (TECO)"][COL_CASE].nunique()

    over_prod = 0
    if "Deviation_Over_Produced" in df.columns:
        over_prod = df[df["Deviation_Over_Produced"] == True][COL_CASE].nunique()

    mat_delay = 0
    if "Deviation_Material_Delay_Days" in df.columns:
        mat_delay = df[df["Deviation_Material_Delay_Days"] > 0][COL_CASE].nunique()

    return {
        "total_cases":          _safe_int(total),
        "avg_lead_time":        _safe_float(avg_lt, 1),
        "scrap_rate":           _safe_float((scrap_cases / total) * 100, 1) if total > 0 else 0,
        "rework_rate":          _safe_float((rework_cases / total) * 100, 1) if total > 0 else 0,
        "teco_cases":           _safe_int(teco_cases),
        "over_prod_cases":      _safe_int(over_prod),
        "material_delay_cases": _safe_int(mat_delay),
    }


# ── Chart: Lead Time by Material Type ────────────────────────────────────────
@router.get("/charts/lead_time_mtart")
def chart_lead_time_mtart(
    username: str = Query("Unknown"),
    order_id: str = Query("ALL"),
    mtart:    str = Query("ALL"),
    plant:    str = Query("ALL"),
    year:     str = Query("ALL"),
    month:    str = Query("ALL"),
    deviation:str = Query("ALL"),
):
    df = apply_filters(get_user_df(username), order_id, mtart, plant, year, month, deviation)
    if df.empty or "MTART" not in df.columns:
        return []

    created  = df[df[COL_ACT] == "Create Production Order"].groupby(COL_CASE)[COL_TS].min()
    received = df[df[COL_ACT] == "Goods Receipt (Finished Good)"].groupby(COL_CASE)[COL_TS].max()
    m_type   = df.groupby(COL_CASE)["MTART"].first()

    lt_df = pd.DataFrame({"s": created, "e": received, "mtart": m_type}).dropna()
    if lt_df.empty:
        return []

    lt_df["days"] = (lt_df["e"] - lt_df["s"]).dt.total_seconds() / 86400
    result = (
        lt_df.groupby("mtart")["days"]
        .mean().round(2)
        .reset_index()
        .rename(columns={"mtart": "name", "days": "value"})
        .sort_values("value", ascending=False)
    )
    return result.to_dict(orient="records")


# ── Chart: Deviation Breakdown ────────────────────────────────────────────────
@router.get("/charts/deviations")
def chart_deviations(
    username: str = Query("Unknown"),
    order_id: str = Query("ALL"),
    mtart:    str = Query("ALL"),
    plant:    str = Query("ALL"),
    year:     str = Query("ALL"),
    month:    str = Query("ALL"),
    deviation:str = Query("ALL"),
):
    df = apply_filters(get_user_df(username), order_id, mtart, plant, year, month, deviation)
    if df.empty:
        return []

    DEV_ACTS = [
        "Reverse Goods Issue (Deviation)",
        "Reverse Goods Receipt (Deviation)",
        "Record Scrap (Deviation)",
        "Record Rework (Deviation)",
    ]
    counts = df[df[COL_ACT].isin(DEV_ACTS)][COL_ACT].value_counts().to_dict()

    total = df[COL_CASE].nunique()

    over_prod  = 0
    mat_delay  = 0
    if "Deviation_Over_Produced" in df.columns:
        over_prod = int(df[df["Deviation_Over_Produced"] == True][COL_CASE].nunique())
    if "Deviation_Material_Delay_Days" in df.columns:
        mat_delay = int(df[df["Deviation_Material_Delay_Days"] > 0][COL_CASE].nunique())

    result = []
    for act in DEV_ACTS:
        result.append({"name": act.replace(" (Deviation)", ""), "value": int(counts.get(act, 0))})

    result.append({"name": "Over-Production",   "value": over_prod})
    result.append({"name": "Material Delay",    "value": mat_delay})

    return [r for r in result if r["value"] > 0]


# ── Chart: Monthly Production Trend ──────────────────────────────────────────
@router.get("/charts/monthly")
def chart_monthly(
    username: str = Query("Unknown"),
    order_id: str = Query("ALL"),
    mtart:    str = Query("ALL"),
    plant:    str = Query("ALL"),
    year:     str = Query("ALL"),
    month:    str = Query("ALL"),
    deviation:str = Query("ALL"),
):
    df = apply_filters(get_user_df(username), order_id, mtart, plant, year, month, deviation)
    if df.empty or COL_TS not in df.columns:
        return []

    df2 = df.copy()
    df2["_ym"] = df2[COL_TS].dt.to_period("M").astype(str)
    result = (
        df2.groupby("_ym")[COL_CASE]
        .nunique()
        .reset_index()
        .rename(columns={"_ym": "month", COL_CASE: "count"})
        .sort_values("month")
    )
    return result.to_dict(orient="records")


# ── Chart: Orders by Plant ────────────────────────────────────────────────────
@router.get("/charts/plant")
def chart_plant(
    username: str = Query("Unknown"),
    order_id: str = Query("ALL"),
    mtart:    str = Query("ALL"),
    plant:    str = Query("ALL"),
    year:     str = Query("ALL"),
    month:    str = Query("ALL"),
    deviation:str = Query("ALL"),
):
    df = apply_filters(get_user_df(username), order_id, mtart, plant, year, month, deviation)
    if df.empty or "WERKS" not in df.columns:
        return []

    result = (
        df.groupby("WERKS")[COL_CASE]
        .nunique()
        .reset_index()
        .rename(columns={"WERKS": "name", COL_CASE: "value"})
        .sort_values("value", ascending=False)
        .head(20)
    )
    return result.to_dict(orient="records")


# ── Single Order Timeline ─────────────────────────────────────────────────────
@router.get("/order_timeline")
def get_order_timeline(username: str = Query("Unknown"), order_id: str = Query(None)):
    if not order_id or order_id == "ALL":
        return []
    df = get_user_df(username)
    if df.empty:
        return []
    d = df[df[COL_CASE].astype(str) == str(order_id)].copy()
    if d.empty:
        return []
    d = d.sort_values(COL_TS)
    res = []
    for _, row in d.iterrows():
        res.append({
            "activity":  str(row[COL_ACT]),
            "timestamp": row[COL_TS].isoformat() if hasattr(row[COL_TS], 'isoformat') else str(row[COL_TS]),
            "is_dev":    "(Deviation)" in str(row[COL_ACT]),
            "is_bridge": str(row[COL_ACT]) in {
                "Create Purchase Requisition", "Create Purchase Order", "Goods Receipt (Raw Material)"
            }
        })
    return res


# ── CSV Upload (pre-built event log) ─────────────────────────────────────────
@router.post("/upload")
async def upload_csv(
    file:     UploadFile = File(...),
    username: str        = Form("Unknown"),
    column_mapping: str  = Form("{}"),
):
    content = await file.read()
    try:
        try:
            df = pd.read_csv(io.BytesIO(content), low_memory=False, encoding='utf-8')
        except UnicodeDecodeError:
            df = pd.read_csv(io.BytesIO(content), low_memory=False, encoding='latin1')

        # Apply column mapping if provided
        try:
            mapping = json.loads(column_mapping) if column_mapping else {}
            if mapping:
                df = df.rename(columns={k: v for k, v in mapping.items() if k in df.columns})
        except Exception:
            pass

        processed = process_df(df)

        USER_DFS[username]  = processed
        USER_DFS["Unknown"] = processed

        log_audit(username, "UPLOAD", f"Pre-built CSV uploaded: {len(processed):,} rows")

        # Persist
        ts      = datetime.now().strftime("%Y%m%d_%H%M%S")
        u_dir   = os.path.join(UPLOAD_DIR, username)
        os.makedirs(u_dir, exist_ok=True)
        path    = os.path.join(u_dir, f"{ts}_upload.json")
        processed.to_json(path, orient="records", date_format="iso")

        return {"status": "ok", "rows": len(processed)}
    except Exception as e:
        raise HTTPException(400, f"Upload failed: {e}")


# ── File history ──────────────────────────────────────────────────────────────
@router.get("/my_files")
def get_my_files(username: str = Query("Unknown")):
    u_dir = os.path.join(UPLOAD_DIR, username)
    if not os.path.exists(u_dir):
        return []

    files = []
    for fname in os.listdir(u_dir):
        if not fname.endswith(".json"):
            continue
        path = os.path.join(u_dir, fname)
        files.append({
            "id":     fname,
            "name":   fname.replace("_", " ").replace(".json", ""),
            "ts":     os.path.getmtime(path),
            "source": "table_build" if "transform" in fname else "upload",
        })
    return sorted(files, key=lambda x: x["ts"], reverse=True)


# ── Load a persisted file ─────────────────────────────────────────────────────
@router.post("/load_file")
def load_file(data: dict):
    username = data.get("username", "Unknown")
    file_id  = data.get("file_id")
    if not file_id:
        raise HTTPException(400, "file_id is required")

    path = os.path.join(UPLOAD_DIR, username, file_id)
    if not os.path.exists(path):
        raise HTTPException(404, f"File '{file_id}' not found for user '{username}'")

    try:
        df = pd.read_json(path)
        df = process_df(df)
        USER_DFS[username]  = df
        USER_DFS["Unknown"] = df
        log_audit(username, "LOAD", f"Loaded: {file_id} — {len(df):,} rows")
        return {"status": "ok", "rows": len(df)}
    except Exception as e:
        raise HTTPException(500, f"Failed to load file: {e}")


# ── Frontend action logging ───────────────────────────────────────────────────
@router.post("/log")
def log_action(data: dict):
    log_audit(
        data.get("username", "Unknown"),
        data.get("action", "UI_ACTION"),
        data.get("details", ""),
    )
    return {"status": "ok"}