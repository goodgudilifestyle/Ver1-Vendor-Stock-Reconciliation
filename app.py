
import streamlit as st
import pandas as pd
import numpy as np
import sqlite3
import io
import os
import re
from datetime import datetime, date

st.set_page_config(page_title="Vendor Movement Dashboard (SSD-based)", layout="wide")

DB_PATH = "vendor_match.db"
UPLOAD_DIR = "uploads"

# ---------- Professional UI styling ----------
st.markdown("""
<style>
[data-testid="stAppViewContainer"]{
  background: radial-gradient(1200px 800px at 15% 10%, rgba(99,102,241,0.14), transparent 60%),
              radial-gradient(900px 650px at 85% 20%, rgba(34,197,94,0.10), transparent 55%),
              linear-gradient(180deg, #0b1220 0%, #0a1020 100%);
}
h1, h2, h3 { letter-spacing: -0.02em; }
[data-testid="stDataFrame"]{
  border: 1px solid rgba(255,255,255,0.10);
  border-radius: 14px;
  overflow: hidden;
}
[data-baseweb="select"] > div{
  background: rgba(255,255,255,0.05) !important;
  border-color: rgba(255,255,255,0.12) !important;
}
.stTextInput input, .stNumberInput input{
  background: rgba(255,255,255,0.05) !important;
}
</style>
""", unsafe_allow_html=True)

# ---------- Canonical columns (extras ignored) ----------
CANON = {
    "VendorOrderReport": ["Order Date", "PO Number", "Product Name", "Product SKU", "Vendor Name", "Delivered Qty"],
    "AllWhStocksReport": ["Product Name", "Product SKU", "Available Qty"],
    "AllStoreStocksReport": ["Product Name", "Product SKU", "Available Qty"],
    "SalesReport": ["Sales Date", "Product Name", "SKU", "Vendor Name", "Quantity"],
}

# Common header variations
ALIASES = {
    "Order Date": ["Order Date", "OrderDate", "Order_Date", "Order date"],
    "PO Number": ["PO Number", "P.O. Number", "PONumber", "PO_No", "PO No.", "PO No"],
    "Product Name": ["Product Name", "Product", "Item Name", "ProductName", "Item"],
    "Product SKU": ["Product SKU", "ProductSKU", "Item SKU", "ItemSKU", "Product Code", "SKU"],
    "Vendor Name": ["Vendor Name", "Vendor", "Supplier", "Supplier Name", "Party Name"],
    "Delivered Qty": ["Delivered Qty", "Delivered Quantity", "DeliveredQty", "GRN Qty", "Received Qty", "Qty Delivered"],
    "Available Qty": ["Available Qty", "Available Quantity", "Qty Available", "On Hand", "Onhand", "Stock Qty", "Closing Qty"],
    "Sales Date": ["Sales Date", "Sale Date", "SalesDate", "Bill Date", "Invoice Date", "Date"],
    "Quantity": ["Quantity", "Qty", "Sold Qty", "Sales Qty", "Units"],
    "SKU": ["SKU", "Product SKU", "ProductSKU", "Item SKU", "ItemSKU", "Product Code"],
}

WRITE_OFF_REASONS = [
    "Defective / Damaged",
    "Missing / Shrinkage",
    "Store Adjustment",
    "Transfer not captured",
    "Vendor short supply",
    "Data issue",
    "Other",
]

def norm(s: str) -> str:
    s = str(s).strip().lower()
    s = re.sub(r"\s+", " ", s)
    s = s.replace("_", " ").replace("-", " ")
    s = re.sub(r"[^a-z0-9 ]", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def map_headers(df: pd.DataFrame, label: str, required: list[str]) -> pd.DataFrame:
    cols = list(df.columns)
    norm_to_real = {norm(c): c for c in cols}
    rename = {}
    missing = []

    for canon in required:
        found_real = None
        for cand in ALIASES.get(canon, [canon]):
            key = norm(cand)
            if key in norm_to_real:
                found_real = norm_to_real[key]
                break

        if found_real is None:
            canon_key = norm(canon)
            for k, real in norm_to_real.items():
                if canon_key == k or canon_key in k or k in canon_key:
                    found_real = real
                    break

        if found_real is None:
            missing.append(canon)
        else:
            rename[found_real] = canon

    if missing:
        raise ValueError(
            f"{label}: Couldn't find required columns {missing}. "
            f"Please check the header row. First 30 columns: {cols[:30]}"
        )

    return df.rename(columns=rename)

def parse_dt(series):
    return pd.to_datetime(series, errors="coerce", dayfirst=True)

def ym(series_dt):
    return series_dt.dt.strftime("%Y-%m")

def read_any(file) -> pd.DataFrame:
    name = file.name.lower()
    if name.endswith(".xlsx") or name.endswith(".xls"):
        return pd.read_excel(file)
    b = file.getvalue()
    for enc in ("utf-8", "utf-8-sig", "latin1", "cp1252"):
        try:
            return pd.read_csv(io.BytesIO(b), encoding=enc)
        except Exception:
            continue
    return pd.read_csv(io.BytesIO(b), encoding_errors="ignore")

def clean_vendor_orders(df: pd.DataFrame) -> pd.DataFrame:
    df = map_headers(df, "VendorOrderReport", CANON["VendorOrderReport"])
    df = df[CANON["VendorOrderReport"]].copy()
    od = parse_dt(df["Order Date"])
    df["_OrderDT"] = od
    df["_OrderMonth"] = ym(od)
    df["Order Date"] = od.dt.strftime("%d/%m/%Y")
    df["Delivered Qty"] = pd.to_numeric(df["Delivered Qty"], errors="coerce").fillna(0)
    df["Vendor Name"] = df["Vendor Name"].astype(str).str.strip()
    df["Product SKU"] = df["Product SKU"].astype(str).str.strip()
    df["Product Name"] = df["Product Name"].astype(str).str.strip()
    df["PO Number"] = df["PO Number"].astype(str).str.strip()
    return df

def clean_wh(df: pd.DataFrame) -> pd.DataFrame:
    df = map_headers(df, "AllWhStocksReport", CANON["AllWhStocksReport"])
    df = df[CANON["AllWhStocksReport"]].copy()
    df["Available Qty"] = pd.to_numeric(df["Available Qty"], errors="coerce").fillna(0)
    df["Product SKU"] = df["Product SKU"].astype(str).str.strip()
    df["Product Name"] = df["Product Name"].astype(str).str.strip()
    return df

def clean_store(df: pd.DataFrame) -> pd.DataFrame:
    df = map_headers(df, "AllStoreStocksReport", CANON["AllStoreStocksReport"])
    df = df[CANON["AllStoreStocksReport"]].copy()
    df["Available Qty"] = pd.to_numeric(df["Available Qty"], errors="coerce").fillna(0)
    df["Product SKU"] = df["Product SKU"].astype(str).str.strip()
    df["Product Name"] = df["Product Name"].astype(str).str.strip()
    return df

def clean_sales(df: pd.DataFrame) -> pd.DataFrame:
    df = map_headers(df, "SalesReport", CANON["SalesReport"])
    df = df[CANON["SalesReport"]].copy()
    sd = parse_dt(df["Sales Date"])
    df["_SalesDT"] = sd
    df["_SalesMonth"] = ym(sd)
    df["Sales Date"] = sd.dt.strftime("%d/%m/%Y")
    df["Quantity"] = pd.to_numeric(df["Quantity"], errors="coerce").fillna(0)
    df["Vendor Name"] = df["Vendor Name"].astype(str).str.strip()
    df["SKU"] = df["SKU"].astype(str).str.strip()
    df["Product Name"] = df["Product Name"].astype(str).str.strip()
    return df

# ---------- SSD date from filename ----------
def extract_date_from_filename(fname: str) -> date | None:
    m = re.search(r"(\d{2})-(\d{2})-(\d{2,4})", fname)
    if not m:
        return None
    dd, mm, yy = m.group(1), m.group(2), m.group(3)
    d = int(dd); mo = int(mm)
    if len(yy) == 2:
        y = 2000 + int(yy)
    else:
        y = int(yy)
    try:
        return date(y, mo, d)
    except Exception:
        return None

# ---------- Backend ----------
def init_db():
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()

    cur.execute("""CREATE TABLE IF NOT EXISTS runs(
        run_id TEXT PRIMARY KEY,
        created_at TEXT,
        notes TEXT,
        vendor_filter TEXT,
        product_filter TEXT,
        month_filter TEXT,
        stock_snapshot_date TEXT
    )""")

    cur.execute("""CREATE TABLE IF NOT EXISTS movement_results(
        run_id TEXT,
        Stock_Snapshot_Date TEXT,
        Selected_Month TEXT,
        Product_SKU TEXT,
        Product_Name TEXT,
        Vendor_Name TEXT,
        WH_Qty_SSD REAL,
        Store_Qty_SSD REAL,
        Stock_SSD REAL,
        Delivered_to_SSD REAL,
        Sales_to_SSD REAL,
        Opening_Implied REAL,
        Difference_AsOf REAL,
        Delivered_in_Month REAL,
        Sales_in_Month REAL,
        Net_Movement_Month REAL,
        MatchFlag TEXT
    )""")

    # SKU-level writeoffs
    cur.execute("""CREATE TABLE IF NOT EXISTS writeoffs(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        created_at TEXT,
        stock_snapshot_date TEXT,
        vendor_filter TEXT,
        month_filter TEXT,
        product_sku TEXT,
        writeoff_qty REAL,
        reason TEXT,
        notes TEXT
    )""")

    con.commit()
    con.close()

def save_run(summary_df, notes, vendor_filter, product_filter, month_filter, ssd: date):
    init_db()
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    con = sqlite3.connect(DB_PATH)

    pd.DataFrame([{
        "run_id": run_id,
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "notes": notes,
        "vendor_filter": vendor_filter,
        "product_filter": product_filter,
        "month_filter": month_filter,
        "stock_snapshot_date": ssd.strftime("%Y-%m-%d")
    }]).to_sql("runs", con, if_exists="append", index=False)

    tmp = summary_df.copy()
    tmp = tmp.rename(columns={"Match?": "MatchFlag"})
    tmp["run_id"] = run_id

    for col in ["Delivered_in_Month", "Sales_in_Month", "Net_Movement_Month"]:
        if col not in tmp.columns:
            tmp[col] = 0.0

    keep = [
        "run_id",
        "Stock_Snapshot_Date", "Selected_Month",
        "Product_SKU", "Product_Name", "Vendor_Name",
        "WH_Qty_SSD", "Store_Qty_SSD", "Stock_SSD",
        "Delivered_to_SSD", "Sales_to_SSD",
        "Opening_Implied", "Difference_AsOf",
        "Delivered_in_Month", "Sales_in_Month", "Net_Movement_Month",
        "MatchFlag"
    ]
    tmp = tmp[keep]
    tmp.to_sql("movement_results", con, if_exists="append", index=False)

    con.close()
    return run_id

def load_writeoffs(ssd: date, vendor_filter: str, month_filter: str) -> pd.DataFrame:
    init_db()
    con = sqlite3.connect(DB_PATH)
    try:
        q = """
        SELECT product_sku, writeoff_qty, reason, notes
        FROM writeoffs
        WHERE stock_snapshot_date = ?
          AND vendor_filter = ?
          AND month_filter = ?
        """
        df = pd.read_sql_query(q, con, params=(ssd.strftime("%Y-%m-%d"), vendor_filter, month_filter))
        return df
    finally:
        con.close()

def upsert_writeoff(ssd: date, vendor_filter: str, month_filter: str, sku: str, qty: float, reason: str, notes: str):
    init_db()
    con = sqlite3.connect(DB_PATH)
    try:
        cur = con.cursor()
        # Keep only one active writeoff per SKU per SSD+filters (replace old)
        cur.execute("""
            DELETE FROM writeoffs
            WHERE stock_snapshot_date=? AND vendor_filter=? AND month_filter=? AND product_sku=?
        """, (ssd.strftime("%Y-%m-%d"), vendor_filter, month_filter, sku))

        cur.execute("""
            INSERT INTO writeoffs(created_at, stock_snapshot_date, vendor_filter, month_filter, product_sku, writeoff_qty, reason, notes)
            VALUES(?,?,?,?,?,?,?,?)
        """, (
            datetime.now().isoformat(timespec="seconds"),
            ssd.strftime("%Y-%m-%d"),
            vendor_filter,
            month_filter,
            sku,
            float(qty),
            reason,
            notes
        ))
        con.commit()
    finally:
        con.close()

# ---------- Build summary (Interpretation A) ----------
def build_summary(vendor_orders, sales, wh, store, ssd: date,
                  vendor="(All)", product="(All)", month="(All)"):
    # Stock snapshot (as-of SSD)
    wh_agg = wh.groupby("Product SKU", dropna=False)["Available Qty"].sum().rename("WH_Qty_SSD").reset_index()
    st_agg = store.groupby("Product SKU", dropna=False)["Available Qty"].sum().rename("Store_Qty_SSD").reset_index()
    stock = wh_agg.merge(st_agg, on="Product SKU", how="outer")
    stock["WH_Qty_SSD"] = pd.to_numeric(stock["WH_Qty_SSD"], errors="coerce").fillna(0)
    stock["Store_Qty_SSD"] = pd.to_numeric(stock["Store_Qty_SSD"], errors="coerce").fillna(0)
    stock["Stock_SSD"] = stock["WH_Qty_SSD"] + stock["Store_Qty_SSD"]

    # Deliveries up to SSD
    vo = vendor_orders.copy()
    vo = vo[vo["_OrderDT"].notna()]
    vo = vo[vo["_OrderDT"].dt.date <= ssd]
    if vendor != "(All)":
        vo = vo[vo["Vendor Name"] == vendor]
    if product != "(All)":
        vo = vo[vo["Product Name"] == product]

    delivered_to_ssd = vo.groupby("Product SKU", dropna=False).agg(
        Product_Name=("Product Name", "first"),
        Vendor_Name=("Vendor Name", "first"),
        Delivered_to_SSD=("Delivered Qty", "sum"),
        Last_Order_Date=("Order Date", "max"),
        PO_Count=("PO Number", "nunique"),
    ).reset_index()

    # Sales up to SSD
    sr = sales.copy()
    sr = sr[sr["_SalesDT"].notna()]
    sr = sr[sr["_SalesDT"].dt.date <= ssd]
    if vendor != "(All)":
        sr = sr[sr["Vendor Name"] == vendor]
    if product != "(All)":
        sr = sr[sr["Product Name"] == product]

    sales_to_ssd = sr.groupby("SKU", dropna=False)["Quantity"].sum().rename("Sales_to_SSD").reset_index().rename(columns={"SKU": "Product SKU"})

    # Month movements only
    delivered_in_month = pd.DataFrame(columns=["Product SKU", "Delivered_in_Month"])
    sales_in_month = pd.DataFrame(columns=["Product SKU", "Sales_in_Month"])
    if month != "(All)":
        vo_m = vo[vo["_OrderMonth"] == month]
        delivered_in_month = vo_m.groupby("Product SKU", dropna=False)["Delivered Qty"].sum().rename("Delivered_in_Month").reset_index()

        sr_m = sr[sr["_SalesMonth"] == month]
        sales_in_month = sr_m.groupby("SKU", dropna=False)["Quantity"].sum().rename("Sales_in_Month").reset_index().rename(columns={"SKU": "Product SKU"})

    out = delivered_to_ssd.merge(stock, on="Product SKU", how="left") \
                          .merge(sales_to_ssd, on="Product SKU", how="left") \
                          .merge(delivered_in_month, on="Product SKU", how="left") \
                          .merge(sales_in_month, on="Product SKU", how="left")

    for c in ["Stock_SSD", "WH_Qty_SSD", "Store_Qty_SSD", "Sales_to_SSD", "Delivered_in_Month", "Sales_in_Month", "Delivered_to_SSD"]:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce").fillna(0)

    out["Opening_Implied"] = out["Stock_SSD"] + out["Sales_to_SSD"] - out["Delivered_to_SSD"]
    out["Difference_AsOf"] = out["Delivered_to_SSD"] - (out["Stock_SSD"] + out["Sales_to_SSD"])
    out["Net_Movement_Month"] = 0.0 if month == "(All)" else (out["Delivered_in_Month"] - out["Sales_in_Month"])

    # This flag indicates whether your data window behaves like "opening=0"
    out["Match?"] = np.where(np.isclose(out["Opening_Implied"], 0), "✅", "❌")

    out["Stock_Snapshot_Date"] = ssd.strftime("%d/%m/%Y")
    out["Selected_Month"] = "" if month == "(All)" else month

    out = out.rename(columns={"Product SKU": "Product_SKU"})
    cols = [
        "Stock_Snapshot_Date", "Selected_Month",
        "Product_SKU", "Product_Name", "Vendor_Name",
        "WH_Qty_SSD", "Store_Qty_SSD", "Stock_SSD",
        "Delivered_to_SSD", "Sales_to_SSD",
        "Opening_Implied", "Difference_AsOf",
        "Delivered_in_Month", "Sales_in_Month", "Net_Movement_Month",
        "Match?", "Last_Order_Date", "PO_Count"
    ]
    cols = [c for c in cols if c in out.columns]
    out = out[cols]
    out = out.sort_values(by="Opening_Implied", key=lambda x: x.abs(), ascending=False).reset_index(drop=True)
    return out

def reset_filters():
    st.session_state["selected_vendor"] = "(All)"
    st.session_state["selected_product"] = "(All)"
    st.session_state["selected_month"] = "(All)"
    st.session_state["notes"] = ""

# ---------------- UI ----------------
st.title("Vendor Movement Dashboard (SSD-based)")

tab1, tab2 = st.tabs(["Upload files", "Dashboard"])

with tab1:
    st.subheader("Upload all 4 raw files (CSV or XLSX) — extra columns are OK ✅")
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        f_vendor = st.file_uploader("VendorOrderReport", type=["csv", "xlsx", "xls"], key="f_vendor")
    with c2:
        f_wh = st.file_uploader("AllWhStocksReport", type=["csv", "xlsx", "xls"], key="f_wh")
    with c3:
        f_store = st.file_uploader("AllStoreStocksReport", type=["csv", "xlsx", "xls"], key="f_store")
    with c4:
        f_sales = st.file_uploader("SalesReport", type=["csv", "xlsx", "xls"], key="f_sales")

    if st.button("Process (clean columns)"):
        try:
            if not (f_vendor and f_wh and f_store and f_sales):
                st.error("Please upload all 4 files.")
            else:
                st.session_state["vendor_orders"] = clean_vendor_orders(read_any(f_vendor))
                st.session_state["wh"] = clean_wh(read_any(f_wh))
                st.session_state["store"] = clean_store(read_any(f_store))
                st.session_state["sales"] = clean_sales(read_any(f_sales))

                wh_ssd = extract_date_from_filename(f_wh.name)
                st_ssd = extract_date_from_filename(f_store.name)

                if wh_ssd is None or st_ssd is None:
                    st.warning("Could not detect date from WH/Store filename. "
                               "Expected something like: AllWhStocksReport_29-01-26 07_30_03.csv")
                    st.session_state["ssd"] = None
                else:
                    if wh_ssd != st_ssd:
                        st.warning(f"WH file date ({wh_ssd.strftime('%d/%m/%Y')}) and Store file date "
                                   f"({st_ssd.strftime('%d/%m/%Y')}) do not match. "
                                   f"Dashboard will use WH date as SSD.")
                    st.session_state["ssd"] = wh_ssd

                st.success("Cleaned successfully ✅ (extra columns were ignored)")
        except Exception as e:
            st.exception(e)

    if "vendor_orders" in st.session_state:
        st.markdown("#### Cleaned preview (only the required columns)")
        a, b = st.columns(2)
        with a:
            st.write("VendorOrderReport (clean)")
            st.dataframe(st.session_state["vendor_orders"][CANON["VendorOrderReport"]].head(15), use_container_width=True)
        with b:
            st.write("SalesReport (clean)")
            st.dataframe(st.session_state["sales"][CANON["SalesReport"]].head(15), use_container_width=True)

        c, d = st.columns(2)
        with c:
            st.write("AllWhStocksReport (clean)")
            st.dataframe(st.session_state["wh"].head(15), use_container_width=True)
        with d:
            st.write("AllStoreStocksReport (clean)")
            st.dataframe(st.session_state["store"].head(15), use_container_width=True)

        if st.session_state.get("ssd"):
            st.info(f"✅ Stock Snapshot Date (SSD) detected from filename: **{st.session_state['ssd'].strftime('%d/%m/%Y')}**")

with tab2:
    if "vendor_orders" not in st.session_state:
        st.info("Upload + process files in Upload files tab first.")
    elif st.session_state.get("ssd") is None:
        st.warning("SSD (stock snapshot date) not detected from WH/Store file names. "
                   "Please rename files like: AllWhStocksReport_29-01-26 07_30_03.csv")
    else:
        vendor_orders = st.session_state["vendor_orders"]
        wh = st.session_state["wh"]
        store = st.session_state["store"]
        sales = st.session_state["sales"]
        ssd = st.session_state["ssd"]

        vendors = sorted(set(vendor_orders["Vendor Name"].dropna().unique()).union(set(sales["Vendor Name"].dropna().unique())))
        products = sorted(set(pd.concat([
            vendor_orders["Product Name"], wh["Product Name"], store["Product Name"], sales["Product Name"]
        ]).dropna().unique()))

        all_months = sorted(set(vendor_orders["_OrderMonth"].dropna().unique()).union(set(sales["_SalesMonth"].dropna().unique())))
        all_months = [m for m in all_months if isinstance(m, str) and re.match(r"^\d{4}-\d{2}$", m)]
        ssd_month = f"{ssd.year:04d}-{ssd.month:02d}"
        months = [m for m in all_months if m <= ssd_month]

        # Filters row
        f1, f2, f3, f4, f5 = st.columns([2.0, 1.4, 2.2, 2.6, 1.0])
        with f1:
            selected_vendor = st.selectbox("Vendor Name", options=["(All)"] + vendors, key="selected_vendor")
        with f2:
            selected_month = st.selectbox("Month (movement view)", options=["(All)"] + months, key="selected_month")
        with f3:
            selected_product = st.selectbox("Product Name", options=["(All)"] + products, key="selected_product")
        with f4:
            notes = st.text_input("Notes for saving (optional)", key="notes")
        with f5:
            st.write("")
            st.write("")
            st.button("Reset", on_click=reset_filters, use_container_width=True)

        st.caption(
            f"SSD-based logic: WH+Store is a stock snapshot as of **{ssd.strftime('%d/%m/%Y')}** (from filename). "
            f"Month filter shows movement within the month, not month-end closing."
        )

        summary_df = build_summary(
            vendor_orders, sales, wh, store, ssd,
            vendor=selected_vendor,
            product=selected_product,
            month=selected_month
        )

        # --- Merge writeoffs for this SSD + current filters (vendor + month)
        wdf = load_writeoffs(ssd, selected_vendor, selected_month)
        if not wdf.empty:
            wdf = wdf.rename(columns={"product_sku":"Product_SKU", "writeoff_qty":"Writeoff_Qty", "reason":"Writeoff_Reason", "notes":"Writeoff_Notes"})
        else:
            wdf = pd.DataFrame(columns=["Product_SKU","Writeoff_Qty","Writeoff_Reason","Writeoff_Notes"])

        summary_df = summary_df.merge(wdf, on="Product_SKU", how="left")
        summary_df["Writeoff_Qty"] = pd.to_numeric(summary_df.get("Writeoff_Qty"), errors="coerce").fillna(0)
        summary_df["Adjusted_Diff_AsOf"] = summary_df["Difference_AsOf"] + summary_df["Writeoff_Qty"]
        summary_df["Adjusted_OK"] = np.where(np.isclose(summary_df["Adjusted_Diff_AsOf"], 0), "✅", "❌")

        # Metrics
        sku_n = int(summary_df.shape[0])
        matched_n = int((summary_df["Match?"] == "✅").sum()) if sku_n else 0
        mismatched_n = int((summary_df["Match?"] == "❌").sum()) if sku_n else 0

        total_delivered = float(summary_df["Delivered_to_SSD"].sum()) if sku_n else 0.0
        total_sales = float(summary_df["Sales_to_SSD"].sum()) if sku_n else 0.0
        total_wh = float(summary_df["WH_Qty_SSD"].sum()) if sku_n else 0.0
        total_store = float(summary_df["Store_Qty_SSD"].sum()) if sku_n else 0.0
        total_wh_store_sales = (total_wh + total_store + total_sales) if sku_n else 0.0

        net_opening = float(summary_df["Opening_Implied"].sum()) if sku_n else 0.0
        abs_opening = float(summary_df["Opening_Implied"].abs().sum()) if sku_n else 0.0

        # 9 cards (fits better than long strings)
        m1, m2, m3, m4, m5, m6, m7, m8, m9 = st.columns(9)
        m1.metric("SKUs", sku_n)
        m2.metric("Matched", matched_n)
        m3.metric("Unmatched", mismatched_n)
        m4.metric("Delivered", round(total_delivered, 2))
        m5.metric("Sales", round(total_sales, 2))
        m6.metric("WH Qty", round(total_wh, 2))
        m7.metric("Store Qty", round(total_store, 2))
        m8.metric("WH+Store+Sales", round(total_wh_store_sales, 2))
        m9.metric("Opening Implied", f"{net_opening:,.0f} / {abs_opening:,.0f}")

        st.subheader("Summary table")
        st.dataframe(summary_df, use_container_width=True, height=560)

        # Downloads + optional save
        c1, c2, c3, c4 = st.columns([1, 1, 1, 1])
        with c1:
            if st.button("Save this run to backend (optional)"):
                run_id = save_run(
                    summary_df=summary_df.drop(columns=["Writeoff_Qty","Writeoff_Reason","Writeoff_Notes","Adjusted_Diff_AsOf","Adjusted_OK"], errors="ignore"),
                    notes=notes,
                    vendor_filter=selected_vendor,
                    product_filter=selected_product,
                    month_filter=selected_month,
                    ssd=ssd
                )
                st.success(f"Saved ✅ Run ID: {run_id} (DB: {DB_PATH})")

        with c2:
            st.download_button(
                "Download SUMMARY (CSV)",
                data=summary_df.to_csv(index=False).encode("utf-8"),
                file_name="summary_table.csv",
                mime="text/csv"
            )

        with c3:
            matched_df = summary_df[summary_df["Adjusted_OK"] == "✅"]
            st.download_button(
                "Download MATCHED (CSV)",
                data=matched_df.to_csv(index=False).encode("utf-8"),
                file_name="summary_matched.csv",
                mime="text/csv"
            )

        with c4:
            mism_df = summary_df[summary_df["Adjusted_OK"] == "❌"]
            st.download_button(
                "Download UNMATCHED (CSV)",
                data=mism_df.to_csv(index=False).encode("utf-8"),
                file_name="summary_unmatched.csv",
                mime="text/csv"
            )

        # ---------- SKU Write-off section ----------
        st.markdown("### SKU Write-off (Defect / Damage / etc.)")

        if summary_df.empty:
            st.info("No SKUs to write-off for the current filters.")
        else:
            # Only show SKUs currently visible under filters (so user doesn't write-off wrong scope)
            sku_options = summary_df["Product_SKU"].dropna().astype(str).unique().tolist()
            sku_options = sorted(sku_options)

            w1, w2, w3, w4 = st.columns([2.2, 1.2, 1.8, 2.8])
            with w1:
                wo_sku = st.selectbox("Select SKU", sku_options, key="wo_sku")
            row = summary_df[summary_df["Product_SKU"].astype(str) == str(wo_sku)].head(1)

            current_writeoff = float(row["Writeoff_Qty"].iloc[0]) if "Writeoff_Qty" in row.columns and not row.empty else 0.0
            current_reason = row["Writeoff_Reason"].iloc[0] if "Writeoff_Reason" in row.columns and not row.empty else None
            current_notes = row["Writeoff_Notes"].iloc[0] if "Writeoff_Notes" in row.columns and not row.empty else ""

            pname = row["Product_Name"].iloc[0] if "Product_Name" in row.columns and not row.empty else ""
            vname = row["Vendor_Name"].iloc[0] if "Vendor_Name" in row.columns and not row.empty else ""
            diff_asof = float(row["Difference_AsOf"].iloc[0]) if "Difference_AsOf" in row.columns and not row.empty else 0.0

            with w2:
                wo_qty = st.number_input("Write-off Qty", min_value=0.0, value=float(current_writeoff), step=1.0, key="wo_qty")
            with w3:
                # default selection
                if current_reason in WRITE_OFF_REASONS:
                    idx = WRITE_OFF_REASONS.index(current_reason)
                else:
                    idx = 0
                wo_reason = st.selectbox("Reason", WRITE_OFF_REASONS, index=idx, key="wo_reason")
            with w4:
                wo_notes = st.text_input("Notes (optional)", value=str(current_notes) if current_notes is not None else "", key="wo_notes")

            st.caption(f"SKU: **{wo_sku}** | Product: **{pname}** | Vendor: **{vname}** | Current Difference_AsOf: **{diff_asof:.2f}**")
            if diff_asof > 0:
                st.info("Note: Difference_AsOf is positive (delivered > stock+sales). Write-off is usually used for missing/defect cases, but you can still record it if needed.")

            if st.button("Save Write-off"):
                if wo_qty > 0 and not wo_reason:
                    st.error("Please select a reason for write-off.")
                else:
                    upsert_writeoff(
                        ssd=ssd,
                        vendor_filter=selected_vendor,
                        month_filter=selected_month,
                        sku=str(wo_sku),
                        qty=float(wo_qty),
                        reason=str(wo_reason),
                        notes=str(wo_notes or "")
                    )
                    st.success("Write-off saved ✅ Refreshing table...")
                    st.rerun()
