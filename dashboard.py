import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import streamlit.components.v1 as components
from datetime import datetime

st.set_page_config(
    page_title="All Stock Reco Dashboard",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("""
<style>
header[data-testid="stHeader"] {
    background: transparent !important;
}

.block-container {
    padding-top: 0.5rem !important;
}
            
/* ===== SIDEBAR TOGGLE BUTTON FIX ===== */

/* Target the toggle button */
button[kind="header"] {
    background-color: #2563eb !important;   /* blue background */
    color: #ffffff !important;
    border-radius: 8px !important;
    padding: 6px 10px !important;
}

/* Hover effect */
button[aria-label="Toggle sidebar"] {
    background-color: #2563eb !important;
    color: #ffffff !important;
    border-radius: 8px !important;
}
</style>
""", unsafe_allow_html=True)

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

SHEET_ID = "1D-TtswH2GU0dlsYwfoeJoQsDuBFV4K3I6H2wfakkV6Q"

# ----------------------------
# Custom styling
# ----------------------------
st.markdown("""
<style>
    .stApp {
        background: linear-gradient(180deg, #f4f8fc 0%, #eef5fb 100%);
        color: #16324f;
    }

    section[data-testid="stSidebar"] {
        background: #ffffff;
        border-right: 1px solid #d8e5f2;
    }

    .block-container {
        padding-top: 1.2rem;
        padding-bottom: 2rem;
        max-width: 96%;
    }

    h1, h2, h3 {
        color: #16324f !important;
        font-weight: 700 !important;
    }

    .kpi-card {
        background: #ffffff;
        border: 1px solid #d8e5f2;
        border-radius: 18px;
        padding: 18px 20px;
        box-shadow: 0 6px 18px rgba(44, 82, 130, 0.08);
    }

    .kpi-label {
        font-size: 13px;
        color: #5f7185;
        margin-bottom: 8px;
    }

    .kpi-value {
        font-size: 32px;
        font-weight: 700;
        color: #16324f;
    }

    .section-card {
        background: #ffffff;
        border: 1px solid #d8e5f2;
        border-radius: 18px;
        padding: 18px;
        box-shadow: 0 6px 18px rgba(44, 82, 130, 0.08);
        margin-bottom: 16px;
    }
            
    /* ===== DARK TABLE TEXT FIX ===== */

    /* Make text white for all tables */
    [data-testid="stDataFrame"] * {
        color: #ffffff !important;
    }

    /* Keep header white */
    [data-testid="stDataFrame"] thead th {
        color: #ffffff !important;
    }

    /* Optional: slightly softer white for readability */
    [data-testid="stDataFrame"] tbody td {
        color: #f9fafb !important;
    }

    .hero-box {
        background: linear-gradient(135deg, #ffffff 0%, #edf5ff 100%);
        border: 1px solid #d8e5f2;
        border-radius: 20px;
        padding: 22px 24px;
        box-shadow: 0 8px 24px rgba(44, 82, 130, 0.08);
        margin-bottom: 18px;
    }

    .hero-title {
        font-size: 20px;
        font-weight: 700;
        color: #16324f;
        margin-bottom: 4px;
    }

    .hero-sub {
        color: #5f7185;
        font-size: 13px;
    }

    .credit-box {
        margin-top: 20px;
        padding: 14px 16px;
        background: #ffffff;
        border: 1px solid #d8e5f2;
        border-radius: 14px;
        color: #4f6479;
        font-size: 13px;
        text-align: center;
        box-shadow: 0 4px 14px rgba(44, 82, 130, 0.06);
    }

    div[data-testid="stDataFrame"] {
        border: 1px solid #d8e5f2;
        border-radius: 14px;
        overflow: hidden;
        background: white;
    }

    label, .stMarkdown, .stTextInput label, .stSelectbox label, .stMultiSelect label, .stCheckbox label {
        color: #16324f !important;
        font-weight: 600 !important;
    }

    .small-note {
        color: #5f7185;
        font-size: 13px;
    }

    [data-testid="stSidebar"] * {
        color: #16324f !important;
    }
            
    /* ===== FIX TABLE TEXT VISIBILITY ===== */

    /* ===== METRIC TEXT FIX ===== */

    /* Metric labels */
    [data-testid="stMetricLabel"] {
        color: #16324f !important;
    }

    /* Metric values (main numbers) */
    [data-testid="stMetricValue"] {
        color: #000000 !important;
        font-weight: 700 !important;
    }

    /* Metric delta (if any) */
    [data-testid="stMetricDelta"] {
        color: #000000 !important;
    }
            
    /* Header styling (dark header like premium dashboards) */
    [data-testid="stDataFrame"] thead {
        background-color: #111827 !important;
    }
    [data-testid="stDataFrame"] thead th {
        color: #ffffff !important;
        font-weight: 600;
    }

    /* Row background cleanup (remove pink wash issue) */
    [data-testid="stDataFrame"] tbody tr {
        background-color: #ffffff !important;
    }
    [data-testid="stDataFrame"] tbody tr:nth-child(even) {
        background-color: #f8fafc !important;
    }

    /* Improve readability */
    [data-testid="stDataFrame"] tbody td {
        font-weight: 500;
    }
    /* ===== TAB TEXT COLOR FIX ===== */
    button[role="tab"] {
        color: #000000 !important;
        font-weight: 600 !important;
    }

    /* Optional: active tab text also black */
    button[role="tab"][aria-selected="true"] {
        color: #000000 !important;
    }
            
    /* ===== TAB BUTTON FULL STYLING ===== */

    /* All tabs */
    button[role="tab"] {
        background-color: #ffffff !important;   /* default background */
        color: #16324f !important;              /* text color */
        border-radius: 8px !important;
        padding: 6px 14px !important;
        margin-right: 6px !important;
        border: 1px solid #d8e5f2 !important;
        font-weight: 600 !important;
        transition: all 0.2s ease-in-out;
    }

    /* Hover effect */
    button[role="tab"]:hover {
        background-color: #e8f1fb !important;
        color: #16324f !important;
    }

    /* Active tab (selected one) */
    button[role="tab"][aria-selected="true"] {
        background-color: #2563eb !important;   /* BLUE active */
        color: #ffffff !important;
        border: none !important;
    }
            
    

    /* Remove extra top spacing after hiding */
    .block-container {
        padding-top: 0.5rem !important;
    }
</style>
""", unsafe_allow_html=True)

# ----------------------------
# Google Sheets connection
# ----------------------------
@st.cache_resource
def get_gsheet_client():
    creds = Credentials.from_service_account_info(
        st.secrets["gcp_service_account"],
        scopes=SCOPES
    )
    return gspread.authorize(creds)

@st.cache_data(ttl=120)
def get_sheet_df(sheet_name):
    gc = get_gsheet_client()
    sh = gc.open_by_key(SHEET_ID)
    ws = sh.worksheet(sheet_name)
    data = ws.get_all_records()
    df = pd.DataFrame(data)
    if not df.empty:
        df.columns = [str(c).strip() for c in df.columns]
    return df

def to_num(series):
    return pd.to_numeric(series, errors="coerce").fillna(0)

def append_decision_row(row_values):
    gc = get_gsheet_client()
    sh = gc.open_by_key(SHEET_ID)
    ws = sh.worksheet("Decision_Log")
    ws.append_row(row_values, value_input_option="USER_ENTERED")

# ----------------------------
# Load data
# ----------------------------
reco_df = get_sheet_df("Reco_Output_Template")
high_value_df = get_sheet_df("High_Value_Mismatch")
negative_df = get_sheet_df("Negative_Mismatch")
ops_df = get_sheet_df("Ops_Action")
matched_df = get_sheet_df("Matched")
decision_log_df = get_sheet_df("Decision_Log")

# ----------------------------
# Merge decision log into reco
# ----------------------------
if not decision_log_df.empty:

    decision_log_df["SKU"] = decision_log_df["SKU"].astype(str)
    reco_df["SKU"] = reco_df["SKU"].astype(str)

    # keep latest decision per SKU
    decision_log_df = decision_log_df.sort_values("Decision Date", ascending=False)
    decision_log_latest = decision_log_df.drop_duplicates(subset=["SKU"])

    reco_df = reco_df.merge(
        decision_log_latest[[
            "SKU",
            "Decision Taken",
            "Decision By",
            "Decision Date",
            "Remarks"
        ]],
        on="SKU",
        how="left"
    )

    # Add final status
    reco_df["Final Status"] = reco_df["Decision Taken"].apply(
        lambda x: "Action Taken" if pd.notna(x) else "Pending"
    )

    today = pd.Timestamp.now()

    if "Decision Date" in reco_df.columns:
        reco_df["Decision Date"] = pd.to_datetime(reco_df["Decision Date"], errors="coerce")

    reco_df["Pending Age (Days)"] = reco_df["Decision Date"].apply(
        lambda x: (today - x).days if pd.notna(x) else None
    )

else:
    reco_df["Decision Taken"] = None
    reco_df["Decision By"] = None
    reco_df["Decision Date"] = None
    reco_df["Remarks"] = None
    reco_df["Final Status"] = "Pending"

# ----------------------------
# Prepare main reco data
# ----------------------------
if reco_df.empty:
    st.error("Reco_Output_Template is empty. First run reco_build.py")
    st.stop()

num_cols = ["Purchased Qty", "Store Qty", "WH Qty", "Sold Qty", "Difference"]
for col in num_cols:
    if col in reco_df.columns:
        reco_df[col] = to_num(reco_df[col])

reco_df["Abs Difference"] = reco_df["Difference"].abs()
# Priority tagging
def get_priority(diff):
    diff = abs(diff)
    if diff > 5000:
        return "High"
    elif diff > 1000:
        return "Medium"
    else:
        return "Low"

reco_df["Priority"] = reco_df["Difference"].apply(get_priority)

# ----------------------------
# Sidebar
# ----------------------------
st.sidebar.markdown("## Filters")

vendor_list = sorted([x for x in reco_df["Vendor Name"].dropna().astype(str).unique() if x.strip()]) if "Vendor Name" in reco_df.columns else []
category_list = sorted([x for x in reco_df["Category"].dropna().astype(str).unique() if x.strip()]) if "Category" in reco_df.columns else []
status_list = sorted([x for x in reco_df["Reco Status"].dropna().astype(str).unique() if x.strip()]) if "Reco Status" in reco_df.columns else []

selected_vendor = st.sidebar.multiselect("Vendor Name", vendor_list, placeholder="Select vendor")
selected_category = st.sidebar.multiselect("Category", category_list, placeholder="Select category")
selected_status = st.sidebar.multiselect("Reco Status", status_list, placeholder="Select status")
search_text = st.sidebar.text_input("Search SKU / Product", placeholder="Type SKU or product")
show_mismatch_only = st.sidebar.checkbox("Show mismatches only", value=False)

decision_filter = st.sidebar.selectbox(
    "Decision Status",
    ["All", "Pending", "Action Taken"]
)

priority_filter = st.sidebar.multiselect(
    "Priority",
    ["High", "Medium", "Low"]
)


if st.sidebar.button("Refresh Data"):
    st.cache_data.clear()
    st.rerun()

filtered_df = reco_df.copy()

if selected_vendor:
    filtered_df = filtered_df[filtered_df["Vendor Name"].isin(selected_vendor)]

if selected_category:
    filtered_df = filtered_df[filtered_df["Category"].isin(selected_category)]

if selected_status:
    filtered_df = filtered_df[filtered_df["Reco Status"].isin(selected_status)]

if search_text:
    q = search_text.strip().lower()
    filtered_df = filtered_df[
        filtered_df["SKU"].astype(str).str.lower().str.contains(q, na=False) |
        filtered_df["Product Name"].astype(str).str.lower().str.contains(q, na=False)
    ]

if show_mismatch_only:
    filtered_df = filtered_df[filtered_df["Difference"] != 0]


# ✅ ADD THIS BELOW mismatch filter
if decision_filter != "All":
    filtered_df = filtered_df[filtered_df["Final Status"] == decision_filter]

if priority_filter:
    filtered_df = filtered_df[filtered_df["Priority"].isin(priority_filter)]

# ----------------------------
# KPIs
# ----------------------------
total_skus = len(filtered_df)
matched_skus = int((filtered_df["Reco Status"] == "Matched").sum()) if "Reco Status" in filtered_df.columns else 0
mismatch_skus = total_skus - matched_skus
net_diff_qty = float(filtered_df["Difference"].sum()) if "Difference" in filtered_df.columns else 0
abs_diff_qty = float(filtered_df["Abs Difference"].sum()) if "Abs Difference" in filtered_df.columns else 0
high_priority_count = len(filtered_df[filtered_df["Priority"] == "High"]) if "Priority" in filtered_df.columns else 0


st.markdown("""
<div class="hero-box">
    <div class="hero-title">All Stock Reconciliation Dashboard</div>
    <div class="hero-sub">Live connected with Google Sheets • Reconciliation + action review + decision logging</div>
</div>
""", unsafe_allow_html=True)

k1, k2, k3, k4, k5, k6, k7, k8 = st.columns(8)

with k1:
    st.markdown(f'<div class="kpi-card"><div class="kpi-label">Total SKUs</div><div class="kpi-value">{total_skus:,}</div></div>', unsafe_allow_html=True)
with k2:
    st.markdown(f'<div class="kpi-card"><div class="kpi-label">Matched SKUs</div><div class="kpi-value">{matched_skus:,}</div></div>', unsafe_allow_html=True)
with k3:
    st.markdown(f'<div class="kpi-card"><div class="kpi-label">Mismatched SKUs</div><div class="kpi-value">{mismatch_skus:,}</div></div>', unsafe_allow_html=True)
with k4:
    st.markdown(f'<div class="kpi-card"><div class="kpi-label">Net Difference Qty</div><div class="kpi-value">{net_diff_qty:,.0f}</div></div>', unsafe_allow_html=True)
with k5:
    st.markdown(f'<div class="kpi-card"><div class="kpi-label">Absolute Difference Qty</div><div class="kpi-value">{abs_diff_qty:,.0f}</div></div>', unsafe_allow_html=True)
with k6:
    st.markdown(f'<div class="kpi-card"><div class="kpi-label">High Priority</div><div class="kpi-value">{high_priority_count:,}</div></div>', unsafe_allow_html=True)

action_taken_count = 0
pending_count = 0

if "Final Status" in filtered_df.columns:
    status_series = filtered_df["Final Status"].fillna("").astype(str).str.strip().str.lower()
    
    
    action_taken_count = (status_series.isin(["action taken", "resolved", "done"])).sum()
    pending_count = (status_series.isin(["pending", "open"])).sum()

with k7:
    st.markdown(f'<div class="kpi-card"><div class="kpi-label">Action Taken</div><div class="kpi-value">{action_taken_count:,}</div></div>', unsafe_allow_html=True)

with k8:
    st.markdown(f'<div class="kpi-card"><div class="kpi-label">Pending</div><div class="kpi-value">{pending_count:,}</div></div>', unsafe_allow_html=True)

st.markdown("")

# ----------------------------
# Top mismatch table
# ----------------------------
st.markdown('<div class="section-card">', unsafe_allow_html=True)
st.subheader("Top 15 Mismatch SKUs")
top_mismatch = filtered_df.sort_values("Abs Difference", ascending=False).head(15)
show_cols = [
    c for c in [
        "SKU", "Product Name", "Vendor Name", "Category",
        "Purchased Qty", "Store Qty", "WH Qty", "Sold Qty",
        "Difference", "Priority", "Reco Status", "Suggested Action", "Owner",
        "Decision Taken", "Decision By", "Decision Date","Pending Age (Days)", "Final Status"
    ] if c in top_mismatch.columns
]
st.dataframe(top_mismatch[show_cols], use_container_width=True, height=420)
st.markdown('</div>', unsafe_allow_html=True)

# ----------------------------
# Action breakdown
# ----------------------------
st.markdown('<div class="section-card">', unsafe_allow_html=True)
st.subheader("Suggested Action Breakdown")
if "Suggested Action" in filtered_df.columns and not filtered_df.empty:
    action_chart = filtered_df["Suggested Action"].value_counts().reset_index()
    action_chart.columns = ["Suggested Action", "Count"]
    st.dataframe(action_chart, use_container_width=True, height=220)
else:
    st.info("No action data available")
st.markdown('</div>', unsafe_allow_html=True)
st.markdown('<div class="section-card">', unsafe_allow_html=True)
st.subheader("Ops Performance (Decisions by User)")


if not decision_log_df.empty and "Decision By" in decision_log_df.columns:
    perf = decision_log_df["Decision By"].value_counts().reset_index()
    perf.columns = ["User", "Decisions Taken"]
    st.dataframe(perf, use_container_width=True, height=220)
else:
    st.info("No decision data available")

st.markdown('</div>', unsafe_allow_html=True)


st.markdown('<div class="section-card">', unsafe_allow_html=True)
st.subheader("Closure Summary")

if "Final Status" in filtered_df.columns and not filtered_df.empty:
    closure_df = filtered_df["Final Status"].value_counts().reset_index()
    closure_df.columns = ["Final Status", "Count"]
    st.dataframe(closure_df, use_container_width=True, height=160)
else:
    st.info("No closure data available")

st.markdown('</div>', unsafe_allow_html=True)

# ----------------------------
# Tabs
# ----------------------------
tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs([
    "Full Reco",
    "High Value Mismatch",
    "Negative Mismatch",
    "Ops Action",
    "Matched",
    "Decision Center",
    "Decision Log"
])

with tab1:
    st.subheader("Full Reconciliation View")

    def highlight_priority(row):
        if row["Priority"] == "High":
            return ["background-color: #ffe5e5; color: #000000"] * len(row)
        elif row["Priority"] == "Medium":
            return ["background-color: #fff7e0; color: #000000"] * len(row)
        else:
            return ["color: #000000"] * len(row)

    full_reco_display_cols = [
        c for c in [
            "SKU", "Product Name", "Vendor Name", "Category", "Sub-Category",
            "Purchased Qty", "Store Qty", "WH Qty", "Sold Qty",
            "Difference", "Priority", "Reco Status", "Suggested Action",
            "Owner", "Decision Taken", "Decision By", "Decision Date","Pending Age (Days)", "Final Status"
        ] if c in filtered_df.columns
    ]

    styled_df = (
        filtered_df[full_reco_display_cols]
        .style
        .apply(highlight_priority, axis=1)
        .set_properties(**{
            "color": "#000000"
        })
    )
    st.dataframe(styled_df, use_container_width=True, height=500)

with tab2:
    st.subheader("High Value Mismatch")

    styled_high_value = high_value_df.style.set_properties(**{
        "color": "#ffffff",              # WHITE TEXT
        "background-color": "#0f172a"    # DARK BACKGROUND
    })

    st.dataframe(styled_high_value, use_container_width=True, height=500)

with tab3:
    st.subheader("Negative Mismatch")
    st.dataframe(negative_df, use_container_width=True, height=500)

with tab4:
    st.subheader("Ops Action")
    st.dataframe(ops_df, use_container_width=True, height=500)

with tab5:
    st.subheader("Matched")
    st.dataframe(matched_df, use_container_width=True, height=500)

with tab6:
    st.subheader("Decision Center")
    st.markdown('<div class="small-note">Take action here. It will write directly to the Decision_Log sheet in Google Sheets.</div>', unsafe_allow_html=True)

    decision_source_df = filtered_df.copy()

    decision_cols = [
        "SKU", "Product Name", "Vendor Name",
        "Purchased Qty", "Store Qty", "WH Qty", "Sold Qty",
        "Difference", "Reco Status", "Suggested Action"
    ]

    decision_source_df = decision_source_df[[c for c in decision_cols if c in decision_source_df.columns]].drop_duplicates()

    sku_options = decision_source_df["SKU"].astype(str).tolist()

    if sku_options:
        selected_sku = st.selectbox("Select SKU", sku_options)
        selected_row = decision_source_df[decision_source_df["SKU"].astype(str) == str(selected_sku)].iloc[0]

        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Summary box
        st.markdown("### Stock Summary for Decision")
        s1, s2, s3, s4, s5 = st.columns(5)
        s1.metric("Purchased Qty", int(selected_row["Purchased Qty"]))
        s2.metric("Store Qty", int(selected_row["Store Qty"]))
        s3.metric("WH Qty", int(selected_row["WH Qty"]))
        s4.metric("Sold Qty", int(selected_row["Sold Qty"]))
        s5.metric("Difference", int(selected_row["Difference"]))

        with st.expander("View detailed SKU snapshot", expanded=True):
            d1, d2 = st.columns(2)
            with d1:
                st.text_input("Product Name", value=str(selected_row["Product Name"]), disabled=True)
                st.text_input("Vendor Name", value=str(selected_row["Vendor Name"]), disabled=True)
                st.text_input("Current Reco Status", value=str(selected_row["Reco Status"]), disabled=True)
            with d2:
                st.text_input("SKU", value=str(selected_row["SKU"]), disabled=True)
                st.text_input("Current Suggested Action", value=str(selected_row["Suggested Action"]), disabled=True)
                st.text_input("Snapshot Time", value=current_time, disabled=True)

        # Ready message
        whatsapp_message = f"""Stock reconciliation check required

    Time: {current_time}
    Product Name: {selected_row['Product Name']}
    SKU: {selected_row['SKU']}
    Vendor Name: {selected_row['Vendor Name']}
    Purchased Qty: {int(selected_row['Purchased Qty'])}
    Store Qty: {int(selected_row['Store Qty'])}
    WH Qty: {int(selected_row['WH Qty'])}
    Sold Qty: {int(selected_row['Sold Qty'])}
    Difference: {int(selected_row['Difference'])}

    Kindly reconfirm this SKU from your end and share the correct update/status."""

        st.markdown("### WhatsApp Message Preview")
        st.text_area("Ready-to-send message", value=whatsapp_message, height=220)

        # Copy button
        copy_html = f"""
        <textarea id="copyText" style="position:absolute; left:-9999px;">{whatsapp_message}</textarea>
        <button onclick="
            const text = document.getElementById('copyText').value;
            navigator.clipboard.writeText(text).then(() => {{
                const btn = document.getElementById('copyBtn');
                btn.innerText = 'Copied!';
                setTimeout(() => btn.innerText = 'Copy Message', 1500);
            }});
        " id="copyBtn"
        style="
            background:#3b82f6;
            color:white;
            border:none;
            padding:10px 16px;
            border-radius:8px;
            cursor:pointer;
            font-weight:600;
        ">
            Copy Message
        </button>
        """
        components.html(copy_html, height=60)

        st.markdown("---")

        col1, col2 = st.columns(2)
        with col1:
            st.text_input("Decision By", key="decision_by_input", placeholder="Enter your name")
        with col2:
            decision_taken = st.selectbox(
                "Decision Taken",
                [
                    "Close",
                    "Ignore",
                    "Check Store",
                    "Check WH",
                    "Check Purchase",
                    "Check Sales",
                    "Write-off",
                    "Escalate"
                ]
            )

        remarks = st.text_area("Remarks", placeholder="Enter decision note / comments")

        if st.button("Save Decision"):
            decision_by = st.session_state.get("decision_by_input", "").strip()
            if not decision_by:
                st.warning("Please enter Decision By before saving.")
            else:
                decision_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                append_decision_row([
                    str(selected_row["SKU"]),
                    str(selected_row["Product Name"]),
                    str(selected_row["Reco Status"]),
                    str(selected_row["Suggested Action"]),
                    str(decision_taken),
                    str(decision_by),
                    str(decision_date),
                    str(remarks)
                ])
                st.cache_data.clear()
                st.success("Decision saved to Google Sheets successfully.")
                st.rerun()
    else:
        st.info("No SKU available for decision entry.")

with tab7:
    st.subheader("Decision Log")
    if not decision_log_df.empty:
        st.dataframe(decision_log_df, use_container_width=True, height=500)
    else:
        st.info("No decisions saved yet.")

# ----------------------------
# Download
# ----------------------------
st.markdown('<div class="section-card">', unsafe_allow_html=True)
st.subheader("Download Current Filtered Data")
csv_data = filtered_df.to_csv(index=False).encode("utf-8")
st.download_button(
    label="Download filtered reconciliation CSV",
    data=csv_data,
    file_name="filtered_reconciliation.csv",
    mime="text/csv"
)
st.markdown('</div>', unsafe_allow_html=True)

# ----------------------------
# Credit
# ----------------------------
st.markdown("""
<div class="credit-box">
Dashboard Developed By Rohit_Chougule @ Goodgudi Retail Pvt Ltd Co.
</div>
""", unsafe_allow_html=True)
