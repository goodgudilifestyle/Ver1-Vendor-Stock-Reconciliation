import pandas as pd
import gspread
import google.auth

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

SHEET_ID = "1D-TtswH2GU0dlsYwfoeJoQsDuBFV4K3I6H2wfakkV6Q"

# ----------------------------
# Connect to Google Sheet
# ----------------------------
creds, _ = google.auth.default(scopes=SCOPES)
gc = gspread.authorize(creds)
sh = gc.open_by_key(SHEET_ID)

def get_df(sheet_name):
    ws = sh.worksheet(sheet_name)
    data = ws.get_all_records()
    return pd.DataFrame(data)

def to_num(series):
    return pd.to_numeric(series, errors="coerce").fillna(0)


def make_json_safe(value):
    if pd.isna(value):
        return ""
    if hasattr(value, "item"):
        try:
            return value.item()
        except:
            pass
    return value

# ----------------------------
# Read sheets
# ----------------------------
vendor_df = get_df("Vendor_Purchase_Raw")
store_df = get_df("Store_Stock_Raw")
wh_df = get_df("WH_Stock_Raw")
sales_df = get_df("Sales_Raw")
action_df = get_df("Action_Master")

# ----------------------------
# Clean headers
# ----------------------------
for df in [vendor_df, store_df, wh_df, sales_df, action_df]:
    df.columns = [str(c).strip() for c in df.columns]

# ----------------------------
# Numeric cleanup
# ----------------------------
vendor_df["Delivered Qty"] = to_num(vendor_df["Delivered Qty"])
vendor_df["Unit Price"] = to_num(vendor_df["Unit Price"])

store_df["Available Qty"] = to_num(store_df["Available Qty"])
wh_df["Available Qty"] = to_num(wh_df["Available Qty"])

sales_df["Quantity"] = to_num(sales_df["Quantity"])

action_df["Min Difference"] = to_num(action_df["Min Difference"])
action_df["Max Difference"] = to_num(action_df["Max Difference"])

# ----------------------------
# Aggregate purchase
# ----------------------------
purchase_agg = (
    vendor_df.groupby("Product SKU", dropna=False)
    .agg({
        "Product Name": "first",
        "Vendor Name": "first",
        "Category": "first",
        "Sub-Category": "first",
        "Delivered Qty": "sum",
        "Unit Price": "mean"
    })
    .reset_index()
    .rename(columns={
        "Product SKU": "SKU",
        "Delivered Qty": "Purchased Qty",
        "Unit Price": "Avg Unit Price"
    })
)

# ----------------------------
# Aggregate store stock
# ----------------------------
store_agg = (
    store_df.groupby("Product SKU", dropna=False)
    .agg({
        "Available Qty": "sum"
    })
    .reset_index()
    .rename(columns={
        "Product SKU": "SKU",
        "Available Qty": "Store Qty"
    })
)

# ----------------------------
# Aggregate warehouse stock
# ----------------------------
wh_agg = (
    wh_df.groupby("Product SKU", dropna=False)
    .agg({
        "Available Qty": "sum"
    })
    .reset_index()
    .rename(columns={
        "Product SKU": "SKU",
        "Available Qty": "WH Qty"
    })
)

# ----------------------------
# Aggregate sales
# ----------------------------
sales_agg = (
    sales_df.groupby("SKU", dropna=False)
    .agg({
        "Product Name": "first",
        "Vendor Name": "first",
        "Category Name": "first",
        "Sub-Category Name": "first",
        "Quantity": "sum"
    })
    .reset_index()
    .rename(columns={
        "Quantity": "Sold Qty"
    })
)

# ----------------------------
# Merge all
# ----------------------------
reco = purchase_agg.merge(store_agg, on="SKU", how="outer")
reco = reco.merge(wh_agg, on="SKU", how="outer")
reco = reco.merge(sales_agg, on="SKU", how="outer", suffixes=("", "_sales"))

# fallback text fields
if "Product Name_sales" in reco.columns:
    reco["Product Name"] = reco["Product Name"].replace("", pd.NA).fillna(reco["Product Name_sales"])
if "Vendor Name_sales" in reco.columns:
    reco["Vendor Name"] = reco["Vendor Name"].replace("", pd.NA).fillna(reco["Vendor Name_sales"])
if "Category Name" in reco.columns:
    reco["Category"] = reco["Category"].replace("", pd.NA).fillna(reco["Category Name"])
if "Sub-Category Name" in reco.columns:
    reco["Sub-Category"] = reco["Sub-Category"].replace("", pd.NA).fillna(reco["Sub-Category Name"])

required_cols = [
    "SKU", "Product Name", "Vendor Name", "Category", "Sub-Category",
    "Purchased Qty", "Store Qty", "WH Qty", "Sold Qty", "Avg Unit Price"
]

for col in required_cols:
    if col not in reco.columns:
        reco[col] = 0 if ("Qty" in col or "Price" in col) else ""

reco = reco[required_cols]

for col in ["Purchased Qty", "Store Qty", "WH Qty", "Sold Qty", "Avg Unit Price"]:
    reco[col] = to_num(reco[col])

# ----------------------------
# Core reconciliation
# ----------------------------
reco["Difference"] = reco["Purchased Qty"] - (
    reco["Store Qty"] + reco["WH Qty"] + reco["Sold Qty"]
)

reco["Difference Value"] = reco["Difference"] * reco["Avg Unit Price"]
reco["Abs Difference"] = reco["Difference"].abs()

# ----------------------------
# Better status logic
# ----------------------------
def classify_status(diff):
    if diff == 0:
        return "Matched"
    elif -2 <= diff <= 2:
        return "Minor Mismatch"
    elif diff < -2:
        return "Negative Mismatch"
    else:
        return "Major Mismatch"

reco["Reco Status"] = reco["Difference"].apply(classify_status)

# ----------------------------
# Smarter business action logic
# ----------------------------
def smart_action(row):
    purchased = row["Purchased Qty"]
    store = row["Store Qty"]
    wh = row["WH Qty"]
    sold = row["Sold Qty"]
    diff = row["Difference"]
    diff_value = abs(row["Difference Value"])

    # 1. Perfect match
    if diff == 0:
        return pd.Series([
            "Reconciled - Close",
            "System",
            "No action required"
        ])

    # 2. Small variance
    if -2 <= diff <= 2:
        return pd.Series([
            "Ignore Small Variance",
            "Inventory Team",
            "Very small variance, monitor only"
        ])

    # 3. No purchase but stock/sales exist
    if purchased == 0 and (store > 0 or wh > 0 or sold > 0):
        return pd.Series([
            "Check Missing Purchase / Opening Stock",
            "Accounts + Ops",
            "Purchase not found, but stock/sales exist"
        ])

    # 4. Purchased exists but nothing in stock/sales
    if purchased > 0 and store == 0 and wh == 0 and sold == 0:
        return pd.Series([
            "Check Missing Stock Posting",
            "Ops Team",
            "Purchased qty exists but not visible in stock/sales"
        ])

    # 5. Positive mismatch = purchased more than accounted
    if diff > 2:
        if diff_value >= 10000:
            return pd.Series([
                "Escalate / Write-off Review",
                "Ops Manager",
                "High value positive mismatch, urgent review needed"
            ])
        return pd.Series([
            "Check Store / WH / Sales",
            "Ops Team",
            "Purchased qty higher than stock + sales"
        ])

    # 6. Negative mismatch = more stock/sales than purchase
    if diff < -2:
        if diff_value >= 10000:
            return pd.Series([
                "Escalate / Check Purchase History",
                "Accounts + Ops",
                "High value negative mismatch, verify old purchases/opening stock"
            ])
        return pd.Series([
            "Check Purchase / SKU Mapping",
            "Accounts + Ops",
            "Stock/sales exceed purchase qty"
        ])

    # 7. Fallback
    return pd.Series([
        "Review Manually",
        "Ops Team",
        "No rule matched"
    ])

reco[["Suggested Action", "Owner", "Remarks"]] = reco.apply(smart_action, axis=1)

# ----------------------------
# Sort by biggest mismatch first
# ----------------------------
reco = reco.sort_values(
    by=["Abs Difference", "Difference"],
    ascending=[False, False]
).reset_index(drop=True)

# ----------------------------
# Final columns
# ----------------------------
final_cols = [
    "SKU", "Product Name", "Vendor Name", "Category", "Sub-Category",
    "Purchased Qty", "Store Qty", "WH Qty", "Sold Qty", "Difference",
    "Reco Status", "Suggested Action", "Owner", "Remarks"
]

reco_output = reco[final_cols].fillna("")

# ----------------------------
# Summary metrics
# ----------------------------
total_skus = len(reco)
matched_skus = (reco["Reco Status"] == "Matched").sum()
mismatch_skus = total_skus - matched_skus
total_diff_qty = reco["Difference"].sum()
total_abs_diff_qty = reco["Abs Difference"].sum()
total_diff_value = reco["Difference Value"].sum()

high_value_mismatch_count = int((reco["Difference Value"].abs() >= 10000).sum())

summary_rows = [
    ["Metric", "Value"],
    ["Total SKUs", int(total_skus)],
    ["Matched SKUs", int(matched_skus)],
    ["Mismatched SKUs", int(mismatch_skus)],
    ["High Value Mismatch SKUs", high_value_mismatch_count],
    ["Net Difference Qty", float(total_diff_qty)],
    ["Absolute Difference Qty", float(total_abs_diff_qty)],
    ["Net Difference Value", float(round(total_diff_value, 2))],
]

# ----------------------------
# Write back to sheet
# ----------------------------
ws_out = sh.worksheet("Reco_Output_Template")

# Clear old output
ws_out.batch_clear(["A2:N100000", "P1:Q20"])

# Write summary at P1
safe_summary_rows = [
    [make_json_safe(v) for v in row]
    for row in summary_rows
]
ws_out.update(range_name="P1:Q8", values=safe_summary_rows)

# Write main output from A2
rows = [
    [make_json_safe(v) for v in row]
    for row in reco_output.values.tolist()
]
if rows:
    ws_out.update(range_name=f"A2:N{len(rows)+1}", values=rows)

print("✅ Step 4 completed: enhanced reconciliation written to Reco_Output_Template")
print("\nSummary:")
print(pd.DataFrame(summary_rows[1:], columns=summary_rows[0]))
print("\nTop 10 mismatches:")
print(reco_output.head(10))



# ----------------------------
# Create filtered views
# ----------------------------

def write_sheet(sheet_name, df):
    try:
        ws = sh.worksheet(sheet_name)
    except:
        ws = sh.add_worksheet(title=sheet_name, rows=1000, cols=20)

    ws.clear()

    data = [df.columns.tolist()] + [
        [make_json_safe(v) for v in row]
        for row in df.values.tolist()
    ]

    ws.update(range_name=f"A1", values=data)

# ----------------------------
# 1. High Value Mismatch
# ----------------------------
high_value_df = reco_output[reco["Difference Value"].abs() >= 10000]
write_sheet("High_Value_Mismatch", high_value_df)

# ----------------------------
# 2. Negative Mismatch
# ----------------------------
negative_df = reco_output[reco["Difference"] < -2]
write_sheet("Negative_Mismatch", negative_df)

# ----------------------------
# 3. Ops Action Required
# ----------------------------
ops_df = reco_output[
    reco_output["Suggested Action"].str.contains("Check Store|WH|Sales", na=False)
]
write_sheet("Ops_Action", ops_df)

# ----------------------------
# 4. Matched
# ----------------------------
matched_df = reco_output[reco_output["Reco Status"] == "Matched"]
write_sheet("Matched", matched_df)

print("✅ Step 6 completed: Filtered sheets created")