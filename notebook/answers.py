
# Imports
import pandas as pd
import numpy as np

# Paths
DATA = "../data"
POS_DIR = f"{DATA}/positions"
TRD_DIR = f"{DATA}/trades"

pd.set_option("display.float_format", lambda v: f"{v:,.4f}")

# STEP 1
# Load positions
fa_raw = pd.read_csv(f"{POS_DIR}/fund_admin_positions_2025-09-11.csv", parse_dates=["date"])
br_raw = pd.read_csv(f"{POS_DIR}/broker_positions_2025-09-11.csv", parse_dates=["date"])

# Converters to update broker/fund admin column names to LTA schema names
pos_col_converters = {
'date': 'date',
'ticker': 'security_description',
'unique_id': 'security_id',
'quantity': 'quantity',
'price': 'price',
'contract_multiplier': 'multiplier',
'average_cost': 'avg_cost'
}

# LTA only keeps these columns in our position schemas
keep_pos_columns = [
'date',
'security_description',
'security_id',
'quantity',
'price',
'multiplier',
'avg_cost',
'start_of_month_price',
]

br_norm = br_raw.rename(columns=pos_col_converters).copy()[keep_pos_columns].sort_values(by='security_id')
br_norm_answer = br_norm.copy()

def normalize_fa_pos(fa_pos_raw, lot_col, avg_cost_col):
    # 1. Copy raw data
    d = fa_pos_raw.copy()
    # 2. Sign the quantity
    d['lot_signed'] = d[lot_col] * d['lot_sign']
    # 3. Compute average cost for lots and total quantity for each ticker
    # Get quantity * price
    d['qty_times_price'] = d[avg_cost_col] * d['lot_signed'] 
    # Groupby and aggregate
    pos_group_cols = [
        'date',
        'security_description',
        'price',
        'contract_multiplier',
        'start_of_month_price',
        'unique_id',
        ]
    
    agg = d.groupby(pos_group_cols).agg(
        quantity=('lot_signed', 'sum'), total_qty_times_price=('qty_times_price', 'sum')
        )
    # Average cost equals quantity * price / total quantity
    agg['avg_cost'] = agg['total_qty_times_price'] / agg['quantity']
    # reset index
    agg = agg.reset_index()
    # name columns
    agg = agg.rename(columns=pos_col_converters)
    # only keep LTA schema columns
    agg = agg[keep_pos_columns]
    # return dataframe
    return agg

fa_norm = normalize_fa_pos(fa_raw, 'lot_qty', 'average_cost').sort_values(by='security_id')
fa_norm_answer = fa_norm.copy() 

# One-to-one join on unique_id
pos = (fa_norm.add_suffix("_fa")
       .merge(br_norm.add_suffix("_br"),
              left_on="security_id_fa", right_on="security_id_br",
              how="outer", validate="one_to_one"))

# TODO: Build break table
# 1. Keep only 1 security_id column
pos['security_id'] = ''
for _, r in pos.iterrows():
    assert r.security_id_fa == r.security_id_br
    r['security_id'] = r.security_id_fa
pos = pos.drop(columns=['security_id_fa', 'security_id_br'])

# Compute quantity breaks
pos['qty_diff'] = pos["quantity_br"].fillna(0) - pos["quantity_fa"].fillna(0)

# Compute average cost difference
pos["avg_cost_diff"] = pos["avg_cost_br"] - pos["avg_cost_fa"]

pos_answer = pos.copy()

# Output a clean dataframe, sort values by security_id
break_pos = pos[[
    "security_id",
    "security_description_fa","security_description_br",
    "quantity_fa","quantity_br","qty_diff",
    "avg_cost_fa","avg_cost_br","avg_cost_diff"
]].sort_values("security_id").reset_index(drop=True)

break_pos_answer = break_pos.copy()

# Step 2
# Load trades
fa_trd = pd.read_csv(f"{TRD_DIR}/fund_admin_trades_2025-09-12.csv", parse_dates=["trade_date","settle_date"])
br_trd = pd.read_csv(f"{TRD_DIR}/broker_trades_2025-09-12.csv", parse_dates=["trade_date","settle_date"])
# This time we have not normalized the data for you, 
# TODO: Normalize trades data
# Feel free to refer above to copy code
# The schema requirements are below

# Converters to update broker/fund admin column names to LTA schema names
trd_col_converters = {
    'date': 'date',
    'ticker': 'security_description',
    'unique_id': 'security_id',
    'quantity': 'quantity',
    'price': 'price',
    'contract_multiplier': 'multiplier',
}

# Final columns that should be in your trades dataframe
keep_trd_cols = [
    'security_description',
    'security_id',
    'trade_date',
    'settle_date',
    'price',
    'commissions',
    'quantity',
    'multiplier']

# TODO: normalize broker trade data to LTA Schema Requirements, sort values by security_id
# Name the normalized dataframe br_trd_norm
br_trd_norm = br_trd.rename(columns=trd_col_converters).copy()[keep_trd_cols].sort_values(by='security_id')

br_trd_norm_answer = br_trd_norm.copy()

# Hints available:
# 1. here are the columns you need to group by and aggregate over
d = fa_trd.copy()
d = d.rename(columns={'unique_id':'security_id'})
trd_grp_cols =     ['ticker', 
                    'security_id',
                    'trade_date',
                    'settle_date',
                    'contract_multiplier']


# Groupby and aggregate
# 1. Total quantity
# 2. Total commissions
# 3. Price (how will you agg this?)
d['qty_times_price'] = d['quantity'] * d['price']

agg = d.groupby(trd_grp_cols).agg(
        quantity=('quantity', 'sum'), 
        total_qty_times_price=('qty_times_price', 'sum'),
        commissions=('commissions', 'sum')
        )

# Average price equals quantity * price / total quantity
agg['price'] = agg['total_qty_times_price'] / agg['quantity']

# Reset Index, rename columns, keep LTA schema columns, sort_values
agg = agg.reset_index()
fa_trd_norm = agg.rename(columns=trd_col_converters).copy()[keep_trd_cols].sort_values(by='security_id')

fa_trd_norm_answer = fa_trd_norm.copy()

# Now we have our daily trades
# We want to calculate our daily PNL to see if broker and fund admin are matching
# Here are the end of day marks for 09/12
MARKS = f"{DATA}/marks_2025-09-12.csv"
marks  = pd.read_csv(MARKS, parse_dates=["date"])

# Function to merge closing marks with normalized trades
def merge_close_marks_on_trades(df, marks):
    d = df.copy()
    out = df.merge(marks[["security_id","close"]], on="security_id", how="left", validate="many_to_one")
    return out

fa_trd_cl = merge_close_marks_on_trades(fa_trd_norm, marks)
br_trd_cl = merge_close_marks_on_trades(br_trd_norm, marks)

# TODO: Write a function that takes in a trades dataframe and calculates trade PNL
# You can ignore comms -- but tell us how you would account for it
# The returned df should have one additional column called pnl

def calculate_notional_and_pnl(df):
    df = df.copy()
    df["notional"] = df["price"] * df["quantity"] * df["multiplier"]
    df["pnl"] = (df["close"] - df["price"]) * df["quantity"] * df["multiplier"] 
    return df

fa_p = calculate_notional_and_pnl(fa_trd_cl)
fa_p_answer = fa_p.copy()

br_p = calculate_notional_and_pnl(br_trd_cl)
br_p_answer = br_p.copy()

# Aggregate per instrument
fa_p = fa_p.groupby("security_id", as_index=False)["pnl"].sum().rename(columns={"pnl":"pnl_admin"})
br_p = br_p.groupby("security_id", as_index=False)["pnl"].sum().rename(columns={"pnl":"pnl_broker"})

pnl_cmp = (fa_p.merge(br_p, on="security_id", how="outer").fillna(0.0))
pnl_cmp["pnl_break"] = pnl_cmp["pnl_broker"] - pnl_cmp["pnl_admin"]
pnl_cmp = pnl_cmp.sort_values("security_id").reset_index(drop=True)
pnl_cmp_answer = pnl_cmp.copy()

# STEP 3
br_trd_v2 = pd.read_csv(f"{TRD_DIR}/broker_trades_2025-09-12_v2.csv", parse_dates=["trade_date","settle_date"])

br_trd_v2

# TODO: normalize broker trade data to LTA Schema Requirements, sort values by security_id
# Name the normalized dataframe br_trd_norm
br_trd_norm_v2 = br_trd_v2.rename(columns=trd_col_converters).copy()[keep_trd_cols].sort_values(by='security_id')
# display(br_trd_norm_v2)

br_trd_cl_v2 = merge_close_marks_on_trades(br_trd_norm_v2, marks)

br_trd_cl_v2

def calculate_notional_and_pnl(df):
    df = df.copy()
    df["notional"] = df["price"] * df["quantity"] * df["multiplier"]
    df["pnl"] = (df["close"] - df["price"]) * df["quantity"] * df["multiplier"] 
    return df

calculate_notional_and_pnl(br_trd_cl_v2)

br_p_v2 = calculate_notional_and_pnl(br_trd_cl_v2)

br_p_v2

br_p_v2 = br_p_v2.groupby("security_id", as_index=False)["pnl"].sum().rename(columns={"pnl":"pnl_broker"})

pnl_cmp_v2 = (fa_p.merge(br_p_v2, on="security_id", how="outer").fillna(0.0))
pnl_cmp_v2["pnl_break"] = pnl_cmp_v2["pnl_broker"] - pnl_cmp["pnl_admin"]
pnl_cmp_v2 = pnl_cmp_v2.sort_values("security_id").reset_index(drop=True)

pnl_cmp_v2_answer = pnl_cmp_v2.copy()