
# Ops Mini-Interview: Futures Recon → P&L Breaks → Comms Sim

**Report dates**: Positions as of 2025-09-11; Trades and marks for 2025-09-12.  
**Instruments**: GOLD CMX (GC_Z5), EMINI (ES_Z5), LEAN HOGS (HE_Z5).  
**Contract multipliers (interview assumption)**: GC=100, ES=50, HE=400.

## Structure
```
interview_case/
├─ data/
│  ├─ positions/
│  │  ├─ fund_admin_positions_2025-09-11.csv
│  │  └─ broker_positions_2025-09-11.csv
│  ├─ trades/
│  │  ├─ fund_admin_trades_2025-09-12.csv
│  │  ├─ broker_trades_2025-09-12.csv
│  │  └─ broker_trades_2025-09-12_v2.csv
│  └─ marks_2025-09-12.csv
└─ notebook/
   └─ candidate_starter.ipynb
```

## Step 1 — Positions Recon (as of 2025-09-11)
- **Goal**: Normalize and reconcile positions from Fund Admin vs Broker.
- **Quirk**: Fund Admin `quantity` is positive and direction is implied by `avg_cost_sign` (±1). Broker quantity is already signed.
- **Task**:
  1. Load both files; normalize to common schema: `date, unique_id, symbol, qty_signed, avg_cost, multiplier`.
  2. Merge on `unique_id` (expect `validate='one_to_one'`).
  3. Calculate breaks: `qty_diff`, `avg_cost_diff` and show a tidy table.

## Step 2 — Trades + EOD P&L (2025-09-12)
- **Goal**: Compute **trade P&L** to EOD marks and reconcile Admin vs Broker; ES has an intentional **price break** in the broker file.
- **Task**:
  1. Load both trade files; compute `notional = price * quantity * contract_multiplier` (signed).
  2. Merge in `marks_2025-09-12.csv` on `unique_id` (`validate='many_to_one'`).
  3. Compute per-trade EOD P&L: `(close - price) * quantity * contract_multiplier - commissions`.
  4. Aggregate per `unique_id`: compare Admin vs Broker totals; identify P&L breaks.
  5. (Optional) Add **carry P&L** from 09/11 to 09/12 using positions price as prior mark.

## Step 3 — Comms Simulation
- **Scenario**: Call broker about the ES price discrepancy (Admin 5589.50 vs Broker 5588.25). Quantify P&L impact given multipliers and trade size, request corrected file.
- **Fix**: After the call, use `data/trades/broker_trades_2025-09-12_v2.csv` (ES price corrected to 5589.50) and confirm the break clears when you re-run.

## Starter Notebook
Open `notebook/candidate_starter.ipynb` for scaffolding and prompts.
