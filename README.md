# Financial Reconciliation System

A production-style Python reconciliation system for a payments company, with:
- Synthetic test data generation
- Robust reconciliation engine (including edge-case handling)
- Automated test suite
- Streamlit frontend dashboard
- Deployment guidance

---

## 1) Business Context

A payments platform records customer transactions instantly, while bank settlements arrive later in batches (typically 1-2 days). At month-end, both books should align, but they often do not.

This project identifies and classifies reconciliation gaps so finance/operations teams can quickly answer:
- What mismatched?
- Why did it mismatch?
- Which records need immediate action?

---

## 2) What This Project Delivers

- End-to-end reconciliation pipeline in `reconciliation_system.py`
- Synthetic datasets with realistic reconciliation gaps
- Issue classification into business-friendly categories
- Summary metrics and CSV export
- Streamlit frontend (`streamlit_app.py`) with:
  - Dark UI
  - Critical Issues tab
  - Issue-first filtering
  - Downloadable report
- Unit/integration tests in `tests/test_reconciliation_system.py`

---

## 3) Repository Structure

```text
Financial-Reconciliation-System/
|-- reconciliation_system.py          # Core reconciliation logic + synthetic data + CLI pipeline
|-- streamlit_app.py                  # Frontend dashboard (Streamlit)
|-- tests/
|   |-- test_reconciliation_system.py # Test suite (logic + smoke tests)
|-- requirements.txt                  # Python dependencies
|-- DEPLOYMENT.md                     # Deployment instructions
|-- reconciliation_report.csv         # Generated output sample
```

---

## 4) Reconciliation Categories

The engine classifies each record into one of the following issue types:

- `Matched`
- `Timing Difference`
- `Duplicate`
- `Missing in Settlement`
- `Missing in Transactions`
- `Amount Mismatch`
- `Invalid Refund`
- `Partial Settlement`
- `Data Quality Issue`

---

## 5) Synthetic Data Design

No input files are required. The system generates synthetic `transactions` and `settlements` tables and deliberately injects gap scenarios.

### Base datasets

`transactions`
- `transaction_id`
- `transaction_date`
- `amount`

`settlements`
- `transaction_id`
- `settlement_date`
- `amount`

### Injected scenarios

1. Transaction settled in next month (timing difference)
2. Aggregate-only rounding drift
- Not visible on individual rows
- Visible only in totals due to sub-cent accumulation and final rounding
3. Multiple exact duplicates in settlements
4. Missing settlement for an existing transaction
5. Settlement without source transaction
6. Invalid refund (negative settlement) without original transaction
7. Partial settlement (multiple settlement rows totaling less than transaction amount)
8. Null/missing values (for data-quality handling)

---

## 6) Core Reconciliation Logic

The core flow in `reconciliation_system.py`:

1. **Sanitize input data**
- Normalize `transaction_id` (trim/uppercase/null handling)
- Parse dates safely (`errors='coerce'`)
- Normalize amounts with `Decimal` for precision-safe arithmetic

2. **Duplicate handling**
- Exact duplicate detection based on:
  - `transaction_id`
  - `settlement_date`
  - `settlement_amount`
- Tracks duplicate count per transaction

3. **Settlement aggregation**
- Aggregates settlement rows by `transaction_id`
- Preserves flags:
  - row count
  - duplicate count
  - null-date/null-amount indicators
  - negative amount indicators
  - earliest/latest settlement dates

4. **Matching step**
- Full outer join between transactions and aggregated settlements by `transaction_id`

5. **Issue classification (priority-based)**
- Data quality issues
- Missing records (left-only/right-only)
- Invalid refunds
- Duplicates
- Partial settlement
- Amount mismatch
- Timing difference
- Matched

6. **Summary computation**
- Totals computed with high precision and rounded at final total level
- Preserves aggregate rounding drift visibility
- Issue counts reported by category

---

## 7) Assumptions

These assumptions were used explicitly:

1. `transaction_id` is the reconciliation key between platform and bank data.
2. Typical settlement lag is 1-2 days.
3. Cross-month settlement is valid and flagged as `Timing Difference`.
4. Negative settlement with no matching transaction is `Invalid Refund`.
5. Exact repeated settlement entries are treated as `Duplicate`.
6. Multiple settlement rows for one transaction can be valid; if total is short, classify as `Partial Settlement`.
7. Critical null fields imply `Data Quality Issue`.
8. Monetary comparisons are precision-safe using `Decimal`, with tolerance support.

---

## 8) Frontend (Streamlit)

The UI in `streamlit_app.py` is designed for issue-first triage.

### Key UX features

- Dark theme (black-first background)
- KPI cards for totals and net difference
- Issue distribution chart
- Color-coded issue legend chips
- Exception banner (`Exceptions requiring attention: X / Y`)
- **Critical Issues tab** (high-priority categories only)
- Full Report tab with issue filters and exception-only toggle
- Input Preview tab
- CSV download button

### Input modes

1. Synthetic Data mode
2. Upload CSV mode
- Transactions CSV required columns: `transaction_id, transaction_date, amount`
- Settlements CSV required columns: `transaction_id, settlement_date, amount`

---

## 9) Testing

Test file: `tests/test_reconciliation_system.py`

### Covered scenarios

- Matched transaction
- Timing difference (cross-month)
- Amount mismatch
- Multiple duplicates
- Partial settlement
- Floating-point precision handling
- Missing in settlement
- Missing in transactions
- Invalid refund
- Null-value data-quality classification
- Pipeline smoke execution
- Minimum synthetic record counts
- Aggregate-only rounding drift validation

### Run tests

```bash
python -m unittest discover -s tests -v
```

---

## 10) How to Run Locally

From project root:

### Install dependencies

```bash
python -m pip install -r requirements.txt
```

### Run backend pipeline

```bash
python reconciliation_system.py
```

Outputs:
- Console summary dashboard
- `reconciliation_report.csv`

### Run frontend

```bash
streamlit run streamlit_app.py
```

If `streamlit` is not recognized:

```bash
python -m streamlit run streamlit_app.py
```

---

## 11) Deployment

Recommended: **Streamlit Community Cloud**

Why:
- Native Python data app deployment
- Fast setup from GitHub
- Good for demos/submissions

Basic deploy flow:
1. Push repo to GitHub
2. Open `https://share.streamlit.io`
3. Create app from repo
4. Entrypoint: `streamlit_app.py`
5. Deploy

Alternative: Render (more infra control).

Detailed instructions: see `DEPLOYMENT.md`.

---

## 12) Commands Cheat Sheet

```bash
# install
python -m pip install -r requirements.txt

# run backend
python reconciliation_system.py

# run tests
python -m unittest discover -s tests -v

# run frontend
streamlit run streamlit_app.py
```

---

## 13) Known Limitations

- Matching currently uses only `transaction_id` (no fuzzy fallback keys).
- Time-zone/currency conversions are not modeled.
- No persistent database layer yet (file/in-memory only).
- Frontend authentication/role-based access is not included.

---

## 14) Future Improvements

1. Add support for multi-currency reconciliation and FX conversions.
2. Add database-backed ingestion and history tracking.
3. Add SLA alerts for unresolved critical issues.
4. Add PDF/Excel executive reports.
5. Add CI pipeline for tests and deployment checks.

---

## 15) Tech Stack

- Python 3
- Pandas
- Streamlit
- `unittest` (standard library)
- `decimal` (standard library precision control)

---

## 16) Submission Checklist

- [x] Synthetic data generation with required gap scenarios
- [x] Full outer join reconciliation
- [x] Categorized issue report
- [x] Summary metrics and issue counts
- [x] CSV export
- [x] Console dashboard
- [x] Frontend visualization
- [x] Test cases proving behavior
- [x] Deployment documentation

