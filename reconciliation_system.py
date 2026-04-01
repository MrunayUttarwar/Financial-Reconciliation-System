from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import timedelta
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Dict, List, Optional, Tuple

import pandas as pd


ISSUE_MATCHED = "Matched"
ISSUE_TIMING_DIFFERENCE = "Timing Difference"
ISSUE_DUPLICATE = "Duplicate"
ISSUE_MISSING_IN_SETTLEMENT = "Missing in Settlement"
ISSUE_MISSING_IN_TRANSACTIONS = "Missing in Transactions"
ISSUE_AMOUNT_MISMATCH = "Amount Mismatch"
ISSUE_INVALID_REFUND = "Invalid Refund"
ISSUE_PARTIAL_SETTLEMENT = "Partial Settlement"
ISSUE_DATA_QUALITY = "Data Quality Issue"


@dataclass(frozen=True)
class ReconciliationConfig:
    """Configuration values for synthetic generation and reconciliation logic."""

    record_count: int = 80
    random_seed: int = 42
    amount_tolerance: float = 0.005
    export_path: str = "reconciliation_report.csv"


@dataclass(frozen=True)
class ReconciliationSummary:
    """Summary metrics generated after reconciliation."""

    total_transactions_amount: float
    total_settlements_amount: float
    difference: float
    issue_counts: Dict[str, int]


def _to_decimal(value: object) -> Optional[Decimal]:
    """Convert numeric input to 2-decimal Decimal for precision-safe comparisons."""
    if pd.isna(value):
        return None
    try:
        return Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    except (InvalidOperation, ValueError, TypeError):
        return None


def _abs_decimal_difference(left: Optional[Decimal], right: Optional[Decimal]) -> Optional[Decimal]:
    """Return absolute difference between two Decimal values, or None when any side is missing."""
    if left is None or right is None:
        return None
    return abs(left - right)


def _is_true_flag(value: object) -> bool:
    """Return True only for explicit boolean-like truthy values, not NaN."""
    if pd.isna(value):
        return False
    return bool(value)


def _normalize_transaction_id(series: pd.Series) -> pd.Series:
    """Normalize transaction IDs (strip whitespace, uppercase, convert blanks to NA)."""
    normalized = series.astype("string").str.strip().str.upper()
    normalized = normalized.replace({"": pd.NA, "NAN": pd.NA, "NONE": pd.NA})
    return normalized


def create_base_transactions(record_count: int, rng: random.Random) -> pd.DataFrame:
    """Create base transaction records for January 2026."""
    january_dates = pd.date_range("2026-01-01", "2026-01-31", freq="D")
    transaction_ids = [f"TXN{i:05d}" for i in range(1, record_count + 1)]

    return pd.DataFrame(
        {
            "transaction_id": transaction_ids,
            "transaction_date": [rng.choice(january_dates) for _ in range(record_count)],
            "amount": [round(rng.uniform(10, 500), 2) for _ in range(record_count)],
        }
    )


def create_base_settlements(transactions: pd.DataFrame, rng: random.Random) -> pd.DataFrame:
    """Create settlements from transactions with a typical 1-2 day lag."""
    settlements = transactions.rename(columns={"transaction_date": "settlement_date"}).copy()
    settlements["settlement_date"] = settlements["settlement_date"].apply(
        lambda date_value: date_value + timedelta(days=rng.choice([1, 2]))
    )
    return settlements[["transaction_id", "settlement_date", "amount"]]


def apply_gap_scenarios(
    transactions: pd.DataFrame,
    settlements: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Inject explicit reconciliation gap scenarios into the datasets."""
    transaction_ids = transactions["transaction_id"].tolist()

    _inject_timing_difference(transactions, settlements, transaction_ids)
    _inject_rounding_difference(settlements, transaction_ids)
    settlements = _inject_multiple_duplicates(settlements, transaction_ids)
    settlements = _inject_missing_settlement(settlements, transaction_ids)
    settlements = _inject_missing_transaction(settlements)
    settlements = _inject_invalid_refund(settlements)
    settlements = _inject_partial_settlement(settlements, transaction_ids)
    settlements = _inject_null_values(settlements)

    transactions["transaction_date"] = pd.to_datetime(transactions["transaction_date"]).dt.date
    settlements["settlement_date"] = pd.to_datetime(settlements["settlement_date"]).dt.date

    return transactions, settlements


def _inject_timing_difference(
    transactions: pd.DataFrame,
    settlements: pd.DataFrame,
    transaction_ids: List[str],
) -> None:
    """Force select January transactions to settle in February."""
    timing_transaction_ids = transaction_ids[:3]
    transactions.loc[
        transactions["transaction_id"].isin(timing_transaction_ids), "transaction_date"
    ] = pd.Timestamp("2026-01-31")
    settlements.loc[
        settlements["transaction_id"].isin(timing_transaction_ids), "settlement_date"
    ] = pd.to_datetime(["2026-02-01", "2026-02-02", "2026-02-01"])


def _inject_rounding_difference(settlements: pd.DataFrame, transaction_ids: List[str]) -> None:
    """
    Create aggregate-only rounding drift.

    Small sub-cent adjustments are applied to multiple settlement rows so each row still rounds
    to the same 2-decimal value as the corresponding transaction. The variance becomes visible
    only when totals are summed at higher precision and rounded at the end.
    """
    rounding_candidate_ids = transaction_ids[45:50]
    settlements.loc[settlements["transaction_id"].isin(rounding_candidate_ids), "amount"] += 0.004


def _inject_multiple_duplicates(settlements: pd.DataFrame, transaction_ids: List[str]) -> pd.DataFrame:
    """Add multiple exact duplicate rows for one transaction."""
    duplicate_transaction_id = transaction_ids[20]
    original_row = settlements.loc[settlements["transaction_id"] == duplicate_transaction_id].head(1)
    duplicates = pd.concat([original_row, original_row], ignore_index=True)
    return pd.concat([settlements, duplicates], ignore_index=True)


def _inject_missing_settlement(settlements: pd.DataFrame, transaction_ids: List[str]) -> pd.DataFrame:
    """Remove a settlement record to simulate missing settlement."""
    missing_settlement_transaction_id = transaction_ids[30]
    candidate_indices = settlements.index[settlements["transaction_id"] == missing_settlement_transaction_id]
    if len(candidate_indices) > 0:
        settlements = settlements.drop(candidate_indices[0])
    return settlements


def _inject_missing_transaction(settlements: pd.DataFrame) -> pd.DataFrame:
    """Add a settlement row with no corresponding transaction record."""
    missing_transaction_row = pd.DataFrame(
        [
            {
                "transaction_id": "EXT90001",
                "settlement_date": pd.Timestamp("2026-02-05"),
                "amount": 120.00,
            }
        ]
    )
    return pd.concat([settlements, missing_transaction_row], ignore_index=True)


def _inject_invalid_refund(settlements: pd.DataFrame) -> pd.DataFrame:
    """Add a negative settlement with no original transaction to represent an invalid refund."""
    invalid_refund_row = pd.DataFrame(
        [
            {
                "transaction_id": "RFND9001",
                "settlement_date": pd.Timestamp("2026-02-06"),
                "amount": -75.25,
            }
        ]
    )
    return pd.concat([settlements, invalid_refund_row], ignore_index=True)


def _inject_partial_settlement(settlements: pd.DataFrame, transaction_ids: List[str]) -> pd.DataFrame:
    """Replace one settlement with two partial settlements that do not fully cover original amount."""
    partial_transaction_id = transaction_ids[40]
    candidate_rows = settlements.loc[settlements["transaction_id"] == partial_transaction_id].head(1)
    if candidate_rows.empty:
        return settlements

    original_row = candidate_rows.iloc[0]
    original_amount = float(original_row["amount"])
    first_part = round(original_amount * 0.6, 2)
    second_part = round(original_amount * 0.25, 2)

    settlements = settlements.drop(candidate_rows.index)
    partial_rows = pd.DataFrame(
        [
            {
                "transaction_id": partial_transaction_id,
                "settlement_date": pd.to_datetime(original_row["settlement_date"]),
                "amount": first_part,
            },
            {
                "transaction_id": partial_transaction_id,
                "settlement_date": pd.to_datetime(original_row["settlement_date"]) + timedelta(days=1),
                "amount": second_part,
            },
        ]
    )
    return pd.concat([settlements, partial_rows], ignore_index=True)


def _inject_null_values(settlements: pd.DataFrame) -> pd.DataFrame:
    """Inject null fields to test null-safe reconciliation behavior."""
    null_rows = pd.DataFrame(
        [
            {
                "transaction_id": None,
                "settlement_date": pd.Timestamp("2026-02-07"),
                "amount": 50.00,
            },
            {
                "transaction_id": "TXN_NULL1",
                "settlement_date": None,
                "amount": None,
            },
        ]
    )
    return pd.concat([settlements, null_rows], ignore_index=True)


def generate_synthetic_data(record_count: int = 80, seed: int = 42) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Generate synthetic transactions and settlements including known reconciliation gaps."""
    rng = random.Random(seed)
    transactions = create_base_transactions(record_count=record_count, rng=rng)
    settlements = create_base_settlements(transactions=transactions, rng=rng)
    return apply_gap_scenarios(transactions=transactions, settlements=settlements)


def sanitize_transactions(transactions: pd.DataFrame) -> pd.DataFrame:
    """Normalize transaction dataframe types and null handling for reconciliation."""
    tx = transactions.copy()
    tx["transaction_id"] = _normalize_transaction_id(tx["transaction_id"])
    tx["transaction_date"] = pd.to_datetime(tx["transaction_date"], errors="coerce")
    tx["transaction_amount"] = tx["amount"].apply(_to_decimal)
    return tx[["transaction_id", "transaction_date", "transaction_amount"]]


def sanitize_settlements(settlements: pd.DataFrame) -> pd.DataFrame:
    """Normalize settlement dataframe types and null handling for reconciliation."""
    st = settlements.copy()
    st["transaction_id"] = _normalize_transaction_id(st["transaction_id"])
    st["settlement_date"] = pd.to_datetime(st["settlement_date"], errors="coerce")
    st["settlement_amount"] = st["amount"].apply(_to_decimal)
    return st[["transaction_id", "settlement_date", "settlement_amount"]]


def mark_exact_duplicate_settlements(settlements: pd.DataFrame) -> pd.DataFrame:
    """Mark exact duplicate settlement rows by transaction_id + settlement_date + settlement_amount."""
    st = settlements.copy().reset_index(drop=True)
    st["settlement_amount_key"] = st["settlement_amount"].astype("string")
    st["duplicate_sequence"] = st.groupby(
        ["transaction_id", "settlement_date", "settlement_amount_key"],
        dropna=False,
    ).cumcount()
    st["is_exact_duplicate"] = st["duplicate_sequence"] > 0
    return st.drop(columns=["settlement_amount_key"])


def aggregate_settlements(settlements: pd.DataFrame) -> pd.DataFrame:
    """Aggregate settlements per transaction while preserving duplicate and quality flags."""
    st = settlements.copy()

    non_duplicate_rows = st[~st["is_exact_duplicate"]].copy()

    aggregated = (
        non_duplicate_rows.groupby("transaction_id", dropna=False)
        .agg(
            settlement_rows=("settlement_amount", "size"),
            settlement_date_min=("settlement_date", "min"),
            settlement_date_max=("settlement_date", "max"),
            has_null_settlement_date=("settlement_date", lambda col: col.isna().any()),
            has_null_settlement_amount=("settlement_amount", lambda col: col.isna().any()),
            has_negative_settlement=("settlement_amount", lambda col: any(v is not None and v < 0 for v in col)),
            settlement_amount=(
                "settlement_amount",
                lambda col: (
                    sum((v for v in col if v is not None), Decimal("0.00"))
                    if any(v is not None for v in col)
                    else None
                ),
            ),
        )
        .reset_index()
    )

    duplicate_counts = (
        st.groupby("transaction_id", dropna=False)["is_exact_duplicate"].sum().reset_index(name="duplicate_count")
    )

    return aggregated.merge(duplicate_counts, on="transaction_id", how="left")


def _is_data_quality_issue(row: pd.Series) -> bool:
    """Return True if row contains null or invalid fields that break reliable reconciliation."""
    if pd.isna(row["transaction_id"]):
        return True
    if row["_merge"] == "both":
        if pd.isna(row["transaction_date"]) or pd.isna(row["settlement_date_min"]):
            return True
        if row["transaction_amount"] is None or row["settlement_amount"] is None:
            return True
    return _is_true_flag(row.get("has_null_settlement_date", False)) or _is_true_flag(
        row.get("has_null_settlement_amount", False)
    )


def classify_reconciliation_issue(row: pd.Series, amount_tolerance: float) -> str:
    """Classify a reconciled row into one business issue category."""
    if _is_data_quality_issue(row):
        return ISSUE_DATA_QUALITY

    merge_flag = row["_merge"]

    if merge_flag == "left_only":
        return ISSUE_MISSING_IN_SETTLEMENT

    if merge_flag == "right_only":
        if bool(row.get("has_negative_settlement", False)):
            return ISSUE_INVALID_REFUND
        return ISSUE_MISSING_IN_TRANSACTIONS

    tolerance_decimal = _to_decimal(amount_tolerance)
    amount_difference = _abs_decimal_difference(row["transaction_amount"], row["settlement_amount"])

    if bool(row.get("duplicate_count", 0) > 0):
        return ISSUE_DUPLICATE

    if amount_difference is not None and tolerance_decimal is not None:
        if amount_difference > tolerance_decimal:
            if row.get("settlement_rows", 0) > 1 and row["settlement_amount"] < row["transaction_amount"]:
                return ISSUE_PARTIAL_SETTLEMENT
            return ISSUE_AMOUNT_MISMATCH

    transaction_month = pd.Period(pd.to_datetime(row["transaction_date"]), freq="M")
    settlement_month = pd.Period(pd.to_datetime(row["settlement_date_min"]), freq="M")
    if transaction_month != settlement_month:
        return ISSUE_TIMING_DIFFERENCE

    return ISSUE_MATCHED


def reconcile_transactions(
    transactions: pd.DataFrame,
    settlements: pd.DataFrame,
    amount_tolerance: float,
) -> pd.DataFrame:
    """Perform reconciliation with full outer join and issue classification."""
    sanitized_transactions = sanitize_transactions(transactions)
    sanitized_settlements = sanitize_settlements(settlements)

    settlements_with_duplicate_flags = mark_exact_duplicate_settlements(sanitized_settlements)
    settlements_aggregated = aggregate_settlements(settlements_with_duplicate_flags)

    merged = sanitized_transactions.merge(
        settlements_aggregated,
        on="transaction_id",
        how="outer",
        indicator=True,
    )

    merged["issue_type"] = merged.apply(
        classify_reconciliation_issue,
        axis=1,
        amount_tolerance=amount_tolerance,
    )

    report = merged[["transaction_id", "transaction_amount", "settlement_amount", "issue_type"]].copy()
    report["transaction_amount"] = report["transaction_amount"].apply(
        lambda v: float(v) if isinstance(v, Decimal) else None
    )
    report["settlement_amount"] = report["settlement_amount"].apply(
        lambda v: float(v) if isinstance(v, Decimal) else None
    )
    return report.sort_values(["issue_type", "transaction_id"], na_position="last").reset_index(drop=True)


def build_reconciliation_summary(
    transactions: pd.DataFrame,
    settlements: pd.DataFrame,
    reconciliation_report: pd.DataFrame,
) -> ReconciliationSummary:
    """Compute reconciliation totals and issue distribution."""
    # Sum with original precision first, then round at the final total level.
    # This preserves aggregate-only rounding differences that are not visible row-by-row.
    transaction_total_raw = sum(
        (Decimal(str(value)) for value in transactions["amount"] if pd.notna(value)),
        Decimal("0.00"),
    )
    settlement_total_raw = sum(
        (Decimal(str(value)) for value in settlements["amount"] if pd.notna(value)),
        Decimal("0.00"),
    )

    total_transactions_amount = transaction_total_raw.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    total_settlements_amount = settlement_total_raw.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    difference = total_transactions_amount - total_settlements_amount

    issue_counts_series = reconciliation_report["issue_type"].value_counts().sort_index()
    issue_counts = {str(issue): int(count) for issue, count in issue_counts_series.items()}

    return ReconciliationSummary(
        total_transactions_amount=float(total_transactions_amount),
        total_settlements_amount=float(total_settlements_amount),
        difference=float(difference),
        issue_counts=issue_counts,
    )


def print_summary_dashboard(summary: ReconciliationSummary) -> None:
    """Print a readable reconciliation summary dashboard."""
    print("=" * 56)
    print("RECONCILIATION SUMMARY DASHBOARD")
    print("=" * 56)
    print(f"Total Transactions Amount : {summary.total_transactions_amount:,.2f}")
    print(f"Total Settlements Amount  : {summary.total_settlements_amount:,.2f}")
    print(f"Difference (Tx - Settle)  : {summary.difference:,.2f}")
    print("-" * 56)
    print("Issue Counts:")
    for issue_type, count in summary.issue_counts.items():
        print(f"  {issue_type:24s} {count:>5d}")
    print("=" * 56)


def export_reconciliation_report(report: pd.DataFrame, file_path: str) -> None:
    """Persist reconciliation report as CSV."""
    report.to_csv(file_path, index=False)
    print(f"Reconciliation report exported to: {file_path}")


def run_reconciliation_pipeline(config: ReconciliationConfig) -> Tuple[pd.DataFrame, ReconciliationSummary]:
    """Execute full reconciliation flow from data generation to summary."""
    transactions, settlements = generate_synthetic_data(
        record_count=config.record_count,
        seed=config.random_seed,
    )

    report = reconcile_transactions(
        transactions=transactions,
        settlements=settlements,
        amount_tolerance=config.amount_tolerance,
    )

    summary = build_reconciliation_summary(
        transactions=transactions,
        settlements=settlements,
        reconciliation_report=report,
    )

    export_reconciliation_report(report=report, file_path=config.export_path)
    return report, summary


def main() -> None:
    """Entry point for local execution."""
    config = ReconciliationConfig()
    reconciliation_report, summary = run_reconciliation_pipeline(config)

    print_summary_dashboard(summary)
    print("\nSample Reconciliation Output:")
    print(reconciliation_report.head(20).to_string(index=False))


if __name__ == "__main__":
    main()
