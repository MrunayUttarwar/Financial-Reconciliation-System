import io
from dataclasses import asdict

import pandas as pd
import streamlit as st

from reconciliation_system import (
    ReconciliationConfig,
    build_reconciliation_summary,
    generate_synthetic_data,
    reconcile_transactions,
)


st.set_page_config(page_title="Financial Reconciliation System", layout="wide")


def _validate_columns(df: pd.DataFrame, expected: list[str], name: str) -> None:
    missing = [col for col in expected if col not in df.columns]
    if missing:
        raise ValueError(f"{name} is missing required columns: {missing}")


def _load_csv(uploaded_file, expected_columns: list[str], dataset_name: str) -> pd.DataFrame:
    df = pd.read_csv(uploaded_file)
    _validate_columns(df, expected_columns, dataset_name)
    return df


def _run_reconciliation(transactions: pd.DataFrame, settlements: pd.DataFrame, tolerance: float):
    report = reconcile_transactions(
        transactions=transactions,
        settlements=settlements,
        amount_tolerance=tolerance,
    )
    summary = build_reconciliation_summary(
        transactions=transactions,
        settlements=settlements,
        reconciliation_report=report,
    )
    return report, summary


def _download_report_button(report_df: pd.DataFrame) -> None:
    csv_bytes = report_df.to_csv(index=False).encode("utf-8")
    st.download_button(
        label="Download Reconciliation Report (CSV)",
        data=csv_bytes,
        file_name="reconciliation_report.csv",
        mime="text/csv",
    )


def _render_summary(summary_obj) -> None:
    summary = asdict(summary_obj)

    col1, col2, col3 = st.columns(3)
    col1.metric("Total Transactions Amount", f"{summary['total_transactions_amount']:,.2f}")
    col2.metric("Total Settlements Amount", f"{summary['total_settlements_amount']:,.2f}")
    col3.metric("Difference (Tx - Settle)", f"{summary['difference']:,.2f}")

    issue_counts = pd.DataFrame(
        [
            {"issue_type": issue, "count": count}
            for issue, count in summary["issue_counts"].items()
        ]
    ).sort_values("issue_type")

    st.subheader("Issue Counts")
    st.dataframe(issue_counts, use_container_width=True)


def main() -> None:
    st.title("Financial Reconciliation System")
    st.caption("Reconcile transactions vs settlements with issue classification and reporting.")

    with st.sidebar:
        st.header("Run Settings")
        mode = st.radio(
            "Data Source",
            ["Synthetic Data", "Upload CSV Files"],
            index=0,
        )
        tolerance = st.number_input(
            "Amount Tolerance",
            min_value=0.0,
            max_value=1.0,
            value=0.005,
            step=0.001,
            format="%.3f",
            help="Differences below or equal to this tolerance are treated as matched.",
        )

    try:
        if mode == "Synthetic Data":
            seed = st.sidebar.number_input("Random Seed", min_value=1, max_value=99999, value=42, step=1)
            record_count = st.sidebar.number_input("Record Count", min_value=50, max_value=5000, value=80, step=10)

            config = ReconciliationConfig(
                record_count=int(record_count),
                random_seed=int(seed),
                amount_tolerance=float(tolerance),
            )
            transactions_df, settlements_df = generate_synthetic_data(
                record_count=config.record_count,
                seed=config.random_seed,
            )
        else:
            st.subheader("Upload Input Files")
            tx_file = st.file_uploader("Transactions CSV", type=["csv"], key="tx")
            st_file = st.file_uploader("Settlements CSV", type=["csv"], key="st")

            if not tx_file or not st_file:
                st.info("Upload both files to run reconciliation.")
                return

            transactions_df = _load_csv(
                tx_file,
                ["transaction_id", "transaction_date", "amount"],
                "Transactions CSV",
            )
            settlements_df = _load_csv(
                st_file,
                ["transaction_id", "settlement_date", "amount"],
                "Settlements CSV",
            )

        report_df, summary = _run_reconciliation(
            transactions=transactions_df,
            settlements=settlements_df,
            tolerance=float(tolerance),
        )

        _render_summary(summary)

        st.subheader("Reconciliation Report")
        st.dataframe(report_df, use_container_width=True, height=420)
        _download_report_button(report_df)

        with st.expander("Preview Input Data"):
            st.markdown("**Transactions**")
            st.dataframe(transactions_df.head(20), use_container_width=True)
            st.markdown("**Settlements**")
            st.dataframe(settlements_df.head(20), use_container_width=True)

    except Exception as exc:
        st.error(f"Failed to run reconciliation: {exc}")


if __name__ == "__main__":
    main()
