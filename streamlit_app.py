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

ISSUE_COLORS = {
    "Matched": "#2f9e44",
    "Timing Difference": "#f59f00",
    "Duplicate": "#f76707",
    "Missing in Settlement": "#fa5252",
    "Missing in Transactions": "#15aabf",
    "Amount Mismatch": "#e64980",
    "Invalid Refund": "#be4bdb",
    "Partial Settlement": "#fd7e14",
    "Data Quality Issue": "#adb5bd",
}

ISSUE_PRIORITY = [
    "Data Quality Issue",
    "Invalid Refund",
    "Missing in Settlement",
    "Missing in Transactions",
    "Duplicate",
    "Partial Settlement",
    "Amount Mismatch",
    "Timing Difference",
    "Matched",
]

CRITICAL_ISSUES = [
    "Data Quality Issue",
    "Invalid Refund",
    "Missing in Settlement",
    "Missing in Transactions",
    "Duplicate",
]


def _apply_theme() -> None:
    """Inject custom CSS to improve visual hierarchy and issue visibility."""
    st.markdown(
        """
        <style>
        .stApp {
            background: radial-gradient(circle at top right, #1a1d23, #0f1117 40%, #050608 100%);
            color: #f1f3f5;
        }
        [data-testid="stSidebar"] {
            background: #0b0d12;
            border-right: 1px solid #1f2430;
        }
        .stMarkdown, .stCaption, label, p, div, span {
            color: #f1f3f5;
        }
        .issue-banner {
            border: 1px solid #3b1f26;
            background: linear-gradient(90deg, #2b0f17, #3d1322);
            padding: 0.7rem 1rem;
            border-radius: 0.6rem;
            font-weight: 600;
            color: #ffd8e1;
            margin-bottom: 0.8rem;
        }
        .summary-card {
            border-radius: 0.8rem;
            padding: 0.9rem 1rem;
            border: 1px solid #293040;
            background: #11151d;
            box-shadow: 0 2px 8px rgba(0, 0, 0, 0.4);
            margin-bottom: 0.6rem;
        }
        .summary-label {
            color: #adb5bd;
            font-size: 0.85rem;
            margin-bottom: 0.2rem;
        }
        .summary-value {
            color: #f8f9fa;
            font-size: 1.3rem;
            font-weight: 700;
        }
        .issue-chip {
            display: inline-block;
            color: #ffffff;
            border-radius: 999px;
            padding: 0.18rem 0.6rem;
            font-size: 0.78rem;
            font-weight: 700;
            margin-right: 0.35rem;
            margin-bottom: 0.35rem;
        }
        .critical-heading {
            color: #ff8787;
            font-weight: 700;
            letter-spacing: 0.02rem;
            margin-bottom: 0.25rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


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


def _render_summary_cards(summary_dict: dict) -> None:
    """Render headline KPI cards for overall balance status."""
    col1, col2, col3 = st.columns(3)
    col1.markdown(
        f"""
        <div class='summary-card'>
            <div class='summary-label'>Total Transactions Amount</div>
            <div class='summary-value'>{summary_dict['total_transactions_amount']:,.2f}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    col2.markdown(
        f"""
        <div class='summary-card'>
            <div class='summary-label'>Total Settlements Amount</div>
            <div class='summary-value'>{summary_dict['total_settlements_amount']:,.2f}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    diff = summary_dict["difference"]
    diff_color = "#b02a37" if diff != 0 else "#1f7a4d"
    col3.markdown(
        f"""
        <div class='summary-card'>
            <div class='summary-label'>Difference (Tx - Settle)</div>
            <div class='summary-value' style='color:{diff_color};'>{diff:,.2f}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _issue_counts_dataframe(summary_dict: dict) -> pd.DataFrame:
    """Build ordered issue count frame with color metadata for plotting and display."""
    rows = [
        {"issue_type": issue, "count": count, "color": ISSUE_COLORS.get(issue, "#6c757d")}
        for issue, count in summary_dict["issue_counts"].items()
    ]
    issue_counts = pd.DataFrame(rows)
    if issue_counts.empty:
        return issue_counts

    issue_counts["sort_order"] = issue_counts["issue_type"].apply(
        lambda issue: ISSUE_PRIORITY.index(issue) if issue in ISSUE_PRIORITY else len(ISSUE_PRIORITY)
    )
    return issue_counts.sort_values(["sort_order", "issue_type"]).drop(columns=["sort_order"]).reset_index(drop=True)


def _render_issue_banner(issue_counts_df: pd.DataFrame) -> None:
    """Highlight unresolved issue volume before showing detailed tables."""
    if issue_counts_df.empty:
        st.info("No issues found.")
        return

    matched_count = int(issue_counts_df.loc[issue_counts_df["issue_type"] == "Matched", "count"].sum())
    total_count = int(issue_counts_df["count"].sum())
    exception_count = total_count - matched_count

    st.markdown(
        f"<div class='issue-banner'>Exceptions requiring attention: {exception_count} / {total_count} records</div>",
        unsafe_allow_html=True,
    )


def _render_issue_legend(issue_counts_df: pd.DataFrame) -> None:
    """Render color legend chips for issue types present in the current run."""
    chips = []
    for _, row in issue_counts_df.iterrows():
        chips.append(
            f"<span class='issue-chip' style='background:{row['color']};'>{row['issue_type']}: {int(row['count'])}</span>"
        )
    st.markdown("".join(chips), unsafe_allow_html=True)


def _style_issue_row(row: pd.Series) -> list[str]:
    """Apply row highlight color by issue type for easier scanning."""
    base_color = ISSUE_COLORS.get(row["issue_type"], "#6c757d")
    alpha_color = f"{base_color}22"
    return [f"background-color: {alpha_color}"] * len(row)


def _render_visual_summary(summary_obj) -> pd.DataFrame:
    """Render all summary visuals and return issue counts dataframe."""
    summary = asdict(summary_obj)
    _render_summary_cards(summary)

    issue_counts_df = _issue_counts_dataframe(summary)
    if not issue_counts_df.empty:
        left, right = st.columns([1.3, 1])
        with left:
            st.subheader("Issue Distribution")
            chart_data = issue_counts_df.set_index("issue_type")["count"]
            st.bar_chart(chart_data)
        with right:
            st.subheader("Issue Counts")
            st.dataframe(issue_counts_df[["issue_type", "count"]], use_container_width=True, height=280)

        _render_issue_banner(issue_counts_df)
        _render_issue_legend(issue_counts_df)

    return issue_counts_df


def _render_report(report_df: pd.DataFrame) -> None:
    """Render interactive issue-first report with filters and styled rows."""
    st.subheader("Reconciliation Report")

    available_issues = sorted(report_df["issue_type"].dropna().unique().tolist())
    default_issues = [issue for issue in available_issues if issue != "Matched"] or available_issues

    filter_col1, filter_col2 = st.columns([1.2, 1])
    with filter_col1:
        selected_issues = st.multiselect(
            "Filter by issue type",
            options=available_issues,
            default=default_issues,
            help="Focus on exception categories first, then include Matched if needed.",
        )
    with filter_col2:
        exceptions_only = st.toggle("Show exceptions only", value=True)

    filtered = report_df.copy()
    if selected_issues:
        filtered = filtered[filtered["issue_type"].isin(selected_issues)]
    if exceptions_only:
        filtered = filtered[filtered["issue_type"] != "Matched"]

    filtered = filtered.sort_values(["issue_type", "transaction_id"], na_position="last").reset_index(drop=True)

    st.caption(f"Showing {len(filtered):,} of {len(report_df):,} records")

    if filtered.empty:
        st.warning("No records match the current filters.")
        return

    styled = filtered.style.apply(_style_issue_row, axis=1)
    st.dataframe(styled, use_container_width=True, height=460)


def _render_critical_issues(report_df: pd.DataFrame) -> None:
    """Render a high-priority tab with only critical reconciliation issues."""
    critical_df = report_df[report_df["issue_type"].isin(CRITICAL_ISSUES)].copy()
    critical_df = critical_df.sort_values(["issue_type", "transaction_id"], na_position="last").reset_index(drop=True)

    st.markdown("<div class='critical-heading'>Critical Issues Queue</div>", unsafe_allow_html=True)
    st.caption("This view excludes lower-risk categories and highlights records that need immediate action.")

    if critical_df.empty:
        st.success("No critical issues found in this run.")
        return

    counts = critical_df["issue_type"].value_counts().to_dict()
    metrics_cols = st.columns(len(counts))
    for idx, (issue_name, issue_count) in enumerate(counts.items()):
        metrics_cols[idx].metric(issue_name, int(issue_count))

    critical_styled = critical_df.style.apply(_style_issue_row, axis=1)
    st.dataframe(critical_styled, use_container_width=True, height=520)


def _load_input_data(mode: str, tolerance: float) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Load data from synthetic generator or user-uploaded CSV files."""
    if mode == "Synthetic Data":
        seed = st.sidebar.number_input("Random Seed", min_value=1, max_value=99999, value=42, step=1)
        record_count = st.sidebar.number_input("Record Count", min_value=50, max_value=5000, value=80, step=10)

        config = ReconciliationConfig(
            record_count=int(record_count),
            random_seed=int(seed),
            amount_tolerance=float(tolerance),
        )
        return generate_synthetic_data(
            record_count=config.record_count,
            seed=config.random_seed,
        )

    st.subheader("Upload Input Files")
    tx_file = st.file_uploader("Transactions CSV", type=["csv"], key="tx")
    st_file = st.file_uploader("Settlements CSV", type=["csv"], key="st")

    if not tx_file or not st_file:
        st.info("Upload both files to run reconciliation.")
        st.stop()

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
    return transactions_df, settlements_df


def main() -> None:
    _apply_theme()

    st.title("Financial Reconciliation System")
    st.caption("Reconcile platform transactions vs bank settlements with issue-first visibility.")

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
        transactions_df, settlements_df = _load_input_data(mode=mode, tolerance=float(tolerance))

        report_df, summary = _run_reconciliation(
            transactions=transactions_df,
            settlements=settlements_df,
            tolerance=float(tolerance),
        )

        _render_visual_summary(summary)
        tab_critical, tab_report, tab_inputs = st.tabs(["Critical Issues", "Full Report", "Input Preview"])
        with tab_critical:
            _render_critical_issues(report_df)
        with tab_report:
            _render_report(report_df)
            _download_report_button(report_df)
        with tab_inputs:
            st.markdown("**Transactions**")
            st.dataframe(transactions_df.head(20), use_container_width=True)
            st.markdown("**Settlements**")
            st.dataframe(settlements_df.head(20), use_container_width=True)

    except Exception as exc:
        st.error(f"Failed to run reconciliation: {exc}")


if __name__ == "__main__":
    main()
