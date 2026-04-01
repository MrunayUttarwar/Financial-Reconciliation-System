import unittest

import pandas as pd

from reconciliation_system import (
    ISSUE_AMOUNT_MISMATCH,
    ISSUE_DATA_QUALITY,
    ISSUE_DUPLICATE,
    ISSUE_INVALID_REFUND,
    ISSUE_MATCHED,
    ISSUE_MISSING_IN_SETTLEMENT,
    ISSUE_MISSING_IN_TRANSACTIONS,
    ISSUE_PARTIAL_SETTLEMENT,
    ISSUE_TIMING_DIFFERENCE,
    ReconciliationConfig,
    build_reconciliation_summary,
    generate_synthetic_data,
    reconcile_transactions,
    run_reconciliation_pipeline,
)


class ReconciliationLogicTests(unittest.TestCase):
    def _reconcile(self, transactions_rows, settlements_rows, tolerance=0.005):
        transactions = pd.DataFrame(transactions_rows, columns=["transaction_id", "transaction_date", "amount"])
        settlements = pd.DataFrame(settlements_rows, columns=["transaction_id", "settlement_date", "amount"])
        return reconcile_transactions(transactions, settlements, amount_tolerance=tolerance)

    def _issue_for(self, report: pd.DataFrame, transaction_id):
        row = report.loc[report["transaction_id"] == transaction_id].iloc[0]
        return row["issue_type"]

    def test_matched_transaction(self):
        report = self._reconcile(
            [("TXN1", "2026-01-10", 100.00)],
            [("TXN1", "2026-01-11", 100.00)],
        )
        self.assertEqual(self._issue_for(report, "TXN1"), ISSUE_MATCHED)

    def test_timing_difference_cross_month(self):
        report = self._reconcile(
            [("TXN2", "2026-01-31", 200.00)],
            [("TXN2", "2026-02-01", 200.00)],
        )
        self.assertEqual(self._issue_for(report, "TXN2"), ISSUE_TIMING_DIFFERENCE)

    def test_amount_mismatch(self):
        report = self._reconcile(
            [("TXN3", "2026-01-10", 150.00)],
            [("TXN3", "2026-01-11", 149.90)],
        )
        self.assertEqual(self._issue_for(report, "TXN3"), ISSUE_AMOUNT_MISMATCH)

    def test_multiple_exact_duplicates(self):
        report = self._reconcile(
            [("TXN4", "2026-01-10", 120.00)],
            [
                ("TXN4", "2026-01-11", 120.00),
                ("TXN4", "2026-01-11", 120.00),
                ("TXN4", "2026-01-11", 120.00),
            ],
        )
        self.assertEqual(self._issue_for(report, "TXN4"), ISSUE_DUPLICATE)

    def test_partial_settlement(self):
        report = self._reconcile(
            [("TXN5", "2026-01-10", 100.00)],
            [
                ("TXN5", "2026-01-11", 30.00),
                ("TXN5", "2026-01-12", 50.00),
            ],
        )
        self.assertEqual(self._issue_for(report, "TXN5"), ISSUE_PARTIAL_SETTLEMENT)

    def test_floating_point_precision_handled(self):
        report = self._reconcile(
            [("TXN6", "2026-01-10", 0.30)],
            [("TXN6", "2026-01-11", 0.1 + 0.2)],
        )
        self.assertEqual(self._issue_for(report, "TXN6"), ISSUE_MATCHED)

    def test_missing_in_settlement(self):
        report = self._reconcile(
            [("TXN7", "2026-01-10", 80.00)],
            [],
        )
        self.assertEqual(self._issue_for(report, "TXN7"), ISSUE_MISSING_IN_SETTLEMENT)

    def test_missing_in_transactions(self):
        report = self._reconcile(
            [],
            [("EXT1", "2026-01-11", 80.00)],
        )
        self.assertEqual(self._issue_for(report, "EXT1"), ISSUE_MISSING_IN_TRANSACTIONS)

    def test_invalid_refund_without_transaction(self):
        report = self._reconcile(
            [],
            [("RFND1", "2026-01-11", -80.00)],
        )
        self.assertEqual(self._issue_for(report, "RFND1"), ISSUE_INVALID_REFUND)

    def test_null_values_marked_data_quality_issue(self):
        report = self._reconcile(
            [("TXN8", "2026-01-10", 50.00)],
            [("TXN8", None, None)],
        )
        self.assertEqual(self._issue_for(report, "TXN8"), ISSUE_DATA_QUALITY)


class PipelineSmokeTests(unittest.TestCase):
    def test_pipeline_executes_and_returns_report(self):
        config = ReconciliationConfig(record_count=60, random_seed=42, export_path="reconciliation_report.csv")
        report, summary = run_reconciliation_pipeline(config)

        self.assertFalse(report.empty)
        self.assertIn("issue_type", report.columns)
        self.assertGreaterEqual(sum(summary.issue_counts.values()), 1)

    def test_synthetic_data_minimum_record_count(self):
        transactions, settlements = generate_synthetic_data(record_count=60, seed=42)
        self.assertGreaterEqual(len(transactions), 60)
        self.assertGreaterEqual(len(settlements), 60)

    def test_rounding_difference_visible_in_totals_only(self):
        transactions = pd.DataFrame(
            [
                ("TXN100", "2026-01-10", 10.00),
                ("TXN101", "2026-01-10", 10.00),
                ("TXN102", "2026-01-10", 10.00),
                ("TXN103", "2026-01-10", 10.00),
                ("TXN104", "2026-01-10", 10.00),
            ],
            columns=["transaction_id", "transaction_date", "amount"],
        )
        settlements = pd.DataFrame(
            [
                ("TXN100", "2026-01-11", 10.004),
                ("TXN101", "2026-01-11", 10.004),
                ("TXN102", "2026-01-11", 10.004),
                ("TXN103", "2026-01-11", 10.004),
                ("TXN104", "2026-01-11", 10.004),
            ],
            columns=["transaction_id", "settlement_date", "amount"],
        )

        report = reconcile_transactions(transactions, settlements, amount_tolerance=0.005)
        summary = build_reconciliation_summary(transactions, settlements, report)

        self.assertTrue((report["issue_type"] == ISSUE_MATCHED).all())
        self.assertEqual(summary.total_transactions_amount, 50.00)
        self.assertEqual(summary.total_settlements_amount, 50.02)
        self.assertEqual(summary.difference, -0.02)


if __name__ == "__main__":
    unittest.main()
