"""提交前读取、提交后读取、回滚、当前读与异常输入。"""

import unittest

from mvcc_validator import validate
from mvcc_validator.reasons import (
    HIDDEN_ABORTED,
    HIDDEN_DELETED,
    HIDDEN_FUTURE_XID,
    HIDDEN_IN_TRANSACTION,
    MARKER_XMAX_MISMATCH,
    UNKNOWN_XID,
)

from .helpers import build_catalog, evidence_codes, hidden_ids, reasons_for, visible_ids


def base_transactions():
    return [
        {"xid": 100, "start_ts": 1.0, "commit_ts": 5.0, "commit_lsn": "0/100"},
        {"xid": 101, "start_ts": 2.0},
        {"xid": 102, "start_ts": 3.0, "abort_ts": 6.0},
        {"xid": 103, "start_ts": 4.0, "commit_ts": 7.0, "commit_lsn": "0/200"},
    ]


def base_versions():
    return [
        {"version_id": "v100", "row_id": "r1", "xmin": 100},
        {"version_id": "v101", "row_id": "r2", "xmin": 101},
        {"version_id": "v102", "row_id": "r3", "xmin": 102},
        {"version_id": "v103", "row_id": "r4", "xmin": 103},
    ]


class BasicVisibilityTest(unittest.TestCase):
    def setUp(self):
        self.catalog = build_catalog(
            transactions=base_transactions(),
            versions=base_versions(),
            nodes=[{"name": "primary", "role": "primary"}],
            snapshots=[
                # 时刻 4.5：100 已提交，101/102 进行中，103 尚未分配。
                {"name": "s_before", "xmin": 101, "xmax": 103, "xip": [101, 102], "taken_ts": 4.5},
                # 时刻 8：100/103 已提交，102 已回滚，101 仍进行。
                {"name": "s_after", "xmin": 101, "xmax": 104, "xip": [101], "taken_ts": 8.0},
            ],
            queries=[
                {"query_id": "q_before", "node": "primary", "snapshot": "s_before"},
                {"query_id": "q_after", "node": "primary", "snapshot": "s_after"},
                {"query_id": "q_current", "node": "primary", "mode": "current", "at_ts": 8.0},
            ],
        )

    def test_read_before_commit(self):
        report = validate(self.catalog)
        result = report.results[0]
        self.assertEqual(visible_ids(result), {"v100"})
        self.assertEqual(hidden_ids(result), {"v101", "v102", "v103"})
        reasons = reasons_for(result)
        self.assertEqual(reasons["v101"], HIDDEN_IN_TRANSACTION)
        # xip 记录的是快照取得时刻的运行集合；事务之后回滚不改变当时的隐藏判定。
        self.assertEqual(reasons["v102"], HIDDEN_IN_TRANSACTION)
        self.assertEqual(reasons["v103"], HIDDEN_FUTURE_XID)

    def test_read_after_commit_and_rollback(self):
        report = validate(self.catalog)
        result = report.results[1]
        self.assertEqual(visible_ids(result), {"v100", "v103"})
        self.assertEqual(hidden_ids(result), {"v101", "v102"})
        reasons = reasons_for(result)
        self.assertEqual(reasons["v102"], HIDDEN_ABORTED)
        self.assertEqual(reasons["v101"], HIDDEN_IN_TRANSACTION)

    def test_current_read_only_shows_committed(self):
        report = validate(self.catalog)
        result = report.results[2]
        self.assertEqual(visible_ids(result), {"v100", "v103"})
        self.assertEqual(hidden_ids(result), {"v101", "v102"})
        self.assertEqual(result.snapshot_watermark["xip"], [101])
        self.assertEqual(result.snapshot_watermark["xmin"], 101)
        self.assertEqual(result.snapshot_watermark["xmax"], 104)

    def test_unknown_xid_in_window_is_hidden(self):
        catalog = build_catalog(
            transactions=[{"xid": 10, "start_ts": 1.0, "commit_ts": 2.0, "commit_lsn": "0/10"}],
            versions=[{"version_id": "v_ghost", "row_id": "r1", "xmin": 11}],
            nodes=[{"name": "primary", "role": "primary"}],
            snapshots=[{"name": "s", "xmin": 10, "xmax": 12, "xip": []}],
            queries=[{"query_id": "q", "node": "primary", "snapshot": "s"}],
        )
        report = validate(catalog)
        self.assertEqual(visible_ids(report.results[0]), set())
        self.assertEqual(
            reasons_for(report.results[0])["v_ghost"],
            UNKNOWN_XID,
        )
        self.assertIn(UNKNOWN_XID, evidence_codes(report))

    def test_marker_xmax_mismatch_keeps_version_visible(self):
        catalog = build_catalog(
            transactions=[
                {"xid": 10, "start_ts": 1.0, "commit_ts": 2.0, "commit_lsn": "0/10"},
                {"xid": 11, "start_ts": 3.0, "commit_ts": 4.0, "commit_lsn": "0/20"},
            ],
            versions=[{"version_id": "v1", "row_id": "r1", "xmin": 10, "xmax": 11}],
            deletion_markers=[{"version_id": "v1", "row_id": "r1", "xmax": 99}],
            nodes=[{"name": "primary", "role": "primary"}],
            snapshots=[{"name": "s", "xmin": 12, "xmax": 13, "xip": []}],
            queries=[{"query_id": "q", "node": "primary", "snapshot": "s"}],
        )
        report = validate(catalog)
        result = report.results[0]
        self.assertEqual(visible_ids(result), {"v1"})
        self.assertEqual(reasons_for(result)["v1"], MARKER_XMAX_MISMATCH)

    def test_committed_delete_hides_version(self):
        catalog = build_catalog(
            transactions=[
                {"xid": 10, "start_ts": 1.0, "commit_ts": 2.0, "commit_lsn": "0/10"},
                {"xid": 11, "start_ts": 3.0, "commit_ts": 4.0, "commit_lsn": "0/20"},
            ],
            versions=[{"version_id": "v1", "row_id": "r1", "xmin": 10, "xmax": 11}],
            deletion_markers=[{"version_id": "v1", "row_id": "r1", "xmax": 11}],
            nodes=[{"name": "primary", "role": "primary"}],
            snapshots=[{"name": "s", "xmin": 12, "xmax": 13, "xip": []}],
            queries=[{"query_id": "q", "node": "primary", "snapshot": "s"}],
        )
        report = validate(catalog)
        result = report.results[0]
        self.assertEqual(hidden_ids(result), {"v1"})
        self.assertEqual(reasons_for(result)["v1"], HIDDEN_DELETED)
