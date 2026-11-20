"""32 位 XID 回绕与纯算术边界。"""

import unittest

from mvcc_validator import age, in_range, normalize, oldest, validate
from mvcc_validator.reasons import (
    HIDDEN_FUTURE_XID,
    HIDDEN_IN_TRANSACTION,
    VISIBLE_FROZEN,
    VISIBLE_LIVE,
)

from .helpers import build_catalog, reasons_for, visible_ids


class XidArithmeticTest(unittest.TestCase):
    def test_normalize_and_age(self):
        self.assertEqual(normalize(2 ** 32 + 5), 5)
        self.assertEqual(age(2 ** 32 - 1, 1), 2)

    def test_in_range_wraps(self):
        # 区间 [FFFFFFFC, 3) 跨过回绕点。
        self.assertTrue(in_range(0xFFFFFFFC, 0xFFFFFFFC, 3))
        self.assertTrue(in_range(0, 0xFFFFFFFC, 3))
        self.assertTrue(in_range(2, 0xFFFFFFFC, 3))
        self.assertFalse(in_range(3, 0xFFFFFFFC, 3))
        self.assertFalse(in_range(0xFFFFFFFB, 0xFFFFFFFC, 3))

    def test_oldest_picks_wrapped_age(self):
        self.assertEqual(oldest([0, 2, 0xFFFFFFFE], reference=3), 0xFFFFFFFE)


class WraparoundVisibilityTest(unittest.TestCase):
    def test_snapshot_across_wraparound(self):
        # 快照窗口 [0xFFFFFFFF, 6) 跨过回绕点：
        # 旧事务 0xFFFFFFFD 在窗口之前，回绕后的普通事务 3/4/5 在窗口内，
        # 6 是 xmax（未来事务）。所有 XID 避开 0/1/2 保留值。
        xid_old = 0xFFFFFFFD
        xid_open = 0xFFFFFFFF
        xid_abort = 3
        xid_new = 4
        xid_future = 6

        catalog = build_catalog(
            transactions=[
                {"xid": xid_old, "start_ts": 1.0, "commit_ts": 2.0, "commit_lsn": "0/10"},
                {"xid": xid_open, "start_ts": 3.0},
                {"xid": xid_abort, "start_ts": 4.0, "abort_ts": 8.0},
                {"xid": xid_new, "start_ts": 5.0, "commit_ts": 6.0, "commit_lsn": "0/20"},
            ],
            versions=[
                {"version_id": "v_old", "row_id": "r1", "xmin": xid_old},
                {"version_id": "v_open", "row_id": "r2", "xmin": xid_open},
                {"version_id": "v_abort", "row_id": "r3", "xmin": xid_abort},
                {"version_id": "v_new", "row_id": "r4", "xmin": xid_new},
                # xid_future 正好等于快照 xmax，代表“未来事务”。
                {"version_id": "v_future", "row_id": "r5", "xmin": xid_future},
            ],
            nodes=[{"name": "primary", "role": "primary"}],
            snapshots=[
                {
                    "name": "s_wrap",
                    "xmin": xid_open,
                    "xmax": xid_future,
                    "xip": [xid_open, xid_abort],
                    "taken_ts": 5.5,
                }
            ],
            queries=[{"query_id": "q", "node": "primary", "snapshot": "s_wrap"}],
        )
        report = validate(catalog)
        result = report.results[0]
        self.assertEqual(visible_ids(result), {"v_old", "v_new"})
        reasons = reasons_for(result)
        self.assertEqual(reasons["v_old"], VISIBLE_LIVE)
        self.assertEqual(reasons["v_open"], HIDDEN_IN_TRANSACTION)
        # 回滚事务（XID 3）在快照 xip 中，快照取得时它还在运行。
        self.assertEqual(reasons["v_abort"], HIDDEN_IN_TRANSACTION)
        self.assertEqual(reasons["v_future"], HIDDEN_FUTURE_XID)
        self.assertTrue(report.is_consistent)

    def test_frozen_xid_always_visible(self):
        catalog = build_catalog(
            transactions=[],
            versions=[{"version_id": "v_frozen", "row_id": "r1", "xmin": 2}],
            nodes=[{"name": "primary", "role": "primary"}],
            snapshots=[{"name": "s", "xmin": 100, "xmax": 101, "xip": []}],
            queries=[{"query_id": "q", "node": "primary", "snapshot": "s"}],
        )
        report = validate(catalog)
        self.assertEqual(visible_ids(report.results[0]), {"v_frozen"})
        self.assertEqual(
            report.results[0].judgements["v_frozen"]["xmin_hint"]["reason"],
            VISIBLE_FROZEN,
        )
