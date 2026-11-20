"""清理水位、长事务保护与并发删除。"""

import unittest

from mvcc_validator import Query, validate
from mvcc_validator.reasons import (
    HIDDEN_DELETED,
    HIDDEN_IN_TRANSACTION,
    MARKER_PREEMPTED,
    UNSAFE_VACUUM_HORIZON,
    VACUUM_PREEMPTED,
    VISIBLE_LIVE,
)

from .helpers import build_catalog, evidence_codes, reasons_for, visible_ids


class VacuumTest(unittest.TestCase):
    def test_old_dead_tuple_is_legally_removed(self):
        catalog = build_catalog(
            transactions=[
                {"xid": 100, "start_ts": 1.0, "commit_ts": 2.0, "commit_lsn": "0/100"},
                {"xid": 101, "start_ts": 3.0, "commit_ts": 4.0, "commit_lsn": "0/200"},
            ],
            versions=[{"version_id": "v_new", "row_id": "r1", "xmin": 101}],
            deletion_markers=[],
            cleaned_versions=[
                {"version_id": "v_old", "row_id": "r1", "xmin": 100, "xmax": 101}
            ],
            nodes=[
                {"name": "primary", "role": "primary", "vacuum_horizon": 102},
            ],
            snapshots=[{"name": "s", "xmin": 102, "xmax": 102, "xip": []}],
            queries=[{"query_id": "q", "node": "primary", "snapshot": "s"}],
        )
        report = validate(catalog)
        self.assertEqual(report.results[0].missing, [])
        self.assertTrue(report.is_consistent)

    def test_long_transaction_protects_dead_tuple(self):
        catalog = build_catalog(
            transactions=[
                {"xid": 100, "start_ts": 1.0, "commit_ts": 2.0, "commit_lsn": "0/100"},
                {"xid": 101, "start_ts": 3.0, "commit_ts": 4.0, "commit_lsn": "0/200"},
                # 长事务一直未提交，保护水位停在 102。
                {"xid": 102, "start_ts": 3.5},
            ],
            versions=[],
            cleaned_versions=[
                {
                    "version_id": "v_old",
                    "row_id": "r1",
                    "xmin": 100,
                    "xmax": 101,
                    "removed_by_node": "primary",
                }
            ],
            nodes=[
                {"name": "primary", "role": "primary", "vacuum_horizon": 105},
            ],
            # 删除事务 101 在快照取得时仍被视为运行中（在 xip 中），
            # 因此死元组对该快照仍然可见，不允许被清理。
            snapshots=[{"name": "s", "xmin": 101, "xmax": 103, "xip": [101, 102], "taken_ts": 5.0}],
            queries=[{"query_id": "q", "node": "primary", "snapshot": "s"}],
        )
        report = validate(catalog)
        result = report.results[0]
        codes = evidence_codes(report)
        self.assertIn(MARKER_PREEMPTED, codes)
        self.assertIn(UNSAFE_VACUUM_HORIZON, codes)
        self.assertEqual([item["version_id"] for item in result.missing], ["v_old"])

    def test_live_tuple_removed_early_is_vacuum_preempted(self):
        catalog = build_catalog(
            transactions=[
                {"xid": 100, "start_ts": 1.0, "commit_ts": 2.0, "commit_lsn": "0/100"},
                {"xid": 102, "start_ts": 3.0},
            ],
            versions=[],
            cleaned_versions=[
                {
                    "version_id": "v_live",
                    "row_id": "r1",
                    "xmin": 100,
                    "xmax": 0,
                    "removed_by_node": "primary",
                }
            ],
            nodes=[{"name": "primary", "role": "primary"}],
            snapshots=[{"name": "s", "xmin": 102, "xmax": 103, "xip": [102]}],
            queries=[{"query_id": "q", "node": "primary", "snapshot": "s"}],
        )
        report = validate(catalog)
        self.assertIn(VACUUM_PREEMPTED, evidence_codes(report))
        self.assertEqual(
            [item["version_id"] for item in report.results[0].missing],
            ["v_live"],
        )

    def test_concurrent_delete_visibility_changes_after_commit(self):
        catalog = build_catalog(
            transactions=[
                {"xid": 100, "start_ts": 1.0, "commit_ts": 2.0, "commit_lsn": "0/100"},
                {"xid": 101, "start_ts": 3.0},
                {"xid": 101, "start_ts": 3.0, "commit_ts": 6.0, "commit_lsn": "0/300"},
            ],
            versions=[{"version_id": "v1", "row_id": "r1", "xmin": 100, "xmax": 101}],
            deletion_markers=[{"version_id": "v1", "row_id": "r1", "xmax": 101}],
            nodes=[{"name": "primary", "role": "primary"}],
            snapshots=[
                {"name": "s_during", "xmin": 101, "xmax": 102, "xip": [101], "taken_ts": 4.0},
                {"name": "s_after", "xmin": 102, "xmax": 102, "xip": [], "taken_ts": 7.0},
            ],
            queries=[
                {"query_id": "q_during", "node": "primary", "snapshot": "s_during"},
                {"query_id": "q_after", "node": "primary", "snapshot": "s_after"},
            ],
        )
        report = validate(catalog)
        during, after = report.results
        self.assertEqual(visible_ids(during), {"v1"})
        self.assertEqual(reasons_for(during)["v1"], VISIBLE_LIVE)
        self.assertEqual(reasons_for(after)["v1"], HIDDEN_DELETED)

    def test_replica_slot_holds_horizon(self):
        catalog = build_catalog(
            transactions=[
                {"xid": 100, "start_ts": 1.0, "commit_ts": 2.0, "commit_lsn": "0/100"},
                {"xid": 101, "start_ts": 3.0, "commit_ts": 4.0, "commit_lsn": "0/200"},
            ],
            versions=[],
            cleaned_versions=[
                {"version_id": "v_old", "row_id": "r1", "xmin": 100, "xmax": 101}
            ],
            nodes=[
                {
                    "name": "replica",
                    "role": "replica",
                    "replayed_lsn": "0/150",
                    "slot_xmin": 101,
                    "vacuum_horizon": 105,
                }
            ],
            snapshots=[{"name": "s", "xmin": 101, "xmax": 102, "xip": [101], "taken_ts": 4.5}],
            queries=[{"query_id": "q", "node": "replica", "snapshot": "s"}],
        )
        report = validate(catalog)
        self.assertIn(MARKER_PREEMPTED, evidence_codes(report))
        self.assertIn(UNSAFE_VACUUM_HORIZON, evidence_codes(report))

    def test_uncommitted_delete_keeps_tuple_live(self):
        catalog = build_catalog(
            transactions=[
                {"xid": 100, "start_ts": 1.0, "commit_ts": 2.0, "commit_lsn": "0/100"},
                {"xid": 101, "start_ts": 3.0},
            ],
            versions=[{"version_id": "v1", "row_id": "r1", "xmin": 100, "xmax": 101}],
            deletion_markers=[{"version_id": "v1", "row_id": "r1", "xmax": 101}],
            nodes=[{"name": "primary", "role": "primary"}],
            snapshots=[{"name": "s", "xmin": 101, "xmax": 102, "xip": [101], "taken_ts": 4.0}],
            queries=[{"query_id": "q", "node": "primary", "snapshot": "s"}],
        )
        report = validate(catalog)
        result = report.results[0]
        self.assertEqual(visible_ids(result), {"v1"})
        self.assertEqual(result.judgements["v1"]["xmax_hint"]["reason"], HIDDEN_IN_TRANSACTION)
