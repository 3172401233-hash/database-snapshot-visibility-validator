"""两次读一致性、副本延迟与同快照多节点分歧。"""

import unittest

from mvcc_validator import validate
from mvcc_validator.reasons import (
    CURRENT_READ_DRIFT,
    HIDDEN_IN_TRANSACTION,
    REPLICA_LAG,
    SNAPSHOT_DIVERGENCE,
)

from .helpers import build_catalog, evidence_codes, hidden_ids, visible_ids


def lag_catalog():
    return build_catalog(
        transactions=[
            {"xid": 100, "start_ts": 1.0, "commit_ts": 2.0, "commit_lsn": "0/100"},
            {"xid": 101, "start_ts": 3.0, "commit_ts": 5.0, "commit_lsn": "0/200"},
        ],
        versions=[
            {"version_id": "v100", "row_id": "r1", "xmin": 100},
            {"version_id": "v101", "row_id": "r2", "xmin": 101},
        ],
        nodes=[
            {"name": "primary", "role": "primary"},
            {"name": "replica", "role": "replica", "replayed_lsn": "0/100"},
        ],
        snapshots=[
            {
                "name": "s_logical",
                "xmin": 102,
                "xmax": 102,
                "xip": [],
                "taken_at_node": "primary",
                "taken_lsn": "0/200",
                "taken_ts": 5.0,
            }
        ],
        queries=[
            {"query_id": "q_primary", "node": "primary", "snapshot": "s_logical"},
            {"query_id": "q_replica", "node": "replica", "snapshot": "s_logical"},
            {"query_id": "q_current_replica", "node": "replica", "mode": "current", "at_ts": 5.0},
        ],
    )


class ConsistencyTest(unittest.TestCase):
    def test_same_snapshot_twice_is_stable(self):
        catalog = lag_catalog()
        from mvcc_validator import Query

        catalog.queries = [
            Query(query_id="q_first", node="primary", mode="snapshot", snapshot="s_logical"),
            Query(query_id="q_repeat", node="primary", mode="snapshot", snapshot="s_logical"),
        ]
        report = validate(catalog)
        self.assertEqual(
            visible_ids(report.results[0]),
            visible_ids(report.results[1]),
        )
        self.assertTrue(report.is_consistent)

    def test_replica_lag_hides_committed_version(self):
        report = validate(lag_catalog())
        primary_result, replica_result, current_replica = report.results

        self.assertEqual(visible_ids(primary_result), {"v100", "v101"})
        self.assertEqual(visible_ids(replica_result), {"v100"})
        self.assertEqual(visible_ids(current_replica), {"v100"})

        reasons = {
            key: value["reason"] for key, value in replica_result.judgements.items()
        }
        self.assertEqual(reasons["v101"], REPLICA_LAG)

        current_reasons = {
            key: value["reason"] for key, value in current_replica.judgements.items()
        }
        self.assertEqual(current_reasons["v101"], HIDDEN_IN_TRANSACTION)
        self.assertEqual(current_replica.snapshot_watermark["xip"], [101])

        codes = evidence_codes(report)
        self.assertIn(REPLICA_LAG, codes)
        self.assertFalse(report.is_consistent)

    def test_current_read_drift_is_informational_only(self):
        catalog = lag_catalog()
        from mvcc_validator import Query

        catalog.queries = [
            Query(query_id="cur_primary", node="primary", mode="current", at_ts=5.0),
            Query(query_id="cur_replica", node="replica", mode="current", at_ts=5.0),
        ]
        report = validate(catalog)
        codes = evidence_codes(report)
        self.assertIn(CURRENT_READ_DRIFT, codes)
        self.assertTrue(
            all(item.severity != "violation" for item in report.evidence)
        )

    def test_divergence_without_lag_is_separate_evidence(self):
        catalog = build_catalog(
            transactions=[
                {"xid": 100, "start_ts": 1.0, "commit_ts": 2.0, "commit_lsn": "0/100"},
                {"xid": 101, "start_ts": 3.0, "commit_ts": 9.0, "commit_lsn": "0/900"},
            ],
            # 主库上版本仍在；副本 n2 的同名版本已被本地提前清理。
            versions=[{"version_id": "v100", "row_id": "r1", "xmin": 100, "xmax": 101}],
            deletion_markers=[{"version_id": "v100", "row_id": "r1", "xmax": 101}],
            cleaned_versions=[
                {
                    "version_id": "v100",
                    "row_id": "r1",
                    "xmin": 100,
                    "xmax": 101,
                    "removed_by_node": "n2",
                }
            ],
            nodes=[
                {"name": "n1", "role": "primary"},
                {"name": "n2", "role": "replica", "replayed_lsn": "0/999"},
            ],
            snapshots=[
                # 删除事务 101 在快照之后才提交，因此该版本对两个节点都应可见；
                # 副本却已经把它清理掉，属于与复制延迟无关的快照分歧。
                {"name": "s", "xmin": 101, "xmax": 102, "xip": [101]},
            ],
            queries=[
                {"query_id": "q1", "node": "n1", "snapshot": "s"},
                {"query_id": "q2", "node": "n2", "snapshot": "s"},
            ],
        )
        report = validate(catalog)
        self.assertEqual(visible_ids(report.results[0]), {"v100"})
        self.assertEqual(hidden_ids(report.results[1]), {"v100"})
        self.assertIn(SNAPSHOT_DIVERGENCE, evidence_codes(report))
