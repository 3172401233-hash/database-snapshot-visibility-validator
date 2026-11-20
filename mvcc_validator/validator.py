"""校验编排：把输入目录转换为逐查询结果与一致性证据。"""

from dataclasses import asdict, dataclass, field
from typing import Dict, List, Optional

from . import reasons, xid
from .model import Catalog, DeletionMarker, RowVersion
from .visibility import (
    build_current_snapshot,
    is_removable,
    judge_version,
    protected_horizon,
)

SEVERITY_VIOLATION = "violation"
SEVERITY_INFO = "info"


@dataclass
class Evidence:
    """一条可追溯的违规或信息性证据。"""

    code: str
    severity: str
    message: str
    query_id: Optional[str] = None
    node: Optional[str] = None
    snapshot: Optional[str] = None
    version_id: Optional[str] = None
    details: Dict = field(default_factory=dict)


@dataclass
class MissingVersion:
    """查询本应看到、但已被清理掉的版本。"""

    version_id: str
    row_id: str
    reason: str
    detail: str


@dataclass
class QueryResult:
    """单次查询的可见 / 隐藏版本清单与缺失版本证据。"""

    query_id: str
    node: str
    mode: str
    snapshot_name: Optional[str]
    snapshot_watermark: Dict
    visible: List[Dict]
    hidden: List[Dict]
    missing: List[Dict]
    judgements: Dict[str, Dict]


@dataclass
class ValidationReport:
    results: List[QueryResult]
    evidence: List[Evidence]

    @property
    def violations(self):
        return [item for item in self.evidence if item.severity == SEVERITY_VIOLATION]

    @property
    def is_consistent(self):
        return not self.violations

    def to_dict(self):
        return asdict(self)


def _judgement_to_dict(judgement):
    payload = {
        "version_id": judgement.version_id,
        "row_id": judgement.row_id,
        "visible": judgement.visible,
        "reason": judgement.reason,
        "detail": judgement.detail,
    }
    if judgement.xmin_hint is not None:
        payload["xmin_hint"] = asdict(judgement.xmin_hint)
    if judgement.xmax_hint is not None:
        payload["xmax_hint"] = asdict(judgement.xmax_hint)
    return payload


def _watermark(snapshot):
    return {
        "xmin": snapshot.xmin,
        "xmax": snapshot.xmax,
        "xip": sorted(snapshot.xip),
        "taken_at_node": snapshot.taken_at_node,
        "taken_lsn": snapshot.taken_lsn,
    }


def _row_in_scope(catalog, query, row_id):
    if query.row_ids is None:
        return True
    return row_id in query.row_ids


def _check_xip_snapshot(snapshot, catalog, evidence, query_id, node_name):
    """快照 xip 与事务事件矛盾时给出证据（提交了却仍挂在运行集合里）。"""
    if snapshot.taken_ts is None:
        return
    for xid_value in snapshot.xip_set:
        transaction = catalog.transaction(xid_value)
        if transaction is None:
            continue
        if (
            transaction.commit_ts is not None
            and transaction.commit_ts <= snapshot.taken_ts
            and xid.in_range(xid_value, snapshot.xmin, snapshot.xmax)
        ):
            evidence.append(
                Evidence(
                    reasons.XIP_SET_MISMATCH,
                    SEVERITY_VIOLATION,
                    "事务 %d 已在 %.3f 提交，却仍出现在快照 %s 的 xip 中"
                    % (xid_value, transaction.commit_ts, snapshot.name),
                    query_id=query_id,
                    node=node_name,
                    snapshot=snapshot.name,
                    details={"xid": xid_value, "xip": sorted(snapshot.xip)},
                )
            )


def _check_vacuum_horizons(catalog, horizon, evidence):
    """节点清理水位越过集群保护水位时属于不安全清理。"""
    if horizon is None:
        return
    for node in catalog.nodes.values():
        if node.vacuum_horizon is None:
            continue
        distance = xid.age(horizon, node.vacuum_horizon)
        if 0 < distance < xid.HALF:
            evidence.append(
                Evidence(
                    reasons.UNSAFE_VACUUM_HORIZON,
                    SEVERITY_VIOLATION,
                    "节点 %s 的清理水位 %d 越过保护水位 %d，长事务 / 复制槽仍需要旧版本"
                    % (node.name, node.vacuum_horizon, horizon),
                    node=node.name,
                    details={
                        "vacuum_horizon": node.vacuum_horizon,
                        "protected_horizon": horizon,
                    },
                )
            )


def _cleaned_violation_code(cleaned):
    if cleaned.xmax in (0, xid.INVALID_XID):
        return reasons.VACUUM_PREEMPTED
    return reasons.MARKER_PREEMPTED


def _consistency_checks(results, evidence):
    """同名快照在不同节点必须给出相同可见集合；当前读允许漂移但留痕。"""
    snapshot_groups: Dict[str, List[QueryResult]] = {}
    current_reads: List[QueryResult] = []

    for result in results:
        if result.mode == "snapshot":
            snapshot_groups.setdefault(result.snapshot_name, []).append(result)
        else:
            current_reads.append(result)

    for snapshot_name, group in snapshot_groups.items():
        baseline = group[0]
        baseline_visible = {item["version_id"] for item in baseline.visible}
        for other in group[1:]:
            other_visible = {item["version_id"] for item in other.visible}
            if baseline_visible == other_visible:
                continue
            missing_here = baseline_visible - other_visible
            extra_here = other_visible - baseline_visible
            lag_driven = any(
                other.judgements.get(version_id, {}).get("reason") == reasons.REPLICA_LAG
                or baseline.judgements.get(version_id, {}).get("reason") == reasons.REPLICA_LAG
                for version_id in missing_here | extra_here
            )
            # 节点本地提前清理也会让版本只在一侧消失，但它的原因是
            # HIDDEN_VACUUMED / 抢占证据，不应归为复制延迟。
            vacuumed_ids = {
                item["version_id"]
                for item in other.hidden + baseline.hidden
                if item["reason"] == reasons.HIDDEN_VACUUMED
            }
            vacuum_driven = bool(vacuumed_ids & (missing_here | extra_here))
            if vacuum_driven:
                lag_driven = False
            code = reasons.REPLICA_LAG if lag_driven else reasons.SNAPSHOT_DIVERGENCE
            evidence.append(
                Evidence(
                    code,
                    SEVERITY_VIOLATION,
                    "快照 %s 在节点 %s 与 %s 上可见版本不一致"
                    % (snapshot_name, baseline.node, other.node),
                    query_id=other.query_id,
                    node=other.node,
                    snapshot=snapshot_name,
                    details={
                        "baseline_node": baseline.node,
                        "only_on_baseline": sorted(missing_here),
                        "only_on_other": sorted(extra_here),
                    },
                )
            )

    if len(current_reads) > 1:
        baseline = current_reads[0]
        baseline_visible = {item["version_id"] for item in baseline.visible}
        for other in current_reads[1:]:
            other_visible = {item["version_id"] for item in other.visible}
            if baseline_visible != other_visible:
                evidence.append(
                    Evidence(
                        reasons.CURRENT_READ_DRIFT,
                        SEVERITY_INFO,
                        "当前读在节点 %s 与 %s 上结果不同（当前读不保证快照一致，仅记录漂移）"
                        % (baseline.node, other.node),
                        query_id=other.query_id,
                        node=other.node,
                        details={
                            "baseline_node": baseline.node,
                            "only_on_baseline": sorted(baseline_visible - other_visible),
                            "only_on_other": sorted(other_visible - baseline_visible),
                        },
                    )
                )


def validate(catalog):
    """执行完整校验，返回 :class:`ValidationReport`。"""
    evidence: List[Evidence] = []
    horizon = protected_horizon(catalog)
    _check_vacuum_horizons(catalog, horizon, evidence)

    results: List[QueryResult] = []

    for query in catalog.queries:
        node = catalog.nodes[query.node]
        if query.is_current_read:
            snapshot = build_current_snapshot(
                "current@%s" % query.query_id, catalog, node, query
            )
            snapshot_name = None
        else:
            if query.snapshot not in catalog.snapshots:
                raise KeyError("查询 %s 引用了不存在的快照 %s" % (query.query_id, query.snapshot))
            snapshot = catalog.snapshots[query.snapshot]
            snapshot_name = snapshot.name
            _check_xip_snapshot(snapshot, catalog, evidence, query.query_id, node.name)

        visible = []
        hidden = []
        judgements = {}

        for version in catalog.versions:
            if not _row_in_scope(catalog, query, version.row_id):
                continue
            judgement = judge_version(version, snapshot, catalog, node, query)
            judgements[judgement.version_id] = _judgement_to_dict(judgement)
            target = visible if judgement.visible else hidden
            target.append(_judgement_to_dict(judgement))

            if judgement.reason == reasons.UNKNOWN_XID:
                evidence.append(
                    Evidence(
                        reasons.UNKNOWN_XID,
                        SEVERITY_VIOLATION,
                        "版本 %s 引用了缺少事务事件的 XID，无法确认 MVCC 可见性"
                        % judgement.version_id,
                        query_id=query.query_id,
                        node=node.name,
                        snapshot=snapshot_name,
                        version_id=judgement.version_id,
                    )
                )

        missing = []
        local_hidden = []
        locally_removed = set()
        for cleaned in catalog.cleaned:
            if not _row_in_scope(catalog, query, cleaned.row_id):
                continue
            # 清理是节点本地的物理状态：只有执行清理的节点丢失该版本，
            # 其他节点的同名快照仍应读到它。
            if cleaned.removed_by_node is not None and cleaned.removed_by_node != node.name:
                continue
            simulated = RowVersion(
                version_id=cleaned.version_id,
                row_id=cleaned.row_id,
                xmin=cleaned.xmin,
                xmax=cleaned.xmax,
            )
            marker_override = None
            if cleaned.xmax not in (0, xid.INVALID_XID):
                # 回收的是带删除标记的死元组，需要补回标记才能正确复现判定。
                marker_override = DeletionMarker(
                    version_id=cleaned.version_id,
                    row_id=cleaned.row_id,
                    xmax=cleaned.xmax,
                )
            judgement = judge_version(
                simulated, snapshot, catalog, node, query, marker_override=marker_override
            )
            if not judgement.visible:
                continue

            # 对该节点而言版本物理上已经不存在，因此即使逻辑上可见，
            # 实际返回给查询的结果也只能是“被隐藏”。
            locally_removed.add(cleaned.version_id)
            local_hidden.append(
                {
                    "version_id": cleaned.version_id,
                    "row_id": cleaned.row_id,
                    "visible": False,
                    "reason": reasons.HIDDEN_VACUUMED,
                    "detail": "版本已在节点 %s 被清理" % (cleaned.removed_by_node or node.name),
                }
            )

            if is_removable(cleaned, horizon):
                continue
            missing.append(
                asdict(
                    MissingVersion(
                        cleaned.version_id,
                        cleaned.row_id,
                        _cleaned_violation_code(cleaned),
                        "版本仍受保护水位 %s 保护，却已被节点 %s 清理，且本查询本应读到它"
                        % (horizon, cleaned.removed_by_node or node.name),
                    )
                )
            )
            evidence.append(
                Evidence(
                    _cleaned_violation_code(cleaned),
                    SEVERITY_VIOLATION,
                    "版本 %s 在查询 %s 中本应可见，但已被提前清理"
                    % (cleaned.version_id, query.query_id),
                    query_id=query.query_id,
                    node=node.name,
                    snapshot=snapshot_name,
                    version_id=cleaned.version_id,
                    details={
                        "protected_horizon": horizon,
                        "removed_by_node": cleaned.removed_by_node,
                        "xmin": cleaned.xmin,
                        "xmax": cleaned.xmax,
                    },
                )
            )

        results.append(
            QueryResult(
                query_id=query.query_id,
                node=node.name,
                mode=query.mode,
                snapshot_name=snapshot_name,
                snapshot_watermark=_watermark(snapshot),
            visible=[item for item in visible if item["version_id"] not in locally_removed],
            hidden=hidden + local_hidden,
                missing=missing,
                judgements=judgements,
            )
        )

    _consistency_checks(results, evidence)
    return ValidationReport(results=results, evidence=evidence)
