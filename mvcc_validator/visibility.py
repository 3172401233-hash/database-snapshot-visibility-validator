"""MVCC 可见性判定核心。

实现 PostgreSQL 风格的快照判定规则，并额外考虑副本回放延迟：
提交事件只有在查询节点已经回放对应 LSN 后才对该节点生效。
"""

from dataclasses import dataclass
from typing import Optional

from . import reasons, xid
from .model import (
    LSN_INFINITY,
    STATUS_ABORTED,
    STATUS_COMMITTED,
    STATUS_IN_PROGRESS,
    Node,
    RowVersion,
    Snapshot,
)


@dataclass
class Hint:
    """单个 XID 对某节点上某快照的判定结果。"""

    visible: bool
    reason: str
    detail: str


@dataclass
class VersionJudgement:
    """单个行版本对一次查询的判定结果。"""

    version_id: str
    row_id: str
    visible: bool
    reason: str
    detail: str
    xmin_hint: Optional[Hint] = None
    xmax_hint: Optional[Hint] = None


def effective_replay_lsn(node, snapshot=None, query=None):
    """节点对本次查询实际可见的最大提交 LSN。

    主库不受回放限制；副本同时受自身回放位置和快照取得位置约束，
    因此快照在落后副本上重放时，提交可见性会被进一步收紧。
    """
    if node.is_primary:
        return LSN_INFINITY
    upper = node.replayed_lsn
    if snapshot is not None and snapshot.taken_lsn is not None:
        upper = min(upper, snapshot.taken_lsn)
    if query is not None and query.at_lsn is not None:
        upper = min(upper, query.at_lsn)
    return upper


def _commit_replayed(transaction, replay_lsn):
    if transaction.commit_lsn is None:
        # 没有记录 LSN 时退化为“已提交即可见”，主库路径才会走到这里。
        return True
    return transaction.commit_lsn <= replay_lsn


def judge_xid(target, snapshot, catalog, node, query=None):
    """判断 XID 在给定节点 / 快照下是否已提交可见。"""
    target = xid.normalize(target)

    replay_lsn = effective_replay_lsn(node, snapshot, query)

    # 窗口判断优先于保留 XID 的特殊语义：若保留 XID 恰好落在快照窗口内
    # （例如回绕后 xmax=0），它对该快照仍是不可见的未来事务。
    if not xid.in_range(target, snapshot.xmin, snapshot.xmax):
        target_age = xid.age(target, snapshot.xmax)
        floor_age = xid.age(snapshot.xmin, snapshot.xmax)
        if target_age == 0 or target_age < floor_age:
            return Hint(False, reasons.HIDDEN_FUTURE_XID, "XID 大于等于快照 xmax，属于未来事务")

    if target == xid.FROZEN_XID:
        return Hint(True, reasons.HIDDEN_FROZEN, "冻结 XID，始终视为已提交")
    if target in (xid.INVALID_XID, xid.BOOTSTRAP_XID):
        return Hint(True, reasons.VISIBLE_INSERT, "特殊保留 XID，视为始终可见")

    def _finished_hint(transaction, old):
        if transaction is None:
            if old:
                return Hint(True, reasons.VISIBLE_LIVE, "早于快照 xmin 的已完成事务")
            return Hint(False, reasons.UNKNOWN_XID, "窗口内 XID 缺少事务事件记录，按不可见处理")
        if transaction.status == STATUS_ABORTED:
            return Hint(False, reasons.HIDDEN_ABORTED, "事务已回滚")
        if transaction.status == STATUS_IN_PROGRESS:
            return Hint(False, reasons.HIDDEN_IN_TRANSACTION, "事务仍在进行中")
        if not node.is_primary and not _commit_replayed(transaction, replay_lsn):
            return Hint(
                False,
                reasons.REPLICA_LAG,
                "提交 LSN %.0f 尚未在节点 %s 回放（回放位置 %.0f）"
                % (transaction.commit_lsn, node.name, replay_lsn),
            )
        return Hint(
            True,
            reasons.VISIBLE_LIVE,
            "事务在快照取得前已提交" if old else "事务已提交且不在快照运行集合中",
        )

    if xid.in_range(target, snapshot.xmin, snapshot.xmax):
        # 落在 [xmin, xmax) 窗口内：xip 中的事务在快照取得时尚未提交。
        if target in snapshot.xip_set:
            return Hint(False, reasons.HIDDEN_IN_TRANSACTION, "XID 位于快照运行集合 xip 中")
        return _finished_hint(catalog.transaction(target), old=False)

    # 窗口之外：xmax 本身以及比 xmin 更“年轻”的属于未来事务，
    # 其余是快照取得前已经结束的旧事务。
    target_age = xid.age(target, snapshot.xmax)
    floor_age = xid.age(snapshot.xmin, snapshot.xmax)
    if target_age == 0 or target_age < floor_age:
        return Hint(False, reasons.HIDDEN_FUTURE_XID, "XID 大于等于快照 xmax，属于未来事务")
    return _finished_hint(catalog.transaction(target), old=True)


def judge_version(version, snapshot, catalog, node, query=None, marker_override=None):
    """按 MVCC 规则判定一个现存版本对查询是否可见。"""
    insert_hint = judge_xid(version.xmin, snapshot, catalog, node, query)
    if not insert_hint.visible:
        return VersionJudgement(
            version.version_id,
            version.row_id,
            False,
            insert_hint.reason,
            insert_hint.detail,
            xmin_hint=insert_hint,
        )

    if version.xmax == xid.INVALID_XID:
        return VersionJudgement(
            version.version_id,
            version.row_id,
            True,
            reasons.VISIBLE_LIVE,
            "插入已提交且无删除标记",
            xmin_hint=insert_hint,
        )

    marker = marker_override
    if marker is None:
        marker = catalog.marker_for(version.version_id)
    if marker is not None and marker.xmax != version.xmax:
        return VersionJudgement(
            version.version_id,
            version.row_id,
            True,
            reasons.MARKER_XMAX_MISMATCH,
            "版本 xmax=%d 与删除标记 xmax=%d 不一致，删除不予采信" % (version.xmax, marker.xmax),
            xmin_hint=insert_hint,
        )

    delete_hint = judge_xid(version.xmax, snapshot, catalog, node, query)
    if delete_hint.visible:
        return VersionJudgement(
            version.version_id,
            version.row_id,
            False,
            reasons.HIDDEN_DELETED,
            "删除事务 %d 对快照可见" % version.xmax,
            xmin_hint=insert_hint,
            xmax_hint=delete_hint,
        )

    detail = "删除事务 %d 对快照不可见（%s），版本仍存活" % (version.xmax, delete_hint.reason)
    return VersionJudgement(
        version.version_id,
        version.row_id,
        True,
        reasons.VISIBLE_LIVE,
        detail,
        xmin_hint=insert_hint,
        xmax_hint=delete_hint,
    )


def _finished_before(transaction, lsn_limit):
    """事务是否在给定 LSN 位置之前结束（提交且已回放，或已回滚）。"""
    if transaction.status == STATUS_ABORTED:
        return True
    if transaction.status != STATUS_COMMITTED:
        return False
    if transaction.commit_lsn is None:
        return True
    return transaction.commit_lsn <= lsn_limit


def build_current_snapshot(name, catalog, node, query=None):
    """为当前读在指定节点上即时构造一个快照水位。

    副本上尚未回放提交的事务在该节点视角下仍属于运行中事务，
    因此会进入 xip，这正是复制延迟影响可见性的入口。
    """
    replay_lsn = effective_replay_lsn(node, None, query)
    active = []
    for transaction in catalog.transactions.values():
        if not _finished_before(transaction, replay_lsn):
            active.append(transaction.xid)

    if active:
        reference = max(active)
        xmin = xid.oldest(active, reference)
    else:
        xmin = xid.next_xid(max(catalog.transactions.keys())) if catalog.transactions else xid.FIRST_NORMAL_XID
    # xmax 跟随全局最新分配的 XID（含已结束事务），
    # 这样已提交但不在 xip 中的事务仍落在快照窗口内并按已提交判定。
    latest = max(catalog.transactions.keys()) if catalog.transactions else xid.FIRST_NORMAL_XID
    xmax = xid.next_xid(latest)

    return Snapshot(
        name=name,
        xmin=xmin,
        xmax=xmax,
        xip=sorted(active),
        taken_at_node=node.name,
        taken_lsn=(None if node.is_primary else replay_lsn),
        taken_ts=(query.at_ts if query is not None else None),
    )


def protected_horizon(catalog, reference_xid=None):
    """计算集群层面必须被清理保护的最旧 XID。

    取所有长事务（运行中事务）、所有快照登记的 ``xmin``（持有旧快照的
    后端会把自己的 xmin 注册到进程数组）与复制槽 ``slot_xmin`` 中最旧的一个。
    任何仍被该水位保护的已提交版本都不允许被清理。
    """
    candidates = []
    for transaction in catalog.transactions.values():
        if transaction.status == STATUS_IN_PROGRESS:
            candidates.append(transaction.xid)
    for snapshot in catalog.snapshots.values():
        candidates.append(snapshot.xmin)
    for node in catalog.nodes.values():
        if node.slot_xmin is not None:
            candidates.append(node.slot_xmin)

    if not candidates:
        return None

    if reference_xid is None:
        reference_xid = max(candidates)
    return xid.oldest(candidates, reference_xid)


def is_removable(version_or_cleaned, horizon):
    """版本是否已老于清理保护水位，可以被合法回收。

    活元组（xmax=0）永远不会被 VACUUM 回收；死元组则要求插入与删除
    （若有）都已提交且年龄严格大于 0，即没有任何仍存活快照可能把
    对应 XID 视为运行中。
    """
    if horizon is None:
        return False
    if version_or_cleaned.xmax in (0, xid.INVALID_XID):
        return False
    if xid.age(version_or_cleaned.xmin, horizon) <= 0:
        return False
    if xid.age(version_or_cleaned.xmax, horizon) <= 0:
        return False
    return True
