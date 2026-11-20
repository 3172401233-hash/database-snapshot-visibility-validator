"""校验器的输入数据模型与字典解析。

所有结构均为纯 Python dataclass，输入可以通过 :meth:`Catalog.from_dict`
从普通字典构建，方便从 JSON / 测试用例构造场景，全程不连接数据库。
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from . import xid

LSN_INFINITY = float("inf")

STATUS_COMMITTED = "committed"
STATUS_ABORTED = "aborted"
STATUS_IN_PROGRESS = "in_progress"


def parse_lsn(value):
    """解析 LSN，支持 ``"0/16B3748"``、整数和 ``None``（视为无穷大）。"""
    if value is None:
        return LSN_INFINITY
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if "/" not in text:
        return float(int(text, 0))
    high, low = text.split("/", 1)
    return (int(high, 16) << 32) | int(low, 16)


@dataclass
class Transaction:
    """一个事务的开始 / 提交 / 中止元数据。"""

    xid: int
    start_ts: float
    commit_ts: Optional[float] = None
    abort_ts: Optional[float] = None
    commit_lsn: Optional[float] = None

    @property
    def status(self):
        if self.abort_ts is not None:
            return STATUS_ABORTED
        if self.commit_ts is not None:
            return STATUS_COMMITTED
        return STATUS_IN_PROGRESS

    @property
    def end_ts(self):
        return self.commit_ts if self.commit_ts is not None else self.abort_ts

    @classmethod
    def from_dict(cls, data):
        return cls(
            xid=xid.normalize(data["xid"]),
            start_ts=float(data["start_ts"]),
            commit_ts=(float(data["commit_ts"]) if data.get("commit_ts") is not None else None),
            abort_ts=(float(data["abort_ts"]) if data.get("abort_ts") is not None else None),
            commit_lsn=(
                parse_lsn(data["commit_lsn"])
                if data.get("commit_lsn") is not None
                else None
            ),
        )


@dataclass
class Snapshot:
    """事务开始时取得的快照水位（xmin, xmax, 运行中事务集合）。"""

    name: str
    xmin: int
    xmax: int
    xip: List[int] = field(default_factory=list)
    taken_at_node: Optional[str] = None
    taken_lsn: Optional[float] = None
    taken_ts: Optional[float] = None

    @property
    def xip_set(self):
        return {xid.normalize(value) for value in self.xip}

    @classmethod
    def from_dict(cls, data):
        return cls(
            name=data["name"],
            xmin=xid.normalize(data["xmin"]),
            xmax=xid.normalize(data["xmax"]),
            xip=[xid.normalize(value) for value in data.get("xip", [])],
            taken_at_node=data.get("taken_at_node"),
            taken_lsn=(parse_lsn(data["taken_lsn"]) if data.get("taken_lsn") is not None else None),
            taken_ts=(float(data["taken_ts"]) if data.get("taken_ts") is not None else None),
        )


@dataclass
class Node:
    """查询读取的节点：复制回放位置、清理水位和复制槽保护点。"""

    name: str
    role: str = "primary"
    replayed_lsn: float = LSN_INFINITY
    vacuum_horizon: Optional[int] = None
    slot_xmin: Optional[int] = None

    @property
    def is_primary(self):
        return self.role == "primary"

    @classmethod
    def from_dict(cls, data):
        return cls(
            name=data["name"],
            role=data.get("role", "primary"),
            replayed_lsn=parse_lsn(data.get("replayed_lsn")),
            vacuum_horizon=(
                xid.normalize(data["vacuum_horizon"])
                if data.get("vacuum_horizon") is not None
                else None
            ),
            slot_xmin=(
                xid.normalize(data["slot_xmin"])
                if data.get("slot_xmin") is not None
                else None
            ),
        )


@dataclass
class RowVersion:
    """一行的一个物理版本（元组）。

    ``xmax`` 为 0 表示仍存活，否则由删除标记解释删除事务。
    """

    version_id: str
    row_id: str
    xmin: int
    xmax: int = 0

    @classmethod
    def from_dict(cls, data):
        return cls(
            version_id=data["version_id"],
            row_id=data["row_id"],
            xmin=xid.normalize(data["xmin"]),
            xmax=xid.normalize(data.get("xmax", 0)),
        )


@dataclass
class DeletionMarker:
    """与版本配套的删除标记。"""

    version_id: str
    row_id: str
    xmax: int

    @classmethod
    def from_dict(cls, data):
        return cls(
            version_id=data["version_id"],
            row_id=data["row_id"],
            xmax=xid.normalize(data["xmax"]),
        )


@dataclass
class CleanedVersion:
    """已被后台清理（VACUUM / 死元组回收）移除的版本记录。"""

    version_id: str
    row_id: str
    xmin: int
    xmax: int = 0
    removed_by_node: Optional[str] = None

    @classmethod
    def from_dict(cls, data):
        return cls(
            version_id=data["version_id"],
            row_id=data["row_id"],
            xmin=xid.normalize(data["xmin"]),
            xmax=xid.normalize(data.get("xmax", 0)),
            removed_by_node=data.get("removed_by_node"),
        )


@dataclass
class Query:
    """一次查询：在哪个节点、用哪个快照（或当前读）读取哪些行。"""

    query_id: str
    node: str
    mode: str = "snapshot"
    snapshot: Optional[str] = None
    at_lsn: Optional[float] = None
    at_ts: Optional[float] = None
    row_ids: Optional[List[str]] = None

    @property
    def is_current_read(self):
        return self.mode == "current"

    @classmethod
    def from_dict(cls, data):
        mode = data.get("mode", "snapshot")
        return cls(
            query_id=data["query_id"],
            node=data["node"],
            mode=mode,
            snapshot=data.get("snapshot"),
            at_lsn=(parse_lsn(data["at_lsn"]) if data.get("at_lsn") is not None else None),
            at_ts=(float(data["at_ts"]) if data.get("at_ts") is not None else None),
            row_ids=list(data["row_ids"]) if data.get("row_ids") is not None else None,
        )


@dataclass
class Catalog:
    """一次校验所需的完整输入目录。"""

    transactions: Dict[int, Transaction] = field(default_factory=dict)
    snapshots: Dict[str, Snapshot] = field(default_factory=dict)
    nodes: Dict[str, Node] = field(default_factory=dict)
    versions: List[RowVersion] = field(default_factory=list)
    markers: List[DeletionMarker] = field(default_factory=list)
    cleaned: List[CleanedVersion] = field(default_factory=list)
    queries: List[Query] = field(default_factory=list)

    def transaction(self, target):
        return self.transactions.get(xid.normalize(target))

    def marker_for(self, version_id):
        for marker in self.markers:
            if marker.version_id == version_id:
                return marker
        return None

    @classmethod
    def from_dict(cls, data):
        catalog = cls()
        catalog.transactions = {
            xid.normalize(item["xid"]): Transaction.from_dict(item)
            for item in data.get("transactions", [])
        }
        catalog.snapshots = {
            item["name"]: Snapshot.from_dict(item) for item in data.get("snapshots", [])
        }
        catalog.nodes = {
            item["name"]: Node.from_dict(item) for item in data.get("nodes", [])
        }
        catalog.versions = [RowVersion.from_dict(item) for item in data.get("versions", [])]
        catalog.markers = [DeletionMarker.from_dict(item) for item in data.get("deletion_markers", [])]
        catalog.cleaned = [CleanedVersion.from_dict(item) for item in data.get("cleaned_versions", [])]
        catalog.queries = [Query.from_dict(item) for item in data.get("queries", [])]
        return catalog
