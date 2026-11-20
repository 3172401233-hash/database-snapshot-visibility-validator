"""MVCC 快照可见性校验器（纯内存模拟，不连接数据库）。"""

from .model import (
    Catalog,
    CleanedVersion,
    DeletionMarker,
    Node,
    Query,
    RowVersion,
    Snapshot,
    Transaction,
    parse_lsn,
)
from .reasons import VIOLATIONS
from .validator import Evidence, QueryResult, ValidationReport, validate
from .visibility import (
    VersionJudgement,
    build_current_snapshot,
    is_removable,
    judge_version,
    judge_xid,
    protected_horizon,
)
from .xid import HALF, MOD, age, cmp, in_range, next_xid, normalize, oldest

__all__ = [
    "Catalog",
    "CleanedVersion",
    "DeletionMarker",
    "Evidence",
    "Node",
    "Query",
    "QueryResult",
    "RowVersion",
    "Snapshot",
    "Transaction",
    "ValidationReport",
    "VersionJudgement",
    "VIOLATIONS",
    "age",
    "build_current_snapshot",
    "cmp",
    "in_range",
    "is_removable",
    "judge_version",
    "judge_xid",
    "next_xid",
    "normalize",
    "oldest",
    "parse_lsn",
    "protected_horizon",
    "validate",
    "HALF",
    "MOD",
]
