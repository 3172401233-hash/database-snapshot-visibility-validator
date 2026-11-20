# database-snapshot-visibility-validator

用纯 Python 实现的 **MVCC 快照可见性校验器**。给定事务事件、事务 ID
区间、行版本、删除标记、快照水位、清理水位、复制位置和查询节点，判断
每个行版本对指定快照是否可见，并输出违反快照一致性的证据。

## 重要边界

本校验器 **不连接任何数据库，也不执行 SQL**。它是一个离线的、纯内存的
规则模拟器：调用方把数据库内部状态（事务开始/提交/中止事件、
`xmin/xmax/xip` 水位、VACUUM 清理记录、副本回放 LSN 等）整理成字典或
dataclass 传入，校验器按照 PostgreSQL 风格的 MVCC 规则复算可见性。
它适合做规则推演、故障复盘、教学验证和单元测试，不能用来直接巡检线上库。

## 判定模型

- **快照读（snapshot read）**：使用输入中给定的 `(xmin, xmax, xip)`
  水位，严格复算快照取得时刻的可见性。
- **当前读（current read）**：根据事务事件和节点回放位置即时构造水位，
  副本上未回放提交的事务会进入 `xip`，因此同一时刻不同节点可能读到不同结果。
- **XID 可见性**：
  - `xid >= xmax`（回绕安全的比较）→ 未来事务，隐藏；
  - `xid < xmin` → 快照前已结束，按提交/回滚/回放状态判定；
  - `xmin <= xid < xmax` 且在 `xip` 中 → 快照取得时仍在运行，隐藏；
  - 其余窗口内事务按事件表判定提交、回滚或未知。
- **复制延迟**：副本只有回放位置（`replayed_lsn`）覆盖事务的
  `commit_lsn` 后，该提交才在该节点生效；未回放时给出 `REPLICA_LAG` 证据。
- **删除标记**：`xmax` 对应的删除事务可见则版本被隐藏，否则版本仍存活；
  标记与版本 `xmax` 不一致时给出 `MARKER_XMAX_MISMATCH`，删除不予采信。
- **清理保护**：保护水位取运行中长事务、各快照登记的 `xmin` 和复制槽
  `slot_xmin` 中最旧的一个。仍受保护却被清理的版本产生
  `VACUUM_PREEMPTED`（活元组）或 `MARKER_PREEMPTED`（死元组）证据；
  节点清理水位越过保护水位产生 `UNSAFE_VACUUM_HORIZON`。
- **XID 回绕**：所有比较基于模 `2^32` 的循环年龄（`mvcc_validator.xid`），
  并对 `0/1/2` 三个保留 XID（Invalid / Bootstrap / Frozen）单独处理。

## 输入结构

通过 `Catalog.from_dict` 构建，字段名与数据库概念对应：

```python
from mvcc_validator import Catalog, validate

catalog = Catalog.from_dict({
    "transactions": [
        {"xid": 100, "start_ts": 1.0, "commit_ts": 2.0, "commit_lsn": "0/100"},
        {"xid": 101, "start_ts": 3.0},                      # 仍在运行
        {"xid": 102, "start_ts": 4.0, "abort_ts": 6.0},     # 回滚
    ],
    "snapshots": [
        {"name": "s1", "xmin": 101, "xmax": 103, "xip": [101, 102],
         "taken_ts": 5.0, "taken_lsn": "0/180",
         "taken_at_node": "primary"},
    ],
    "nodes": [
        {"name": "primary", "role": "primary"},
        {"name": "replica", "role": "replica",
         "replayed_lsn": "0/100", "slot_xmin": 101,
         "vacuum_horizon": 105},
    ],
    "versions": [
        {"version_id": "v1", "row_id": "r1", "xmin": 100},
        {"version_id": "v2", "row_id": "r1", "xmin": 100, "xmax": 101},
    ],
    "deletion_markers": [
        {"version_id": "v2", "row_id": "r1", "xmax": 101},
    ],
    "cleaned_versions": [
        {"version_id": "v0", "row_id": "r1", "xmin": 90, "xmax": 100,
         "removed_by_node": "replica"},
    ],
    "queries": [
        {"query_id": "q1", "node": "primary", "snapshot": "s1"},
        {"query_id": "q2", "node": "replica", "mode": "current", "at_ts": 5.0},
    ],
})

report = validate(catalog)
report.to_dict()          # 可 JSON 序列化的完整报告
report.is_consistent      # 是否存在 violation 级证据
```

LSN 支持 `"高32位/低32位"` 十六进制字符串或整数；`None` 表示无穷大
（主库不受回放位置限制）。

## 输出与证据码

每个查询返回 `visible`、`hidden`、`missing`（本应可见但已被清理的版本）
和完整 `judgements`（含 `xmin_hint` / `xmax_hint` 判定链）。证据码定义在
`mvcc_validator/reasons.py`：

- 可见性原因：`VISIBLE_LIVE`、`VISIBLE_FROZEN`、`HIDDEN_IN_TRANSACTION`、
  `HIDDEN_ABORTED`、`HIDDEN_FUTURE_XID`、`HIDDEN_DELETED`、
  `HIDDEN_VACUUMED`、`UNKNOWN_XID`、`MARKER_XMAX_MISMATCH`；
- 违规证据：`VACUUM_PREEMPTED`、`MARKER_PREEMPTED`、
  `UNSAFE_VACUUM_HORIZON`、`REPLICA_LAG`、`SNAPSHOT_DIVERGENCE`、
  `XIP_SET_MISMATCH`；
- 信息性证据：`CURRENT_READ_DRIFT`（当前读不保证快照一致，仅记录漂移）。

同名快照在不同节点上的可见集合必须一致；由复制延迟导致的分歧标记为
`REPLICA_LAG`，由节点本地提前清理等导致的分歧标记为 `SNAPSHOT_DIVERGENCE`。

## 目录结构

- `mvcc_validator/xid.py`：32 位 XID 回绕算术（`age`、`in_range`、`oldest`）；
- `mvcc_validator/model.py`：输入 dataclass 与字典解析；
- `mvcc_validator/visibility.py`：XID / 版本可见性、当前读水位、清理保护边界；
- `mvcc_validator/validator.py`：校验编排、一致性比对、报告序列化；
- `tests/`：覆盖提交前/后读取、回滚、两次读一致性、副本延迟、
  清理水位、并发删除和 XID 回绕的单元测试。

## 运行测试

代码仅使用 Python 3.8 标准库（`dataclasses`、`typing`），在 Python 3.8.2
及以上可直接运行：

```bash
python -m unittest
```
