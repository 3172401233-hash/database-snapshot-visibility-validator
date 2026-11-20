"""32 位事务 ID（XID）的回绕运算。

PostgreSQL 把事务 ID 视为模 2**32 的循环计数，普通 XID 与参考点的
距离取“后半圆”为正向。本模块不依赖任何数据库，只实现这套算术规则。
"""

MOD = 1 << 32
HALF = 1 << 31

INVALID_XID = 0
BOOTSTRAP_XID = 1
FROZEN_XID = 2
FIRST_NORMAL_XID = 3

SPECIAL_XIDS = frozenset({INVALID_XID, BOOTSTRAP_XID, FROZEN_XID})


def normalize(xid):
    """把任意整数归一化到 32 位无符号区间。"""
    return int(xid) % MOD


def is_special(xid):
    return normalize(xid) in SPECIAL_XIDS


def age(xid, reference):
    """reference 观察到 xid 已经过去的“年龄”（0 .. 2**32-1）。"""
    return (normalize(reference) - normalize(xid)) % MOD


def in_range(xid, oldest, newest):
    """回绕安全的区间判断：oldest <= xid < newest。

    要求区间长度不超过 2**31，这与 MVCC 快照的基本前提一致。
    """
    low = age(xid, newest)
    high = age(oldest, newest)
    return 0 < low <= high


def cmp(xid_a, xid_b, reference):
    """在 reference 的后半圆视角下比较两个 XID 的新旧。

    返回 -1 / 0 / 1，分别表示 a 更旧 / 相同 / 更新。
    """
    age_a = age(xid_a, reference)
    age_b = age(xid_b, reference)
    if age_a == age_b:
        return 0
    return 1 if age_a < age_b else -1


def oldest(xids, reference):
    """返回一组 XID 中相对 reference 最旧的一个。"""
    candidates = [normalize(xid) for xid in xids]
    return max(candidates, key=lambda value: age(value, reference))


def next_xid(xid):
    """XID 分配顺序上的下一个值（会回绕）。"""
    return (normalize(xid) + 1) % MOD
