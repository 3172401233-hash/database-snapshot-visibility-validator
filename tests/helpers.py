"""测试场景构造辅助。"""

from mvcc_validator import Catalog


def build_catalog(**overrides):
    data = {
        "transactions": [],
        "snapshots": [],
        "nodes": [],
        "versions": [],
        "deletion_markers": [],
        "cleaned_versions": [],
        "queries": [],
    }
    data.update(overrides)
    return Catalog.from_dict(data)


def visible_ids(result):
    return {item["version_id"] for item in result.visible}


def hidden_ids(result):
    return {item["version_id"] for item in result.hidden}


def reasons_for(result):
    return {key: value["reason"] for key, value in result.judgements.items()}


def evidence_codes(report):
    return [item.code for item in report.evidence]
