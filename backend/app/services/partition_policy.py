"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
# 数据工程核心：数据入湖存储与分区管理 — 跨模块数据产物
from __future__ import annotations

from app.services.base import BaseModuleService


class PartitionPolicyService(BaseModuleService):
    MODULE = "partition_policy"
    ENTITY = "分区规则"
    OPERATION_TERM = "合并"
    LIST_FIELDS = ["规则名称", "目标数据集", "分区键", "分区粒度", "生命周期", "合并策略", "最大分区数"]
    REQUIRED_FIELDS = ["规则名称", "目标数据集", "分区键"]
    STATUS_ORDER = ["草稿", "已生效", "已过期"]
    ACTION_RULES = {"启用规则": "草稿", "废弃规则": "已生效"}
    NEGATIVE_ACTIONS = []
