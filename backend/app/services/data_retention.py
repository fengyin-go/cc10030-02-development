"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
# 数据工程核心：数据归档存储与快照管理 — 跨模块数据产物
from __future__ import annotations

from app.services.base import BaseModuleService


class DataRetentionService(BaseModuleService):
    MODULE = "data_retention"
    ENTITY = "留存规则"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["规则名称", "目标湖区", "保留天数", "过渡策略", "清理方式", "例外条件", "通知渠道"]
    REQUIRED_FIELDS = ["规则名称", "目标湖区", "保留天数"]
    STATUS_ORDER = ["草稿", "已启用", "已停用"]
    ACTION_RULES = {"启用规则": "草稿", "停用规则": "已启用"}
    NEGATIVE_ACTIONS = ["停用规则"]
