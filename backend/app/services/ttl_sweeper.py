"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
# 数据工程核心：数据标准化与字段映射转换 — 跨模块数据产物
from __future__ import annotations

from app.services.base import BaseModuleService


class TtlSweeperService(BaseModuleService):
    MODULE = "ttl_sweeper"
    ENTITY = "清理策略"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["策略名称", "清理范围", "匹配条件", "清理动作", "执行周期", "安全确认", "清理进度"]
    REQUIRED_FIELDS = ["策略名称", "清理范围", "匹配条件"]
    STATUS_ORDER = ["草稿", "已生效", "已暂停"]
    ACTION_RULES = {"启用策略": "草稿", "暂停策略": "已生效"}
    NEGATIVE_ACTIONS = []
