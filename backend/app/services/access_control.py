"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
# 数据工程核心：数据校验与去重策略执行 — 跨模块数据产物
from __future__ import annotations

from app.services.base import BaseModuleService


class AccessControlService(BaseModuleService):
    MODULE = "access_control"
    ENTITY = "访问策略"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["策略名称", "资源范围", "主体类型", "操作集合", "生效时间", "失效时间", "优先级"]
    REQUIRED_FIELDS = ["策略名称", "资源范围", "主体类型"]
    STATUS_ORDER = ["草稿", "已生效", "已过期", "已撤销"]
    ACTION_RULES = {"启用策略": "草稿", "撤销策略": "已生效"}
    NEGATIVE_ACTIONS = ["撤销策略"]
