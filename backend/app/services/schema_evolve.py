"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
# 数据工程核心：数据标准化与字段映射转换 — 跨模块数据产物
from __future__ import annotations

from app.services.base import BaseModuleService


class SchemaEvolveService(BaseModuleService):
    MODULE = "schema_evolve"
    ENTITY = "演进方案"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["方案名称", "目标Schema", "变更类型", "新增字段", "删除字段", "兼容级别", "回滚方案"]
    REQUIRED_FIELDS = ["方案名称", "目标Schema", "变更类型"]
    STATUS_ORDER = ["草稿", "已验证", "已执行", "已回滚"]
    ACTION_RULES = {"执行演进": "已验证", "回滚演进": "已执行"}
    NEGATIVE_ACTIONS = ["回滚演进"]
