"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
# 数据工程核心：数据导入导出与批次追踪 — 跨模块数据产物
from __future__ import annotations

from app.services.base import BaseModuleService


class ColdWarmTierService(BaseModuleService):
    MODULE = "cold_warm_tier"
    ENTITY = "分层策略"
    OPERATION_TERM = "存储"
    LIST_FIELDS = ["策略名称", "热度评估", "冷热阈值", "迁移动作", "目标存储", "回迁条件", "成本预估"]
    REQUIRED_FIELDS = ["策略名称", "热度评估", "冷热阈值"]
    STATUS_ORDER = ["草稿", "已启用", "已停用"]
    ACTION_RULES = {"启用策略": "草稿", "停用策略": "已启用"}
    NEGATIVE_ACTIONS = ["停用策略"]
