"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
# 数据工程核心：水位追踪与增量采集调度 — 跨模块数据产物
from __future__ import annotations

from app.services.base import BaseModuleService


class QualityGateService(BaseModuleService):
    MODULE = "quality_gate"
    ENTITY = "门禁规则"
    OPERATION_TERM = "入湖"
    LIST_FIELDS = ["规则名称", "检查维度", "阈值条件", "采样比例", "阻断级别", "豁免范围", "生效数据集"]
    REQUIRED_FIELDS = ["规则名称", "检查维度", "阈值条件"]
    STATUS_ORDER = ["草稿", "已启用", "已关闭"]
    ACTION_RULES = {"启用门禁": "草稿", "关闭门禁": "已启用"}
    NEGATIVE_ACTIONS = []
