"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
# 数据工程核心：数据入湖存储与分区管理 — 跨模块数据产物
from __future__ import annotations

from app.services.base import BaseModuleService


class LineageTrackService(BaseModuleService):
    MODULE = "lineage_track"
    ENTITY = "变更记录"
    OPERATION_TERM = "归档"
    LIST_FIELDS = ["变更ID", "目标对象", "变更类型", "变更前后", "操作时间", "触发原因", "影响范围"]
    REQUIRED_FIELDS = ["变更ID", "目标对象", "变更类型"]
    STATUS_ORDER = ["已记录", "已审计", "已归档"]
    ACTION_RULES = {"标记审计": "已记录", "归档记录": "已审计"}
    NEGATIVE_ACTIONS = []
