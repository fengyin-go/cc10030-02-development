"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
# 数据工程核心：数据导入导出与批次追踪 — 跨模块数据产物
from __future__ import annotations

from app.services.base import BaseModuleService


class IngestMonitorService(BaseModuleService):
    MODULE = "ingest_monitor"
    ENTITY = "监控面板"
    OPERATION_TERM = "入湖"
    LIST_FIELDS = ["监控项", "数据管道", "吞吐速率", "延迟时长", "错误率", "积压量", "健康分"]
    REQUIRED_FIELDS = ["监控项", "数据管道", "吞吐速率"]
    STATUS_ORDER = ["正常", "预警", "异常", "中断"]
    ACTION_RULES = {"确认预警": "预警", "标记中断": "异常"}
    NEGATIVE_ACTIONS = []
