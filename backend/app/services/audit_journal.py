"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
# 数据工程核心：数据清洗与转换链路处理 — 跨模块数据产物
from __future__ import annotations

from app.services.base import BaseModuleService


class AuditJournalService(BaseModuleService):
    MODULE = "audit_journal"
    ENTITY = "审计日志"
    OPERATION_TERM = "归档"
    LIST_FIELDS = ["操作时间", "操作主体", "操作类型", "目标资源", "请求参数", "执行结果", "耗时"]
    REQUIRED_FIELDS = ["操作时间", "操作主体", "操作类型"]
    STATUS_ORDER = ["执行成功", "执行失败", "访问拒绝"]
    ACTION_RULES = {"归档日志": "执行成功"}
    NEGATIVE_ACTIONS = []
