"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
# 数据工程核心：数据清洗与转换链路处理 — 跨模块数据产物
from __future__ import annotations

from app.services.base import BaseModuleService


class CrossZoneCopyService(BaseModuleService):
    MODULE = "cross_zone_copy"
    ENTITY = "复制任务"
    OPERATION_TERM = "转换"
    LIST_FIELDS = ["任务名称", "源湖区", "目标湖区", "过滤条件", "转换规则", "并发度", "计划时间"]
    REQUIRED_FIELDS = ["任务名称", "源湖区", "目标湖区"]
    STATUS_ORDER = ["待执行", "复制中", "已完成", "复制失败"]
    ACTION_RULES = {"启动复制": "待执行", "取消复制": "复制中"}
    NEGATIVE_ACTIONS = []
