"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
# 数据工程核心：数据导入导出与批次追踪 — 跨模块数据产物
from __future__ import annotations

from app.services.base import BaseModuleService


class DedupLakeService(BaseModuleService):
    MODULE = "dedup_lake"
    ENTITY = "去重配置"
    OPERATION_TERM = "去重"
    LIST_FIELDS = ["配置名称", "目标数据集", "去重键", "去重窗口", "判断方式", "覆盖策略", "历史保留"]
    REQUIRED_FIELDS = ["配置名称", "目标数据集", "去重键"]
    STATUS_ORDER = ["草稿", "运行中", "已关闭"]
    ACTION_RULES = {"启动去重": "草稿", "关闭任务": "运行中"}
    NEGATIVE_ACTIONS = []
