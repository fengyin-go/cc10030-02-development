"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
# 数据工程核心：数据归档存储与快照管理 — 跨模块数据产物
from __future__ import annotations

from app.services.base import BaseModuleService


class SnapshotMgrService(BaseModuleService):
    MODULE = "snapshot_mgr"
    ENTITY = "快照任务"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["快照名称", "目标数据集", "快照时间", "存储路径", "快照大小", "校验和", "过期时间"]
    REQUIRED_FIELDS = ["快照名称", "目标数据集", "快照时间"]
    STATUS_ORDER = ["创建中", "已完成", "校验失败", "已过期"]
    ACTION_RULES = {"创建快照": "已完成", "清理快照": "已过期"}
    NEGATIVE_ACTIONS = []
