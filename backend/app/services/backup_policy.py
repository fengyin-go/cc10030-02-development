"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
# 数据工程核心：数据校验与去重策略执行 — 跨模块数据产物
from __future__ import annotations

from app.services.base import BaseModuleService


class BackupPolicyService(BaseModuleService):
    MODULE = "backup_policy"
    ENTITY = "灾备计划"
    OPERATION_TERM = "存储"
    LIST_FIELDS = ["计划名称", "保护范围", "备份周期", "副本数量", "恢复目标", "异地存储", "测试频率"]
    REQUIRED_FIELDS = ["计划名称", "保护范围", "备份周期"]
    STATUS_ORDER = ["草稿", "已激活", "已暂停", "已过期"]
    ACTION_RULES = {"激活计划": "草稿", "暂停计划": "已激活"}
    NEGATIVE_ACTIONS = []
