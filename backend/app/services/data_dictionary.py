"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
# 数据工程核心：数据归档存储与快照管理 — 跨模块数据产物
from __future__ import annotations

from app.services.base import BaseModuleService


class DataDictionaryService(BaseModuleService):
    MODULE = "data_dictionary"
    ENTITY = "字典条目"
    OPERATION_TERM = "关联"
    LIST_FIELDS = ["条目编码", "字段名称", "数据类型", "业务含义", "取值范围", "关联实体", "来源湖区"]
    REQUIRED_FIELDS = ["条目编码", "字段名称", "数据类型"]
    STATUS_ORDER = ["草稿", "已发布", "已停用"]
    ACTION_RULES = {"发布条目": "草稿", "停用条目": "已发布"}
    NEGATIVE_ACTIONS = ["停用条目"]
