"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
# 数据工程核心：数据标准化与字段映射转换 — 跨模块数据产物
from __future__ import annotations

from app.services.base import BaseModuleService


class MetadataCatalogService(BaseModuleService):
    MODULE = "metadata_catalog"
    ENTITY = "元数据条目"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["条目ID", "数据集名称", "所属湖区", "字段清单", "数据量级", "最近更新", "数据Owner"]
    REQUIRED_FIELDS = ["条目ID", "数据集名称", "所属湖区"]
    STATUS_ORDER = ["草稿", "已发布", "已过期", "已删除"]
    ACTION_RULES = {"发布条目": "草稿", "标记过期": "已发布"}
    NEGATIVE_ACTIONS = []
