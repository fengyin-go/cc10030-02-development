"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
# 数据工程核心：数据聚合计算与指标产出 — 跨模块数据产物
from __future__ import annotations

from app.services.base import BaseModuleService


class SchemaRegistryService(BaseModuleService):
    MODULE = "schema_registry"
    ENTITY = "Schema版本"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["主题名称", "Schema定义", "版本号", "兼容策略", "序列化格式", "校验模式", "注册时间"]
    REQUIRED_FIELDS = ["主题名称", "Schema定义", "版本号"]
    STATUS_ORDER = ["草稿", "已注册", "已弃用"]
    ACTION_RULES = {"注册版本": "草稿", "弃用版本": "已注册"}
    NEGATIVE_ACTIONS = []
