"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
# 数据工程核心：数据归档存储与快照管理 — 跨模块数据产物
from __future__ import annotations

from app.services.base import BaseModuleService


class IngestPipelineService(BaseModuleService):
    MODULE = "ingest_pipeline"
    ENTITY = "管道配置"
    OPERATION_TERM = "转换"
    LIST_FIELDS = ["管道名称", "源端类型", "目标湖区", "数据格式", "分区字段", "写入模式", "并发度"]
    REQUIRED_FIELDS = ["管道名称", "源端类型", "目标湖区"]
    STATUS_ORDER = ["草稿", "测试中", "已生效", "已停用"]
    ACTION_RULES = {"测试管道": "草稿", "启用管道": "测试中", "停用管道": "已生效"}
    NEGATIVE_ACTIONS = ["停用管道"]
