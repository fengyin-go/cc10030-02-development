"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
# 数据工程核心：数据导入导出与批次追踪 — 跨模块数据产物
from __future__ import annotations

from app.services.base import BaseModuleService


class LakeZoneService(BaseModuleService):
    MODULE = "lake_zone"
    ENTITY = "湖区定义"
    OPERATION_TERM = "清洗"
    LIST_FIELDS = ["湖区编码", "湖区名称", "分层级别", "存储路径", "数据Owner", "访问协议", "保留策略"]
    REQUIRED_FIELDS = ["湖区编码", "湖区名称", "分层级别"]
    STATUS_ORDER = ["草稿", "已建区", "已上线", "已封存"]
    ACTION_RULES = {"初始化湖区": "草稿", "正式上线": "已建区", "封存湖区": "已上线"}
    NEGATIVE_ACTIONS = []
