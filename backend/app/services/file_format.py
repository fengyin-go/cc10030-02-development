"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
# 数据工程核心：数据入湖存储与分区管理 — 跨模块数据产物
from __future__ import annotations

from app.services.base import BaseModuleService


class FileFormatService(BaseModuleService):
    MODULE = "file_format"
    ENTITY = "格式模板"
    OPERATION_TERM = "存储"
    LIST_FIELDS = ["模板名称", "文件格式", "压缩算法", "块大小", "索引策略", "编码方式", "排序键"]
    REQUIRED_FIELDS = ["模板名称", "文件格式", "压缩算法"]
    STATUS_ORDER = ["草稿", "已应用", "已废弃"]
    ACTION_RULES = {"应用模板": "草稿", "废弃模板": "已应用"}
    NEGATIVE_ACTIONS = []
