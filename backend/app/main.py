"""数据湖入湖治理中心 后端服务入口。

启动：uvicorn app.main:app --host 127.0.0.1 --port 8000
健康检查：GET /api/health
"""
from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import settings
from app.routers import ROUTERS
from app.services import pipeline_engine
from app.store import store

app = FastAPI(title="数据湖入湖治理中心", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/api/health")
def health() -> dict[str, object]:
    """健康检查：确认服务已经监听、示例数据已经就绪。"""
    return {"ok": True, "app": settings.app_name, "modules": len(store.module_names())}


@app.get("/api/overview")
def overview() -> dict[str, object]:
    """运营概览：把各业务模块的待处理量汇总成看板卡片。"""
    return store.overview()


@app.get("/api/pipeline/ingest-to-catalog")
def pipeline_ingest_to_catalog(pipeline_id: int = 1) -> dict[str, object]:
    """流水线1: 入湖管道→湖区写入→质量门禁→目录注册"""
    return pipeline_engine.pipeline_ingest_to_catalog(pipeline_id)


@app.get("/api/pipeline/schema-evolve-to-lineage")
def pipeline_schema_evolve_to_lineage(schema_id: int = 1, evolve_id: int = 1) -> dict[str, object]:
    """流水线2: Schema注册→模式演进→变更追踪"""
    return pipeline_engine.pipeline_schema_evolve_to_lineage(schema_id, evolve_id)


@app.get("/api/pipeline/snapshot-to-tier")
def pipeline_snapshot_to_tier(snapshot_id: int = 1) -> dict[str, object]:
    """流水线3: 快照管理→过期清理→冷热分层"""
    return pipeline_engine.pipeline_snapshot_to_tier(snapshot_id, ttl_policy_id=1, tier_policy_id=1)


@app.get("/api/pipeline/partition-to-monitor")
def pipeline_partition_to_monitor(policy_id: int = 1) -> dict[str, object]:
    """流水线4: 分区策略→跨区复制→入湖监控"""
    return pipeline_engine.pipeline_partition_to_monitor(policy_id, copy_task_id=1)


@app.get("/api/pipeline/retention-to-audit")
def pipeline_retention_to_audit(retention_id: int = 1) -> dict[str, object]:
    """流水线5: 留存策略→审计日志"""
    return pipeline_engine.pipeline_retention_to_audit(retention_id)


@app.get("/api/pipeline/dedup-to-dictionary")
def pipeline_dedup_to_dictionary(dedup_id: int = 1) -> dict[str, object]:
    """流水线6: 湖内去重→数据字典"""
    return pipeline_engine.pipeline_dedup_to_dictionary(dedup_id)


@app.get("/api/pipeline/format-to-backup")
def pipeline_format_to_backup(format_id: int = 1) -> dict[str, object]:
    """流水线7: 存储格式→灾备策略"""
    return pipeline_engine.pipeline_format_to_backup(format_id, backup_policy_id=1)


@app.get("/api/pipeline/access-to-quality")
def pipeline_access_to_quality(access_id: int = 1, quality_gate_id: int = 1) -> dict[str, object]:
    """流水线8: 访问管控→质量门禁"""
    return pipeline_engine.pipeline_access_to_quality(access_id, quality_gate_id)


@app.get("/api/pipeline/run-all")
def pipeline_run_all() -> dict[str, object]:
    """一键运行全部8条跨模块数据流水线"""
    return pipeline_engine.run_all_pipelines()


@app.get("/api/pipeline/overview")
def pipeline_overview() -> dict[str, object]:
    """流水线数据通道全景"""
    return store.pipeline_overview()


# 资源路由含 /api/{module}，必须放在 health / overview 之后注册，
# 否则固定路径会被动态路径抢先匹配。
for module in ROUTERS:
    app.include_router(module.router)
