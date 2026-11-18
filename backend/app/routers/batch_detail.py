"""入湖批次明细数据集接口：解析落库、批次/明细查询、重跑、质量门禁回执与跨模块核对。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter
from pydantic import BaseModel, Field

from app.services.batch_detail import (
    CALIBER_ARRIVAL,
    CALIBER_BIZ_DATE,
    CALIBER_LABELS,
    FILTERABLE_RESULTS,
    batch_detail_service,
)

router = APIRouter(prefix="/api/batch-detail", tags=["入湖批次明细数据集"])


class ParsePayload(BaseModel):
    pipeline_id: int = 1
    caliber: str = CALIBER_ARRIVAL
    collection_time: str | None = None
    samples: list[dict[str, Any]] | None = None


class QualityGatePayload(BaseModel):
    gate_rule_id: int | None = None
    sample_ratio: float | None = Field(default=None, ge=0.0, le=1.0)


@router.get("/options")
def options() -> dict[str, Any]:
    """前端工作台选项：两种口径与五类清洗结果。"""
    return {
        "calibers": [
            {"value": CALIBER_ARRIVAL, "label": CALIBER_LABELS[CALIBER_ARRIVAL]},
            {"value": CALIBER_BIZ_DATE, "label": CALIBER_LABELS[CALIBER_BIZ_DATE]},
        ],
        "clean_results": FILTERABLE_RESULTS,
    }


@router.post("/parse")
def parse_batch(payload: ParsePayload) -> dict[str, Any]:
    """解析管道配置与待入湖样本，事务落库产出入湖批次明细数据集。"""
    return batch_detail_service.parse_and_build(
        pipeline_id=payload.pipeline_id,
        caliber=payload.caliber,
        samples=payload.samples,
        collection_time=payload.collection_time,
    )


@router.get("/batches")
def list_batches(pipeline_id: int | None = None,
                 status: str | None = None) -> dict[str, Any]:
    items = batch_detail_service.list_batches(pipeline_id, status)
    return {"items": items, "total": len(items)}


@router.get("/batches/{batch_id}/lines")
def list_lines(batch_id: int, clean_result: str | None = None,
               keyword: str | None = None) -> dict[str, Any]:
    """按清洗结果定位每一行：来源文件、原始行号、去重键、隔离通道与回执。"""
    return batch_detail_service.list_lines(batch_id, clean_result, keyword)


@router.post("/batches/{batch_id}/rerun")
def rerun_batch(batch_id: int) -> dict[str, Any]:
    """批次重跑：未完成部分重试，已确认来源行不重复累计、不被改变。"""
    return batch_detail_service.rerun_batch(batch_id)


@router.post("/batches/{batch_id}/quality-gate")
def quality_gate(batch_id: int, payload: QualityGatePayload) -> dict[str, Any]:
    """质量门禁按明细数据集版本采样，只接收完整拒绝回执。"""
    return batch_detail_service.quality_gate_sampling(
        batch_id, payload.gate_rule_id, payload.sample_ratio)


@router.post("/batches/{batch_id}/confirm")
def confirm_batch(batch_id: int) -> dict[str, Any]:
    """跨模块核对确认：回写待复核清单并冻结明细行。"""
    return batch_detail_service.confirm_batch(batch_id)


@router.get("/reviews")
def list_reviews() -> dict[str, Any]:
    """待复核清单：质量门禁与入湖管道之间的跨模块状态。"""
    items = batch_detail_service.list_pending_reviews()
    return {"items": items, "total": len(items)}


@router.post("/reviews/{review_id}/refresh")
def refresh_review(review_id: int) -> dict[str, Any]:
    """回到入湖管道刷新后再次查询跨模块状态。"""
    return batch_detail_service.refresh_review(review_id)


@router.get("/lines/{line_id}/receipt")
def line_receipt(line_id: int) -> dict[str, Any]:
    """定位单行对应的采样拒绝回执，供质量门禁复核被拦截样本。"""
    return batch_detail_service.line_receipt(line_id)
