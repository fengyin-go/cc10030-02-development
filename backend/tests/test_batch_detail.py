"""入湖批次明细数据集测试：逐行可核对、版本与隔离原因同落库、重跑幂等、
事务回滚、完整回执门禁、跨模块核对闭环。
"""
from __future__ import annotations

import pytest

from app.services.batch_detail import (
    CALIBER_ARRIVAL,
    CALIBER_BIZ_DATE,
    RESULT_DIRTY_FORMAT,
    RESULT_DIRTY_MISSING_KEY,
    RESULT_DUPLICATE,
    RESULT_VALID,
    STATUS_CONFIRMED,
    STATUS_INTERRUPTED,
    STATUS_PENDING,
    STATUS_RECEIPTED,
    build_default_samples,
    batch_detail_service as svc,
)
from app.store import store

TABLE_BATCH = "batch_detail_batch"
TABLE_LINE = "batch_detail_line"
TABLE_RECEIPT = "batch_detail_receipt"
TABLE_REVIEW = "batch_detail_pending_review"


def _line_counts(batch_id: int) -> dict[str, int]:
    lines = svc.list_lines(batch_id)["items"]
    return {r: sum(1 for ln in lines if ln["清洗结果"] == r)
            for r in {ln["清洗结果"] for ln in lines}}


class TestParseAndBuild:
    """解析管道配置与待入湖样本 → 清洗 → 批次明细数据集。"""

    def test_build_dataset_with_full_line_provenance(self):
        result = svc.parse_and_build(1, caliber=CALIBER_ARRIVAL)
        assert result["ok"] is True
        batch = result["batch"]
        assert batch["批次状态"] == STATUS_PENDING
        payload = svc.list_lines(batch["id"])
        assert payload["ok"] is True

        required_fields = [
            "来源文件", "原始行号", "源文件偏移", "样本序号", "去重键",
            "清洗结果", "隔离原因", "隔离通道", "净数据版本", "净数据路径",
            "校验指纹", "异常摘要", "更新时间", "采集时间", "业务生效日",
        ]
        for line in payload["items"]:
            for field in required_fields:
                assert field in line, f"明细行缺少可核对字段：{field}"

        summary = result["summary"]
        assert (summary["有效行数"] + summary["重复行数"] + summary["脏行数"]
                == summary["总来源行数"])
        assert summary["去重前行数"] >= summary["去重后行数"]

    def test_valid_duplicate_dirty_classification(self):
        result = svc.parse_and_build(1, caliber=CALIBER_ARRIVAL)
        counts = _line_counts(result["batch"]["id"])
        assert counts.get(RESULT_VALID, 0) > 0
        assert counts.get(RESULT_DUPLICATE, 0) > 0
        assert counts.get(RESULT_DIRTY_FORMAT, 0) >= 1
        assert counts.get(RESULT_DIRTY_MISSING_KEY, 0) >= 1

    def test_isolation_reason_and_version_persisted_together(self):
        """净数据版本与每条隔离原因必须一起落库。"""
        result = svc.parse_and_build(1, caliber=CALIBER_ARRIVAL)
        batch_id = result["batch"]["id"]
        clean_version = result["batch"]["净数据版本"]
        for line in svc.list_lines(batch_id)["items"]:
            assert line["净数据版本"] == clean_version
            assert line["明细版本"] == result["batch"]["明细版本"]
            if line["清洗结果"] != RESULT_VALID:
                assert line["隔离原因"], "隔离行必须带原因"
                assert line["隔离通道"].startswith("batch_quarantine.")

        # 脏行隔离通道同样按版本发布：行内同时带版本号与隔离原因
        dirty_channels = [name for name in store.channel.channel_names()
                          if name.startswith("batch_quarantine.")]
        assert dirty_channels
        for name in dirty_channels:
            version = store.channel.latest_of(name)
            for row in version.rows:
                assert row["净数据版本"] == clean_version
                assert row["_isolated_reason"]
                assert clean_version in row["_isolated_reason"]

    def test_detail_dataset_published_as_versioned_artifact(self):
        result = svc.parse_and_build(1, caliber=CALIBER_ARRIVAL)
        detail = store.channel.latest_confirmed_of("batch_detail_dataset")
        clean = store.channel.latest_confirmed_of("batch_clean_data")
        assert detail is not None and clean is not None
        assert detail.metadata["detail_version"] == result["batch"]["明细版本"]
        # 净数据通道只含有效行
        assert len(clean.rows) == result["summary"]["有效行数"]
        assert all(row["清洗结果"] == RESULT_VALID for row in clean.rows)

    def test_two_calibers_produce_different_version_lineage(self):
        """甲按到达、乙按业务生效日：同键赢家不同，版本口径各自独立。"""
        r_arrival = svc.parse_and_build(1, caliber=CALIBER_ARRIVAL)
        r_biz = svc.parse_and_build(1, caliber=CALIBER_BIZ_DATE)
        assert r_arrival["batch"]["明细版本"].startswith("DET-ARR-")
        assert r_biz["batch"]["明细版本"].startswith("DET-BIZ-")

        def winner_of(batch_id, key):
            return next(ln for ln in svc.list_lines(batch_id)["items"]
                        if ln["去重键"] == key and ln["清洗结果"] == RESULT_VALID)

        # O1002 在补报文件中改动：甲保留首次采集，乙保留生效日更新的补报行
        a = winner_of(r_arrival["batch"]["id"], "O1002")
        b = winner_of(r_biz["batch"]["id"], "O1002")
        assert a["来源文件"] != b["来源文件"]
        assert "首次采集" in a["来源文件"]
        assert "补报" in b["来源文件"]

    def test_change_point_located_across_collection_times(self):
        result = svc.parse_and_build(1, caliber=CALIBER_ARRIVAL)
        points = result["batch"]["改动点"]
        assert any(p["去重键"] == "O1002" for p in points)
        point = next(p for p in points if p["去重键"] == "O1002")
        assert len(point["采集时间"]) == 2  # 补报与首次采集时间不同

    def test_unknown_pipeline_rejected(self):
        result = svc.parse_and_build(999)
        assert result["ok"] is False

    def test_default_samples_cover_all_result_types(self):
        samples = build_default_samples(1)
        assert len({s["source_file"] for s in samples}) == 2
        assert len({s["collection_time"] for s in samples}) == 2

    def test_explicit_empty_samples_rejected_without_fallback(self):
        """显式空样本不能回退默认样本，必须直接拦截。"""
        result = svc.parse_and_build(1, caliber=CALIBER_ARRIVAL, samples=[])
        assert result["ok"] is False
        assert "样本为空" in result["error"]
        # 没有产生任何新批次行
        assert not store.rows(TABLE_LINE) or all(
            ln["批次ID"] != result.get("batch_id", -1)
            for ln in store.rows(TABLE_LINE))


class TestRerunIdempotency:
    """批次重跑不能重复累计同一来源行，已确认行不得改变。"""

    def test_rerun_keeps_same_line_count_and_fingerprints(self):
        result = svc.parse_and_build(1, caliber=CALIBER_ARRIVAL)
        batch_id = result["batch"]["id"]
        before = {
            tuple(sorted(ln["原始样本"].items())): ln["校验指纹"]
            for ln in svc.list_lines(batch_id)["items"]
        }
        rerun = svc.rerun_batch(batch_id)
        assert rerun["ok"] is True
        assert rerun["batch"]["重跑次数"] == 1
        after = svc.list_lines(batch_id)["items"]
        assert len(after) == len(before)
        for line in after:
            key = tuple(sorted(line["原始样本"].items()))
            assert before[key] == line["校验指纹"]
        # 同一 (批次, 来源文件, 原始行号) 只有一行，没有重复累计
        locators = [(ln["来源文件"], ln["原始行号"]) for ln in after]
        assert len(locators) == len(set(locators))

    def test_confirmed_lines_frozen_on_rerun(self):
        result = svc.parse_and_build(1, caliber=CALIBER_ARRIVAL)
        batch_id = result["batch"]["id"]
        svc.quality_gate_sampling(batch_id, sample_ratio=1.0)
        svc.confirm_batch(batch_id)
        # 已确认批次整体禁止重跑
        assert svc.rerun_batch(batch_id)["ok"] is False

    def test_rerun_recovers_batch_that_failed_during_first_parse(self):
        """首次解析即中断（无任何明细行），重跑重建样本并完整恢复。"""
        # 让第一次解析在发布阶段失败，批次以中断状态保留、明细行为零
        def boom(*_args, **_kwargs):
            raise RuntimeError("首次解析发布中断")

        svc._publish_artifacts = boom  # type: ignore[method-assign]
        failed = svc.parse_and_build(1, caliber=CALIBER_ARRIVAL)
        del svc._publish_artifacts
        assert failed["ok"] is False
        batch_id = failed["batch_id"]
        assert svc.list_lines(batch_id)["total"] == 0

        # 重跑：无历史行也能重建默认样本，恢复到完整明细
        recovered = svc.rerun_batch(batch_id)
        assert recovered["ok"] is True
        assert recovered["batch"]["批次状态"] == STATUS_PENDING
        payload = svc.list_lines(batch_id)
        assert payload["total"] == recovered["batch"]["校验摘要"]["总来源行数"]
        assert payload["total"] > 0
        result = svc.parse_and_build(1, caliber=CALIBER_ARRIVAL)
        batch_id = result["batch"]["id"]
        original_count = svc.list_lines(batch_id)["total"]

        # 人为让发布阶段失败，验证事务回滚 + 中断状态 + 回滚依据
        def boom(*_args, **_kwargs):
            raise RuntimeError("模拟发布中断")

        svc._publish_artifacts = boom  # type: ignore[method-assign]
        failed = svc.rerun_batch(batch_id)
        assert failed["ok"] is False
        batch = svc._find_batch(batch_id)
        assert batch["批次状态"] == STATUS_INTERRUPTED
        assert batch["回滚依据"] and "rolled_back_at" in batch["回滚依据"]
        # 回滚后恢复到重跑前快照：行不丢失、不产生半成品新行、重跑次数不累计
        assert svc.list_lines(batch_id)["total"] == original_count
        assert batch["重跑次数"] == 0

        del svc._publish_artifacts
        recovered = svc.rerun_batch(batch_id)
        assert recovered["ok"] is True
        assert recovered["batch"]["批次状态"] == STATUS_PENDING
        assert svc.list_lines(batch_id)["total"] == original_count
        locators = [(ln["来源文件"], ln["原始行号"])
                    for ln in svc.list_lines(batch_id)["items"]]
        assert len(locators) == len(set(locators))


class TestTransactionRollback:
    """事务落库：中断即回滚，保留回滚依据，禁止半成品。"""

    def test_parse_interruption_rolls_back_and_keeps_basis(self):
        before_batches = len(store.rows(TABLE_BATCH))
        before_lines = len(store.rows(TABLE_LINE))

        def boom(*_args, **_kwargs):
            raise RuntimeError("模拟解析发布中断")

        svc._publish_artifacts = boom  # type: ignore[method-assign]
        result = svc.parse_and_build(1, caliber=CALIBER_ARRIVAL)
        del svc._publish_artifacts

        assert result["ok"] is False
        assert "回滚" in result["error"]
        assert result["rollback_basis"]["error"] == "模拟解析发布中断"
        # 批次主数据保留中断记录，但没有任何半成品明细行落库
        assert len(store.rows(TABLE_BATCH)) == before_batches + 1
        assert len(store.rows(TABLE_LINE)) == before_lines
        interrupted = store.rows(TABLE_BATCH)[-1]
        assert interrupted["批次状态"] == STATUS_INTERRUPTED
        assert interrupted["回滚依据"]["rolled_back_at"]


class TestQualityGateReceipt:
    """质量门禁按版本读取明细数据集并生成采样拒绝回执。"""

    def test_sampling_receipt_complete_and_written_back(self):
        result = svc.parse_and_build(1, caliber=CALIBER_ARRIVAL)
        batch_id = result["batch"]["id"]
        gate = svc.quality_gate_sampling(batch_id, sample_ratio=1.0)
        assert gate["ok"] is True
        receipt = gate["receipt"]
        assert receipt["完整回执"] is True
        assert receipt["明细版本"] == result["batch"]["明细版本"]
        assert receipt["拦截行数"] > 0
        # 每个被采样的拦截行都能在明细上回查到回执编号
        lines = svc.list_lines(batch_id)["items"]
        tagged = [ln for ln in lines if ln["拒绝回执ID"] == receipt["回执编号"]]
        assert len(tagged) == receipt["采样行数"]
        # 单行可定位拒绝回执
        blocked = next(ln for ln in tagged if ln["清洗结果"] != RESULT_VALID)
        lookup = svc.line_receipt(blocked["id"])
        assert lookup["ok"] is True
        assert lookup["receipt"]["回执编号"] == receipt["回执编号"]

    def test_gate_rejects_incomplete_receipt(self):
        result = svc.parse_and_build(1, caliber=CALIBER_ARRIVAL)
        batch_id = result["batch"]["id"]
        # 抹掉一条隔离原因，模拟不完整明细 → 门禁必须拒收且不落回执
        line = next(ln for ln in svc.list_lines(batch_id)["items"]
                    if ln["清洗结果"] != RESULT_VALID)
        line["隔离原因"] = ""
        before_receipts = len(store.rows(TABLE_RECEIPT))
        gate = svc.quality_gate_sampling(batch_id, sample_ratio=1.0)
        assert gate["ok"] is False
        assert "完整回执" in gate["error"]
        assert len(store.rows(TABLE_RECEIPT)) == before_receipts

    def test_gate_requires_receivable_batch_status(self):
        result = svc.parse_and_build(1, caliber=CALIBER_ARRIVAL)
        batch_id = result["batch"]["id"]
        svc.quality_gate_sampling(batch_id, sample_ratio=1.0)
        svc.confirm_batch(batch_id)
        # 已确认批次不再接收门禁采样
        assert svc.quality_gate_sampling(batch_id)["ok"] is False


class TestCrossModuleReview:
    """质量门禁 → 待复核清单 → 入湖管道确认的跨模块闭环。"""

    def test_confirm_writes_back_review_and_freezes_lines(self):
        result = svc.parse_and_build(1, caliber=CALIBER_ARRIVAL)
        batch_id = result["batch"]["id"]
        svc.quality_gate_sampling(batch_id, sample_ratio=0.5)
        reviews = svc.list_pending_reviews()
        assert any(r["批次ID"] == batch_id for r in reviews)

        confirm = svc.confirm_batch(batch_id)
        assert confirm["ok"] is True
        assert confirm["batch"]["批次状态"] == STATUS_CONFIRMED
        for line in svc.list_lines(batch_id)["items"]:
            assert line["已确认"] is True

        review = next(r for r in svc.list_pending_reviews()
                      if r["批次ID"] == batch_id)
        assert review["跨模块状态"] == "入湖管道已确认·跨模块核对完成"
        refreshed = svc.refresh_review(review["id"])
        assert refreshed["ok"] is True
        assert refreshed["review"]["批次状态快照"] == STATUS_CONFIRMED

    def test_confirm_requires_receipt_first(self):
        result = svc.parse_and_build(1, caliber=CALIBER_ARRIVAL)
        batch_id = result["batch"]["id"]
        response = svc.confirm_batch(batch_id)
        assert response["ok"] is False
        assert svc._find_batch(batch_id)["批次状态"] == STATUS_PENDING

    def test_pipeline_master_data_maintained(self):
        result = svc.parse_and_build(2, caliber=CALIBER_ARRIVAL)
        pipeline = store.find("ingest_pipeline", 2)
        stats = pipeline["批次统计"]
        assert stats["累计批次数"] >= 1
        assert stats["最新批次号"] == result["batch"]["批次号"]
        assert any(item["批次号"] == result["batch"]["批次号"]
                   for item in stats["批次明细"])


class TestGenericResourceRouterCompatibility:
    """模块仍可通过统一资源路由访问批次主数据。"""

    def test_internal_tables_not_exposed_as_modules(self):
        names = store.module_names()
        assert "batch_detail" not in names  # 主数据存于内部表
        for internal in (TABLE_BATCH, TABLE_LINE, TABLE_RECEIPT, TABLE_REVIEW):
            assert internal not in names
        # 但模块本身已注册，统一资源路由仍可访问
        from app.services import service_for

        assert service_for("batch_detail") is not None
        # 运营概览也不混入内部表
        overview_modules = {m["name"] for m in store.overview()["modules"]}
        assert not any(name.startswith("batch_detail_") for name in overview_modules)

    def test_service_registered_for_generic_router(self):
        from app.services import service_for

        service = service_for("batch_detail")
        assert service is not None
        items, total = service.list_entries()
        assert total == len(items)
        svc.parse_and_build(1, caliber=CALIBER_ARRIVAL)
        items, total = service.list_entries(status=STATUS_PENDING)
        assert total >= 1
        assert all(row["批次状态"] == STATUS_PENDING for row in items)


class TestChecksumStability:
    def test_same_inputs_same_checksum_across_batches(self):
        r1 = svc.parse_and_build(3, caliber=CALIBER_ARRIVAL)
        r2 = svc.parse_and_build(3, caliber=CALIBER_ARRIVAL)
        assert r1["batch"]["校验和"] == r2["batch"]["校验和"]
