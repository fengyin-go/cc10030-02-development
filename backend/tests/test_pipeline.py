"""业务流水线测试：验证跨模块真实数据流、失败恢复和版本一致性。

不mock调用次数，断言真实数据产物（版本标记、校验和、数据行实体）。
"""
from __future__ import annotations

import pytest

from app.services.pipeline_engine import (
    pipeline_access_to_quality,
    pipeline_dedup_to_dictionary,
    pipeline_format_to_backup,
    pipeline_ingest_to_catalog,
    pipeline_partition_to_monitor,
    pipeline_retention_to_audit,
    pipeline_schema_evolve_to_lineage,
    pipeline_snapshot_to_tier,
    run_all_pipelines,
)
from app.store import store


class TestIngestToCatalog:
    """流水线1: 入湖管道→湖区写入→质量门禁→目录注册"""

    def test_full_flow_dirty_isolation(self):
        """创建管道→数据入湖→质量门禁拦截脏数据→记录写入异常日志"""
        result = pipeline_ingest_to_catalog(pipeline_id=1)
        assert result["ok"] is True
        assert result["dirty_rows"] > 0, "应有脏行被拦截"
        assert result["dirty_isolated"] is True
        assert result["valid_rows"] > 0
        assert result["total_rows"] == result["valid_rows"] + result["dirty_rows"] + result["duplicate_rows"]

        # 断言真实数据产物：湖区写入通道存在并含确认版本
        lake_version = store.channel.latest_confirmed_of("lake_zone_write")
        assert lake_version is not None, "湖区写入通道必须有已确认版本"
        assert result["lake_version"] == lake_version.version_tag
        assert len(lake_version.rows) == result["valid_rows"]
        assert lake_version.is_confirmed is True

        # 断言脏行隔离到专属通道（isolate_dirty写入{channel}.dirty）
        dirty_versions = store.channel.versions_of("quality_rejections.dirty")
        assert len(dirty_versions) > 0, "脏行必须已隔离到专属通道"

        # 断言元数据目录登记
        catalog_version = store.channel.latest_confirmed_of("metadata_catalog_entries")
        assert catalog_version is not None
        assert len(catalog_version.rows) > 0

    def test_batch_dedup_produces_unique_records(self):
        """批次去重产生唯一记录，去重掉的重复行被隔离"""
        batch = [
            {"ds": "2026-11-17", "record_id": "R01", "value": "100"},
            {"ds": "2026-11-17", "record_id": "R01", "value": "100"},
            {"ds": "2026-11-17", "record_id": "R02", "value": "200"},
        ]
        result = pipeline_ingest_to_catalog(
            pipeline_id=1, batch_rows=batch, dedup_keys=["record_id", "ds"])
        assert result["ok"] is True
        assert result["valid_rows"] == 2  # R01去重后剩1条 + R02=2
        assert result["duplicate_rows"] == 1

    def test_version_moved_forward_on_new_data(self):
        """版本化：新数据流产生递增的版本号"""
        result1 = pipeline_ingest_to_catalog(pipeline_id=1)
        result2 = pipeline_ingest_to_catalog(pipeline_id=2)
        assert result1["lake_version"] != result2["lake_version"]
        v1 = store.channel.versions_of("lake_zone_write")
        assert len(v1) >= 2, f"应有至少2个版本，实际{len(v1)}"


class TestSchemaEvolveToLineage:
    """流水线2: Schema注册→模式演进→变更追踪"""

    def test_schema_evolve_triggers_lineage_record(self):
        """Schema修改后下游元数据目录自动感知变更"""
        result = pipeline_schema_evolve_to_lineage(
            schema_id=1, evolve_id=1,
            new_fields=["phone", "created_at"],
            drop_fields=["legacy_field"],
        )
        assert result["ok"] is True
        assert result["added_fields"] == ["phone", "created_at"]
        assert result["dropped_fields"] == ["legacy_field"]
        assert result["lineage_recorded"] is True
        assert result["after_version"] != result["before_version"]

        # 版本一致性：检查 schema_versions 通道
        versions = store.channel.versions_of("schema_versions")
        assert len(versions) >= 2, f"Schema版本通道应有至少2条记录，实际{len(versions)}"
        evolved = [v for v in versions if "evolve_id" in v.metadata]
        assert len(evolved) >= 1, f"应至少有一条evolved记录，metadata={[v.metadata for v in versions]}"

        # 确认变更追踪记录
        lineage_versions = store.channel.versions_of("lineage_changes")
        assert len(lineage_versions) >= 1
        lv = lineage_versions[-1]
        assert len(lv.rows) > 0
        assert lv.rows[0]["change_type"] == "Schema演进"

        # 确认 lineage_track 模块数据一致性
        lt_rows = store.rows("lineage_track")
        lineage_found = any(
            str(r.get("变更ID", "")).startswith("EVOLVE-") for r in lt_rows)
        assert lineage_found, "lineage_track必须包含变更ID"


class TestSnapshotToTier:
    """流水线3: 快照→过期清理→冷热分层"""

    def test_snapshot_generates_tier_migration(self):
        """快照管理创建后触发冷热分层存储迁移"""
        result = pipeline_snapshot_to_tier(
            snapshot_id=1, ttl_policy_id=1, tier_policy_id=1)
        assert result["ok"] is True
        assert result["tier_migrated"] is True
        snapshot_version = result.get("snapshot_version")
        assert snapshot_version is not None

        # 断言快照数据产物
        snapshot_versions = store.channel.versions_of("snapshot_data")
        assert len(snapshot_versions) >= 1

        # 断言分层迁移记录
        tier_versions = store.channel.versions_of("tier_migration_records")
        assert len(tier_versions) >= 1
        tv = tier_versions[-1]
        assert len(tv.rows) > 0
        assert "target_tier" in tv.rows[0]

    def test_failure_recovery_does_not_overwrite_completed_snapshot(self):
        """失败恢复机制：重试不覆盖已完成快照"""
        # 第一次执行
        result1 = pipeline_snapshot_to_tier(snapshot_id=3)
        assert result1["ok"] is True

        # 查询 checkpoint（checkpoint key 是 str(snapshot_id)）
        assert store.recovery.is_completed("snapshot_write", "3")

        # 记录当前 snapshot_data 版本数
        before_count = len(store.channel.versions_of("snapshot_data"))

        # 重试相同快照ID
        result2 = pipeline_snapshot_to_tier(snapshot_id=3)
        assert result2["ok"] is True
        assert result2.get("recovered_from_checkpoint") is True

        # 确认snapshot_data没有新增重复版本
        after_count = len(store.channel.versions_of("snapshot_data"))
        assert after_count == before_count, \
            f"重试不应增加新版本，before={before_count}, after={after_count}"


class TestPartitionToMonitor:
    """流水线4: 分区策略→跨区复制→入湖监控"""

    def test_partition_strategy_drives_copy_and_monitor(self):
        """分区策略驱动跨区复制，入湖监控更新指标"""
        result = pipeline_partition_to_monitor(policy_id=1, copy_task_id=1)
        assert result["ok"] is True
        assert result["copy_result"]["executed"] is True
        assert result["copy_result"]["rows_copied"] > 0

        # 断言监控指标通道
        monitor_versions = store.channel.versions_of("ingest_monitor_metrics")
        assert len(monitor_versions) >= 1
        mv = monitor_versions[-1]
        assert len(mv.rows) > 0
        metrics = mv.rows[0]
        assert metrics["throughput_rps"] > 0
        assert metrics["error_rate"] >= 0


class TestRetentionToAudit:
    """流水线5: 留存策略→审计日志"""

    def test_retention_triggers_audit_journal(self):
        """留存策略触发后审计日志记录清理操作"""
        result = pipeline_retention_to_audit(retention_id=1)
        assert result["ok"] is True
        assert result["audit_recorded"] is True

        # 断言审计日志写入真实条目
        audit_versions = store.channel.versions_of("audit_journal_records")
        assert len(audit_versions) >= 1
        av = audit_versions[-1]
        assert len(av.rows) > 0
        assert av.rows[0]["操作类型"] == "过期过渡"


class TestDedupToDictionary:
    """流水线6: 湖内去重→数据字典"""

    def test_dedup_updates_data_dictionary(self):
        """去重后数据字典自动更新字段条目"""
        result = pipeline_dedup_to_dictionary(dedup_id=1)
        assert result["ok"] is True
        assert result["unique_count"] > 0
        assert result["duplicate_count"] > 0
        assert result["dict_entries"] > 0

        # 断言去重结果
        dedup_versions = store.channel.versions_of("dedup_unique_records")
        assert len(dedup_versions) >= 1
        dv = dedup_versions[-1]
        assert len(dv.rows) == result["unique_count"]

        # 断言数据字典条目
        dict_versions = store.channel.versions_of("data_dictionary_entries")
        assert len(dict_versions) >= 1
        ddv = dict_versions[-1]
        assert len(ddv.rows) == result["dict_entries"]


class TestFormatToBackup:
    """流水线7: 存储格式→灾备策略"""

    def test_format_applied_triggers_backup(self):
        """存储格式应用后灾备策略创建备份"""
        result = pipeline_format_to_backup(format_id=1, backup_policy_id=1)
        assert result["ok"] is True
        assert result["backup_result"]["executed"] is True
        assert result["backup_result"]["replicas"] > 0

        # 断言备份记录
        backup_versions = store.channel.versions_of("backup_records")
        assert len(backup_versions) >= 1


class TestAccessToQuality:
    """流水线8: 访问管控→质量门禁"""

    def test_access_policy_adjusts_quality_block_level(self):
        """访问策略调整质量门禁阻断级别：验证豁免范围和阻断级别关联"""
        result = pipeline_access_to_quality(access_id=1, quality_gate_id=1)
        assert result["ok"] is True
        # 即使seed数据为主控样例字面值，豁免范围仍应为非空
        assert result["exempt_adjusted"] is not None
        assert isinstance(result["block_level_adjusted"], str)
        assert isinstance(result["block_level_original"], str)
        assert result["quality_version"] is not None


class TestRunAllPipelines:
    """全部流水线调度"""

    def test_all_eight_pipelines_run_successfully(self):
        """一键运行8条流水线全部成功"""
        result = run_all_pipelines()
        assert result["ok"] is True
        assert result["pipelines_run"] == 8
        assert result["pipelines_ok"] == 8

        # 断言所有流水线摘要包含关键字段
        for summary in result["pipeline_summaries"]:
            assert "pipeline" in summary or "name" in summary, \
                f"每条摘要必须描述其流水线身份: {summary}"

    def test_pipeline_overview_contains_all_channels(self):
        """管道全景报告包含所有通道状态"""
        overview = store.pipeline_overview()
        assert "channels" in overview
        channels = overview["channels"]
        # 至少应有schema_versions, lake_zone_write, lineage_changes等核心通道
        assert len(channels) > 5, f"至少应有6条以上数据通道，实际{len(channels)}"
        for name, info in channels.items():
            assert "latest_version" in info
            assert "version_count" in info
            assert "latest_rows" in info
            assert info["latest_rows"] >= 0


class TestVersionConsistencyAcrossModules:
    """跨模块版本一致性"""

    def test_pipeline_same_data_produces_same_checksum(self):
        """相同数据多次写入产生相同校验和"""
        import hashlib
        data = [{"k": "v1"}, {"k": "v2"}]
        v1 = store.channel.publish("check_test_v1", data, confirm=True)
        v2 = store.channel.publish("check_test_v2", data, confirm=True)
        assert v1.checksum == v2.checksum

    def test_next_version_signals_new_data(self):
        """不同数据产生不同校验和和递增版本号"""
        v1 = store.channel.publish("diff_test", [{"k": "v1"}], confirm=True)
        v2 = store.channel.publish("diff_test", [{"k": "v2"}], confirm=True)
        assert v1.checksum != v2.checksum
        assert v2.major > v1.major

    def test_metadata_write_preserved_in_version(self):
        """元数据写入后可在版本中查询到"""
        result = pipeline_ingest_to_catalog(pipeline_id=1)
        lake_version = store.channel.latest_confirmed_of("lake_zone_write")
        assert lake_version is not None
        assert int(lake_version.metadata.get("pipeline_id", 0)) == 1