"""跨模块数据流编排引擎：实现至少8组"输入→转换→版本化产物→下游消费"贯通链路。

每个流水线是一个独立单元：从上游模块读取输入、应用转换逻辑、
生成版本化产物并发布到数据通道，下游模块再按版本消费。
支持批次处理（脏行隔离、重复检测）、版本化管理和失败恢复。
"""
from __future__ import annotations

import time
from typing import Any

from app.store import store


# ============================================================================
# 流水线 1: 入湖管道 → 湖区写入 → 质量门禁拦截 → 元数据目录注册
# ingest_pipeline 创建数据批次 → lake_zone 写入分区 →
# quality_gate 检查脏/重复数据并隔离 → metadata_catalog 登记目录条目
# ============================================================================

def pipeline_ingest_to_catalog(
    pipeline_id: int,
    batch_rows: list[dict[str, Any]] | None = None,
    dedup_keys: list[str] | None = None,
    quality_threshold: float = 0.05,
) -> dict[str, Any]:
    """执行入湖到目录注册的完整数据流。

    - 从 ingest_pipeline 读取管道配置
    - 批量数据写入目标湖区分区
    - 质量门禁检查（空值率、格式异常、重复检测）
    - 脏行隔离到专属通道，净数据发布版本
    - 元数据目录自动登记新数据集
    """
    pipeline = store.find("ingest_pipeline", pipeline_id)
    if not pipeline:
        return {"ok": False, "error": f"入湖管道 {pipeline_id} 不存在"}

    # 采集: 读取管道配置并生成模拟数据批次
    target_zone = pipeline.get("目标湖区", "default")
    source_type = pipeline.get("源端类型", "unknown")
    data_format = pipeline.get("数据格式", "JSON")
    partition_field = pipeline.get("分区字段", "ds")

    if batch_rows is None:
        batch_rows = [
            {"ds": "2026-11-17", "record_id": f"REC-{i:04d}",
             "value": str(100 + i), "type": source_type, "format": data_format}
            for i in range(1, 21)
        ]
        # 注入脏数据（空值、异常格式等）
        batch_rows[5]["value"] = ""  # 空值脏行
        batch_rows[10].pop("record_id", None)  # 缺少必填键
        batch_rows[15]["type"] = "INVALID_FORMAT_@#"  # 格式异常

    # 清洗: 批次去重检查
    keys = dedup_keys or ["record_id", "ds"]
    clean_rows, duplicate_rows = store.channel.batch_dedup(
        "ingest_clean", batch_rows, keys)

    # 校验: 质量门禁检查
    dirty_rows: list[dict[str, Any]] = []
    valid_rows: list[dict[str, Any]] = []
    for row in clean_rows:
        issues = []
        # 空值检查
        if not str(row.get("value") or "").strip():
            issues.append("空值: value字段为空")
        # 必填键检查
        if "record_id" not in row or not str(row.get("record_id") or "").strip():
            issues.append("缺少必填键: record_id")
        # 格式检查
        type_val = str(row.get("type", ""))
        if any(c in type_val for c in "@#$%^&*"):
            issues.append("格式异常: type含非法字符")

        if issues:
            row["_quality_issues"] = "; ".join(issues)
            dirty_rows.append(row)
        else:
            valid_rows.append(row)

    # 脏行隔离
    dirty_isolated = False
    if dirty_rows:
        store.channel.isolate_dirty(
            "quality_rejections", dirty_rows,
            reason=f"质量门禁: 空值/格式/缺键拦截 (管道{pipeline_id})")
        dirty_isolated = True

    # 入湖: 写入湖区通道
    lake_version = store.channel.publish(
        "lake_zone_write", valid_rows,
        metadata={"pipeline_id": pipeline_id, "target_zone": target_zone,
                  "source_type": source_type},
        confirm=True)

    # 注册: 元数据目录登记
    catalog_entries = [
        {"数据集名称": f"{target_zone}_batch_{lake_version.version_tag}",
         "所属湖区": target_zone,
         "字段清单": ", ".join(valid_rows[0].keys()) if valid_rows else "",
         "数据量级": len(valid_rows),
         "最近更新": lake_version.timestamp,
         "数据Owner": "数据湖治理中心"}
    ]
    catalog_version = store.channel.publish(
        "metadata_catalog_entries", catalog_entries,
        metadata={"generated_from": "pipeline_ingest_to_catalog",
                  "pipeline_id": pipeline_id},
        confirm=True)

    # 记录批次去重中产生的重复行
    if duplicate_rows:
        store.channel.isolate_dirty(
            "dedup_rejections", duplicate_rows,
            reason=f"批次去重重复行 (管道{pipeline_id})")

    return {
        "ok": True,
        "pipeline": "入湖管道→湖区写入→质量门禁→目录注册",
        "pipeline_id": pipeline_id,
        "total_rows": len(batch_rows),
        "valid_rows": len(valid_rows),
        "dirty_rows": len(dirty_rows),
        "duplicate_rows": len(duplicate_rows),
        "dirty_isolated": dirty_isolated,
        "lake_version": lake_version.version_tag,
        "catalog_version": catalog_version.version_tag,
        "clean_checksum": lake_version.checksum,
    }


# ============================================================================
# 流水线 2: Schema注册 → 模式演进 → 变更追踪
# schema_registry 登记版本 → schema_evolve 执行演进方案 →
# lineage_track 记录变更→审计→归档
# ============================================================================

def pipeline_schema_evolve_to_lineage(
    schema_id: int,
    evolve_id: int,
    new_fields: list[str] | None = None,
    drop_fields: list[str] | None = None,
) -> dict[str, Any]:
    """Schema注册后触发模式演进，并自动记录变更追踪。

    - 读取 schema_registry 主题当前版本
    - 读取 schema_evolve 演进方案（含新增/删除字段、兼容级别）
    - 执行 Schema 演进，生成新版本定义
    - lineage_track 自动感知变更并记录条目
    """
    schema = store.find("schema_registry", schema_id)
    evolve = store.find("schema_evolve", evolve_id)
    if not schema:
        return {"ok": False, "error": f"Schema {schema_id} 不存在"}
    if not evolve:
        return {"ok": False, "error": f"演进方案 {evolve_id} 不存在"}

    schema_topic = schema.get("主题名称", "unknown")
    current_schema_def = schema.get("Schema定义", "{}")
    current_version = schema.get("版本号", "v1.0")
    evolve_target = evolve.get("目标Schema", "")
    compatibility = evolve.get("兼容级别", "FORWARD")

    # 解析要新增和删除的字段
    add_fields = new_fields or [
        f.strip()
        for f in str(evolve.get("新增字段", "")).split(",")
        if f.strip()
    ]
    drop_fields = drop_fields or [
        f.strip()
        for f in str(evolve.get("删除字段", "")).split(",")
        if f.strip()
    ]

    # 执行演进: 构造新版本Schema
    old_schema_written = store.channel.publish(
        "schema_versions", [{"schema_id": schema_id, "topic": schema_topic,
         "version": current_version, "definition": current_schema_def,
         "action": "baseline"}],
        metadata={"operation": "baseline_snapshot"}, confirm=True)

    # 派生新版本号
    if current_version.startswith("v"):
        try:
            next_major = int(current_version[1:].split(".")[0]) + 1
        except (ValueError, IndexError):
            next_major = 2
        new_version_tag = f"v{next_major}.0"
    else:
        new_version_tag = "v2.0"

    new_schema_def = {
        "topic": schema_topic,
        "version": new_version_tag,
        "added_fields": add_fields,
        "dropped_fields": drop_fields,
        "compatibility": compatibility,
        "evolved_from": current_version,
        "evolve_plan_id": evolve_id,
    }

    schema_evolved_version = store.channel.publish(
        "schema_versions",
        [{"schema_id": schema_id, "topic": schema_topic,
          "version": new_version_tag,
          "definition": str(new_schema_def),
          "action": "evolved"}],
        metadata={"evolve_id": evolve_id, "compatibility": compatibility,
                  "added": add_fields, "dropped": drop_fields},
        confirm=True)

    # 变更追踪: 记录变更条目
    change_record = {
        "target_schema_id": schema_id,
        "target_schema_topic": schema_topic,
        "change_type": "Schema演进",
        "before_version": current_version,
        "after_version": new_version_tag,
        "added_count": len(add_fields),
        "dropped_count": len(drop_fields),
        "compatibility": compatibility,
        "triggered_by": f"schema_evolve:{evolve_id}",
    }

    lineage_version = store.channel.publish(
        "lineage_changes",
        [change_record],
        metadata={"operation": "schema_evolve_changed"},
        confirm=True)

    # 将变更事件通知到 lineage_track 模块
    store.rows("lineage_track").append({
        "id": max((int(r.get("id", 0)) for r in store.rows("lineage_track")), default=0) + 1,
        "status": "已记录", "pending": True, "abnormal": False,
        "变更ID": f"EVOLVE-{schema_id}-{evolve_id}",
        "目标对象": f"{schema_topic}({current_version}→{new_version_tag})",
        "变更类型": "Schema演进",
        "变更前后": f"+{len(add_fields)}/-{len(drop_fields)}",
        "操作时间": time.strftime("%Y-%m-%d", time.gmtime()),
        "触发原因": f"schema_evolve:{evolve_id}",
        "影响范围": f"所有订阅{schema_topic}的下游数据集",
    })

    return {
        "ok": True,
        "pipeline": "Schema注册→模式演进→变更追踪",
        "schema_id": schema_id,
        "evolve_id": evolve_id,
        "topic": schema_topic,
        "before_version": current_version,
        "after_version": new_version_tag,
        "added_fields": add_fields,
        "dropped_fields": drop_fields,
        "compatibility": compatibility,
        "lineage_recorded": True,
        "lineage_version": lineage_version.version_tag,
    }


# ============================================================================
# 流水线 3: 快照管理 → 过期清理 → 冷热分层
# snapshot_mgr 创建快照 → ttl_sweeper 判断过期并清理 →
# cold_warm_tier 根据访问热度迁移存储层级
# ============================================================================

def pipeline_snapshot_to_tier(
    snapshot_id: int,
    ttl_policy_id: int | None = None,
    tier_policy_id: int | None = None,
) -> dict[str, Any]:
    """快照创建后，TTL清道夫检查过期情况，冷热分层策略执行存储迁移。

    支持失败恢复：快照创建失败后重试不覆盖已完成快照。
    """
    snapshot = store.find("snapshot_mgr", snapshot_id)
    if not snapshot:
        return {"ok": False, "error": f"快照 {snapshot_id} 不存在"}

    snapshot_name = snapshot.get("快照名称", "unknown")
    dataset = snapshot.get("目标数据集", "unknown")
    snapshot_time = snapshot.get("快照时间", "")
    snapshot_path = snapshot.get("存储路径", "")

    # 创建快照: 先检查是否已完成（恢复机制）
    if store.recovery.is_completed("snapshot_write", str(snapshot_id)):
        latest = store.channel.latest_confirmed_of("snapshot_data")
        return {
            "ok": True,
            "pipeline": "快照管理→过期清理→冷热分层",
            "snapshot_id": snapshot_id,
            "snapshot_version": latest.version_tag if latest else "unknown",
            "recovered_from_checkpoint": True,
            "message": "快照已完成，重试跳过",
        }

    # 记录创建尝试
    store.recovery.record_attempt(
        "snapshot_write", "attempting", str(snapshot_id))

    snapshot_data = {
        "snapshot_name": snapshot_name, "dataset": dataset,
        "snapshot_time": snapshot_time, "snapshot_path": snapshot_path,
        "snapshot_id": snapshot_id,
    }

    snapshot_version = store.channel.publish(
        "snapshot_data", [snapshot_data],
        metadata={"snapshot_id": snapshot_id, "dataset": dataset},
        confirm=True)

    store.recovery.set_checkpoint("snapshot_write", str(snapshot_id))
    store.recovery.record_attempt(
        "snapshot_write", "completed", snapshot_version.version_tag)

    # TTL清理: 检查快照是否过期
    ttl_expired = False
    ttl_action = "none"
    if ttl_policy_id:
        ttl_policy = store.find("ttl_sweeper", ttl_policy_id)
        if ttl_policy:
            clean_range = ttl_policy.get("清理范围", "")
            match_condition = ttl_policy.get("匹配条件", "")
            clean_action = ttl_policy.get("清理动作", "标记")
            # 模拟过期判断：如果匹配条件包含数据集名
            if dataset in match_condition or match_condition == "all":
                ttl_expired = True
                ttl_action = clean_action

                expiry_record = {
                    "snapshot_id": snapshot_id, "dataset": dataset,
                    "snapshot_name": snapshot_name, "action": ttl_action,
                    "expired": True, "ttl_policy_id": ttl_policy_id,
                }
                store.channel.publish(
                    "ttl_expiry_records", [expiry_record],
                    metadata={"ttl_policy_id": ttl_policy_id,
                              "dataset": dataset},
                    confirm=True)

    # 冷热分层: 根据访问热度决定存储层级
    tier_migrated = False
    tier_target = "warm"
    if tier_policy_id:
        tier_policy = store.find("cold_warm_tier", tier_policy_id)
        if tier_policy:
            hotness = tier_policy.get("热度评估", "cold")
            target = tier_policy.get("目标存储", "S3-COLD")
            tier_target = "cold" if hotness == "cold" else "warm"
            tier_migrated = True

            tier_record = {
                "snapshot_id": snapshot_id, "dataset": dataset,
                "hotness": hotness, "target_tier": tier_target,
                "target_storage": target,
                "migrated_at": time.strftime(
                    "%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            }
            store.channel.publish(
                "tier_migration_records", [tier_record],
                metadata={"tier_policy_id": tier_policy_id,
                          "hotness": hotness},
                confirm=True)

    return {
        "ok": True,
        "pipeline": "快照管理→过期清理→冷热分层",
        "snapshot_id": snapshot_id,
        "snapshot_version": snapshot_version.version_tag,
        "ttl_expired": ttl_expired,
        "ttl_action": ttl_action,
        "tier_migrated": tier_migrated,
        "tier_target": tier_target,
    }


# ============================================================================
# 流水线 4: 分区策略 → 跨区复制 → 入湖监控
# partition_policy 定义分区 → cross_zone_copy 执行跨区拷贝 →
# ingest_monitor 更新吞吐和延迟指标
# ============================================================================

def pipeline_partition_to_monitor(
    policy_id: int,
    copy_task_id: int | None = None,
) -> dict[str, Any]:
    """分区策略生效后触发跨区数据复制，入湖监控面板同步更新指标。

    - 读取分区策略，确定分区键和粒度
    - 跨区复制任务将数据从源区拷贝到目标区
    - 入湖监控面板更新管道的吞吐量、延迟和错误率
    """
    policy = store.find("partition_policy", policy_id)
    if not policy:
        return {"ok": False, "error": f"分区策略 {policy_id} 不存在"}

    partition_key = policy.get("分区键", "ds")
    partition_granularity = policy.get("分区粒度", "daily")
    lifecycle = policy.get("生命周期", "90d")

    # 发布分区策略生效记录
    policy_version = store.channel.publish(
        "partition_policy_active", [{
            "policy_id": policy_id, "partition_key": partition_key,
            "granularity": partition_granularity, "lifecycle": lifecycle,
            "status": "生效中"
        }], metadata={"policy_id": policy_id}, confirm=True)

    # 跨区复制
    copy_result = {"executed": False, "rows_copied": 0, "zone_from": "",
                   "zone_to": ""}
    if copy_task_id:
        copy_task = store.find("cross_zone_copy", copy_task_id)
        if copy_task:
            zone_from = copy_task.get("源湖区", "raw")
            zone_to = copy_task.get("目标湖区", "clean")
            filter_cond = copy_task.get("过滤条件", "")

            # 模拟复制数据（按分区切分）
            copied_partitions = [
                f"{partition_key}=2026-11-15",
                f"{partition_key}=2026-11-16",
                f"{partition_key}=2026-11-17",
            ]
            copy_version = store.channel.publish(
                "cross_zone_copy_records", [{
                    "copy_task_id": copy_task_id, "source": zone_from,
                    "target": zone_to, "filter": filter_cond,
                    "partitions_copied": copied_partitions,
                    "rows_copied": len(copied_partitions) * 1000,
                    "partition_policy_id": policy_id,
                }], metadata={"copy_task_id": copy_task_id,
                              "partition_policy_id": policy_id},
                confirm=True)
            copy_result = {
                "executed": True,
                "rows_copied": len(copied_partitions) * 1000,
                "zone_from": zone_from, "zone_to": zone_to,
                "version": copy_version.version_tag,
            }

    # 入湖监控更新
    monitor_metrics = [{
        "pipeline_name": f"分区复制(策略{policy_id})",
        "throughput_rps": 1500.0,
        "latency_ms": 45.0,
        "error_rate": 0.001,
        "backlog": 0,
        "health_score": 95,
        "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }]
    monitor_version = store.channel.publish(
        "ingest_monitor_metrics", monitor_metrics,
        metadata={"policy_id": policy_id, "copy_task_id": copy_task_id},
        confirm=True)

    return {
        "ok": True,
        "pipeline": "分区策略→跨区复制→入湖监控",
        "policy_id": policy_id,
        "partition_key": partition_key,
        "granularity": partition_granularity,
        "copy_result": copy_result,
        "monitor_version": monitor_version.version_tag,
    }


# ============================================================================
# 流水线 5: 数据留存 → 审计日志
# data_retention 触发过期过渡 → audit_journal 记录清理操作
# ============================================================================

def pipeline_retention_to_audit(
    retention_id: int,
    affected_dataset: str = "",
) -> dict[str, Any]:
    """留存策略触发数据过期过渡，操作审计日志记录完整的清理过程。"""
    retention = store.find("data_retention", retention_id)
    if not retention:
        return {"ok": False, "error": f"留存策略 {retention_id} 不存在"}

    target_lake_zone = retention.get("目标湖区", "")
    retention_days = retention.get("保留天数", "365")
    transition_policy = retention.get("过渡策略", "冷存储过渡")
    clean_method = retention.get("清理方式", "软删除")

    # 留存策略生效记录
    retention_version = store.channel.publish(
        "retention_active", [{
            "retention_id": retention_id,
            "target_zone": target_lake_zone,
            "retention_days": retention_days,
            "transition_policy": transition_policy,
            "clean_method": clean_method,
            "affected_dataset": affected_dataset or target_lake_zone,
        }], metadata={"retention_id": retention_id}, confirm=True)

    # 审计日志记录
    audit_entries = [{
        "操作时间": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "操作主体": "数据留存系统",
        "操作类型": "过期过渡",
        "目标资源": f"{target_lake_zone}(保留{retention_days}天)",
        "请求参数": f"过渡策略={transition_policy}, 清理方式={clean_method}",
        "执行结果": "已触发过渡",
        "耗时": "120ms",
    }]

    audit_version = store.channel.publish(
        "audit_journal_records", audit_entries,
        metadata={"retention_id": retention_id, "operation": "retention_transition"},
        confirm=True)

    return {
        "ok": True,
        "pipeline": "留存策略→审计日志",
        "retention_id": retention_id,
        "target_zone": target_lake_zone,
        "retention_days": retention_days,
        "audit_recorded": True,
        "audit_version": audit_version.version_tag,
    }


# ============================================================================
# 流水线 6: 湖内去重 → 数据字典
# dedup_lake 执行去重 → data_dictionary 更新字段登记
# ============================================================================

def pipeline_dedup_to_dictionary(
    dedup_id: int,
    sample_rows: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """湖内去重配置触发去重管道，数据字典根据去重结果更新字段条目。"""
    dedup = store.find("dedup_lake", dedup_id)
    if not dedup:
        return {"ok": False, "error": f"去重配置 {dedup_id} 不存在"}

    dataset = dedup.get("目标数据集", "unknown")
    dedup_keys_str = dedup.get("去重键", "record_id")
    dedup_keys = [k.strip() for k in dedup_keys_str.split(",") if k.strip()]
    dedup_window = dedup.get("去重窗口", "24h")
    array_policy = dedup.get("覆盖策略", "last_write_wins")

    # 执行去重
    if sample_rows is None:
        sample_rows = [
            {"record_id": "R001", "name": "用户A", "value": 100},
            {"record_id": "R001", "name": "用户A-更新", "value": 120},
            {"record_id": "R002", "name": "用户B", "value": 200},
            {"record_id": "R003", "name": "用户C", "value": 300},
            {"record_id": "R003", "name": "用户C-最新", "value": 350},
        ]

    unique_rows, dup_rows = store.channel.batch_dedup(
        "dedup_results", sample_rows, dedup_keys)

    dedup_version = store.channel.publish(
        "dedup_unique_records", unique_rows,
        metadata={"dedup_id": dedup_id, "dataset": dataset,
                  "dedup_keys": dedup_keys_str, "window": dedup_window,
                  "policy": array_policy},
        confirm=True)

    if dup_rows:
        store.channel.publish(
            "dedup_duplicates", dup_rows,
            metadata={"dedup_id": dedup_id}, confirm=False)

    # 数据字典更新
    field_names = list(sample_rows[0].keys()) if sample_rows else []
    dict_entries = [{
        "条目编码": f"FIELD-{i+1:04d}",
        "字段名称": field,
        "数据类型": type(sample_rows[0][field]).__name__ if sample_rows and field in sample_rows[0] else "string",
        "业务含义": f"数据集{dataset}的{field}字段",
        "取值范围": "自动推导",
        "关联实体": dataset,
        "来源湖区": "dedup_channel",
    } for i, field in enumerate(field_names)]

    dict_version = store.channel.publish(
        "data_dictionary_entries", dict_entries,
        metadata={"dedup_id": dedup_id, "dataset": dataset},
        confirm=True)

    return {
        "ok": True,
        "pipeline": "湖内去重→数据字典",
        "dedup_id": dedup_id,
        "dataset": dataset,
        "dedup_keys": dedup_keys,
        "unique_count": len(unique_rows),
        "duplicate_count": len(dup_rows),
        "dict_entries": len(dict_entries),
        "dict_version": dict_version.version_tag,
    }


# ============================================================================
# 流水线 7: 存储格式 → 灾备策略
# file_format 应用模板 → backup_policy 基于格式创建副本
# ============================================================================

def pipeline_format_to_backup(
    format_id: int,
    backup_policy_id: int | None = None,
) -> dict[str, Any]:
    """存储格式模板应用后，灾备策略基于该格式创建备份副本。"""
    fmt = store.find("file_format", format_id)
    if not fmt:
        return {"ok": False, "error": f"存储格式 {format_id} 不存在"}

    file_format = fmt.get("文件格式", "Parquet")
    compression = fmt.get("压缩算法", "Snappy")
    block_size = fmt.get("块大小", "128MB")
    encoding = fmt.get("编码方式", "UTF-8")
    sort_key = fmt.get("排序键", "")

    # 格式生效
    format_version = store.channel.publish(
        "file_format_active", [{
            "format_id": format_id, "file_format": file_format,
            "compression": compression, "block_size": block_size,
            "encoding": encoding, "sort_key": sort_key,
        }], metadata={"format_id": format_id}, confirm=True)

    # 灾备
    backup_result = {"executed": False, "replicas": 0}
    if backup_policy_id:
        backup = store.find("backup_policy", backup_policy_id)
        if backup:
            protection_scope = backup.get("保护范围", "all")
            backup_period = backup.get("备份周期", "daily")
            replica_count = int(backup.get("副本数量", 3))

            backup_version = store.channel.publish(
                "backup_records", [{
                    "backup_policy_id": backup_policy_id,
                    "source_format": file_format,
                    "compression": compression,
                    "block_size": block_size,
                    "protection_scope": protection_scope,
                    "backup_period": backup_period,
                    "replica_count": replica_count,
                    "based_on_format_id": format_id,
                }], metadata={"backup_policy_id": backup_policy_id,
                              "format_id": format_id},
                confirm=True)
            backup_result = {"executed": True,
                             "replicas": replica_count,
                             "version": backup_version.version_tag}

    return {
        "ok": True,
        "pipeline": "存储格式→灾备策略",
        "format_id": format_id,
        "file_format": file_format,
        "compression": compression,
        "backup_result": backup_result,
    }


# ============================================================================
# 流水线 8: 访问管控 → 质量门禁（权限驱动的质量检查豁免）
# access_control 授权 → quality_gate 根据权限调整阻断级别
# ============================================================================

def pipeline_access_to_quality(
    access_id: int,
    quality_gate_id: int,
) -> dict[str, Any]:
    """访问管控策略影响质量门禁的阻断级别和豁免范围。"""
    access = store.find("access_control", access_id)
    quality = store.find("quality_gate", quality_gate_id)
    if not access:
        return {"ok": False, "error": f"访问策略 {access_id} 不存在"}
    if not quality:
        return {"ok": False, "error": f"质量门禁 {quality_gate_id} 不存在"}

    resource_from = access.get("资源范围", "")
    subject_type = access.get("主体类型", "")
    effective_to = access.get("失效时间", "")
    check_dim = quality.get("检查维度", "")
    block_level_from = quality.get("阻断级别", "HARD")
    exempt_from = quality.get("豁免范围", "")

    # 计算权限驱动的阻断级别
    # 主体类型为admin或有效期未到时，阻断降级为SOFT
    block_level_adjusted = block_level_from
    exempt_adjusted = exempt_from
    if subject_type == "admin":
        block_level_adjusted = "SOFT"
        exempt_adjusted = exempt_from + f",{resource_from}(admin豁免)"
    elif not effective_to or effective_to > time.strftime("%Y-%m-%d", time.gmtime()):
        block_level_adjusted = "SOFT"
        exempt_adjusted = exempt_from + f",{resource_from}(有效期豁免)"

    # 发布权限驱动的质量策略
    quality_version = store.channel.publish(
        "access_driven_quality", [{
            "access_id": access_id, "quality_gate_id": quality_gate_id,
            "check_dimension": check_dim,
            "block_level_original": block_level_from,
            "block_level_adjusted": block_level_adjusted,
            "exempt_original": exempt_from,
            "exempt_adjusted": exempt_adjusted,
            "driven_by": f"访问策略{access_id}({subject_type})",
        }], metadata={"access_id": access_id, "quality_gate_id": quality_gate_id},
        confirm=True)

    return {
        "ok": True,
        "pipeline": "访问管控→质量门禁",
        "access_id": access_id,
        "quality_gate_id": quality_gate_id,
        "block_level_original": block_level_from,
        "block_level_adjusted": block_level_adjusted,
        "exempt_adjusted": exempt_adjusted,
        "quality_version": quality_version.version_tag,
    }


# ============================================================================
# 流水线调度：一键运行所有流水线并返回汇总
# ============================================================================

def run_all_pipelines() -> dict[str, Any]:
    """运行全部8条跨模块数据流水线并输出汇总报告。"""
    results = []

    # 流水线1: 入湖管道→目录注册
    r1 = pipeline_ingest_to_catalog(1)
    results.append(("入湖管道→湖区写入→质量门禁→目录注册", r1))

    # 流水线2: Schema注册→演化→变更追踪
    r2 = pipeline_schema_evolve_to_lineage(1, 1)
    results.append(("Schema注册→模式演进→变更追踪", r2))

    # 流水线3: 快照管理→过期清理→冷热分层
    r3 = pipeline_snapshot_to_tier(1, ttl_policy_id=1, tier_policy_id=1)
    results.append(("快照管理→过期清理→冷热分层", r3))

    # 流水线4: 分区策略→跨区复制→入湖监控
    r4 = pipeline_partition_to_monitor(1, copy_task_id=1)
    results.append(("分区策略→跨区复制→入湖监控", r4))

    # 流水线5: 留存策略→审计日志
    r5 = pipeline_retention_to_audit(1)
    results.append(("留存策略→审计日志", r5))

    # 流水线6: 湖内去重→数据字典
    r6 = pipeline_dedup_to_dictionary(1)
    results.append(("湖内去重→数据字典", r6))

    # 流水线7: 存储格式→灾备策略
    r7 = pipeline_format_to_backup(1, backup_policy_id=1)
    results.append(("存储格式→灾备策略", r7))

    # 流水线8: 访问管控→质量门禁
    r8 = pipeline_access_to_quality(1, 1)
    results.append(("访问管控→质量门禁", r8))

    all_ok = all(result[1].get("ok") for result in results)
    pipeline_summaries = [
        {"name": name, "ok": result.get("ok"), **{
            k: v for k, v in result.items() if k not in ("ok",)}
        }
        for name, result in results
    ]

    return {
        "ok": all_ok,
        "pipelines_run": len(results),
        "pipelines_ok": sum(1 for _, r in results if r.get("ok")),
        "pipeline_summaries": pipeline_summaries,
        "pipeline_overview": store.pipeline_overview(),
    }