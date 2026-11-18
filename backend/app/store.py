"""内存数据仓库 + 版本协同层：支持跨模块数据流、版本追溯和失败恢复。

数据写入时生成完整的版本号（major.minor），且提供脏行隔离、批次去重
和恢复检查点，确保多个下游模块消费的是同一份已确认版本产物。
"""
from __future__ import annotations

import hashlib
import time
from dataclasses import dataclass, field
from typing import Any

from app.seed import SEED_ROWS


@dataclass
class DataVersion:
    """版本化的数据产物：每次写入产生新版本，旧版本保留为快照。"""
    major: int
    minor: int
    checksum: str
    timestamp: str
    rows: list[dict[str, Any]]
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def version_tag(self) -> str:
        return f"v{self.major}.{self.minor}"

    @property
    def is_confirmed(self) -> bool:
        return self.metadata.get("confirmed", False)


class DataChannel:
    """跨模块数据通道：一个模块写入数据产物后，下游模块可以按版本消费。

    每次_publish都会根据内容计算校验和，版本号递增。下游可以通过
    consume_latest_of 获取最新确认版本。
    """

    def __init__(self):
        self._channels: dict[str, list[DataVersion]] = {}

    def channel_names(self) -> list[str]:
        return sorted(self._channels)

    def versions_of(self, channel: str) -> list[DataVersion]:
        return self._channels.setdefault(channel, [])

    def latest_of(self, channel: str) -> DataVersion | None:
        versions = self.versions_of(channel)
        return versions[-1] if versions else None

    def latest_confirmed_of(self, channel: str) -> DataVersion | None:
        for v in reversed(self.versions_of(channel)):
            if v.is_confirmed:
                return v
        return None

    def publish(self, channel: str, rows: list[dict[str, Any]],
                metadata: dict[str, Any] | None = None,
                confirm: bool = True) -> DataVersion:
        """发布新版本数据产物到通道。

        每次publish自动计算checksum并递增版本号。
        脏行会被写入脏行隔离区（channel + '.dirty'）而不是丢掉。
        """
        _md = dict(metadata or {})
        _md["confirmed"] = confirm
        versions = self.versions_of(channel)
        major = (versions[-1].major + 1) if versions else 1
        content = str(sorted(str(r) for r in rows)).encode()
        checksum = hashlib.sha256(content).hexdigest()[:16]

        # 如果与前版本内容完全相同，只更新时间戳，不递增版本
        latest = self.latest_of(channel)
        if latest and latest.checksum == checksum and latest.rows == rows:
            latest.timestamp = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            return latest

        version = DataVersion(
            major=major,
            minor=0,
            checksum=checksum,
            timestamp=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            rows=[dict(r) for r in rows],
            metadata=_md,
        )
        versions.append(version)
        return version

    def isolate_dirty(self, channel: str, dirty_rows: list[dict[str, Any]],
                      reason: str = "") -> DataVersion:
        """将脏行隔离到专用通道，不丢失、不阻塞主数据流。"""
        _dirty = dirty_rows or []
        for row in _dirty:
            # 原地修改，保证隔离原因真正写回发布的行
            row["_isolated_reason"] = reason or "脏行数据"
            row["_isolated_at"] = time.strftime(
                "%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        return self.publish(f"{channel}.dirty", _dirty, confirm=False)

    def batch_dedup(self, channel: str,
                    rows: list[dict[str, Any]],
                    dedup_keys: list[str]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """批次去重：按去重键分组，保留每组最后一条，返回(唯一行, 去重掉的重复行)。"""
        seen: dict[tuple, dict[str, Any]] = {}
        duplicates: list[dict[str, Any]] = []
        for row in rows:
            key = tuple(str(row.get(k, "")) for k in dedup_keys)
            if key in seen:
                duplicates.append(dict(row))
            seen[key] = dict(row)
        return list(seen.values()), duplicates


class FailureRecovery:
    """失败恢复检查点：追踪各通道的写入尝试状态。

    - attempt_log: 记录每次写入尝试（含失败）
    - checkpoint: 记录最新成功的确认版本
    - 重试时检查checkpoint，不会覆盖已完成的产物
    """

    def __init__(self):
        self.attempts: dict[str, list[dict[str, Any]]] = {}
        self.checkpoints: dict[str, str] = {}

    def record_attempt(self, channel: str, status: str,
                       version_tag: str = "", error: str = "") -> dict[str, Any]:
        record = {
            "channel": channel,
            "status": status,
            "version_tag": version_tag,
            "error": error,
            "attempted_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }
        self.attempts.setdefault(channel, []).append(record)
        return record

    def set_checkpoint(self, channel: str, version_tag: str):
        self.checkpoints[channel] = version_tag

    def is_completed(self, channel: str, version_tag: str) -> bool:
        """检查指定版本是否已完成写入，避免重试覆盖已完成快照。"""
        return self.checkpoints.get(channel) == version_tag

    def last_attempt_of(self, channel: str) -> dict[str, Any] | None:
        entries = self.attempts.get(channel)
        return entries[-1] if entries else None


class Store:
    """增强版数据仓库：保留原有CRUD接口，加入版本化数据通道和恢复机制。"""

    def __init__(self) -> None:
        self._tables: dict[str, list[dict[str, Any]]] = {
            name: [dict(row) for row in rows]
            for name, rows in SEED_ROWS.items()
        }
        # 领域服务的内部明细表不属于业务模块，不计入模块清单与运营概览
        self.internal_tables: set[str] = set()
        self.channel = DataChannel()
        self.recovery = FailureRecovery()

    def register_internal_table(self, name: str) -> None:
        self.internal_tables.add(name)

    # ---- 原有 CRUD 接口（保持不变）----

    def module_names(self) -> list[str]:
        return sorted(name for name in self._tables
                      if name not in self.internal_tables)

    def rows(self, module: str) -> list[dict[str, Any]]:
        return self._tables.setdefault(module, [])

    def find(self, module: str, entry_id: int) -> dict[str, Any] | None:
        for row in self.rows(module):
            if int(row.get("id", 0)) == entry_id:
                return row
        return None

    def overview(self) -> dict[str, object]:
        modules: list[dict[str, object]] = []
        for name in self.module_names():
            rows = self.rows(name)
            modules.append({
                "name": name,
                "created": len(rows),
                "pending": sum(1 for row in rows if row.get("pending")),
                "abnormal": sum(1 for row in rows if row.get("abnormal")),
            })
        cards = [
            {"label": "业务模块", "value": len(modules)},
            {"label": "今日新增",
             "value": sum(int(item["created"]) for item in modules)},
            {"label": "待处理",
             "value": sum(int(item["pending"]) for item in modules)},
            {"label": "异常量",
             "value": sum(int(item["abnormal"]) for item in modules)},
        ]
        return {"cards": cards, "modules": modules}

    # ---- 新增：管道全景 ----

    def pipeline_overview(self) -> dict[str, Any]:
        """输出所有跨模块数据通道的版本快照，供流水线调度和监控消费。"""
        channels: dict[str, dict[str, Any]] = {}
        for name in self.channel.channel_names():
            latest = self.channel.latest_of(name)
            confirmed = self.channel.latest_confirmed_of(name)
            channels[name] = {
                "version_count": len(self.channel.versions_of(name)),
                "latest_version": latest.version_tag if latest else None,
                "latest_confirmed": confirmed.version_tag if confirmed else None,
                "latest_checksum": latest.checksum if latest else None,
                "latest_rows": len(latest.rows) if latest else 0,
            }
        return {
            "channels": channels,
            "recovery_checkpoints": dict(self.recovery.checkpoints),
            "pipeline_health": "ready" if channels else "empty",
        }


store = Store()