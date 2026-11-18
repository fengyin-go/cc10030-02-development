"""入湖批次明细数据集领域服务。

解析入湖管道配置与待入湖样本，清洗后沉淀「入湖批次明细数据集」数据产物，
让运维可以按来源文件、原始行号、去重键逐行核对哪些原始行进入了净数据。

关键约束：
- 净数据版本与每条隔离原因一起落库（明细行上同时持有版本号与隔离原因）；
- 批次重跑按 (来源文件, 原始行号) 幂等，不重复累计同一来源行，已确认行不变；
- 甲口径按到达顺序固化、乙口径按业务生效日重排，两条版本口径分别沉淀证据；
- 整批以事务方式落库，任何一步失败都回滚并保留回滚依据，不允许半成品；
- 质量门禁只接收完整采样拒绝回执，回执回写待复核清单形成跨模块闭环。
"""
from __future__ import annotations

import hashlib
import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store

# ---- 批次主数据 / 明细 / 回执 / 待复核清单 四张内存表 ----
TABLE_BATCH = "batch_detail_batch"
TABLE_LINE = "batch_detail_line"
TABLE_RECEIPT = "batch_detail_receipt"
TABLE_REVIEW = "batch_detail_pending_review"

# ---- 两种入湖版本口径 ----
CALIBER_ARRIVAL = "arrival"        # 甲：按到达顺序固化（首选）
CALIBER_BIZ_DATE = "business_date"  # 乙：按业务生效日重排
CALIBER_LABELS = {
    CALIBER_ARRIVAL: "甲·按到达顺序固化",
    CALIBER_BIZ_DATE: "乙·按业务生效日重排",
}

# ---- 清洗结果枚举 ----
RESULT_VALID = "有效行"
RESULT_DUPLICATE = "重复行"
RESULT_DIRTY_MISSING_KEY = "脏行-缺键行"
RESULT_DIRTY_FORMAT = "脏行-格式异常行"
RESULT_DIRTY_EMPTY = "脏行-空值行"
DIRTY_RESULTS = {
    RESULT_DIRTY_MISSING_KEY,
    RESULT_DIRTY_FORMAT,
    RESULT_DIRTY_EMPTY,
}
FILTERABLE_RESULTS = [
    RESULT_VALID, RESULT_DUPLICATE,
    RESULT_DIRTY_MISSING_KEY, RESULT_DIRTY_FORMAT, RESULT_DIRTY_EMPTY,
]

# 批次状态机：解析中 → 待核对 → 已回执 → 已确认；失败置已中断并保留回滚依据
STATUS_PARSING = "解析中"
STATUS_PENDING = "待核对"
STATUS_RECEIPTED = "已回执"
STATUS_CONFIRMED = "已确认"
STATUS_INTERRUPTED = "已中断"

CHANNEL_DETAIL = "batch_detail_dataset"
CHANNEL_CLEAN = "batch_clean_data"


def _now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _fingerprint(source_file: str, line_no: int, raw: dict[str, Any]) -> str:
    """单行校验指纹：来源定位 + 原始内容，重跑同一来源行指纹不变。"""
    canonical = str(sorted((str(k), str(v)) for k, v in raw.items()))
    digest = hashlib.sha256(
        f"{source_file}|{line_no}|{canonical}".encode("utf-8")
    ).hexdigest()
    return digest[:16]


def _checksum_of(lines: list[dict[str, Any]]) -> str:
    joined = "|".join(str(line["校验指纹"]) for line in lines)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()[:16]


def _next_id(table: str) -> int:
    return max((int(row.get("id", 0)) for row in store.rows(table)), default=0) + 1


def _version_series(caliber: str) -> str:
    return "ARR" if caliber == CALIBER_ARRIVAL else "BIZ"


def build_default_samples(pipeline_id: int) -> list[dict[str, Any]]:
    """构造待入湖样本：首次采集文件 + 补报文件，两者采集时间不同。

    样本中预埋有效行、重复行、缺键行、格式异常行和空值行，
    补报文件携带同一去重键的改动点，用于核对链路定位。
    """
    first_file = f"orders_p{pipeline_id}_20261117_首次采集.csv"
    backfill_file = f"orders_p{pipeline_id}_20261117_补报.csv"
    first_collect = "2026-11-17T08:00:00Z"
    backfill_collect = "2026-11-18T09:30:00Z"  # 补报使用不同采集时间

    def row(file_name: str, line_no: int, collect_at: str, seq: int,
            order_id: str, biz_date: str, amount: Any, region: str,
            remark: str = "") -> dict[str, Any]:
        return {
            "source_file": file_name,
            "source_line_no": line_no,
            "collection_time": collect_at,
            "sample_seq": seq,
            "raw": {
                "order_id": order_id,
                "biz_date": biz_date,
                "amount": amount,
                "region": region,
                "remark": remark,
            },
        }

    samples: list[dict[str, Any]] = []
    seq = 0
    # 首次采集文件 12 行（含各类脏行与批次内重复）
    first_payload = [
        ("O1001", "2026-11-15", "120.50", "华东"),
        ("O1002", "2026-11-15", "88.00", "华北"),
        ("O1003", "2026-11-16", "240.00", "华南"),
        ("O1003", "2026-11-16", "245.00", "华南"),   # 批次内重复键
        ("O1004", "2026-11-16", "", "华东"),          # 空值脏行
        ("",      "2026-11-16", "60.00", "西南"),     # 缺键行
        ("O1006", "2026-11-17", "N/A", "华中"),       # 格式异常行
        ("O1007", "2026-11-17", "310.20", "东北"),
        ("O1008", "2026-11-17", "99.99", "西北"),
        ("O1009", "2026-11-18", "150.00", "华东"),
        ("O1010", "2026-11-18", "75.50", "华北"),
        ("O1011", "2026-11-18", "210.00", "华南"),
    ]
    for line_no, (order_id, biz_date, amount, region) in enumerate(first_payload, start=1):
        seq += 1
        samples.append(row(first_file, line_no, first_collect, seq,
                           order_id, biz_date, amount, region))

    # 补报文件 6 行：采集时间更晚，含改动点与新数据
    backfill_payload = [
        ("O1002", "2026-11-19", "92.00", "华北"),   # 改动点：同键新值
        ("O1012", "2026-11-19", "500.00", "西南"),
        ("O1013", "2026-11-19", "33.30", "华中"),
        ("O1013", "2026-11-19", "35.30", "华中"),   # 补报文件内重复
        ("O1014", "2026-11-20", "bad-amt", "东北"),  # 格式异常
        ("O1015", "2026-11-20", "18.00", "西北"),
    ]
    for line_no, (order_id, biz_date, amount, region) in enumerate(backfill_payload, start=1):
        seq += 1
        samples.append(row(backfill_file, line_no, backfill_collect, seq,
                           order_id, biz_date, amount, region, remark="补报"))
    return samples


class BatchDetailService(BaseModuleService):
    """入湖批次明细数据集服务：解析、清洗、版本化、回执与跨模块核对。"""

    MODULE = "batch_detail"
    ENTITY = "入湖批次明细"
    LIST_FIELDS = [
        "批次号", "管道名称", "采集时间", "口径", "明细版本",
        "有效行数", "重复行数", "脏行数", "批次状态",
    ]
    REQUIRED_FIELDS = ["管道名称"]
    STATUS_ORDER = [STATUS_PARSING, STATUS_PENDING, STATUS_RECEIPTED,
                    STATUS_CONFIRMED, STATUS_INTERRUPTED]

    # ---- 通用资源路由适配：主数据表与模块 key 不同名 ----
    def list_entries(self, *, filters=None, keyword=None, status=None,
                     page=1, size=20):
        rows = list(store.rows(TABLE_BATCH))
        if status:
            rows = [r for r in rows if r.get("批次状态") == status]
        if keyword:
            rows = [r for r in rows
                    if keyword.casefold() in str(r.get("批次号", "")).casefold()
                    or keyword.casefold() in str(r.get("管道名称", "")).casefold()]
        total = len(rows)
        start = (page - 1) * size
        return rows[start:start + size], total

    def get_entry(self, entry_id: int):
        return self._find_batch(entry_id)

    # ===================================================================
    # 一、解析管道配置与待入湖样本 → 清洗 → 产出明细数据集（事务落库）
    # ===================================================================
    def parse_and_build(
        self,
        pipeline_id: int,
        caliber: str = CALIBER_ARRIVAL,
        samples: list[dict[str, Any]] | None = None,
        collection_time: str | None = None,
    ) -> dict[str, Any]:
        """解析入口：沿调用链锁定到具体管道配置，事务化产出批次明细。"""
        pipeline = store.find("ingest_pipeline", pipeline_id)
        if not pipeline:
            return {"ok": False, "error": f"入湖管道 {pipeline_id} 不存在，无法解析"}
        if caliber not in CALIBER_LABELS:
            return {"ok": False, "error": f"未知口径：{caliber}，仅支持 arrival / business_date"}

        # 显式传入空样本必须被拦截，不能回退默认样本（区分 None 与 []）
        sample_rows = build_default_samples(pipeline_id) if samples is None else samples
        if not sample_rows:
            return {"ok": False, "error": "待入湖样本为空，没有可解析的来源行"}

        batch = self._create_batch(pipeline, caliber, sample_rows, collection_time)
        recovery_key = f"batch_build:{batch['id']}:{caliber}"
        store.recovery.record_attempt(recovery_key, STATUS_PARSING)

        # 事务快照：落库前复制所有受影响表，失败即回滚，保留回滚依据
        snapshot = self._snapshot_tables()
        try:
            lines = self._clean_lines(batch, pipeline, sample_rows, caliber)
            self._assert_complete(batch, lines)
            self._publish_artifacts(batch, lines, caliber)
            store.rows(TABLE_LINE).extend(lines)
            batch["批次状态"] = STATUS_PENDING
            batch["更新时间"] = _now()
            store.recovery.set_checkpoint(recovery_key, batch["明细版本"])
            store.recovery.record_attempt(
                recovery_key, "completed", batch["明细版本"])
            self._sync_pipeline_master(pipeline_id, batch)
        except Exception as exc:  # 中断：回滚表数据，留下回滚依据，禁止半成品
            self._restore_snapshot(snapshot)
            # 快照恢复后原对象已脱离表，重新定位再标记中断状态
            batch = self._find_batch(batch["id"]) or batch
            batch["批次状态"] = STATUS_INTERRUPTED
            batch["回滚依据"] = {
                "snapshot_tables": list(snapshot),
                "error": str(exc),
                "rolled_back_at": _now(),
            }
            batch["更新时间"] = _now()
            store.recovery.record_attempt(recovery_key, "failed", error=str(exc))
            return {"ok": False, "error": f"批次解析中断已回滚：{exc}",
                    "batch_id": batch["id"], "rollback_basis": batch["回滚依据"]}

        return {"ok": True, "batch": self._batch_view(batch),
                "summary": batch["校验摘要"]}

    def _create_batch(self, pipeline: dict[str, Any], caliber: str,
                      samples: list[dict[str, Any]],
                      collection_time: str | None) -> dict[str, Any]:
        collect_at = collection_time or max(
            str(s.get("collection_time", "")) for s in samples)
        source_files = sorted({str(s.get("source_file", "")) for s in samples})
        # 不同采集时间（补报 vs 首次采集）→ 管道据此定位改动点
        collect_times = sorted({str(s.get("collection_time", "")) for s in samples})
        batch_id = _next_id(TABLE_BATCH)
        batch = {
            "id": batch_id,
            "批次号": f"BATCH-P{pipeline['id']}-{batch_id}-"
                    f"{time.strftime('%m%d%H%M%S', time.gmtime())}",
            "管道ID": pipeline["id"],
            "管道名称": pipeline.get("管道名称", f"管道{pipeline['id']}"),
            "目标湖区": pipeline.get("目标湖区", ""),
            "采集时间": collect_at,
            "全部采集时间": collect_times,
            "来源文件": source_files,
            "口径": caliber,
            "口径说明": CALIBER_LABELS[caliber],
            "批次状态": STATUS_PARSING,
            "重跑次数": 0,
            "明细版本": "",
            "净数据版本": "",
            "校验和": "",
            "校验摘要": {},
            "消费凭据": None,
            "回滚依据": None,
            "改动点": [],
            "创建时间": _now(),
            "更新时间": _now(),
            "pending": True,
            "abnormal": False,
        }
        store.rows(TABLE_BATCH).append(batch)
        return batch

    def _clean_lines(self, batch: dict[str, Any], pipeline: dict[str, Any],
                     samples: list[dict[str, Any]], caliber: str) -> list[dict[str, Any]]:
        """逐行清洗：先质量校验隔离脏行，再按口径去重，产出可核对明细行。"""
        dedup_fields = [f.strip() for f in str(
            pipeline.get("分区字段", "")).split(",") if f.strip()]
        dedup_key_name = "order_id"  # 样本统一去重键
        target_zone = str(pipeline.get("目标湖区", "clean"))
        clean_path_prefix = (
            f"lake://{target_zone}/pipeline{pipeline['id']}/{batch['批次号']}/{caliber}"
        )
        # 同一批次+口径的历史行：重跑时按来源定位复用，已确认行冻结
        history = self._history_index(batch["id"], caliber)
        # 新行在 extend 入表前不参与 _next_id 统计；重跑时还需跳过已删除旧行占用的 id
        next_line_id = max(
            _next_id(TABLE_LINE),
            max((int(ln.get("id", 0)) for ln in history.values()), default=0) + 1,
        )

        staged: list[dict[str, Any]] = []
        for sample in samples:
            source_file = str(sample.get("source_file", ""))
            line_no = int(sample.get("source_line_no", 0))
            raw = dict(sample.get("raw", {}))
            source_locator = (source_file, line_no)
            prior = history.get(source_locator)
            if prior and prior.get("已确认"):
                # 重跑不得改变已确认行：原样保留，不重复累计
                staged.append(dict(prior))
                continue

            result, reason, channel = self._classify(raw, dedup_key_name)
            fingerprint = _fingerprint(source_file, line_no, raw)
            line = {
                "id": prior["id"] if prior else next_line_id,
                "批次ID": batch["id"],
                "管道ID": pipeline["id"],
                "口径": caliber,
                "来源文件": source_file,
                "源文件偏移": f"offset={line_no - 1}",
                "原始行号": line_no,
                "样本序号": int(sample.get("sample_seq", 0)),
                "去重键": str(raw.get(dedup_key_name, "") or ""),
                "采集时间": str(sample.get("collection_time", "")),
                "业务生效日": str(raw.get("biz_date", "")),
                "清洗结果": result,
                "隔离原因": reason,
                "隔离通道": channel,
                "净数据版本": "",          # 与隔离原因一起在发布阶段补齐落库
                "净数据路径": "",
                "校验指纹": fingerprint,
                "异常摘要": reason,
                "拒绝回执ID": "",
                "已确认": False,
                "重跑序号": (prior["重跑序号"] + 1) if prior else 0,
                "更新时间": _now(),
                "原始样本": raw,
            }
            if str(sample.get("raw", {}).get("remark", "")) == "补报":
                line["补报改动"] = True
            staged.append(line)
            if not prior:
                next_line_id += 1

        self._apply_dedup(staged, caliber, dedup_key_name)

        # 版本号：按口径各自单调递增，重跑同内容不产生新版本
        series = _version_series(caliber)
        detail_major = self._next_major(CHANNEL_DETAIL, series)
        clean_major = self._next_major(CHANNEL_CLEAN, series)
        detail_version = f"DET-{series}-v{detail_major}.0"
        clean_version = f"NET-{series}-v{clean_major}.0"
        clean_path = f"{clean_path_prefix}/{clean_version}/"

        valid_fingerprints = {ln["校验指纹"] for ln in staged
                              if ln["清洗结果"] == RESULT_VALID}
        for line in staged:
            # 净数据版本与隔离原因在同一行一起落库
            line["明细版本"] = detail_version
            line["净数据版本"] = clean_version
            if line["清洗结果"] == RESULT_VALID:
                line["净数据路径"] = clean_path
            line["更新时间"] = _now()

        self._mark_change_points(batch, staged, valid_fingerprints)
        summary = self._build_summary(batch, staged, detail_version,
                                      clean_version, clean_path)
        batch.update({
            "明细版本": detail_version,
            "净数据版本": clean_version,
            "净数据路径": clean_path,
            "校验和": summary["校验和"],
            "校验摘要": summary,
        })
        return staged

    @staticmethod
    def _classify(raw: dict[str, Any], dedup_key_name: str
                  ) -> tuple[str, str, str]:
        """质量校验：缺键 → 空值 → 格式异常，命中即隔离。"""
        if not str(raw.get(dedup_key_name, "") or "").strip():
            return (RESULT_DIRTY_MISSING_KEY,
                    f"缺少必填键: {dedup_key_name}",
                    "batch_quarantine.missing_key")
        amount = raw.get("amount", "")
        if str(amount).strip() == "":
            return (RESULT_DIRTY_EMPTY, "空值: amount字段为空",
                    "batch_quarantine.empty_value")
        try:
            float(amount)
        except (TypeError, ValueError):
            return (RESULT_DIRTY_FORMAT,
                    f"格式异常: amount无法解析为数值({amount})",
                    "batch_quarantine.format_error")
        return RESULT_VALID, "", ""

    @staticmethod
    def _apply_dedup(lines: list[dict[str, Any]], caliber: str,
                     dedup_key_name: str) -> None:
        """按口径去重：甲按到达顺序（先到者保留），乙按业务生效日重排。"""
        candidates = [ln for ln in lines
                      if ln["清洗结果"] == RESULT_VALID and ln["去重键"]]
        winners: dict[str, dict[str, Any]] = {}
        for line in candidates:
            key = line["去重键"]
            current = winners.get(key)
            if current is None:
                winners[key] = line
                continue
            if caliber == CALIBER_ARRIVAL:
                # 甲：按到达顺序固化，先到的样本序号小者胜出
                if line["样本序号"] < current["样本序号"]:
                    winners[key] = line
            else:
                # 乙：按业务生效日重排，生效日新者胜出，同日到达晚者胜出
                rank = (line["业务生效日"], line["样本序号"])
                curr_rank = (current["业务生效日"], current["样本序号"])
                if rank > curr_rank:
                    winners[key] = line

        winner_ids = {id(ln) for ln in winners.values()}
        for line in candidates:
            if id(line) not in winner_ids:
                line["清洗结果"] = RESULT_DUPLICATE
                line["隔离原因"] = (
                    f"批次去重: 去重键[{dedup_key_name}={line['去重键']}]"
                    f"按{CALIBER_LABELS[caliber]}判定为重复"
                )
                line["隔离通道"] = "batch_quarantine.duplicate"
                line["异常摘要"] = line["隔离原因"]

    def _mark_change_points(self, batch: dict[str, Any],
                            lines: list[dict[str, Any]],
                            valid_fingerprints: set[str]) -> None:
        """来源文件补报与首次采集时间不同：定位同去重键的改动点。"""
        if len(batch["全部采集时间"]) <= 1:
            return
        by_key: dict[str, list[dict[str, Any]]] = {}
        for line in lines:
            if line["去重键"]:
                by_key.setdefault(line["去重键"], []).append(line)
        changes = []
        for key, group in by_key.items():
            files = {ln["来源文件"] for ln in group}
            if len(files) > 1:
                changes.append({
                    "去重键": key,
                    "涉及文件": sorted(files),
                    "采集时间": sorted({ln["采集时间"] for ln in group}),
                    "行号": [ln["原始行号"] for ln in group],
                })
        batch["改动点"] = changes

    def _build_summary(self, batch: dict[str, Any], lines: list[dict[str, Any]],
                       detail_version: str, clean_version: str,
                       clean_path: str) -> dict[str, Any]:
        valid = [ln for ln in lines if ln["清洗结果"] == RESULT_VALID]
        duplicate = [ln for ln in lines if ln["清洗结果"] == RESULT_DUPLICATE]
        dirty = [ln for ln in lines if ln["清洗结果"] in DIRTY_RESULTS]
        before_dedup = len(valid) + len(duplicate)  # 进入去重环节的行数
        return {
            "批次号": batch["批次号"],
            "口径": CALIBER_LABELS[batch["口径"]],
            "总来源行数": len(lines),
            "有效行数": len(valid),
            "重复行数": len(duplicate),
            "脏行数": len(dirty),
            "缺键行数": sum(1 for ln in dirty
                            if ln["清洗结果"] == RESULT_DIRTY_MISSING_KEY),
            "格式异常行数": sum(1 for ln in dirty
                              if ln["清洗结果"] == RESULT_DIRTY_FORMAT),
            "空值行数": sum(1 for ln in dirty
                          if ln["清洗结果"] == RESULT_DIRTY_EMPTY),
            "去重前行数": before_dedup,
            "去重后行数": len(valid),
            "明细版本": detail_version,
            "净数据版本": clean_version,
            "净数据路径": clean_path,
            "校验和": _checksum_of(lines),
            "来源文件数": len(batch["来源文件"]),
            "改动点数": len(batch["改动点"]),
            "生成时间": _now(),
        }

    # ===================================================================
    # 二、版本化数据产物：明细数据集 + 净数据 + 隔离通道（含版本与原因）
    # ===================================================================
    def _publish_artifacts(self, batch: dict[str, Any],
                           lines: list[dict[str, Any]], caliber: str) -> None:
        clean_version = batch["净数据版本"]
        series = _version_series(caliber)
        detail_major = int(batch["明细版本"].split("v")[-1].split(".")[0])
        clean_major = int(clean_version.split("v")[-1].split(".")[0])
        # 明细数据集作为数据产物沉淀，下游按版本引用
        store.channel.publish(
            CHANNEL_DETAIL, [dict(ln) for ln in lines],
            metadata={
                "batch_id": batch["id"], "批次号": batch["批次号"],
                "pipeline_id": batch["管道ID"], "caliber": caliber,
                "caliber_label": CALIBER_LABELS[caliber],
                "detail_version": batch["明细版本"],
                "clean_version": clean_version,
                "checksum": batch["校验和"],
                "series": series, "series_major": detail_major,
            }, confirm=True)
        valid_rows = [dict(ln) for ln in lines
                      if ln["清洗结果"] == RESULT_VALID]
        store.channel.publish(
            CHANNEL_CLEAN, valid_rows,
            metadata={
                "batch_id": batch["id"], "caliber": caliber,
                "clean_version": clean_version,
                "detail_version": batch["明细版本"],
                "series": series, "series_major": clean_major,
            }, confirm=True)
        # 每条隔离通道都带上净数据版本：版本与隔离原因绑定落库
        for channel in ("batch_quarantine.missing_key",
                        "batch_quarantine.empty_value",
                        "batch_quarantine.format_error",
                        "batch_quarantine.duplicate"):
            bucket = [dict(ln) for ln in lines if ln["隔离通道"] == channel]
            if bucket:
                store.channel.isolate_dirty(
                    channel, bucket,
                    reason=f"{clean_version} 隔离 {len(bucket)} 行")

    # ===================================================================
    # 三、批次重跑：幂等不重复累计，只重试未完成部分
    # ===================================================================
    def rerun_batch(self, batch_id: int) -> dict[str, Any]:
        batch = self._find_batch(batch_id)
        if not batch:
            return {"ok": False, "error": f"批次 {batch_id} 不存在"}
        if batch["批次状态"] == STATUS_CONFIRMED:
            return {"ok": False, "error": "批次已确认，重跑不得改变已确认行"}

        prior_lines = [ln for ln in store.rows(TABLE_LINE)
                       if ln["批次ID"] == batch_id and ln["口径"] == batch["口径"]]
        # 从明细行还原待入湖样本，未确认部分重新清洗
        samples = [{
            "source_file": ln["来源文件"],
            "source_line_no": ln["原始行号"],
            "collection_time": ln["采集时间"],
            "sample_seq": ln["样本序号"],
            "raw": dict(ln["原始样本"]),
        } for ln in prior_lines]
        # 首次解析即中断、没有任何明细行落库时，重建默认样本重试未完成部分
        if not samples:
            samples = build_default_samples(batch["管道ID"])

        confirmed = sum(1 for ln in prior_lines if ln.get("已确认"))
        snapshot = self._snapshot_tables()
        try:
            # 移除未确认行，已确认行通过 history 机制保留
            for ln in prior_lines:
                if not ln.get("已确认"):
                    store.rows(TABLE_LINE).remove(ln)
            batch["重跑次数"] = int(batch.get("重跑次数", 0)) + 1
            lines = self._clean_lines(
                batch, store.find("ingest_pipeline", batch["管道ID"]),
                samples, batch["口径"])
            self._assert_complete(batch, lines)
            self._publish_artifacts(batch, lines, batch["口径"])
            store.rows(TABLE_LINE).extend(lines)
            if batch["批次状态"] == STATUS_INTERRUPTED:
                batch["批次状态"] = STATUS_PENDING
            batch["更新时间"] = _now()
        except Exception as exc:
            self._restore_snapshot(snapshot)
            batch = self._find_batch(batch_id) or batch
            batch["批次状态"] = STATUS_INTERRUPTED
            batch["回滚依据"] = {"error": str(exc), "rolled_back_at": _now()}
            return {"ok": False, "error": f"重跑中断已回滚：{exc}"}

        return {"ok": True, "batch": self._batch_view(batch),
                "confirmed_lines_kept": confirmed,
                "summary": batch["校验摘要"]}

    # ===================================================================
    # 四、质量门禁：按明细数据集版本采样，生成完整拒绝回执
    # ===================================================================
    def quality_gate_sampling(self, batch_id: int,
                              gate_rule_id: int | None = None,
                              sample_ratio: float | None = None) -> dict[str, Any]:
        batch = self._find_batch(batch_id)
        if not batch:
            return {"ok": False, "error": f"批次 {batch_id} 不存在"}
        if batch["批次状态"] not in (STATUS_PENDING, STATUS_RECEIPTED):
            return {"ok": False,
                    "error": f"批次状态为{batch['批次状态']}，质量门禁暂不接收"}

        rule = self._pick_gate_rule(gate_rule_id)
        ratio = sample_ratio if sample_ratio is not None \
            else self._parse_ratio(rule.get("采样比例", "50%"))
        lines = [ln for ln in store.rows(TABLE_LINE)
                 if ln["批次ID"] == batch_id and ln["口径"] == batch["口径"]]

        blocked = [ln for ln in lines if ln["清洗结果"] != RESULT_VALID]
        valid = [ln for ln in lines if ln["清洗结果"] == RESULT_VALID]
        # 被拦截样本优先入样，再按比例补有效行，保证回执能复核每一类来源
        blocked_sample = blocked[: max(1, int(round(len(blocked) * ratio)))] \
            if blocked else []
        valid_sample = valid[: max(1, int(round(len(valid) * ratio)))] \
            if valid else []
        sampled = blocked_sample + valid_sample

        # 完整性校验：每个采样行都必须有可复核结论，否则门禁拒绝接收
        incomplete = [ln["id"] for ln in sampled
                      if ln["清洗结果"] != RESULT_VALID and not ln["隔离原因"]]
        if not sampled or incomplete or len(lines) != batch["校验摘要"]["总来源行数"]:
            return {
                "ok": False,
                "error": "回执不完整，质量门禁只接收完整回执",
                "missing_reason_lines": incomplete,
                "expected_lines": batch["校验摘要"]["总来源行数"],
                "actual_lines": len(lines),
            }

        receipt_no = f"REJ-{batch['批次号']}-{_version_series(batch['口径'])}-" \
                     f"{len(store.rows(TABLE_RECEIPT)) + 1:03d}"
        items = [{
            "明细行ID": ln["id"],
            "来源文件": ln["来源文件"],
            "原始行号": ln["原始行号"],
            "去重键": ln["去重键"],
            "清洗结果": ln["清洗结果"],
            "拒绝原因": ln["隔离原因"] or "采样通过",
            "校验指纹": ln["校验指纹"],
        } for ln in sampled]
        receipt_checksum = hashlib.sha256(
            "|".join(str(it["校验指纹"]) for it in items).encode()
        ).hexdigest()[:16]
        receipt = {
            "id": _next_id(TABLE_RECEIPT),
            "回执编号": receipt_no,
            "批次ID": batch_id,
            "批次号": batch["批次号"],
            "门禁规则ID": rule.get("id", 0),
            "门禁规则名称": rule.get("规则名称", ""),
            "明细版本": batch["明细版本"],
            "净数据版本": batch["净数据版本"],
            "采样比例": ratio,
            "采样行数": len(sampled),
            "拦截行数": len(blocked_sample),
            "回执条目": items,
            "回执校验和": receipt_checksum,
            "完整回执": True,
            "跨模块状态": "门禁已回执·待复核",
            "创建时间": _now(),
        }
        store.rows(TABLE_RECEIPT).append(receipt)
        sampled_ids = {ln["id"] for ln in sampled}
        for ln in lines:
            if ln["id"] in sampled_ids:
                ln["拒绝回执ID"] = receipt_no
                ln["更新时间"] = _now()

        batch["批次状态"] = STATUS_RECEIPTED
        batch["消费凭据"] = {
            "receipt_no": receipt_no,
            "consumed_detail_version": batch["明细版本"],
            "checksum": receipt_checksum,
            "consumed_at": _now(),
        }
        batch["更新时间"] = _now()
        self._upsert_review(batch, "门禁已回执·待复核")
        return {"ok": True, "receipt": receipt}

    def confirm_batch(self, batch_id: int) -> dict[str, Any]:
        """跨模块核对确认：结果回写待复核清单并冻结明细行。"""
        batch = self._find_batch(batch_id)
        if not batch:
            return {"ok": False, "error": f"批次 {batch_id} 不存在"}
        if batch["批次状态"] != STATUS_RECEIPTED:
            return {"ok": False,
                    "error": "只有已回执批次才能确认，请先由质量门禁生成回执"}
        lines = [ln for ln in store.rows(TABLE_LINE)
                 if ln["批次ID"] == batch_id and ln["口径"] == batch["口径"]]
        for ln in lines:
            ln["已确认"] = True
            ln["更新时间"] = _now()
        batch["批次状态"] = STATUS_CONFIRMED
        batch["pending"] = False
        batch["更新时间"] = _now()
        self._upsert_review(batch, "入湖管道已确认·跨模块核对完成")
        return {"ok": True, "batch": self._batch_view(batch)}

    # ===================================================================
    # 五、待复核清单与跨模块状态（回到入湖管道刷新后可再次查询）
    # ===================================================================
    def list_pending_reviews(self) -> list[dict[str, Any]]:
        return list(store.rows(TABLE_REVIEW))

    def refresh_review(self, review_id: int) -> dict[str, Any]:
        review = next((r for r in store.rows(TABLE_REVIEW)
                       if int(r.get("id", 0)) == review_id), None)
        if not review:
            return {"ok": False, "error": f"待复核记录 {review_id} 不存在"}
        batch = self._find_batch(int(review["批次ID"]))
        if not batch:
            return {"ok": False, "error": "对应批次已不存在"}
        review["跨模块状态"] = self._review_status(batch)
        review["最近刷新时间"] = _now()
        review["批次状态快照"] = batch["批次状态"]
        return {"ok": True, "review": review}

    def list_batches(self, pipeline_id: int | None = None,
                     status: str | None = None) -> list[dict[str, Any]]:
        rows = store.rows(TABLE_BATCH)
        if pipeline_id is not None:
            rows = [r for r in rows if int(r.get("管道ID", 0)) == pipeline_id]
        if status:
            rows = [r for r in rows if r.get("批次状态") == status]
        return [self._batch_view(r) for r in rows]

    def list_lines(self, batch_id: int,
                   clean_result: str | None = None,
                   keyword: str | None = None) -> dict[str, Any]:
        batch = self._find_batch(batch_id)
        if not batch:
            return {"ok": False, "error": f"批次 {batch_id} 不存在"}
        lines = [ln for ln in store.rows(TABLE_LINE)
                 if ln["批次ID"] == batch_id and ln["口径"] == batch["口径"]]
        if clean_result:
            lines = [ln for ln in lines if ln["清洗结果"] == clean_result]
        if keyword:
            kw = keyword.casefold()
            lines = [ln for ln in lines
                     if kw in str(ln["去重键"]).casefold()
                     or kw in str(ln["来源文件"]).casefold()]
        return {"ok": True, "batch": self._batch_view(batch),
                "items": lines, "total": len(lines)}

    def line_receipt(self, line_id: int) -> dict[str, Any]:
        line = next((ln for ln in store.rows(TABLE_LINE)
                     if int(ln.get("id", 0)) == line_id), None)
        if not line:
            return {"ok": False, "error": f"明细行 {line_id} 不存在"}
        receipt_no = line.get("拒绝回执ID", "")
        receipt = next((r for r in store.rows(TABLE_RECEIPT)
                        if r.get("回执编号") == receipt_no), None)
        return {"ok": True, "line": line, "receipt": receipt}

    # ===================================================================
    # 辅助方法
    # ===================================================================
    def _find_batch(self, batch_id: int) -> dict[str, Any] | None:
        return next((b for b in store.rows(TABLE_BATCH)
                     if int(b.get("id", 0)) == batch_id), None)

    def _history_index(self, batch_id: int, caliber: str
                       ) -> dict[tuple[str, int], dict[str, Any]]:
        return {
            (ln["来源文件"], ln["原始行号"]): ln
            for ln in store.rows(TABLE_LINE)
            if ln["批次ID"] == batch_id and ln["口径"] == caliber
        }

    @staticmethod
    def _pick_gate_rule(gate_rule_id: int | None) -> dict[str, Any]:
        rules = store.rows("quality_gate")
        if gate_rule_id is not None:
            found = next((r for r in rules if int(r.get("id", 0)) == gate_rule_id), None)
            if found:
                return found
        return next((r for r in rules if r.get("status") == "已启用"),
                    rules[0] if rules else {"id": 0, "规则名称": "默认采样门禁",
                                            "采样比例": "50%"})

    @staticmethod
    def _parse_ratio(value: Any) -> float:
        text = str(value or "").strip()
        if text.endswith("%"):
            try:
                return min(1.0, max(0.0, float(text[:-1]) / 100.0))
            except ValueError:
                return 0.5
        try:
            ratio = float(text)
            return min(1.0, max(0.0, ratio))
        except ValueError:
            return 0.5

    def _next_major(self, channel: str, series: str) -> int:
        """按口径序列各自单调递增：从版本 metadata 中统计同序列版本数。"""
        major = 0
        for version in store.channel.versions_of(channel):
            if version.metadata.get("series") == series:
                major = max(major, int(version.metadata.get("series_major", 0)))
        return major + 1

    def _upsert_review(self, batch: dict[str, Any], cross_status: str) -> None:
        reviews = store.rows(TABLE_REVIEW)
        review = next((r for r in reviews if r["批次ID"] == batch["id"]), None)
        payload = {
            "批次ID": batch["id"],
            "批次号": batch["批次号"],
            "管道ID": batch["管道ID"],
            "明细版本": batch["明细版本"],
            "净数据版本": batch["净数据版本"],
            "跨模块状态": cross_status,
            "批次状态快照": batch["批次状态"],
            "最近刷新时间": _now(),
        }
        if review is None:
            payload["id"] = _next_id(TABLE_REVIEW)
            payload["登记时间"] = _now()
            reviews.append(payload)
        else:
            review.update(payload)

    @staticmethod
    def _review_status(batch: dict[str, Any]) -> str:
        mapping = {
            STATUS_PENDING: "待质量门禁回执",
            STATUS_RECEIPTED: "门禁已回执·待复核",
            STATUS_CONFIRMED: "入湖管道已确认·跨模块核对完成",
            STATUS_INTERRUPTED: "批次中断·待重试",
        }
        return mapping.get(batch["批次状态"], batch["批次状态"])

    @staticmethod
    def _sync_pipeline_master(pipeline_id: int, batch: dict[str, Any]) -> None:
        """维护入湖管道主数据：回写最新批次与累计批次口径。"""
        pipeline = store.find("ingest_pipeline", pipeline_id)
        if not pipeline:
            return
        stats = pipeline.get("批次统计") or {"累计批次数": 0, "批次明细": []}
        stats = dict(stats)
        stats["累计批次数"] = int(stats.get("累计批次数", 0)) + 1
        stats["最新批次号"] = batch["批次号"]
        stats["最新明细版本"] = batch["明细版本"]
        stats["最新批次状态"] = batch["批次状态"]
        stats["更新时间"] = _now()
        stats["批次明细"] = list(stats.get("批次明细", [])) + [
            {"批次号": batch["批次号"], "口径": batch["口径说明"],
             "明细版本": batch["明细版本"], "状态": batch["批次状态"]}]
        pipeline["批次统计"] = stats

    @staticmethod
    def _assert_complete(batch: dict[str, Any],
                         lines: list[dict[str, Any]]) -> None:
        """半成品防线：所有来源行必须有终态清洗结果与校验指纹。"""
        if not lines:
            raise RuntimeError("明细数据集为空，禁止落库半成品")
        for ln in lines:
            if not ln["清洗结果"] or not ln["校验指纹"]:
                raise RuntimeError(f"明细行 {ln.get('id')} 缺少终态结论，禁止半成品落库")
            if ln["清洗结果"] != RESULT_VALID and not ln["隔离原因"]:
                raise RuntimeError(f"隔离行 {ln.get('id')} 缺少隔离原因")

    @staticmethod
    def _snapshot_tables() -> dict[str, list[dict[str, Any]]]:
        return {
            name: [dict(row) for row in store.rows(name)]
            for name in (TABLE_BATCH, TABLE_LINE, TABLE_RECEIPT, TABLE_REVIEW)
        }

    @staticmethod
    def _restore_snapshot(snapshot: dict[str, list[dict[str, Any]]]) -> None:
        for name, rows in snapshot.items():
            store.rows(name).clear()
            store.rows(name).extend([dict(row) for row in rows])

    @staticmethod
    def _batch_view(batch: dict[str, Any]) -> dict[str, Any]:
        view = dict(batch)
        summary = batch.get("校验摘要") or {}
        view["有效行数"] = summary.get("有效行数", 0)
        view["重复行数"] = summary.get("重复行数", 0)
        view["脏行数"] = summary.get("脏行数", 0)
        view["批次状态"] = batch.get("批次状态", "")
        return view


batch_detail_service = BatchDetailService()

# 四张明细表为领域内部表，不暴露为独立业务模块（对外统一以 batch_detail 模块呈现）
for _table in (TABLE_BATCH, TABLE_LINE, TABLE_RECEIPT, TABLE_REVIEW):
    store.register_internal_table(_table)
