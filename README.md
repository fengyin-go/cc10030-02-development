# 数据湖入湖治理中心

面向数据湖的多区数据入湖、格式管理、分层治理和生命周期维护的数据湖建设平台

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/WorkbenchView.vue|tsx  全部模块共享工作台
│   ├── src/data/modules.ts              18个业务模块配置
│   ├── src/api/              统一请求封装
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/resource.py  通用查询/导出/登记/动作接口
│   ├── app/services/<key>.py    每个模块独立领域逻辑
│   ├── app/services/base.py     共享服务骨架
│   └── app/store.py          内存数据仓库与示例数据
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8000/api/health`

### 前端

```bash
cd frontend
npm ci
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| 湖区分层 | `lake_zone` | 湖区定义 | 湖区编码、湖区名称、分层级别 |
| 入湖管道 | `ingest_pipeline` | 管道配置 | 管道名称、源端类型、目标湖区 |
| Schema注册 | `schema_registry` | Schema版本 | 主题名称、Schema定义、版本号 |
| 分区策略 | `partition_policy` | 分区规则 | 规则名称、目标数据集、分区键 |
| 存储格式 | `file_format` | 格式模板 | 模板名称、文件格式、压缩算法 |
| 留存策略 | `data_retention` | 留存规则 | 规则名称、目标湖区、保留天数 |
| 元数据目录 | `metadata_catalog` | 元数据条目 | 条目ID、数据集名称、所属湖区 |
| 质量门禁 | `quality_gate` | 门禁规则 | 规则名称、检查维度、阈值条件 |
| 湖内去重 | `dedup_lake` | 去重配置 | 配置名称、目标数据集、去重键 |
| 变更追踪 | `lineage_track` | 变更记录 | 变更ID、目标对象、变更类型 |
| 操作审计 | `audit_journal` | 审计日志 | 操作时间、操作主体、操作类型 |
| 快照管理 | `snapshot_mgr` | 快照任务 | 快照名称、目标数据集、快照时间 |
| 过期清理 | `ttl_sweeper` | 清理策略 | 策略名称、清理范围、匹配条件 |
| 冷热分层 | `cold_warm_tier` | 分层策略 | 策略名称、热度评估、冷热阈值 |
| 跨区复制 | `cross_zone_copy` | 复制任务 | 任务名称、源湖区、目标湖区 |
| 模式演进 | `schema_evolve` | 演进方案 | 方案名称、目标Schema、变更类型 |
| 入湖监控 | `ingest_monitor` | 监控面板 | 监控项、数据管道、吞吐速率 |
| 灾备策略 | `backup_policy` | 灾备计划 | 计划名称、保护范围、备份周期 |
| 数据字典 | `data_dictionary` | 字典条目 | 条目编码、字段名称、数据类型 |
| 访问管控 | `access_control` | 访问策略 | 策略名称、资源范围、主体类型 |
| 入湖批次明细 | `batch_detail` | 入湖批次明细数据集 | 批次号、管道名称、采集时间、口径、明细版本、有效/重复/脏行数 |

## 入湖批次明细数据集

入湖管道只暴露状态时，运维无法核对哪些原始行进入了净数据。`batch_detail` 模块把
「解析管道配置与待入湖样本 → 清洗 → 批次明细数据集」沉淀为可逐行复核的数据产物：

- 明细行记录来源文件、源文件偏移、原始行号、样本序号、去重键、采集时间、业务生效日、
  清洗结果、隔离原因、隔离通道、净数据版本、净数据路径、校验指纹、异常摘要与拒绝回执。
- 工作台按清洗结果（有效行 / 重复行 / 缺键行 / 格式异常行 / 空值行）定位每一行，
  并可查看该行对应的采样拒绝回执。
- 净数据版本与每条隔离原因在同一行一起落库；批次重跑按（来源文件、原始行号）幂等，
  不重复累计同一来源行，已确认行冻结不变。
- 两种版本口径分别沉淀证据：甲（`arrival`，按到达顺序固化，首选）与
  乙（`business_date`，按业务生效日重排），产生 `DET-ARR/NET-ARR` 与 `DET-BIZ/NET-BIZ`
  两套版本号；来源文件补报与首次采集批次使用不同采集时间时，自动定位改动点。
- 整批事务落库，任一步失败即回滚并保留回滚依据，禁止半成品；中断后重跑只重试未完成部分。
- 质量门禁按明细数据集版本采样，只接收完整回执，回执回写待复核清单；入湖管道确认后
  冻结明细并回写跨模块状态，形成「门禁 → 待复核清单 → 管道确认」闭环。

专用接口前缀 `/api/batch-detail`，统一资源路由 `/api/batch_detail` 仍可访问批次主数据。

## 约定

- 全部模块共享`frontend/src/views/WorkbenchView.vue|tsx`和一套资源路由。
- 每个模块仍保留独立`backend/app/services/<模块>.py`；纯前端保留
  `frontend/src/domain/<模块>.ts`，用于承载真实业务差异并维持36题位容量。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

领域：数据工程
子方向：数仓与数据湖
