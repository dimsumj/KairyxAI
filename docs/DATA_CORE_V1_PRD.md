# KairyxAI Data Core v1 PRD

## 4.0 Connector Management（补充归属）

### 目标
将数据连接器配置、健康检查、freshness 与最近导入状态统一归到 Data Core 管理，作为所有 ingestion / mapping / SQL 能力的入口控制面。

### 功能要求
- 保存多个 connector 配置
- 列出可用 connector 与数据来源
- 支持 connector 删除
- 支持 health check
- 支持 freshness / last_ingestion_status / last_ingestion_at 展示

### 归属说明
- 当前 repo 中的 connector control plane 与 source freshness 属于 Data Core 范围
- 连接器 secret 的生产化治理仍由 Master PRD 的 Production Readiness 持有

---

## 4.1 数据源连接与数仓直查能力（新增）

### 目标
在连接 Data Lake / Database（如 BigQuery）后，支持用户直接在数仓层做条件查询，并将查询结果一键生成为 cohort。

### 功能要求

#### FR-4.1.1 数据源直连
- 支持配置并验证 BigQuery 连接（后续可扩 Snowflake/Redshift）
- 支持选择 project / dataset / table（或视图）
- 支持只读权限模式（最小权限）

#### FR-4.1.2 SQL Query Workspace
- 提供可执行 SQL 的查询工作台（模板 + 参数）
- 支持 where/filter、时间窗口、聚合条件
- 支持结果预览（前 N 条）与行数估计
- 支持保存查询模板（Saved Queries）

#### FR-4.1.3 Query → Cohort 生成
- 查询结果必须包含统一主键字段（`canonical_user_id` 或 `player_id`）
- 一键将结果写入 cohort（命名、描述、来源SQL、创建时间）
- 支持静态 cohort（快照）与动态 cohort（定时刷新）

#### FR-4.1.4 Cohort 管理
- cohort 列表、用户数、最近刷新时间
- 支持版本化（同名 cohort 的迭代）
- 支持导出/下游调用（供 Engage/Experiment 使用）

#### FR-4.1.5 安全与治理
- SQL 白名单/只读限制（禁 DDL/DML）
- 查询超时与扫描量限制（防大查询炸成本）
- 审计日志：谁在何时执行了什么 SQL、生成了哪个 cohort

### v1 验收标准（Definition of Done）
1. BigQuery 连通后，可在 UI 执行 SQL 并返回结果预览
2. 能把 SQL 结果（含 user id）一键生成为 cohort
3. cohort 可被后续 `predict/engage/experiment` 模块直接调用
4. 至少支持 1 个动态 cohort 定时刷新（如每天 1 次）
5. 有查询审计日志和失败可追踪信息

### 4.1.6 Backend Runtime / Persistence Ownership（补充归属）
- `operator-api` 下的 `/api/v1/connectors`、`/mappings`、`/imports`、`/predictions` 是 Data Core 的主要 backend control-plane 面
- `import-worker` 负责 connector 分页拉取、checkpoint-aware import 执行、raw shard 发布与恢复
- `dataflow` 负责 `raw shard -> manifest -> standardized -> unified` 的规范化处理
- `prediction-worker` 负责基于统一聚合层执行预测，并把结果写入 BigQuery-backed serving / result storage
- prediction 结果读取必须支持分页，避免无界结果集读取
- Data Core 持有的 control-plane persisted entities 至少包括：
  - `connector configuration`
  - `field mapping`
  - `import job`
  - `prediction job`
  - `ingestion checkpoint`

---

## 4.2 多来源数据 Ingestion 与 Stitch（P0）

> 优先级：**P0（立即处理）**

### 目标
解决从不同来源导出的异构数据在进入系统后的统一接入、清洗、合并、去重与身份拼接问题，产出可直接用于分群与策略执行的统一事件层。

### 4.2.1 Canonical Event Contract（统一事件协议）
所有来源（Analytics 平台 / MMP / 游戏后端）进入标准化层后，必须映射到统一结构：
- `job_id`
- `source`
- `source_event_id`
- `player_id`
- `canonical_user_id`（stitch后补齐）
- `event_type`
- `event_time`
- `event_properties`
- `user_properties`
- `ingested_at`
- `data_quality_flags`

### 4.2.2 三层 Ingestion 架构（P0）

#### Layer A: Raw Landing（原样落地）
- 原始 JSON 按 `source/date/job_id` 落地保存
- 仅追加不覆盖，支持回放与审计

#### Layer B: Standardized（标准化清洗）
- 统一字段映射、时区与时间格式、金额/币种标准化
- 标记质量问题（如 `missing_player_id`, `invalid_event_time`）
- 产出 `stg_events_standardized`

#### Layer C: Unified（合并层）
- 执行 dedupe + identity stitch
- 产出 `fact_events_unified` 作为上层唯一消费入口

#### 4.2.2.1 双运行模式（整体设计）
Data Core 必须长期同时支持两种运行模式，共享 schema contract 与 service interface，但不强制共享同一运行时实现：

- `Local demo mode`
  - 用于本地 UI 调试、connector mock flow、无云基础设施的端到端演示
  - 允许本地文件 raw storage、进程内或本地 queue simulation、parquet / sqlite persistence
  - 保留当前 FastAPI 驱动的同步开发体验，但不以规模为优化目标
- `Production GCP mode`
  - 用于大体量 connector 拉取、可回放 ingestion、分布式 normalization、warehouse-backed dedupe 与 serving
  - 运行形态固定为 `GCS + Pub/Sub + Dataflow + BigQuery`
  - 目标是 replayable、observable、idempotent，且内存占用受控

#### 4.2.2.2 生产数据平面（Scalable Ingestion Blueprint）
生产模式下的数据主链路固定为：
1. connector fetcher 按页拉取外部事件
2. 每页写成一个有界 raw shard（压缩 JSONL）
3. 仅发布 shard metadata manifest，不在消息里传 raw event list
4. Dataflow 消费 manifest 并做 canonical normalization
5. 有效事件写入 `events_staging`
6. 无效事件写入 `pipeline_dead_letters`
7. BigQuery SQL 生成 `events_curated` 与 serving / aggregate tables
8. 上层 API、预测、决策默认读取 curated / aggregate tables，而不是直接扫 raw events

#### 4.2.2.3 Raw Shard / Manifest Contract（P0）
Raw shard 路径规范：
- `gs://<bucket>/raw/source=<source>/dt=YYYY-MM-DD/hour=HH/job=<job_id>/part-000123.jsonl.gz`
- 格式为 newline-delimited JSON，gzip compressed，一行一个 source event

Shard manifest 至少包含：
- `job_id`
- `source`
- `source_config_id`
- `gcs_uri`
- `event_count`
- `start_date`
- `end_date`
- `schema_version`
- `published_at`

约束：
- Pub/Sub 只承载 shard metadata，不承载完整事件数组
- checkpoint 必须能追到 `job_id + source + shard_index`
- replay / resume 以 shard 为最小恢复单元，而不是整 job 全量重跑

#### 4.2.2.4 Canonical Event / Warehouse Contract（P0）
标准化后的 canonical event 除基础字段外，还应显式持有：
- `schema_version`
- `source_config_id`
- `raw_gcs_uri`
- `event_date`
- `event_fingerprint`
- `campaign`
- `adset`
- `media_source`

推荐表布局：
- `raw_ingestion_audit`
- `events_staging`（对应 standardized 层）
- `pipeline_dead_letters`
- `events_curated`（对应 unified / curated 层）
- `identity_links`
- `player_daily_metrics`
- `player_latest_state`
- `player_churn_features`

当前 v1 命名与 canonical alias 对齐关系：
- `events_staging` -> `stg_events_standardized`
- `events_curated` -> `fact_events_unified`
- `player_latest_state` -> `mart_user_daily`

设计要求：
- `events_staging` 作为 append-only landing table，支持 replay/debug
- `events_curated` 作为 deduped / cleaned downstream source of truth
- churn / profile / actioning 默认优先读取 `player_latest_state` 与 `player_churn_features`
- 只有 drill-down 场景才回查 curated event history

#### 4.2.2.5 组件职责边界（P0）
- `IngestionService`
  - `mock`：保留当前开发流，本地 shard / queue simulation 可接受
  - `gcp`：分页拉取、写 GCS shard、发布 Pub/Sub manifest、持久化 checkpoint
- `DataProcessingService / dataflow`
  - `mock`：保留本地 shard-by-shard 处理与 rejected/conflict 日志
  - `gcp`：normalization 执行迁移到 Dataflow；FastAPI 请求内不允许处理整批大 job
- `BigQueryService`
  - 演进为 warehouse facade，显式提供 staging / dead-letter / curated / latest-state 方法
  - `mock` 模式可以继续落 parquet / sqlite，但 public method 要与生产概念一致
- `connectors/normalizer.py`
  - 只负责 deterministic field extraction、timestamp coercion、schema version、fingerprint、required field validation
  - 不负责全历史 dedupe、不负责跨 job reconciliation、不负责全量内存状态

### 4.2.3 Stitch 规则引擎（P0）
采用“确定性优先”的 v1 策略，按优先级拼接：
1. `internal_account_id / game_uid`
2. `login_user_id`
3. `device_id + 登录绑定行为`
4. `email_hash / phone_hash`
5. fallback：`source:source_user_id`

产出 `identity_links`：
- `source`
- `source_user_id`
- `canonical_user_id`
- `method`
- `confidence`
- `first_seen_at`
- `last_seen_at`

要求：每条 stitch 关系必须可追踪、可解释。

### 4.2.4 Dedupe 规则（P0）
- 优先规则：`(source, source_event_id)`
- 回退规则：`(canonical_or_source_user_id, event_type, event_time_rounded, source)`

输出统计：
- `raw_normalized_events`
- `deduped_events`
- `duplicates_removed`
- `dedupe_rate`

### 4.2.5 Source-of-Truth Matrix（P0）
按字段定义真相源优先级（而非全局一刀切）：

#### 用户主身份字段（Identity）
优先级：`游戏后端 > 分析SDK > MMP`

适用字段示例：
- `internal_account_id`
- `game_uid`
- `login_user_id`
- `player_id`（最终写入 canonical 前的候选ID）

#### 归因字段（Attribution）
优先级：`分析SDK > MMP`

适用字段示例：
- `campaign`
- `adset`
- `media_source`
- `channel`

说明：考虑 Apple 隐私策略下 MMP 在部分链路的信号损耗/延迟，v1 中归因口径以 Analytics SDK 为优先来源。

### 4.2.6 冲突与异常处理（P0）
- 对跨源冲突字段（`campaign/adset/media_source`）记录冲突日志
- 字段覆盖需记录审计信息：`old_value/new_value/source/ts/rule_id`
- 严重质量问题事件进入 rejected 队列，不进入 unified 层
- 所有 rejected/conflict 均可按 `job_id/source` 查询

### 4.2.7 Mapping Strengthening（P0）

#### 4.2.7.1 分层 Mapping 体系
- `Global Mapping`：跨来源通用默认映射
- `Source Mapping`：来源级映射（任意 Analytics 平台 / MMP / 游戏后端来源）
- `Job Override`：单次导入任务临时覆盖

优先级：`Job Override > Source Mapping > Global Mapping`

#### 4.2.7.2 必填字段门禁（Hard Gate）
以下字段是 unified 入湖前门禁：
- `player_id`（或 canonical candidate id）
- `event_type`
- `event_time`

规则：required mapping coverage < 95% 时，任务自动进入 `Awaiting Mapping`，禁止写入 unified。

#### 4.2.7.3 Mapping 质量报表增强
- 除 hit rate 外，增加：
  - null rate
  - type mismatch rate
  - sample values（前 N 条）
  - impacted row count（受影响行数）

#### 4.2.7.4 Mapping 版本与回滚
- 每次映射变更记录：
  - `mapping_version`
  - `changed_by`
  - `changed_at`
  - `diff`
- 支持一键回滚到历史版本

#### 4.2.7.5 AI 辅助建议（默认只建议不自动生效）
- 基于字段名相似度 + 样本值模式识别生成建议映射
- 默认进入“建议态”，由用户确认后才写入 mapping

#### 4.2.7.6 Mapping 后重放（Replay）
- 映射修正后，支持 `standardized -> unified` 的重处理
- 不重复拉取源数据，降低重跑成本与时延

### 4.2.8 v1 验收标准（Definition of Done）
1. 至少 2 个来源可稳定接入并进入 `fact_events_unified`
2. `canonical_user_id` 覆盖率 > 90%
3. 关键事件（login/purchase）重复率可解释且可追踪
4. dedupe/stitch 统计可在 job 维度查询
5. 基于 unified 层可直接生成 cohort（无需按来源分别写 SQL）
6. required mapping coverage 门禁生效（<95% 自动 Awaiting Mapping）
7. 映射版本可审计、可回滚，且支持 mapping 修复后 replay

### 4.2.9 字段映射优先处理清单（Top 20，P0）

> 目标：优先打通“可分群、可归因、可策略执行”的最小字段集。

#### A. 身份与设备（Identity）
1. `player_id`（统一候选主键）
2. `internal_account_id`
3. `game_uid`
4. `login_user_id`
5. `anonymous_id`
6. `device_id`
7. `idfa/idfv/gaid`（广告标识，按隐私策略可空）
8. `email_hash/phone_hash`

#### B. 事件核心（Event Core）
9. `event_type`
10. `event_time`
11. `source_event_id`
12. `session_id`
13. `app_version`
14. `platform`（ios/android/web）

#### C. 归因与渠道（Attribution）
15. `campaign`
16. `adset`
17. `media_source`
18. `channel`

#### D. 商业与地域（Business & Geo）
19. `revenue_usd`
20. `country/region`

### 4.2.10 Top 20 映射验收门槛（P0）
- Top 20 字段整体 coverage ≥ 90%
- 关键字段（`player_id`, `event_type`, `event_time`）coverage ≥ 95%
- 归因字段（`campaign/adset/media_source/channel`）coverage ≥ 85%
- `revenue_usd` 类型有效率 ≥ 98%
- 任一关键字段低于阈值，任务自动进入 `Awaiting Mapping`

### 4.2.11 Scalable Ingestion 演进阶段（整体设计）
#### Phase 1：Interface Refactor Without Behavior Break
- 在不破坏当前 local demo 的前提下，引入 production-shaped interfaces
- 增加 shard manifest model、`fetch_and_stage_events()`、显式 `event_fingerprint`

#### Phase 2：Local Shard Processing
- local mode 改成按 shard 写本地 JSONL 并逐 shard 处理
- 移除 job 级全量内存累积，提升生产路径逼真度

#### Phase 3：GCP Ingestion Path
- connector fetch 默认写入 `GCS + Pub/Sub`
- checkpoint persistence 与失败恢复路径正式化

#### Phase 4：Dataflow Normalization Path
- normalization 从 FastAPI 请求路径迁出
- Dataflow 消费 manifest、写 `events_staging`、把 invalid rows 写到 dead-letter table

#### Phase 5：Curated And Aggregate Serving
- 建立 `events_curated`、`player_latest_state`、`player_churn_features`
- player modeling / churn / decision 服务默认读 aggregate-first

### 4.2.12 首次扩容重构的非目标
- 不做完整实时 identity graph resolution
- 不做 online feature store
- 不承诺所有外部 connector 的 exactly-once semantics
- 不把完整统计实验引擎纳入 ingestion 扩容重构范围

---

## 4.3 Audience / Cohort Engine（P0）

> 优先级：**P0（与 4.2 并行）**

### 目标
将 unified 数据层直接转化为可复用的人群资产，供策略执行、实验框架与预测模块统一消费。

### 4.3.1 Cohort 类型
- 静态 Cohort（Snapshot）
- 动态 Cohort（Rule-based，按调度刷新）
- SQL Cohort（由查询直接生成）

### 4.3.2 Cohort 对象模型（统一）
每个 cohort 至少包含：
- `cohort_id`
- `name`
- `type`（static / dynamic / sql）
- `definition`（规则JSON或SQL）
- `refresh_mode`（manual / daily / hourly）
- `status`（draft / validating / ready / materializing / active / paused / failed）
- `member_count`
- `last_refreshed_at`
- `version`
- `owner`
- `source_job_ids`

### 4.3.3 Cohort 生成功能定义（P0）

#### 生成入口（3种）
- Rule Builder（无代码条件拼装）
- SQL Builder（高级查询）
- Import List（上传 user_id / canonical_user_id 列表）

#### 统一生成流程
1. 输入定义（规则/SQL/列表）
2. 预校验（字段合法性、主键完整性、扫描量预估）
3. 预览（sample + 预估人数）
4. 执行生成（materialize snapshot）
5. 输出元数据（member_count、version、source）
6. 可选：立即激活给 Engage/Experiment

#### 必须校验（Hard Checks）
- 结果必须包含 `canonical_user_id`（或可映射到它）
- 空人群禁止激活（允许保存草稿）
- 大查询超限需阻断并提示优化

#### 生成产物
- `cohort_definition`
- `cohort_snapshot`
- `cohort_stats`

### 4.3.4 Cohort 存储与命名管理（P0）

#### 存储要求
- cohort 生成后必须持久化存储（metadata + definition + latest snapshot）
- 支持按 `cohort_id` 与 `name` 检索
- 支持软删除（`deleted_at`）与可恢复（restore）

#### 命名与目录管理
- `name` 全局唯一（或在 workspace/project 维度唯一）
- 支持重命名（保留历史名称变更日志）
- 支持标签（tags）与分组（folder）
- 支持按 name/type/owner/tag/status 搜索和过滤

#### 生命周期操作（CRUD）
- 新增：创建 cohort（草稿/激活）
- 读取：查看定义、成员规模、最近刷新状态
- 更新：修改 definition、刷新策略、名称、标签
- 删除：软删除 + 可恢复；高权限可永久删除（审计记录）

### 4.3.5 Cohort 刷新与存储策略（P0）
- 采用 Hybrid：永久存 definition + 最近快照（snapshot）
- 动态 cohort 支持定时刷新与手动刷新
- 刷新后记录差异（新增/流失人数）

### 4.3.5 Cohort 消费接口（对内）
- 预测模块：按 cohort 拉取用户集合
- 策略模块：按 cohort 触发动作
- 实验模块：按 cohort 进行 A/B/Holdout 分流

建议接口：
- `POST /cohorts`
- `GET /cohorts/{id}`
- `GET /cohorts/{id}/members`
- `POST /cohorts/{id}/refresh`
- `POST /cohorts/{id}/activate`
- `POST /cohorts/{id}/pause`

### 4.3.6 Cohort 质量门槛（P0）
- 同一 cohort 重跑（同时间窗）成员偏差 <= 2%
- 动态 cohort 刷新失败可重试并告警
- cohort 成员全部具备 `canonical_user_id`

### 4.3.7 Rule Builder（P0）

#### v1 语法范围（受控 DSL）
Rule Builder 采用“规则 JSON -> SQL 编译执行”的模式，v1 支持：
- 条件组合：AND / OR（最多 3 层嵌套）
- 用户属性条件：`country/platform/app_version/payer_status` 等
- 事件行为条件：`event_type`、`count(event)`、`last_event_time`
- 数值指标条件：`revenue/session_count/ltv/last_active_days`
- 时间窗口条件：`within_last / before / after`

支持操作符：
- 文本：`=`, `!=`, `in`, `not in`, `contains`
- 数值：`>`, `>=`, `<`, `<=`, `between`
- 时间：`within_last`, `before`, `after`

#### 规则模板（v1 预置）
1. 最近 7 天活跃用户
2. 最近 14 天未登录用户
3. 最近 30 天有付费用户
4. 高价值用户（LTV > X）
5. 新用户（注册 <= N 天）
6. 高流失风险用户（risk = high）
7. 看过活动但未购买用户
8. 指定渠道拉新用户（campaign/media_source）

#### 交互与执行要求
- 支持规则可视化编辑（条件组 + 逻辑关系）
- 生成前显示人群估算（estimate）
- 提供样本预览（前 N 个用户）
- 支持查看编译后的 SQL（只读）
- 一键保存为 cohort 并可选择立即激活

#### 护栏与限制
- 最大条件条数：30
- 最大嵌套层级：3
- 查询超时/扫描量超限自动阻断
- 空人群禁止激活（允许保存草稿）

### 4.3.8 Rule DSL 示例（附录，P0）

#### 示例 A：最近 7 天活跃 + 最近 30 天未付费 + 高流失风险

Rule JSON（DSL）：
```json
{
  "name": "active_7d_no_pay_30d_high_risk",
  "logic": "AND",
  "conditions": [
    {
      "type": "metric",
      "field": "last_active_days",
      "op": "<=",
      "value": 7
    },
    {
      "type": "metric",
      "field": "last_30d_revenue",
      "op": "=",
      "value": 0
    },
    {
      "type": "property",
      "field": "churn_risk",
      "op": "in",
      "value": ["high"]
    }
  ],
  "window": {
    "timezone": "America/Los_Angeles",
    "as_of": "now"
  }
}
```

编译 SQL（示例）：
```sql
SELECT
  canonical_user_id
FROM mart_user_daily
WHERE
  last_active_days <= 7
  AND last_30d_revenue = 0
  AND churn_risk IN ('high');
```

#### 示例 B：过去 14 天看过活动但未购买

Rule JSON（DSL）：
```json
{
  "name": "view_promo_no_purchase_14d",
  "logic": "AND",
  "conditions": [
    {
      "type": "event_count",
      "event": "promo_view",
      "window_days": 14,
      "op": ">=",
      "value": 1
    },
    {
      "type": "event_count",
      "event": "purchase_success",
      "window_days": 14,
      "op": "=",
      "value": 0
    }
  ]
}
```

编译 SQL（示例）：
```sql
WITH base AS (
  SELECT
    canonical_user_id,
    SUM(CASE WHEN event_type = 'promo_view'
              AND event_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 14 DAY)
             THEN 1 ELSE 0 END) AS promo_views_14d,
    SUM(CASE WHEN event_type = 'purchase_success'
              AND event_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 14 DAY)
             THEN 1 ELSE 0 END) AS purchases_14d
  FROM fact_events_unified
  GROUP BY canonical_user_id
)
SELECT canonical_user_id
FROM base
WHERE promo_views_14d >= 1
  AND purchases_14d = 0;
```

### 4.3.9 v1 验收标准
1. 支持静态、动态、SQL 三类 cohort
2. 三种入口（规则/SQL/列表）都可生成 cohort
3. Rule Builder 可生成可执行 SQL 并返回预估规模
4. 生成前有预览与人数预估
5. 至少 1 个动态 cohort 支持每日自动刷新
6. cohort 可被 predict/engage/experiment 三模块直接调用
7. cohort 生成全流程可审计（创建人、定义、时间、版本）

### 4.3.10 详细设计引用
Audience Engine 的详细 scope、模块设计与上线门槛已拆分到独立文档：
- `KairyxAI/docs/AUDIENCE_ENGINE_V1_PRD.md`

总 PRD 中仅保留目标、核心能力与高层验收标准。

---

## 4.4 Data Quality & Observability（P0）

> 优先级：**P0（上线前必须具备）**

### 目标
建立从 source 到 cohort 的全链路可观测能力，确保数据问题可发现、可定位、可回放。

### 4.4.1 Job 级观测
- source ingest 成功/失败统计
- processing 阶段耗时
- dedupe/stitch/reject/conflict 指标
- 当前步骤 + 进度百分比

### 4.4.2 数据质量监控指标
- required mapping coverage
- `canonical_user_id` 覆盖率
- `event_time` 有效率
- `revenue_usd` 类型有效率
- conflict rate / reject rate
- events ingested per connector
- shard creation latency
- Pub/Sub backlog age
- Dataflow processing latency
- dead-letter volume
- duplicate rate
- BigQuery staging-to-curated lag
- aggregate refresh lag

### 4.4.3 观测落点（P0）
- 生产默认观测落点：
  - BigQuery audit tables
  - Cloud Logging
  - Cloud Monitoring alerts
- `mock` 模式继续保留本地 JSONL / 文件日志，便于调试和回放

### 4.4.4 告警规则（P0）
- required coverage < 95%
- canonical 覆盖率 < 90%
- reject rate > 5%
- 动态 cohort 刷新失败
- dead-letter volume 异常升高
- staging-to-curated lag 超阈值
- aggregate refresh lag 超阈值

### 4.4.5 可追溯与回放
- 保留 raw/stg/unified 三层数据痕迹
- 支持按 job_id/source 回放处理
- 保留审计日志（配置变更、字段覆盖、规则命中）

### 4.4.6 v1 验收标准
1. 每个 job 有完整的 source + processing + quality 报告
2. 告警规则能自动触发并记录
3. 关键问题（mapping 错误、冲突）可在 15 分钟内定位到 source/job
4. 支持按 job 触发 replay 恢复

---

## 4.5 Data Governance & Access Control（P0）

> 优先级：**P0（最小治理能力）**

### 目标
在 v1 建立最小可用的数据治理与权限控制，满足安全、合规与团队协作要求。

### 4.5.1 访问控制
- SQL 只读权限（禁 DDL/DML）
- 分角色权限：Admin / Analyst / Operator
- 高风险操作（覆盖映射、批量 replay）需审计

### 4.5.2 数据分级与脱敏
- PII 字段默认脱敏或 hash（email/phone）
- 导出 cohort 时按权限控制字段可见性
- 外部导出记录审计

### 4.5.3 配置治理
- source 配置、mapping、stitch 规则均版本化
- 支持变更 diff 与回滚
- 关键配置变更自动写审计日志

### 4.5.4 成本与资源治理
- 查询超时、扫描量上限
- replay 并发限制
- 动态 cohort 刷新频率上限

### 4.5.5 v1 验收标准
1. 具备角色化访问控制与操作审计
2. PII 字段可按策略脱敏输出
3. 配置变更可追踪、可回滚
4. 查询与重放具备基础资源保护策略

### 4.6 Default Settings（v1 默认配置）

> 以下默认值作为 v1 环境基线，可在项目级配置中覆盖。

#### 4.6.1 数据质量门槛
- required mapping coverage：`95%`
- canonical_user_id 覆盖率：`90%`
- reject rate 告警线：`5%`

#### 4.6.2 Cohort 刷新默认值
- dynamic cohort refresh_mode：`daily`
- static cohort refresh_mode：`manual`

#### 4.6.3 回放与资源控制
- replay 并发上限：`1~2 jobs`
- 查询超时：启用（项目级可配置）
- 扫描量上限：启用（项目级可配置）

#### 4.6.4 权限与审计默认值
- 角色模型：`Admin / Analyst / Operator`
- 高风险操作（规则变更、批量 replay、永久删除）默认强制审计

---

## 5. P0 实施清单（按优先级）

### P0-1（最高优先级）多来源 Ingestion 主链路可用
- 目标：至少 2 个来源稳定进入 `fact_events_unified`
- 关键交付：
  - source 配置/连通性检查
  - raw -> standardized -> unified 三层链路
  - dedupe + stitch 基础统计
- 完成标准：
  - 日常导入可跑通
  - job 状态完整（Processing / Awaiting Mapping / Ready / Failed）

### P0-2 Mapping 门禁与版本化
- 目标：确保关键字段映射质量可控、可回滚
- 关键交付：
  - required coverage 门禁（<95% 自动 Awaiting Mapping）
  - mapping 版本、diff、回滚
  - mapping 修复后 replay
- 完成标准：
  - 关键字段映射质量稳定达标
  - 错误映射可快速恢复

### P0-3 Identity Stitch + Source-of-Truth 生效
- 目标：建立可解释的主身份与归因口径
- 关键交付：
  - deterministic stitch 规则
  - source-of-truth matrix（身份：后端>分析SDK>MMP；归因：分析SDK>MMP）
  - 冲突日志（old/new/source/rule）
- 完成标准：
  - canonical_user_id 覆盖率 >= 90%
  - 冲突可追踪可解释

### P0-4 Cohort 生成与管理（Rule/SQL/List）
- 目标：把数据能力转化为可执行人群资产
- 关键交付：
  - Rule Builder / SQL Builder / Import List 三入口
  - cohort 持久化、命名管理、软删除与恢复
  - cohort 可直接供 engage/experiment/predict 调用
- 完成标准：
  - 生成、检索、刷新、激活全流程可用

### P0-5 Data Quality & Observability
- 目标：问题可发现、可定位、可回放
- 关键交付：
  - job/source/quality 看板
  - P0 告警（coverage/canonical/reject）
  - replay 与审计链路
- 完成标准：
  - 关键质量问题 15 分钟内定位到 job/source

### P0-6 Governance & Access Control
- 目标：在 v1 建立最小治理闭环
- 关键交付：
  - RBAC（Admin/Analyst/Operator）
  - PII 脱敏输出策略
  - 高风险操作审计
- 完成标准：
  - 关键操作全量可审计
  - 导出与查询具备权限边界

### 5.1 建议执行顺序（两周冲刺版）
- Week 1：P0-1 + P0-2 + P0-3
- Week 2：P0-4 + P0-5 + P0-6

### 5.2 上线门槛（Go/No-Go）
- required mapping coverage >= 95%
- canonical_user_id coverage >= 90%
- reject rate <= 5%
- 至少 1 个动态 cohort 每日刷新稳定运行
- 高风险操作审计开启且可查询

---

## 6. 当前 Gap Register（对照 2026-03 仓库状态评审）

### 6.1 当前已落地
- import 任务状态机、quality gate、resume / replay 已存在
- mapping version / rollback / suggestions / quality coverage 已存在
- SQL workspace、saved queries、query audit、query -> cohort 已存在
- identity summary / conflict / rejected 查询与健康告警已存在
- `operator-api + import-worker + prediction-worker + dataflow` 的基础运行形态已存在
- SQLAlchemy + Alembic control-plane persistence 已存在；本地 SQLite fallback 与生产 Postgres 目标已在代码结构中体现
- paged connector ingestion、ingestion checkpoints、BigQuery-backed prediction result storage 与分页读取已存在

### 6.2 仍未完成的 Gap

#### Gap-D1 Manifest-driven Processing 还不是默认路径
- 当前：
  - 导入主链路已具备 raw shard / standardized / unified 结构
  - 但默认工作方式仍然是 job 驱动的应用层 orchestration
- 未完成项：
  - 将 manifest-driven processing 升级为默认入口
  - 统一 raw shard -> manifest -> standardized -> unified 的默认调度语义

#### Gap-D2 Replay / Backfill Tooling 不完整
- 当前：
  - 已支持 mapping 修复后的 replay
  - 已支持按 job 恢复和 rejected rows 重放
- 未完成项：
  - 缺少面向 source/date/job range 的通用 raw-shard backfill / replay 工具
  - 缺少“无需重新拉源”的批量回放控制面

#### Gap-D3 Warehouse Schema Contract 仍未正式化
- 当前：
  - `events_staging / events_curated / player_latest_state` 已可用
  - canonical alias 也已存在
- 未完成项：
  - serving / experimentation tables 的 schema version 没有形成正式 contract 文档
  - 上游/下游兼容规则与变更门禁还不够显式

#### Gap-D4 Dead-letter / Quality Observability 仍偏工程态
- 当前：
  - rejected events、health alerts、identity summary 已可查
- 未完成项：
  - 缺少 operator 视角的 dead-letter remediation 流程
  - 缺少围绕 DLQ / quality gate / source freshness 的稳定 dashboard 与升级告警

#### Gap-D5 GCP-shaped Mode 仍是部分实现
- 当前：
  - 已有 GCS / PubSub / Dataflow / BigQuery 抽象
  - `import-worker / prediction-worker / dataflow` 的基础 entrypoint 已存在
  - mock 仍是默认主运行路径
- 未完成项：
  - 生产模式的默认运行契约、失败恢复和 observability 还未完全对齐

#### Gap-D6 Secret / Access Boundary 仍未达到生产级
- 当前：
  - 连接器和数仓访问主要依赖本地配置与环境变量
- 未完成项：
  - 缺少正式 secret manager
  - 缺少 warehouse/data connector 的生产级访问边界和权限轮转策略

#### Gap-D7 Data Core Console / Contract Hardening 仍未完成
- 当前：
  - connector / import / mapping / SQL / quality 能力都已有 API 与单页入口
- 未完成项：
  - Data Sandbox / connector / import / SQL workspace 仍缺少独立 E2E 契约覆盖
  - freshness / quality / DLQ / mapping remediation 仍需要更清晰的后端 view model 与 UI 契约

### 6.3 本文档持有的下一阶段 Owner
- `Phase 3 Data Platform Completion`
- `Phase 5 Production Readiness` 中与数据、连接器、数仓权限相关的部分
