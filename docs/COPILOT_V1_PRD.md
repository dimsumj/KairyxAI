# KairyxAI Insight Copilot v1 PRD

## 1. 背景与目标

### 1.1 背景
增长团队在“看数—找原因—定动作”链路上仍存在大量手工环节：
- 指标查询口径不一致
- 异常定位慢、跨团队沟通成本高
- 分析结论难以直接转化为可执行动作

### 1.2 目标（v1）
构建一个“会问数、会解释、会建议”的分析副驾，形成从洞察到行动的最小闭环：

**问数 -> 异常解释 -> 行动建议 -> 一键生成人群草稿**

### 1.3 非目标（v1 不做）
- 自主执行高风险运营动作（仅建议，不自动发送）
- 完整 BI 替代
- 复杂因果推断平台

---

## 2. 用户与场景

### 2.1 目标用户
- PM / Growth PM
- 数据分析师
- LiveOps / 运营同学
- 市场与投放负责人

### 2.2 高频场景
1. 快速问数：某指标当前值与趋势
2. 异常定位：为什么今天/本周指标下滑
3. 运营建议：下一步针对谁做什么
4. 复盘总结：日报/周报自动生成

---

## 3. 功能范围（v1）

## 3.1 NL2Metric（自然语言问数）
### 能力
- 支持自然语言查询指标
- 支持常用切片（平台、国家、渠道、版本、时间窗）
- 输出口径说明（指标定义、时间范围、过滤条件）

### 示例
- “过去 7 天 iOS 美国付费率”
- “本周 D1 留存比上周变化多少”

### 输出
- 指标值
- 环比/同比（可用时）
- SQL 摘要（只读）
- 口径说明

## 3.2 Anomaly Explain（异常解释）
### 能力
- 自动识别关键指标异常（跌/涨/波动）
- 输出 Top drivers（2-5 条）
- 给出影响范围（用户数、收入影响估算）

### 驱动因素示例
- 指定国家流量减少
- 某 app_version 转化下降
- 某 campaign 流量质量下滑

### 输出
- 异常摘要
- 驱动因素排名
- 证据数据片段
- 置信度

## 3.3 Action Recommendation（行动建议）
### 能力
- 根据异常与分群给出可执行建议
- 建议映射到已有动作类型（push/email/in-app/实验）
- 一键生成人群草稿（cohort draft）

### 输出
- 建议动作
- 目标人群定义
- 预期影响（方向性）
- 风险提示（打扰频次/预算）

## 3.4 Auto Report（自动报告）
### 能力
- 生成日报/周报（固定模板）
- 包含：核心指标、异常、建议动作、跟进事项

---

## 4. 输出模板（统一）
每条 Copilot 响应必须按结构化输出：
1. **结论**（一句话）
2. **关键证据**（最多 3 条）
3. **影响范围**（用户数/收入影响）
4. **建议动作**（可执行）
5. **置信度**（high/medium/low）
6. **口径与时间窗**（必须）

---

## 5. 数据与系统依赖

## 5.1 数据输入（只读）
- `mart_user_daily`
- `fact_events_unified`
- `cohort metadata`
- `experiment summary`（可用时）

## 5.2 与其他模块关系
- 依赖 Data Core 提供统一口径数据
- 调用 Audience Engine 生成人群草稿
- 调用 Experiment Hub 创建实验草稿（可选）

---

## 6. 安全与治理

1. 所有结论必须可追溯到数据证据
2. 不确定时必须降级输出“低置信度 + 需要补充数据”
3. 不自动执行高风险动作（默认需人工确认）
4. 输出中不得泄露受限字段（遵循 RBAC 与脱敏策略）

---

## 7. 默认配置（v1）
- 异常检测窗口：7d / 14d 双窗口
- 驱动因素最大返回数：5
- 自动报告频率：每日 1 次（可配置）
- 证据要求：每条建议至少 1 个可验证证据点

---

## 8. API 草案（v1）

- `POST /copilot/query`
  - 输入：自然语言问题、时间窗、过滤条件
  - 输出：结构化洞察结果

- `POST /copilot/explain`
  - 输入：指标名、时间窗、切片维度
  - 输出：异常解释 + drivers

- `POST /copilot/recommend`
  - 输入：洞察结果 ID / 指标上下文
  - 输出：动作建议 + cohort 草稿定义

- `POST /copilot/report`
  - 输入：日报/周报参数
  - 输出：结构化报告内容

---

## 9. 验收标准（DoD）

1. 支持至少 20 个高频问数意图
2. 异常解释可输出 >=2 个可验证驱动因素
3. 每条建议均包含口径、证据与置信度
4. 支持“一键生成人群草稿”并成功落库
5. 日报模板可稳定生成并被运营/PM使用

---

## 10. P0 实施优先级（可执行详细版）

### P0-1 问数能力（NL2Metric）
**目标**：让 Copilot 在统一口径下稳定回答高频业务问题。

**详细范围**
1. 建立 `metric registry`（至少 20 个指标）
   - 字段：`metric_id`, `name`, `definition`, `sql_template`, `supported_dimensions`, `default_window`
2. 建立 Query 解析器
   - 将自然语言解析为：指标、时间窗、维度过滤、比较方式
3. 执行层
   - 根据 registry 生成 SQL 并查询 `mart_user_daily`/`fact_events_unified`
4. 输出层
   - 返回结构化结果：结论、证据、口径、SQL 摘要

**接口与产物**
- `POST /copilot/query`
- 表：`copilot_query_logs`

**验收标准（DoD）**
- 20 个高频问数意图可稳定命中
- 每条结果都包含口径与时间窗
- 查询失败可返回明确错误原因（非空泛报错）

---

### P0-2 异常解释（Anomaly Explain）
**目标**：对关键指标波动给出可验证的驱动因素，而不只是“涨了/跌了”。

**详细范围**
1. 异常检测任务
   - 每日计算核心指标基线与偏差（7d/14d）
2. Driver 拆解
   - 按维度自动分解：platform/country/version/channel/campaign
3. 解释输出
   - 输出 Top 2~5 drivers + 每项证据值 + 影响估算
4. 置信度评级
   - 根据样本量、波动稳定性打 `high/medium/low`

**接口与产物**
- `POST /copilot/explain`
- 表：`anomaly_events`, `anomaly_driver_logs`

**验收标准（DoD）**
- 每个异常至少输出 2 个可验证 drivers
- 每个 driver 都带数值证据
- 支持按指标/时间窗复现解释结果

---

### P0-3 行动建议 + 人群草稿（Recommendation + Cohort Draft）
**目标**：把分析结论直接转成可执行动作与目标人群。

**详细范围**
1. 建议模板引擎
   - 输入：异常类型 + 指标上下文
   - 输出：动作类型（push/email/in-app/实验）+ 风险提示
2. 人群草稿生成
   - 将建议绑定 cohort 定义（rule/sql）
   - 写入 cohort draft（不默认自动激活）
3. 可执行联动
   - 一键进入 Audience Engine（cohort 管理）

**接口与产物**
- `POST /copilot/recommend`
- 表：`copilot_recommendations`, `cohort_drafts`

**验收标准（DoD）**
- 建议可落地为 cohort 草稿
- 建议包含目标人群、动作、风险提示
- 不触发高风险自动执行（默认人工确认）

---

### P0-4 报告自动化（Auto Report）
**目标**：让运营/PM 每天收到一致、可执行的分析摘要。

**详细范围**
1. 报告模板
   - 模块：核心指标、异常、建议动作、待跟进事项
2. 调度执行
   - 每日固定时段自动生成
3. 报告存档
   - 保存报告内容、生成时间、数据窗口、生成状态

**接口与产物**
- `POST /copilot/report`
- 表：`copilot_reports`

**验收标准（DoD）**
- 日报可每日稳定生成
- 报告结构固定且字段完整
- 失败任务可重试并记录原因

---

### P0-5 治理与可追溯（Governance & Traceability）
**目标**：保证 Copilot 输出可信、可审计、可控。

**详细范围**
1. 证据链记录
   - 每条输出关联 `query_id`, `metric_id`, `time_window`, `data_sources`
2. 权限与脱敏
   - 按 RBAC 过滤字段（PII 默认脱敏）
3. 审计日志
   - 记录查询、解释、建议、报告生成与人工确认动作
4. 风险保护
   - 对低置信度结论强制提示
   - 高风险建议需要人工确认

**接口与产物**
- 表：`copilot_audit_logs`
- 配置：`copilot_safety_config`

**验收标准（DoD）**
- 任意 Copilot 结论可追溯到数据证据
- 越权字段不可见
- 高风险动作存在审计记录与确认链路

---

## 11. 当前 Gap Register（对照 2026-03 仓库状态评审）

### 11.1 当前已落地
- `query / explain / recommend / report` API 已存在
- query logs、anomalies、reports、weekly report 资源已存在
- 结构化输出、evidence envelope、cohort draft 生成已存在

### 11.2 仍未完成的 Gap

#### Gap-C1 Copilot Operator Console 仍未硬化
- 当前：
  - Copilot 页面已经在单页 operator console 中可用
- 未完成项：
  - 仍然没有独立 Playwright / E2E 契约覆盖
  - query / explain / anomaly / report 视图仍依赖单页静态控制台组织

#### Gap-C2 Auto Report 的运营工作流仍偏轻量
- 当前：
  - 日报/周报资源与 retry 已存在
- 未完成项：
  - 报告订阅、审核、失败分诊和运营消费流程还没有独立产品化
  - 前端侧还没有成熟的报告管理控制台

#### Gap-C3 Recommendation 仍是“建议态”，还不是 outcome-driven 自动优化
- 当前：
  - Copilot 能给建议并生成 cohort draft
- 未完成项：
  - 还没有“真实 outcome -> 自动更新建议模板 / 自动策略调优”的稳定闭环
  - recommendation 仍依赖 Experiment / Action 的 measurement maturity

#### Gap-C4 Evidence Loop 仍受下游测量成熟度限制
- 当前：
  - experiment summary、cohort snapshot、workflow summary 已可作为证据输入
- 未完成项：
  - 当真实 provider outcome / return / conversion 信号不完整时，Copilot 证据链仍会退化
  - 需要与 Action / Experiment 的真实 measurement pipeline 联动补齐

#### Gap-C5 Production Access Boundary 不完整
- 当前：
  - 已有最小 RBAC 与脱敏
- 未完成项：
  - 缺少正式 authN / tenant boundary / production-grade access control
  - 仍不能把当前 Copilot 数据访问边界视为生产完成态

### 11.3 本文档持有的下一阶段 Owner
- `Phase 1 Frontend Hardening` 中 Copilot 页面与契约部分
- `Phase 4 Activation And Measurement` 中 evidence feedback 对 Copilot 的依赖部分
- `Phase 5 Production Readiness` 中 Copilot 的访问边界与治理部分
