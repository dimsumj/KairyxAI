# KairyxAI Experiment Hub v1 PRD

## 1. 模块目标
为策略与人群提供可复用的实验框架（A/B/Holdout），实现“可验证、可归因、可决策”的增长实验闭环。

---

## 2. 模块范围（v1）

### 2.1 In-scope
- 实验创建与配置（A/B/Holdout）
- 流量分配（deterministic assignment）
- 曝光与结果埋点
- 指标看板（基础显著性提示）
- 实验结论输出（winner/neutral/inconclusive）
- 与 Audience/Action 的联动

### 2.2 Out-of-scope
- 多变量实验（MVT）
- 贝叶斯高级推断引擎
- 自动全量 rollout

---

## 3. 子模块详细设计

## 3.1 Experiment Config（实验配置）

### 功能
- 实验类型：A/B + Holdout
- 实验对象：绑定 cohort_id 或规则人群
- 分流比例：holdout_pct、variant_pct
- 时间窗口：start/end，最小样本量、最短运行时长（minimum runtime）
- 指标配置（P0 强制）：
  - 1 个主指标（Primary Metric）
  - 至少 2 个护栏指标（Guardrail Metrics）

### DoD
1. 可创建并保存实验配置
2. 配置具备版本化与审计
3. 支持启用/停用

---

## 3.2 Traffic Assignment（流量分配）

### 功能
- 基于 `experiment_id + user_id` 稳定分桶
- 分组：holdout / treatment_a / treatment_b
- 支持排除名单（黑名单）

### 执行要求
- 幂等：同用户重复请求必须同组
- 可追溯：每个用户分组结果可查
- SRM 检测（P0 强制）：
  - 持续检测样本比例偏差（Sample Ratio Mismatch）
  - SRM 触发时标记实验风险并告警

### DoD
1. 分桶稳定且可复现
2. 组间比例与配置偏差在可接受范围
3. 暴露记录可按用户回查

---

## 3.3 Exposure & Outcome Logging（曝光与结果）

### 功能
- 记录曝光事件（exposure）
- 记录结果事件（outcome）
- 关联 action_id / cohort_id / workflow_id

### DoD
1. exposure/outcome 都可按 experiment_id 查询
2. 数据可追踪到具体执行链路
3. 关键字段缺失时拒绝写入并告警

---

## 3.4 Measurement & Decision（指标与结论）

### 功能
- 基础指标：engagement_rate、return_rate、conversion_rate
- 对照比较：treatment vs holdout
- 输出结论：winner / neutral / inconclusive / invalid
- 显著性提示（v1 用基础阈值提示）
- 结论门禁（P0 强制）：
  - 未达最小样本量或最短运行时长时，禁止输出 winner
  - SRM 命中时优先输出 invalid/inconclusive

### DoD
1. 每个实验可输出分组对比与 uplift
2. 支持至少 1 个主指标 + 2 个辅助指标
3. 结论可追溯到原始曝光与结果日志

---

## 3.5 Rollout Suggestion（发布建议）

### 功能
- 输出建议：继续实验 / 小流量扩量 / 停止
- 风险提示：样本不足、结果不稳定、分组偏差
- 一键同步给 Action Orchestrator（人工确认后）

### DoD
1. 结论后自动给出下一步建议
2. 风险条件命中时强制提示
3. 与 Action 的联动为“建议态”而非自动执行

---

## 4. 数据对象（v1）
- `experiment`
- `experiment_config_version`
- `experiment_assignment`
- `experiment_exposure`
- `experiment_outcome`
- `experiment_summary`
- `experiment_decision_log`

---

## 5. API 草案（v1）
- `POST /experiments/config`
- `GET /experiments/config`
- `POST /experiments/{id}/start`
- `POST /experiments/{id}/stop`
- `GET /experiments/{id}/summary`
- `GET /experiments/{id}/exposures`
- `GET /experiments/{id}/outcomes`
- `POST /experiments/{id}/decision`

---

## 6. 上线门槛（Go/No-Go）
1. A/B/Holdout 分流稳定可复现
2. exposure/outcome 日志完整可追踪
3. summary 可输出 uplift 与基础结论
4. 主指标 + 护栏指标配置必填并生效
5. 最小样本量/最短运行时长门禁生效（未达标不出 winner）
6. SRM 检测生效并可告警
7. 结论可回流 Copilot 与 Action
8. 高风险建议默认人工确认

---

## 7. P0 实施优先级
1. 实验配置 + 稳定分桶
2. exposure/outcome 链路打通
3. summary 指标与基础结论
4. 与 Audience/Action/Copilot 联调
5. 决策建议与审计完善

---

## 8. TODO（v1.1+）
- 多重比较校正（Multiple Comparisons Correction）
  - 目标：在并行评估多个指标/多个实验时，降低假阳性风险
  - 候选方法：Bonferroni / Holm-Bonferroni / Benjamini-Hochberg（FDR）
  - 备注：v1 保持基础显著性提示，v1.1 评估并引入统一校正策略

---

## 9. 当前 Gap Register（对照 repo / `current-state-product-spec.md`，2026-03）

### 9.1 当前已落地
- experiment config / versions / assignments / exposures / outcomes / summary / decision 已存在
- holdout / treatment_a / treatment_b、SRM、guardrails、rollout suggestion 已存在
- 与 Audience / Action / Copilot 的基础联动已存在

### 9.2 仍未完成的 Gap

#### Gap-E1 Outcome Robustness 仍依赖 Action / Provider 成熟度
- 当前：
  - outcome ingest、callback -> outcome、summary / decision 已存在
- 未完成项：
  - 真实 return / conversion / downstream engagement signal 还没有在所有 provider 上稳定打通
  - outcome completeness 与延迟处理仍需要更强的数据契约

#### Gap-E2 Measurement Integrity Tooling 仍偏轻量
- 当前：
  - SRM、guardrails、summary、decision 已可输出
- 未完成项：
  - outcome lag、数据缺失、measurement drift 的监控与告警还不够成熟
  - experiment health 的 operator triage 还未形成稳定工作流

#### Gap-E3 Experiment Review Console 仍未硬化
- 当前：
  - 前端已能调用 experiment 相关接口
- 未完成项：
  - 缺少独立 Playwright / E2E 契约覆盖
  - summary / assignment / rollout / alert 的 operator 视图仍是单页静态控制台

#### Gap-E4 Rollout 仍是“建议态”
- 当前：
  - 系统可输出 rollout suggestion
- 未完成项：
  - 还没有由 Experiment 直接驱动的受控 rollout controller
  - 仍然需要 Action 层和人工确认来执行扩量/停发

#### Gap-E5 生产级权限与边界尚未完成
- 当前：
  - 已有最小 RBAC、审计和高风险确认链路
- 未完成项：
  - 缺少正式 authN / tenant boundary / secret isolation
  - 还不能视为生产级实验平台

### 9.3 本文档持有的下一阶段 Owner
- `Phase 4 Activation And Measurement` 中真实 outcome 与 summary integrity
- `Phase 1 Frontend Hardening` 中 experiment review / rollout UI
- `Phase 5 Production Readiness` 中 experiment 权限与隔离边界
