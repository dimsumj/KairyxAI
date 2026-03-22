# KairyxAI Action Orchestrator v1 PRD

## 1. 模块目标
将 Audience/Copilot 输出的人群与策略，转化为可控、可审计、可回滚的执行流程（Push / Email / In-App / Webhook）。

---

## 2. 模块范围（v1）

### 2.1 In-scope
- Trigger 定义：时间触发、事件触发、阈值触发
- Action 定义：push / email / in-app / webhook（基础）
- Workflow 编排：条件分支、限频、冷却窗口
- 执行控制：草稿、发布、暂停、停止
- 执行日志：发送、失败、重试、跳过原因
- Audience export jobs：面向 Braze / SendGrid / Webhook 的导出、状态、重试、诊断
- 安全机制：人工确认、Kill Switch、预算与频控门槛

### 2.2 Out-of-scope
- 跨渠道复杂旅程编排（多阶段营销编排）
- 自优化自动投放预算分配
- 高复杂模板渲染系统

---

## 3. 子模块详细设计

## 3.1 Workflow Builder（流程编排）

### 功能
- 可视化节点：Trigger -> Filter -> Action -> Wait -> End
- 条件分支：IF/ELSE（基于属性/事件/标签）
- 工作流版本化（draft/published）

### DoD
1. 可创建并保存基础工作流
2. 支持发布新版本并保留旧版本
3. 支持暂停与恢复

---

## 3.2 Trigger Engine（触发引擎）

### 功能
- 时间触发：cron/daily/hourly
- 事件触发：指定 event_type 到达
- 阈值触发：指标超过/低于阈值

### 执行要求
- 触发去重（同用户同规则短时间不重复触发）
- 触发幂等（重复事件不重复执行）
- 幂等键规范（P0 强制）：
  - `idempotency_key = workflow_id + workflow_version + user_id + action_type + window_bucket`
  - 同一幂等键在有效窗口内只能成功执行一次

### DoD
1. 支持三类触发器
2. 触发事件具备幂等保障
3. 触发记录可审计

---

## 3.3 Delivery Engine（发送执行）

### 功能
- 渠道适配：push/email/in-app/webhook
- 发送前校验：用户可触达、频控、退订状态
- 重试策略：失败重试（指数退避）
- 降级策略：渠道失败时 fallback（可选）
- Audience export：
  - 支持 Braze / SendGrid / Webhook 导出 job
  - 返回 provider response、delivery/export diagnostics 与 retry 状态
  - 导出 payload 以 `user_id / email / predicted_churn_risk / suggested_action / metadata` 为主

### 运行时归属（补充）
- `/api/v1/exports` 资源属于 Action Orchestrator 的执行控制面
- `export-worker` 负责 provider export execution、重试、诊断回写与 retry-aware job state
- export 任务资源默认遵循标准 job contract：`id / type / status / created_at / updated_at / progress / error / links`

### DoD
1. 至少 2 个渠道稳定可用（建议 push+email）
2. 失败重试可配置并可追踪
3. 每次发送有 delivery_id 与状态
4. audience export job 具备 status / retry / diagnostics

---

## 3.4 Policy & Safety Guardrails（策略与安全护栏）

### 功能
- 频控：每日/每周触达上限
- 冷却窗口：同类动作最小间隔
- 黑名单/敏感人群排除
- Kill Switch：一键全局停发
- 高风险动作人工确认
- 频控三级策略（P0 强制）：
  1) 全局频控（每用户每日总触达上限）
  2) 渠道频控（push/email 各自上限）
  3) 场景频控（同 campaign/workflow 冷却窗口）
- 静默时段（可配置，默认启用夜间不打扰）

### 默认值（v1）
- 每用户每日触达上限：3
- 同类动作冷却时间：24h
- 全局 Kill Switch：开启即停止新执行

### DoD
1. 频控与冷却规则默认生效
2. Kill Switch 可在 1 分钟内生效
3. 高风险动作必须有确认记录

---

## 3.5 Execution Observability（执行观测）

### 功能
- 工作流级指标：触发数、执行数、成功率、失败率
- 渠道级指标：送达率、点击率、转化率（基础）
- 失败归因标准化（P0 强制）：
  - `policy_blocked`
  - `channel_error`
  - `template_error`
  - `data_missing`
  - `timeout`
- 失败原因 TopN 可视化与趋势跟踪

### DoD
1. 支持 workflow/channel 两层观测
2. Top 失败原因可视化
3. 关键执行日志可按 user_id/workflow_id 回查

---

## 4. 数据对象（v1）
- `workflow`
- `workflow_version`
- `workflow_trigger_event`
- `action_execution`
- `action_delivery`
- `action_policy_log`
- `action_audit_log`

---

## 5. API 草案（v1）
- `POST /workflows`
- `GET /workflows/{id}`
- `POST /workflows/{id}/publish`
- `POST /workflows/{id}/pause`
- `POST /workflows/{id}/resume`
- `POST /workflows/{id}/test-run`（沙箱隔离，禁止触达真实用户）
- `GET /workflows/{id}/executions`
- `POST /orchestrator/kill-switch/on`
- `POST /orchestrator/kill-switch/off`

---

## 6. 上线门槛（Go/No-Go）
1. 基础工作流（Trigger->Action）可稳定发布与执行
2. 发送链路成功率达到目标阈值（项目自定义）
3. 频控、冷却、Kill Switch 全部可用
4. 执行全链路可审计（触发->执行->送达）
5. 可与 Audience/Copilot/Experiment 完成最小闭环联动

---

## 7. P0 实施优先级
1. Workflow + Trigger 最小链路
2. Push/Email 两渠道执行与重试
3. 频控/冷却/Kill Switch
4. 执行观测与审计
5. 与 Audience + Experiment 联调

---

## 8. 当前 Gap Register（对照 2026-03 仓库状态评审）

### 8.1 当前已落地
- workflow / trigger / policy / budget / confirmation / kill switch 已存在
- delivery diagnostics、provider callbacks、policy counters、event/threshold trigger 已存在
- 与 Audience / Experiment 的最小闭环已打通
- `audience export job` 已具备独立 `/api/v1/exports` 资源与 `export-worker` entrypoint

### 8.2 仍未完成的 Gap

#### Gap-O1 Delivery Engine 仍偏 demo / simulator 形态
- 当前：
  - push/email/braze 适配器与执行日志已存在
- 未完成项：
  - push 仍明显依赖 simulator 语义
  - in-app / webhook 还未形成与 push/email 同等级的稳定产品能力

#### Gap-O2 Real Provider Measurement 仍未完全稳态化
- 当前：
  - delivery callbacks 与 diagnostics 已存在
- 未完成项：
  - 不同 provider 的回执归一、失败分类、延迟回流、重试/fallback 仍不够完整
  - “真实 engagement outcome” 还没有在所有渠道上形成一致的数据契约

#### Gap-O3 Operator Console / Execution UX 仍未硬化
- 当前：
  - workflow / delivery / policy 相关能力已在单页控制台中可见
- 未完成项：
  - 缺少独立 Playwright / E2E 覆盖
  - 执行失败、重试、预算消耗、policy block 的产品化排查视图仍偏运维态

#### Gap-O4 Provider Credentials 与运行边界仍未生产化
- 当前：
  - 已有最小治理、审计和 header-based role boundary
- 未完成项：
  - 缺少正式 secret management
  - 缺少 provider 级身份认证、环境隔离和租户边界

#### Gap-O5 自动优化仍未开放
- 当前：
  - Action 层已能执行、记录和回流结果
- 未完成项：
  - rollout / retry / policy 调优仍需要人工判断
  - 系统不会基于真实 outcome 自动修改 workflow 策略

### 8.3 本文档持有的下一阶段 Owner
- `Phase 1 Frontend Hardening` 中 execution / delivery / policy 页面
- `Phase 4 Activation And Measurement` 中 provider-grade delivery + callback + outcome 契约
- `Phase 5 Production Readiness` 中 provider credentials / auth / tenant boundary
