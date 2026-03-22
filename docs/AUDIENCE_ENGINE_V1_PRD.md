# KairyxAI Audience Engine v1 PRD

## 1. 模块目标
将数据洞察转化为可复用、可执行、可追踪的人群资产，服务于运营触达、实验分流与模型策略。

---

## 2. 模块范围（v1）

### 2.1 In-scope
- Cohort 创建：Rule / SQL / List 三入口
- Cohort 生命周期管理：命名、标签、版本、删除恢复
- 成员计算与刷新：snapshot + delta
- 激活分发：供 Engage / Experiment / Copilot 消费
- 效果回流：基础效果指标与版本对比
- 审计追踪：创建/修改/刷新/激活/删除可追踪

### 2.2 Out-of-scope
- 跨租户共享 Cohort
- 小时级大规模实时刷新
- 自动因果归因与自动调参闭环
- 复杂审批流引擎

---

## 3. 子模块详细设计

## 3.1 A) Cohort Lifecycle（生命周期管理）

### 功能
- 创建 Cohort（Rule/SQL/List）
- 管理 metadata（name, description, owner, tags, status）
- definition 版本化（version + diff + rollback）
- 软删除（deleted_at）与恢复（restore）
- 高权限永久删除（审计强制）

### 数据对象
- `cohort`
- `cohort_definition`
- `cohort_version_log`

### DoD
1. 三入口创建成功率 >= 99%
2. 支持重命名、标签、版本回滚
3. 删除/恢复/永久删除全链路可审计

---

## 3.2 B) Membership Compute（成员计算引擎）

### 功能
- 支持 static/dynamic/sql 三类成员计算
- dynamic cohort 默认 daily 刷新
- 手动刷新入口（管理员/分析师）
- 计算后落地 snapshot
- 自动计算 delta（新增/流失）
- 刷新失败自动重试并记录根因

### 数据对象
- `cohort_snapshot`
- `cohort_membership_delta`
- `cohort_refresh_job`

### DoD
1. Dynamic cohort 日刷新成功率 >= 95%
2. 每次刷新产出 member_count + delta
3. 刷新失败支持重试并可定位错误

---

## 3.3 C) Activation & Delivery（激活与下游分发）

### 功能
- Cohort 状态机：`draft -> active -> paused -> archived`
- 激活前校验：
  - 非空人群
  - `canonical_user_id` 完整
  - refresh 状态正常
- 分页成员拉取接口
- 一键供给 Engage / Experiment / Copilot

### 接口建议
- `POST /cohorts`
- `GET /cohorts/{id}`
- `GET /cohorts/{id}/members`
- `POST /cohorts/{id}/refresh`
- `POST /cohorts/{id}/activate`
- `POST /cohorts/{id}/pause`

### DoD
1. Active cohort 可被 Engage/Experiment/Copilot 直接消费
2. 空人群无法激活（可保留草稿）
3. 成员读取支持稳定分页

---

## 3.4 D) Measurement & Feedback（效果回流）

### 功能
- 基础效果指标：
  - 覆盖人数
  - 触达率
  - 转化率
- 版本对比：
  - 成员规模变化
  - 核心指标变化
- 关联 experiment_id 查看 A/B/Holdout 结果

### 数据对象
- `cohort_metrics_daily`
- `cohort_experiment_link`

### DoD
1. 每个 active cohort 可查看基础效果指标
2. 支持最近两版 cohort 的规模/效果对比
3. 支持关联实验结果读取

---

## 4. 全局上线门槛（Go/No-Go）
1. 三类 cohort 创建成功率 >= 99%
2. Dynamic cohort 每日刷新成功率 >= 95%
3. 激活前校验生效（空人群/主键缺失阻断）
4. Engage/Experiment/Copilot 三模块消费链路打通
5. 全链路审计可查（创建/修改/刷新/激活/删除）

---

## 5. 默认配置（v1）
- dynamic cohort 刷新频率：`daily`
- static cohort 刷新频率：`manual`
- 刷新失败重试：`最多 2 次`
- replay 并发：`1~2 jobs`
- 高风险操作：`强制审计`

---

## 6. 与总 PRD 关系
本文件是 Audience Engine 的详细设计文档。总 PRD（`DATA_CORE_V1_PRD.md`）保留高层目标与验收标准，本文件用于工程实现与排期。

---

## 7. 当前 Gap Register（对照 repo / `current-state-product-spec.md`，2026-03）

### 7.1 当前已落地
- Rule / SQL / List cohort 三入口已存在
- lifecycle、版本、rollback、archive / restore、refresh jobs、metrics / compare 已存在
- 激活前 preflight、分页 members、基础效果回流已存在

### 7.2 仍未完成的 Gap

#### Gap-A1 效果回流仍依赖下游 measurement 成熟度
- 当前：
  - cohort metrics 已可读取 workflow delivery、experiment summary、outcome 结果
- 未完成项：
  - 真实 provider 级 return / conversion / delivery 信号还没有完全稳定
  - Audience feedback loop 仍受 Action / Experiment 真实回流质量限制

#### Gap-A2 Audience Operator Console 仍未硬化
- 当前：
  - 前端已具备 cohort 管理入口
- 未完成项：
  - 仍然没有独立 Playwright / E2E 覆盖
  - cohort metrics / compare / refresh history 的 operator UX 仍是单页静态控制台形态

#### Gap-A3 部分运营视图仍缺少专门 backend view model
- 当前：
  - 已有 members / versions / metrics / compare / refresh-jobs API
- 未完成项：
  - 某些页面仍由前端基于通用资源拼接，而非消费更稳定的聚合接口
  - 需要进一步收紧前后端 contract，降低 UI 侧合成逻辑

#### Gap-A4 生产级权限与租户边界尚未完成
- 当前：
  - 已有最小 RBAC 与审计
- 未完成项：
  - 跨租户隔离、正式身份认证和高风险 cohort 操作边界仍未完成

### 7.3 本文档持有的下一阶段 Owner
- `Phase 1 Frontend Hardening` 中 cohort UI / metrics / compare 契约
- `Phase 4 Activation And Measurement` 中 cohort feedback loop 的真实测量依赖
- `Phase 5 Production Readiness` 中 cohort 权限与租户边界
