# KairyxAI v1 Master PRD（总 PRD）

## 1) 产品定位（一句话）
用“实时数据 + AI决策 + 自动执行”把增长运营从分析工具升级为闭环增长引擎。

---

## 2) v1 核心模块（模块化 PRD 架构）

> 说明：总 PRD 只保留目标、边界、里程碑与验收门槛。每个核心模块维护独立 Sub PRD。

### 2.1 Data Core（实时事件层）
- 能力：事件采集、清洗标准化、ID Stitch、质量门禁、可回放
- Sub PRD：`KairyxAI/docs/DATA_CORE_V1_PRD.md`

### 2.2 Insight Copilot（智能分析层）
- 能力：自然语言问数、异常解释、行动建议、自动报告
- Sub PRD：`KairyxAI/docs/COPILOT_V1_PRD.md`

### 2.3 Audience Engine（动态分群层）
- 能力：Rule/SQL/List 生群、刷新、命名管理、激活分发、效果回流
- Sub PRD：`KairyxAI/docs/AUDIENCE_ENGINE_V1_PRD.md`

### 2.4 Action Orchestrator（执行编排层）
- 能力：触发器、动作编排、流程画布、执行控制
- Sub PRD：`KairyxAI/docs/ACTION_ORCHESTRATOR_V1_PRD.md`（待完善）

### 2.5 Experiment Hub（实验层）
- 能力：A/B + Holdout、指标归因、实验结论与推荐
- Sub PRD：`KairyxAI/docs/EXPERIMENT_HUB_V1_PRD.md`（待完善）

---

## 3) v1 关键闭环（结果导向）
洞察发现 → AI解释 → 生成人群 → 触发动作 → 实验验证 → 效果回流 → 策略迭代

### 3.1 改善观测时间窗（v1）
- T+1 天：执行层指标（触达率、点击率、执行成功率）
- T+7 天：中间业务指标（回访率、短期转化率）
- T+28 天：核心业务指标（留存、付费、召回）

### 3.2 观测指标数据来源（v1）
- 执行层指标（触达率/点击率/执行成功率）：`action_execution`, `action_delivery`（Action Orchestrator）
- 中间业务指标（回访率/短期转化率）：`experiment_summary`, `experiment_outcome`, `mart_user_daily`（Experiment Hub + Data Core）
- 核心业务指标（留存/付费/召回）：`mart_user_daily`, `fact_events_unified`, `experiment_summary`（Data Core + Experiment Hub）

### 3.3 归因约束（v1）
- 改善结论优先来自 Experiment Hub（有对照组）
- 无对照组结果仅标记为“观察结果”，不计入收益归因
- 每周输出闭环收益报告（执行→实验→收益）

---

## 4) 首批高价值场景（v1，应用层）

### 4.1 场景A：流失预警挽回（Churn Rescue）
**业务目标**
- 降低中高风险用户流失，提升回访与召回。

**应用层流程**
1. Copilot 识别流失风险上升与关键驱动
2. Audience Engine 生成“高风险待挽回”人群（动态 cohort）
3. Action Orchestrator 执行挽回动作（push/email/in-app）
4. Experiment Hub 对比 holdout 与 treatment 效果

**核心指标**
- 7日回访率
- 14日召回率
- 被触达用户负反馈率（护栏）

**v1 上线标准（场景级）**
- 每日自动刷新挽回人群
- 至少 1 条挽回工作流稳定运行
- 4~8 周内召回率相对提升 >= 15%

---

### 4.2 场景B：付费提升（Monetization Lift）
**业务目标**
- 提升高潜用户转化效率与付费质量。

**应用层流程**
1. Data Core + Copilot 识别高潜未付费/低频付费人群
2. Audience Engine 输出分层人群（高潜、观望、沉默）
3. Action Orchestrator 下发差异化优惠/权益策略
4. Experiment Hub 验证 uplift 与护栏指标

**核心指标**
- 付费转化率
- ARPPU / 收入 uplift
- 退款率或投诉率（护栏）

**v1 上线标准（场景级）**
- 支持按人群层级执行不同策略
- 实验结论可输出 winner/neutral/inconclusive/invalid
- 2~4 周内触达转化率相对提升 >= 10%

---

### 4.3 场景C：新手转化（Onboarding Activation）
**业务目标**
- 提升新用户关键路径通过率与首周留存。

**应用层流程**
1. Copilot 定位新手漏斗关键流失点
2. Audience Engine 生成功能卡点人群（如“看过引导未完成”）
3. Action Orchestrator 触发引导动作（教程提示、奖励触发）
4. Experiment Hub 对比不同引导策略效果

**核心指标**
- 新手关键步骤完成率
- D1 / D7 留存
- 触达打扰率（护栏）

**v1 上线标准（场景级）**
- 至少覆盖 1 条新手关键漏斗
- 支持漏斗卡点自动触发
- 4 周内关键步骤完成率显著提升（以实验结果为准）

---

### 4.4 应用层共性约束（v1）
- 每个场景必须绑定：`目标人群 + 执行动作 + 实验验证 + 护栏指标`
- 无实验对照组的结果仅计为观察，不计为收益归因
- 每周输出场景级收益看板（覆盖人数、触达、转化、护栏、净提升）

---

## 5) 成功指标（上线 90 天）
- 运营策略上线周期：天级 → 小时级
- 触达转化率提升：+10% ~ +20%
- 流失用户召回率：+15%
- 分析到执行闭环比例：>60%

### 5.1 v1 最小改善目标（阶段）
- 2~4 周：触达转化率相对提升 >= 10%
- 4~8 周：流失召回率相对提升 >= 15%
- 2 周内：策略上线周期由“天级”降到“小时级”

---

## 6) 技术/架构原则（详细版）

### 6.1 实时优先（分钟级）
**原则**：核心业务链路默认按分钟级可见设计，不依赖单一 T+1 批处理。

**执行要求**：
- 关键事件（登录、付费、流失信号）进入统一层后，1~5 分钟内可用于分群/触发
- 动态人群默认 daily 刷新，并支持手动即时刷新
- 动作执行回执（delivery/outcome）快速回流，支持 T+1 天观察早期效果

### 6.2 可解释优先（Evidence-first AI）
**原则**：所有 Copilot 结论和策略建议必须可追溯、可验证。

**执行要求**：
- 每条结论必须附：口径、时间窗、数据来源（表/模块）
- 每条建议必须附：目标人群定义、预期影响、风险提示
- 实验结论统一为：winner / neutral / inconclusive / invalid
- 无对照组结果仅计为观察，不计收益归因

### 6.3 人在回路（Human-in-the-loop）
**原则**：高风险动作默认人工确认，避免自动化失控。

**执行要求**：
- 高风险动作（大规模触达/敏感人群/预算超阈）需人工确认
- Kill Switch 一键停发，1 分钟内生效
- 频控、冷却、静默时段默认启用
- test-run 必须沙箱隔离，禁止触达真实用户

### 6.4 可扩展部署（SaaS + 私有化）
**原则**：模块边界稳定，支持 SaaS 与私有化部署并存。

**执行要求**：
- 模块之间以 API contract 对接，避免跨模块硬依赖
- 配置版本化（mapping/规则/实验）并支持回滚
- 数据边界可配置，满足不同部署模式合规要求

### 6.5 模块解耦与故障隔离（P0 强制）
**原则**：每个模块与执行层解耦，单点故障可局部降级，不允许全系统连带 shutdown。

**执行要求**：
- Data Core / Copilot / Audience / Action / Experiment 通过稳定接口通信，不共享进程内强耦合状态
- 任一模块异常时，其他模块保持可用并进入降级模式（例如：Copilot 不可用时仍可执行既有 workflow）
- 每个模块具备独立健康检查、重试策略和故障告警
- 禁止单模块失败触发全局停机；仅允许该模块局部隔离并修复
- 修复策略以“局部修复 + 回放恢复”为默认路径，不影响其他模块持续运行

### 6.6 治理与审计默认开启
**原则**：关键行为可追踪、可审计、可回溯。

**执行要求**：
- RBAC：Admin / Analyst / Operator
- 审计覆盖：配置变更、分群变更、实验决策、执行动作
- PII 脱敏默认开启
- 查询与重放具备资源保护（超时/扫描量/并发上限）

---

## 7) 范围管理（总 PRD 与 Sub PRD 分工）

### 总 PRD 负责
- 产品目标与边界
- 模块依赖关系
- 跨模块里程碑
- 总体验收门槛（Go/No-Go）

### Sub PRD 负责
- 模块详细 Scope（In/Out）
- 数据模型与 API 设计
- 任务拆解与 DoD
- 模块级上线标准

---

## 8) 跨模块依赖关系
1. Data Core 为 Copilot/Audience/Experiment 提供统一口径数据
2. Copilot 输出建议并生成 Audience 草稿
3. Audience 为 Action 与 Experiment 提供人群输入
4. Action 执行结果回流 Data Core
5. Experiment 结果回流 Copilot 与 Audience 优化

---

## 9) 跨模块里程碑（建议）
- M1：Data Core + Audience 基础链路可用
- M2：Copilot 问数/解释 + Audience 联动
- M3：Action 编排 + Experiment 闭环联调

---

## 10) 总体上线门槛（Go/No-Go）
1. Data Core 质量门禁达标（coverage/canonical/reject）
2. Audience 动态刷新稳定可用
3. Copilot 输出具备证据链与口径说明
4. Action 执行具备人工确认与审计能力
5. Experiment 可输出可读结论并可回流

---

## 11) 文档清单
- Master PRD（本文件）：`KairyxAI/docs/KAIRYXAI_V1_MASTER_PRD.md`
- Data Core Sub PRD：`KairyxAI/docs/DATA_CORE_V1_PRD.md`
- Copilot Sub PRD：`KairyxAI/docs/COPILOT_V1_PRD.md`
- Audience Sub PRD：`KairyxAI/docs/AUDIENCE_ENGINE_V1_PRD.md`
- Action Orchestrator Sub PRD：`KairyxAI/docs/ACTION_ORCHESTRATOR_V1_PRD.md`（待完善）
- Experiment Hub Sub PRD：`KairyxAI/docs/EXPERIMENT_HUB_V1_PRD.md`（待完善）