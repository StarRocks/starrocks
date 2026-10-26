---
displayed_sidebar: docs
description: "通过注册的 decision Provider 评估业务状态，并返回经过校验的 JSON 答案。"
sidebar_position: 70
---

# ai_decide

调用管理员配置的阿里云百炼决策模型，返回完整且经过校验的 JSON 响应。问题可以要求分类（`choice`）、肯定概率（`noul`）或有序评分（`score`）。请求使用 [System One 决策 API](https://help.aliyun.com/zh/model-studio/decision-model-api)。

## 语法

```sql
ai_decide(state VARCHAR, questions JSON) -> JSON
ai_decide(state JSON, questions JSON) -> JSON
ai_decide(provider_name VARCHAR, state VARCHAR, questions JSON) -> JSON
ai_decide(provider_name VARCHAR, state JSON, questions JSON) -> JSON
```

- `state`：VARCHAR/CHAR 或 JSON 表达式。文本必须是合法 UTF-8，即使内容类似 JSON 或 URL，也按文本发送。JSON 状态只接受字符串、对象或数组。其他类型需要显式转换或使用 `parse_json`。
- `questions`：定义 1–16 个问题的常量 JSON 对象。不支持 JSON 列或 `parse_json(column)`，也不会将字符串隐式视为 JSON。
- `provider_name`：可选的常量 VARCHAR 表达式，必须非 NULL、非空白，按名称选择已注册的 Provider。省略时使用默认 `decision` Provider。远端模型始终来自 Provider 元数据，该参数不是远端模型名。

本函数不提供 options MAP。两种形式都要求 Provider 类型为 `decision`、协议为 `bailian_decision`，沿用已有的 [AI 函数权限](ai_functions.mdx#ai-function-privileges)、SQL 使用位置限制、异步 AIProject 和请求生命周期。创建或使用 decision Provider 前，请升级所有 FE 和 BE。

## 配置

通过 [CREATE AI PROVIDER](../../sql-statements/cluster-management/ai_provider/CREATE_AI_PROVIDER.md) 注册模型。管理员可设置该类型的默认项，不影响 chat、embedding 或 rerank 的默认项。为已有角色配置示例：

```sql
CREATE AI PROVIDER support_decision TYPE decision
PROPERTIES (
    "endpoint" = "https://YOUR_WORKSPACE_ID.cn-beijing.maas.aliyuncs.com/compatible-mode/v1/systemone",
    "model" = "decision-model-preview",
    "protocol" = "bailian_decision",
    "api_key" = "<api-key>"
);
SET support_decision AS DEFAULT AI PROVIDER;

GRANT USAGE ON AI FUNCTION ai_decide TO ROLE decision_reader;
GRANT USAGE ON AI PROVIDER support_decision TO ROLE decision_reader;
```

端点、模型、凭证及可选的 `timeout_ms` 来自 Provider 元数据，不需要 decision FE 配置或 BE 本地决策凭证，也不回退到 SYSTEM chat/embedding 设置。携带凭证的请求必须使用 HTTPS。Provider 密钥持久化到 FE 元数据，并通过现有内部 RPC 下发到执行 BE；SHOW/DESC 和诊断输出会脱敏。参见 [Provider 设置与快照](ai_custom_functions.md#query-snapshots-and-system-compatibility)。

一个计划固定首次选中的默认 Provider 或每个显式名称，并按 Provider UUID 保存不可变快照。默认与显式选择指向同一 UUID 时复用该快照。后续 ALTER、DROP 或默认项切换不会重定向已有计划；新计划重新解析当前元数据并检查权限。Provider 缺失、类型/协议不符或缺少函数/Provider USAGE 权限时直接失败，不回退 SYSTEM。读取业务表仍需相应表权限。业务状态和问题会发送到配置的服务；模型权限及地域可用性取决于服务账号，参见[模型文档](https://help.aliyun.com/zh/model-studio/decision-model-preview)。

## 问题与返回值

问题 ID 必须非空且唯一。定义只接受 `type`、可选字符串 `instructions`，以及下表中的 `criteria`。

| 类型 | criteria | 答案 |
| --- | --- | --- |
| `choice` | 必填对象，包含 1–255 个唯一且非空的选项名称，对应字符串说明。 | `choice` 标签、覆盖所有选项且归一化的 `probabilities`、范围 `[0,1]` 的 `confidence`。 |
| `noul` | 可选对象，仅接受 `true` 和/或 `false` 的字符串说明。 | `noul` 为范围 `[0,1]` 的肯定概率，并非 BOOLEAN；阈值由调用方选择。 |
| `score` | 必填数组，包含按等级升序排列的 2–255 个字符串说明。 | 范围 `[0,N-1]` 的零基等级期望值，可以是小数；同时保留 `legend`、归一化 `probabilities` 和 `confidence`。 |

响应必须恰好覆盖请求的问题 ID，答案类型和取值范围必须合法。校验后保留完整响应及可选服务元数据。函数调用前的 SQL JSON 转换可能已规范化重复键；原始服务响应中的重复键会被拒绝。

```sql
SELECT ai_decide(
    '客户反映重复扣款，并要求退还多收的费用。',
    parse_json('{
      "category":{"type":"choice","criteria":{"refund":"退款或重复扣款","other":"其他请求"}},
      "refund":{"type":"noul","instructions":"客户是否明确要求退款？"},
      "urgency":{"type":"score","criteria":["常规","尽快","立即"]}
    }')
);
```

可使用普通 JSON 表达式提取字段，例如 `result -> '$.answers.refund.noul'`。每个参与计算的非 NULL 行提交一个包含全部问题的逻辑请求，重试可能增加 HTTP 次数。函数具有非确定性。

## NULL、错误与用量

state 或 questions 为 SQL NULL 时，返回 SQL NULL 且不发送请求。显式 Provider 名称为 NULL 或空白会在分析阶段失败，不会选择默认项。对于实际初始化的 AIProject，非法常量问题定义在 fragment 初始化阶段报错，即使后续没有输入行或 state 为 SQL NULL。被优化器移除的调用（例如 `WHERE 1=0` 或 `LIMIT 0`）不会初始化并校验问题定义；函数及 Provider 授权仍遵循既有的优化前检查。原生 JSON null 与 SQL NULL 不同，不能用作状态或问题定义。

行输入错误、HTTP 失败和非法响应遵循 [ai_function_on_error](../../../administration/configuration/BE_parameters/query_loading.md#ai_function_on_error)：`ignore` 对失败行返回 SQL NULL，`fail` 中止查询。非法配置和查询生命周期失败保留已有 AI 执行错误。错误不会转成否定答案或零分。

合法上报的 `usage.input_tokens` 计入 prompt 和 total token，completion token 为零。答案校验失败时仍保留可信用量。未上报的用量保持未知；非法、重复、负数或溢出的用量不计入统计。本功能不引入 token 预估，复用已有请求、重试、失败和实际用量的 Profile 计数器。

[ai_function_rate_limit_qps_decision](../../../administration/configuration/BE_parameters/query_loading.md#ai_function_rate_limit_qps_decision) 默认按每个 BE、端点、凭证及决策能力限制为每秒 20 次 HTTP 尝试，包含重试。参数必须为正数，支持动态更新，同时受全局 AI 并发上限约束。这不是集群级服务配额。
