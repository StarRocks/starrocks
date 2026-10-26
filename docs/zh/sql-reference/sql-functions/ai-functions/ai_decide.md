---
displayed_sidebar: docs
description: "通过 SYSTEM 决策模型评估业务状态，并返回经过校验的 JSON 答案。"
sidebar_position: 70
---

# ai_decide

调用管理员配置的阿里云百炼决策模型，返回完整且经过校验的 JSON 响应。问题可以要求分类（`choice`）、肯定概率（`noul`）或有序评分（`score`）。请求使用 [System One 决策 API](https://help.aliyun.com/zh/model-studio/decision-model-api)。

## 语法

```sql
ai_decide(state VARCHAR, questions JSON) -> JSON
ai_decide(state JSON, questions JSON) -> JSON
ai_decide(model VARCHAR, state VARCHAR, questions JSON) -> JSON
ai_decide(model VARCHAR, state JSON, questions JSON) -> JSON
```

- `state`：VARCHAR/CHAR 或 JSON 表达式。文本必须是合法 UTF-8，即使内容类似 JSON 或 URL，也按文本发送。JSON 状态只接受字符串、对象或数组。其他类型需要显式转换或使用 `parse_json`。
- `questions`：定义 1–16 个问题的常量 JSON 对象。不支持 JSON 列或 `parse_json(column)`，也不会将字符串隐式视为 JSON。
- `model`：可选的常量 VARCHAR 表达式。省略时使用 `ai_default_decision_model`，只替换模型，不改变端点、协议或凭证。显式 SQL NULL 不回退到默认模型。

本函数不提供 options MAP 或 AI PROVIDER 重载，沿用已有的 [AI 函数权限](ai_functions.mdx#ai-function-privileges)、SQL 使用位置限制、异步 AIProject 和请求生命周期。使用新重载前，请升级所有 FE 和 BE。

## 配置

在每个 FE 上设置独立的决策配置：

```properties
ai_default_decision_endpoint = https://YOUR_WORKSPACE_ID.cn-beijing.maas.aliyuncs.com/compatible-mode/v1/systemone
ai_default_decision_provider = bailian_decision
ai_default_decision_model = decision-model-preview
```

三个参数默认均为空。端点与 provider 必填；所有调用均显式指定模型时，默认模型可以为空。参见 [FE 决策配置](../../../administration/configuration/FE_parameters/user_query_loading.md#ai_default_decision_endpoint)。配置变更作用于新生成的计划，已有计划保留配置快照。

每个执行请求的 BE 必须将环境变量 `AI_FUNCTION_DECISION_ENDPOINT` 设置为与 FE 端点完全相同的 URL，并通过 `AI_FUNCTION_DECISION_API_KEY` 提供对应凭证。修改环境变量后需要重启该 BE。FE 计划不携带 SYSTEM API key，决策端点与凭证也不回退到 chat 或 embedding 设置。端点必须是管理员认可的 HTTPS URL；业务状态和问题定义会发送到该服务。

为已有角色授予权限：

```sql
GRANT USAGE ON AI FUNCTION ai_decide TO ROLE decision_reader;
```

读取业务表仍需相应表权限。模型权限及地域可用性取决于服务账号，参见[模型文档](https://help.aliyun.com/zh/model-studio/decision-model-preview)。

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

state、questions 或显式 model 为 SQL NULL 时，返回 SQL NULL 且不发送请求。对于实际初始化的 AIProject，非法常量问题定义在 fragment 初始化阶段报错，即使后续没有输入行或 state/model 为 SQL NULL。被优化器移除的调用（例如 `WHERE 1=0` 或 `LIMIT 0`）不会初始化或校验。原生 JSON null 与 SQL NULL 不同，不能用作状态或问题定义。

行输入错误、HTTP 失败和非法响应遵循 [ai_function_on_error](../../../administration/configuration/BE_parameters/query_loading.md#ai_function_on_error)：`ignore` 对失败行返回 SQL NULL，`fail` 中止查询。非法配置和查询生命周期失败保留已有 AI 执行错误。错误不会转成否定答案或零分。

合法上报的 `usage.input_tokens` 计入 prompt 和 total token，completion token 为零。答案校验失败时仍保留可信用量。未上报的用量保持未知；非法、重复、负数或溢出的用量不计入统计。本功能不引入 token 预估，复用已有请求、重试、失败和实际用量的 Profile 计数器。

[ai_function_rate_limit_qps_decision](../../../administration/configuration/BE_parameters/query_loading.md#ai_function_rate_limit_qps_decision) 默认按每个 BE、端点、凭证及决策能力限制为每秒 20 次 HTTP 尝试，包含重试。参数必须为正数，支持动态更新，同时受全局 AI 并发上限约束。这不是集群级服务配额。
