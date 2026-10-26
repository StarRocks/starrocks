---
displayed_sidebar: docs
description: "使用 CHAT AI Provider，按指令聚合分组文本。"
sidebar_position: 24
---

# ai_agg

`ai_agg` 根据指令汇总每个 SQL 分组中的文本，每组返回一个可为 NULL 的 VARCHAR 结果，可以与 `count`、`sum` 等普通聚合同时使用。

`ai_agg` 使用 `chat` AI Provider，协议必须为 `openai`。端点、默认模型、API Key 和可选的 `timeout_ms` 均来自该 Provider。使用前需升级全部 FE 和 BE。

:::warning
输入文本、中间摘要和指令会发送到配置的模型服务。每组可能产生多次请求和重试，并产生相应费用。请遵循与其他 AI 函数相同的数据处理及服务端点审批要求。
:::

## 语法

```sql
ai_agg(input, instruction[, provider[, model[, options]]])
```

- `input`：VARCHAR 表达式。跳过 NULL，保留空字符串和重复值。不支持 FILE 输入。
- `instruction`：非 NULL、非空白的常量 VARCHAR 表达式，用于描述目标结果。
- `provider` 和 `model`：可选常量 VARCHAR 表达式。省略、空字符串或 SQL NULL 的 `provider` 使用默认 CHAT Provider；`model` 的相同取值使用 Provider 默认模型，显式模型则覆盖默认值。纯空白字符串无效。显式指定 Provider 时不要求配置集群默认项。
- `options`：遵循[JSON 选项规则](ai_text_functions.md#json-options)的可选常量 JSON 对象。省略或 SQL NULL 表示没有额外选项。`response_format` 只用于最终请求，包括仅需一次请求的分组。其他选项用于每次请求。`max_tokens` 限制单次请求，不限制整组 Token 总量。

支持位置参数和命名参数，遵循[通用调用规则](ai_text_functions.md#公共调用规则)。

不支持 `DISTINCT`、聚合内 `ORDER BY`、窗口执行和聚合状态组合器。需要精确过滤、去重、计数或数值计算时，应在 AI 聚合前使用 SQL 完成。输入顺序不保证，模型输出不确定，分层摘要也可能丢失细节，因此不能用于保证精确算术或无遗漏提取。

## 返回值和错误

空输入的全局聚合或全部为 NULL 的分组返回 NULL，不发送模型请求。带分组的查询在没有输入行时不返回分组，与普通 SQL 聚合一致。空字符串仍参与聚合。

模型调用及归约预算失败遵循既有 [ai_function_on_error](../../../administration/configuration/BE_parameters/query_loading.md#ai_function_on_error)：`ignore` 使**整个分组**返回 NULL，`fail` 终止查询。失败分组不会返回仅由部分成功请求组成的摘要。配置错误、无效聚合状态、原生分组大小超限、本地资源错误（包括调度队列耗尽和内存错误）、取消和查询超时始终终止查询。脱敏后的本地资源错误无法还原原始原因，因此返回通用资源错误；直接检测到的分配失败仍返回内存限制错误。

首版采用以下固定安全边界：

| 边界 | 限制 |
| --- | --- |
| 每组编码后的输入 | 1 MiB，包含 24 字节头和每条非 NULL 记录的 8 字节长度字段 |
| 单次请求的 Prompt | 64 KiB，包含指令、记录转义和 Prompt 包装 |
| 中间请求的选项 | 键与序列化 JSON 值合计 64 KiB，不含 `response_format`；仅执行一次最终请求时不受此限制 |
| 单次请求的中间摘要 | 4 KiB |
| 每组保留的中间摘要 | 当前轮及下一轮合计 1 MiB |
| 每组轮数 | 8 |
| 每组逻辑请求数 | 1024，不含重试次数 |

单条记录无法装入 Prompt 时直接失败，不截断。摘要过大或归约未充分缩小时，该分组失败。这些字节边界不是 Token 预估，也不保证所有模型的上下文窗口都能接受请求。既有响应大小、速率、并发、重试、内存和查询超时限制仍然生效。请求超时针对单个逻辑请求，查询截止时间约束整个聚合过程。

## 权限与示例

除表权限外，执行同时需要 `USAGE ON AI FUNCTION ai_agg`（或 `USAGE ON ALL AI FUNCTIONS`）及所选 CHAT Provider 的 `USAGE ON AI PROVIDER`。参见 [AI 函数权限](ai_functions.mdx#ai-function-privileges)。Provider 配置和授权遵循通用的优化前检查，空输入查询也不例外。

管理员注册 [CHAT Provider](../../sql-statements/cluster-management/ai_provider/CREATE_AI_PROVIDER.md)、设置默认项，并向已有角色授权：

```sql
CREATE AI PROVIDER review_chat TYPE chat
PROPERTIES (
    "endpoint" = "https://api.example.com/v1/chat/completions",
    "model" = "chat-model",
    "api_key" = "<api-key>"
);
SET review_chat AS DEFAULT AI PROVIDER;
GRANT USAGE ON AI FUNCTION ai_agg TO ROLE review_reader;
GRANT USAGE ON AI PROVIDER review_chat TO ROLE review_reader;
```

执行计划保存所选 Provider 的配置快照，每组及每轮归约使用该配置。元数据或默认项变更影响重新规划的查询。凭证存储和配置参见 [CREATE AI PROVIDER](../../sql-statements/cluster-management/ai_provider/CREATE_AI_PROVIDER.md)。

假设存在 `reviews(category, review_text)` 表：

```sql
SELECT category, count(*) AS review_count,
       ai_agg(review_text, 'Summarize the main positive and negative themes.') AS summary
FROM reviews
GROUP BY category;
```

为每次请求指定选项：

```sql
SELECT category,
       ai_agg(input => review_text,
              instruction => 'Summarize the recurring issues.',
              provider => 'review_chat',
              options => json_object('temperature', 0.0)) AS summary
FROM reviews
WHERE review_text IS NOT NULL
GROUP BY category;
```

`EXPLAIN` 只校验并展示计划，不调用模型。Query Profile 提供分组数、空组数、叶子请求、轮数和本地归约预算失败计数。既有 AI 请求、重试及实际 Token 用量统计包含所有已提交轮次，也保留分组失败前已发生的消耗。
