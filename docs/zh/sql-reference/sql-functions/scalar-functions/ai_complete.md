---
displayed_sidebar: docs
description: "调用已配置的 SYSTEM OpenAI 兼容聊天端点，并返回生成的文本。"
---

# ai_complete

调用管理员配置的 SYSTEM 聊天模型，并返回模型生成的文本。StarRocks 从 BE 发出非流式的 OpenAI 兼容聊天补全请求。

:::warning
此函数会将模型名称、提示词和选项发送到已配置的端点。请仅使用可信的 HTTPS 端点；除非提供商已获准接收，否则不要发送密钥或敏感数据。请求可能离开 StarRocks 集群、产生提供商费用，并按提供商的数据处理政策保留。
:::

## 语法

```sql
ai_complete(<prompt>)
ai_complete(<prompt>, <options>)
ai_complete(<model>, <prompt>)
ai_complete(<model>, <prompt>, <options>)
```

## 参数

- `prompt`：包含用户提示词的 VARCHAR 表达式。允许使用空字符串。
- `model`：为本次调用选择模型的 VARCHAR 表达式。省略时，StarRocks 使用
  [`ai_default_chat_model`](../../../administration/configuration/FE_parameters/user_query_loading.md#ai_default_chat_model)。显式模型可以逐行变化，但常量模型不能是空字符串或仅包含空白字符。
- `options`：可选的常量 MAP，用于向提供商请求添加其他字段。带类型的 NULL MAP 会被视为空 MAP。

### Options MAP 规则

- MAP 必须为常量。在每个顶层或嵌套 MAP 中，键都必须是唯一、非 NULL、非空的 VARCHAR 值。
- 选项值必须兼容 JSON：NULL、BOOLEAN、有限数值、字符串、JSON、ARRAY、MAP 或 STRUCT。嵌套 MAP 值的键也必须是 VARCHAR。
- 区分大小写的顶层键 `model`、`messages` 和 `stream` 为保留键，不能指定。StarRocks 会构造这些字段，并始终发送非流式请求。
- 双参数形式中的裸 NULL 会解析为 `ai_complete(<model>, NULL)`。如需将 NULL 作为 `options` 传入，请将其转换为 MAP 类型，例如
  `CAST(NULL AS MAP<VARCHAR, JSON>)`。

## 返回值

返回可为 NULL 的 VARCHAR，其中包含 OpenAI 兼容响应成功时的 `choices[0].message.content`。

- 如果 `prompt` 为 NULL，函数返回 NULL，且不会提交提供商请求。
- 对于显式指定 `model` 的重载，如果 `model` 为 NULL，函数也会返回 NULL，且不会提交请求。
- BE 配置项
  [`ai_function_on_error`](../../../administration/configuration/BE_parameters/query_loading.md#ai_function_on_error)
  控制行级错误。其默认值 `ignore` 会为失败行返回 NULL 并继续查询；`fail` 则会中止查询。
- `ignore` 不会忽略分析错误或配置错误，也不会将查询取消、截止时间到期或 BE 关闭转换为 NULL。

此函数具有非确定性。即使参数相同，提供商状态、模型行为和运行时条件的变化也可能导致返回文本或失败方式不同。

## 配置

### FE SYSTEM 模型配置

管理员使用以下可动态修改的 FE 参数配置 SYSTEM 模型。每次调用都必须配置 endpoint 和 provider；仅传入 prompt 的重载才必须配置默认模型。

| 参数 | 默认值 | 要求 |
|------|--------|------|
| [`ai_default_chat_endpoint`](../../../administration/configuration/FE_parameters/user_query_loading.md#ai_default_chat_endpoint) | 空字符串 | 必填。聊天补全端点的完整 HTTPS POST URL。 |
| [`ai_default_chat_model`](../../../administration/configuration/FE_parameters/user_query_loading.md#ai_default_chat_model) | 空字符串 | 仅传入 prompt 的重载必须配置；如果每次调用都显式指定模型，则可不配置。 |
| [`ai_default_chat_provider`](../../../administration/configuration/FE_parameters/user_query_loading.md#ai_default_chat_provider) | 空字符串 | 必填。唯一有效值为 `openai_compatible`。 |

每个查询计划都会捕获这些值的快照。动态更新仅对更新后新分析和新规划的查询生效；已经构造的计划会保留其捕获的快照。

### BE 本地凭证

在所有可能执行 AI 查询的 BE 的本地进程环境中设置 `AI_FUNCTION_MODEL_API_KEY`。BE 在本地读取该值，并将其作为 Bearer 凭证发送。它不是 FE 配置项，也不会包含在查询计划中。同时，在每个 BE 上将 `AI_FUNCTION_MODEL_ENDPOINT` 设置为与 `ai_default_chat_endpoint` 完全相同的完整 HTTPS URL。若计划中的 endpoint 与此本地绑定不匹配，BE 会拒绝执行。BE 会校验全部 DNS 地址、阻止 link-local 地址，并为请求固定已校验的 DNS 快照。通过精确的本地绑定可以显式授权私网模型 endpoint。修改任一 BE 本地环境变量后都需要重启对应 BE。不要将凭证放入 SQL 文本或 options MAP。

### 运行时限制和重试

- [`ai_function_rate_limit_qps_chat`](../../../administration/configuration/BE_parameters/query_loading.md#ai_function_rate_limit_qps_chat)
  和
  [`ai_function_max_inflight`](../../../administration/configuration/BE_parameters/query_loading.md#ai_function_max_inflight)
  在每个 BE 上独立生效。QPS 限制按 endpoint、credential 和 capability 分桶维护，in-flight 限制为进程级。基于 WorkGroup 和 Query 的公平准入机制会在排队请求之间共享适用的限制。每次初始或重试 HTTP attempt 都必须同时获得这两类准入许可。
- StarRocks 无法保证模型提供商仅执行一次请求。已超时或以其他方式失败的 attempt 可能已经到达提供商，因此重试可能重复提供商侧的工作并产生额外费用。请根据提供商的行为和计费策略配置
  [`ai_function_max_retries`](../../../administration/configuration/BE_parameters/query_loading.md#ai_function_max_retries)
  和
  [`ai_function_max_retries_on_throttle`](../../../administration/configuration/BE_parameters/query_loading.md#ai_function_max_retries_on_throttle)。
- 请求和响应 payload 都会计入 Query 内存跟踪器；存在 outstanding 请求时，执行 Pipeline 会施加背压。
  [`ai_function_max_response_bytes`](../../../administration/configuration/BE_parameters/query_loading.md#ai_function_max_response_bytes)
  是每个 HTTP 响应体的硬上限。
- 独立 task timeout 在 task 创建后保持不变，重试不会重新计时；Query 取消和 deadline 更新会在整个异步执行期间持续生效。超时、工作线程和调度粒度控制参见其他
  [BE AI 函数配置项](../../../administration/configuration/BE_parameters/query_loading.md#ai_function_request_timeout_ms)。

## 限制和安全

- `ai_complete` 不能用于 `GROUP BY`、`SELECT DISTINCT`、聚合函数参数或窗口函数表达式。
- 不能用于 `IF`、`IFNULL`、`NULLIF`、`COALESCE` 或 `CASE` 条件表达式中，也不能作为表函数参数。
- 不能用于物化视图定义或生成列表达式。
- 不能用于 Lambda 表达式 body 或 SQL UDF body。
- 包含 `ai_complete` 的语句不能创建或绑定 SQL Plan Baseline；其他包含该函数的查询也不会应用 SPM 改写。
- 支持 `PREPARE`，但包含 `ai_complete` 的语句会在每次 `EXECUTE` 时完整重新规划，不会复用执行计划。
- 在相关子查询的查询块中，`SELECT` 列表、`WHERE`、`HAVING`、`ORDER BY` 和 `JOIN ON` 中的 AI 表达式
  均会被拒绝，即使 AI 表达式自身只引用本层列。联接条件中的 AI 表达式仅支持 `INNER JOIN` 和 `CROSS JOIN`。
- 每个非 NULL 输入行都可能产生一次远程请求。在对大量行使用此函数前，请考虑网络延迟、提供商配额、查询截止时间、费用和数据出站策略。

## 示例

以下示例使用 `EXPLAIN`，它只分析和规划语句，不会执行函数或发送 HTTP 请求。分析时仍须配置有效的 SYSTEM 配置。

```sql
EXPLAIN SELECT ai_complete('Summarize this local test prompt.');

EXPLAIN SELECT ai_complete(
    'Classify this local test prompt.',
    map{'temperature': 0.0}
);

EXPLAIN SELECT ai_complete(
    'local-test-model',
    'Summarize this local test prompt.'
);

EXPLAIN SELECT ai_complete(
    'local-test-model',
    'Return a JSON object for this local test prompt.',
    map{'response_format': map{'type': 'json_object'}}
);
```

NULL 提示词不会提交提供商请求：

```sql
SELECT ai_complete(CAST(NULL AS VARCHAR)) AS answer;
```

## 关键字

AI_COMPLETE, AI, LLM
