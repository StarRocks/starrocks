---
displayed_sidebar: docs
description: "通过 AI Provider 调用 OpenAI 聊天模型。"
---

# ai_complete

通过 AI Provider 调用 OpenAI 聊天补全端点并返回生成的文本。提示词、模型、options 及 FILE 内容会发送到所选端点。

调用同时需要 `USAGE ON AI FUNCTION <function_name>` 和所选 Provider 的 `USAGE ON AI PROVIDER <provider_name>`，包括默认 Provider。参见 [AI 函数权限](ai_functions.mdx#ai-function-privileges)。

## 语法

```sql
ai_complete(prompt)
ai_complete(prompt, options)
ai_complete(prompt, file)
ai_complete(prompt, file, options)
ai_complete(model, prompt)
ai_complete(model, prompt, options)
ai_complete(model, prompt, file)
ai_complete(model, prompt, file, options)
ai_complete(provider, model, prompt)
ai_complete(provider, model, prompt, options)
ai_complete(provider, model, prompt, file)
ai_complete(provider, model, prompt, file, options)
```

## 参数

省略 `provider` 时，使用对应类型的集群默认 Provider：补全和文本辅助函数使用 `chat`，向量函数使用 `embedding`。省略 `model` 时，使用 Provider 必填的默认模型。显式模型优先，并可逐行变化。`provider` 必须是非空白常量字符串，常量显式模型也不能为空白。显式指定 Provider 的调用不要求设置集群默认值。

文本、显式 model 或 FILE 输入为 NULL 时返回 NULL，不发送模型请求；常量和运行时列值遵循相同规则。NULL options MAP 表示不传选项，NULL 翻译源语言表示自动识别。Provider 名和辅助函数数组仍需满足各自的参数校验。options 内部嵌套 NULL 保持原有 JSON 兼容语义。若裸 NULL 在多个重载中匹配不同参数类型，须用 `CAST` 指定类型，例如 `CAST(NULL AS VARCHAR)` 或 `CAST(NULL AS MAP<VARCHAR, JSON>)`。

### Options MAP 规则

`options` 是可选的常量 MAP；不传额外选项时可省略或使用 `map{}`。所有层级的 MAP 键必须是唯一、非 NULL、非空的 VARCHAR。值支持 NULL、BOOLEAN、有限数值、字符串、JSON、ARRAY、MAP 和 STRUCT，不支持的类型会被拒绝。区分大小写的顶层键 `model`、`endpoint`、`key`、`api_key`、`protocol`、`provider`、`messages` 和 `stream` 是保留键。连接配置和凭证来自 Provider。

### FILE 输入

FILE 重载接受带有内联 JPEG、PNG 或 WebP 图像的 FILE，供 OpenAI 聊天使用；模型必须支持图像输入。不支持视频、音频和文档。URL 或 VARCHAR 不会隐式转换成 FILE，也不会读取 URI。内容只能放在 `inline` 中，`uri`、`offset` 和 `size` 必须为 NULL。MIME 类型从字节检测，提供的 `content_type` 必须与检测结果一致。

`ai_function_max_input_file_bytes` 在复制和 Base64 编码前限制原始字节数，默认 10485760 字节（10 MiB）。无效或超限的内联内容遵循 `ai_function_on_error`。引用型 FILE 始终报 `NotSupported`，包括 `on_error=ignore`。FILE 列的运行时 NULL 返回 NULL，不发送请求。

## 返回值

返回包含 `choices[0].message.content` 的可空 VARCHAR。结果具有非确定性。`ai_function_on_error` 控制行级失败：默认 `ignore` 返回 NULL，`fail` 中止查询。分析或配置错误、取消和截止时间到期不被忽略。

## 配置

创建 `protocol = "openai"` 的 Provider。无论是否配置 API key，端点均支持 HTTP 和 HTTPS。非空 key 生成 `Authorization: Bearer ...` 请求头。URL 格式、控制字符、DNS 和地址校验仍然生效。请使用可信端点和允许向外传输的数据。

使用 `SET <provider_name> AS DEFAULT AI PROVIDER` 设置默认值。若对应默认 Provider 未设置，隐式调用在 FE 报错，`EXPLAIN` 也会报错。每个执行计划固定所选 Provider 的配置快照；ALTER 和默认值切换影响重新规划的查询，预处理语句每次 `EXECUTE` 都根据当前元数据重新规划。

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

```sql
CREATE AI PROVIDER support_chat TYPE chat
PROPERTIES ("endpoint" = "http://localhost:8000/v1/chat/completions",
            "model" = "chat-model", "protocol" = "openai");
SET support_chat AS DEFAULT AI PROVIDER;

EXPLAIN SELECT ai_complete('Summarize this text.');
EXPLAIN SELECT ai_complete('another-model', 'Summarize this text.');
EXPLAIN SELECT ai_complete('support_chat', 'another-model', 'Summarize this text.', map{});
EXPLAIN SELECT ai_complete('support_chat', 'vision-model', 'Describe this image.', image_file)
FROM media_inputs;
EXPLAIN SELECT ai_complete('support_chat', 'another-model', CAST(NULL AS VARCHAR));
EXPLAIN SELECT ai_complete('another-model', 'text', CAST(NULL AS MAP<VARCHAR, JSON>));
```
