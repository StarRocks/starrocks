---
displayed_sidebar: docs
description: "使用 AI Provider 按查询相关性重排候选文档。"
sidebar_position: 23
toc_max_heading_level: 2
---

# ai_rerank

按查询文本对候选文档进行相关性重排。查询和候选数组可以逐行变化。函数使用 `rerank` 类型的 AI Provider，并复用现有异步 AI 执行框架。

## 语法

```sql
ai_rerank(query, documents[, provider[, model[, options]]])
```

一次调用中使用位置参数或 `参数名 => 值` 形式的命名参数，不要混用。

## 参数

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| `query` | VARCHAR | 查询文本，可以是常量或列表达式。 |
| `documents` | `ARRAY<VARCHAR>` | 保持原始顺序的候选文本，可以是常量或列表达式。 |
| `provider` | 常量 VARCHAR | 可选 Provider 名称；省略、空字符串或 SQL NULL 使用默认 `rerank` Provider。 |
| `model` | 常量 VARCHAR | 可选模型；省略、空字符串或 SQL NULL 使用 Provider 的默认模型。 |
| `options` | 常量 JSON | 可选 JSON 对象；省略、`{}` 或 SQL NULL 表示没有额外选项。 |

`options` 中的 `top_n` 必须是精确的正整数，且不超过 2147483647，默认是候选数量。请求发送全部候选，返回 `min(top_n, candidate_count)` 条结果；`top_n` 不会减少输入量。其余选项透传给所配置的服务。选项不能覆盖 `model`、`query`、`documents`、`input` 或 `parameters`。

## Provider

支持 `cohere`（`rerank` 类型的默认协议）及 `dashscope_rerank`（DashScope 原生文本重排）协议。需要配置完整服务端点，StarRocks 不追加路径。原生 DashScope 使用 `https://dashscope.aliyuncs.com/api/v1/services/rerank/text-rerank` 及该服务支持的模型。

候选数量超过 Provider 的 `max_documents` 时，该行失败，不会静默截断。`deadline_ms` 限制单行请求及其重试的总时间，同时受查询及 AI 运行时截止时间约束；`timeout_ms` 限制单次尝试。函数沿用现有 AI 重试和错误处理策略，不会降级为原始候选顺序。

调用需要 AI 函数和 Provider 两项使用权限。配置方式参见 [CREATE AI PROVIDER](../../sql-statements/cluster-management/ai_provider/CREATE_AI_PROVIDER.md)。使用前需将 FE 和 BE 均升级到支持本函数的版本。

省略 `max_documents` 时不增加 Provider 专属候选数限制；省略 `deadline_ms` 时，现有 AI 运行时和查询预算仍然生效。

## 返回值

返回可空的 `ARRAY<STRUCT<index INT, relevance_score DOUBLE>>`，按相关性分数非递增排列。`index` 是文档在原始 `documents` 中**从 0 开始**的位置，而 SQL 数组下标**从 1 开始**。允许相同分数，并保留服务返回顺序。分数由服务定义，不代表概率。

- `query` 或 `documents` 为 SQL NULL 时返回 SQL NULL，不发送 HTTP 请求。
- 查询非 NULL 且候选数组为空时返回 `[]`，不发送 HTTP 请求。
- 保留空字符串和重复文档；文档元素为 NULL 属于行级错误。
- 结果数量错误、索引重复或越界、分数非有限值或分数递增均属于行级错误；不会修补或重排响应。
- `ai_function_on_error = ignore` 将失败行置为 NULL；`fail` 终止查询。配置或选项无效、取消及本地资源错误始终终止查询。

## 示例

```sql
CREATE AI PROVIDER reranking TYPE rerank
PROPERTIES (
    "protocol" = "cohere",
    "endpoint" = "https://api.cohere.com/v2/rerank",
    "model" = "YOUR_RERANK_MODEL",
    "api_key" = "YOUR_API_KEY",
    "max_documents" = "1000",
    "deadline_ms" = "10000"
);
SET reranking AS DEFAULT AI PROVIDER;

SELECT ai_rerank(
    query => 'database for real-time analytics',
    documents => ['StarRocks is an analytical database.', 'A recipe for bread.'],
    provider => 'reranking',
    options => json_object('top_n', 1)
);
```

访问结构体字段时需要引用 `index`。先计算 AI 函数，再将其结果传给 `UNNEST`：

```sql
WITH ranked AS (
    SELECT ai_rerank('database', ['StarRocks', 'bread']) AS hits
)
SELECT hits[1].`index` AS document_index, hits[1].relevance_score AS score
FROM ranked;

WITH ranked AS (
    SELECT ai_rerank('database', ['StarRocks', 'bread']) AS hits
)
SELECT hit.`index`, hit.relevance_score
FROM ranked, UNNEST(hits) AS t(hit);
```

## 限流和用量

`ai_function_rate_limit_qps_rerank` 默认每个 BE 限流桶每秒 4 次请求，桶按端点、凭证和能力划分。计入重试，支持动态修改，并共享现有进程级并发上限。参见 [BE AI 配置](../../../administration/configuration/BE_parameters/query_loading.md#ai_function_rate_limit_qps_rerank)。

只记录服务明确返回的 token 数量，缺失字段保持未知。不会将 Cohere search units 换算为 token，也不引入 token 预估或合成总量。
