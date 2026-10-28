---
displayed_sidebar: docs
description: "通过 SYSTEM OpenAI 兼容端点生成可为 NULL 的 FLOAT 数组文本向量。"
sidebar_position: 22
---

# ai_embed

通过已配置的 SYSTEM embeddings 端点生成文本或图片向量。返回可为 NULL 的 `ARRAY<FLOAT>`，不是 Snowflake VECTOR 类型。使用此函数前，请先升级所有 FE 和 BE 节点。

:::warning
文本或 FILE 内容、模型和选项会发送给已配置的提供商。请求可能离开集群、产生费用，并被提供商保留。请仅使用已批准的端点和数据。
:::

## 语法

```sql
ai_embed(text)
ai_embed(text, options)
ai_embed(model, text)
ai_embed(model, text, options)
ai_embed(file)
ai_embed(file, options)
ai_embed(model, file)
ai_embed(model, file, options)
```

- `text`：VARCHAR 表达式。URL 字符串按文本生成向量，StarRocks 不读取 URL 内容。
- `file`：包含 inline JPEG、PNG 或 WebP 图片的 FILE 类型表达式，要求使用 `dashscope_multimodal` 协议及支持图像向量的模型。`openai_compatible` 向量协议仅支持文本。
- `model`：可选的 VARCHAR 表达式，可逐行变化。常量显式模型不能为空白。省略时使用 SYSTEM 默认向量模型。
- `options`：可选的常量 MAP，用于添加提供商请求字段。带类型的 NULL MAP 视为空 MAP。

适用 [ai_complete 的递归 options MAP 规则](ai_complete.md#options-map-规则)，但区分大小写的顶层保留键为 `model`、`input` 和 `encoding_format`。StarRocks 按配置的协议构造请求，并要求返回数值向量。值必须兼容 JSON；不支持 DATE、BITMAP 等值类型。

双参数形式中的裸 NULL 解析为 `ai_embed(model, NULL)`。如需传入 NULL options，请使用 `CAST(NULL AS MAP<VARCHAR, JSON>)`。

### FILE 输入契约

FILE 重载为每个非 NULL 输入行生成一个向量，复用文本调用的函数名和返回类型，不将额外 prompt 与图片融合，也不新增多模态专用 SQL 函数。
适用 [inline FILE 校验和字节限制](ai_complete.md#file-输入)。本版本的 FILE 向量不支持 URI 引用、视频、音频或文档。
NULL FILE 不发送请求，直接返回 NULL。不支持的媒体与协议组合（包括视频向量化）作为行级失败，遵循 `ai_function_on_error`。
FILE 与 URL 或 VARCHAR 输入之间不会隐式转换，也不会将媒体回退为文本。
在授权 FILE reader 可用前，引用型 FILE 即使在 `on_error=ignore` 时也会报 `NotSupported`。

`dashscope_multimodal` 将单个文本或图片放入 `input.contents`，options 写入 `parameters`，而非请求顶层。
除原有保留键外，不允许传入 `contents`、`parameters`、`messages` 和 `stream`。所选模型必须支持相应模态和选项。

## 配置

向量配置独立于聊天配置，不会回退到 SYSTEM 聊天端点、模型或凭证。

| 可动态修改的 FE 参数 | 默认值 | 要求 |
|---------------------|--------|------|
| [ai_default_embedding_endpoint](../../../administration/configuration/FE_parameters/user_query_loading.md#ai_default_embedding_endpoint) | 空字符串 | embeddings 端点的完整 HTTPS POST URL。 |
| [ai_default_embedding_model](../../../administration/configuration/FE_parameters/user_query_loading.md#ai_default_embedding_model) | 空字符串 | 仅在函数未显式指定模型时必填。 |
| [ai_default_embedding_provider](../../../administration/configuration/FE_parameters/user_query_loading.md#ai_default_embedding_provider) | 空字符串 | 文本可用 `openai_compatible`；文本或图片 FILE 可用 `dashscope_multimodal`。 |

每个计划捕获所用配置的快照。FE 修改无需重启 FE，仅影响修改后新分析和新规划的查询；已有计划保留原快照。

在所有执行向量查询的 BE 本地进程环境中配置：

- `AI_FUNCTION_EMBEDDING_ENDPOINT`：必须与 FE 端点的完整 URL 完全相同。
- `AI_FUNCTION_EMBEDDING_API_KEY`：本地 Bearer 凭证，不得放入 FE 配置、SQL 或查询计划。

修改任一环境变量后需重启对应 BE。凭证缺失或端点绑定不匹配会导致执行失败，不会改用聊天凭证。

[ai_function_rate_limit_qps_embedding](../../../administration/configuration/BE_parameters/query_loading.md#ai_function_rate_limit_qps_embedding) 默认在每个 BE 中为按端点、凭证和向量 capability 分桶的每个桶提供每秒 128 次 HTTP attempt 的准入速率。重试同样消耗准入许可。进程级 in-flight 限制、超时、重试、响应大小限制及[其他运行时控制](ai_complete.md#运行时限制和重试)沿用 AI 执行框架。

## 返回值和限制

响应必须只包含一个 index 为 0 的向量，成功时将其作为非空、所有元素均为有限 FLOAT 的数组返回。多个向量、空数组、无效或超出范围的数值均会被拒绝。长度由模型和提供商支持的选项决定，StarRocks 不固定向量维度。

对于 `dashscope_multimodal`，当顶层 `input_tokens` 和单独返回的 `image_tokens` 均有效时，prompt 用量包含两者之和。嵌套的 `input_tokens_details` 已包含在输入计数中，不会重复相加；缺失的 total 用量不会推算。

文本、FILE 或显式模型为 NULL 时，不发送请求并返回 NULL。行级失败遵循 [ai_function_on_error](../../../administration/configuration/BE_parameters/query_loading.md#ai_function_on_error)。分析或配置错误、引用型 FILE 拒绝、取消和截止时间到期不会被忽略。[SQL 使用位置限制和预处理语句重新规划规则](ai_complete.md#限制和安全)同样适用。

## 示例

在 SYSTEM `openai_compatible` 向量配置有效时，以下示例只规划查询，不发送 HTTP 请求：

```sql
EXPLAIN SELECT ai_embed('A local test sentence.');
EXPLAIN SELECT ai_embed('A local test sentence.', map{'dimensions': 256});
EXPLAIN SELECT ai_embed('approved-embedding-model', 'A local test sentence.');
EXPLAIN SELECT ai_embed(
    'approved-embedding-model', 'A local test sentence.', map{'dimensions': 256}
);
```

提供商和模型必须支持传入的选项，包括 `dimensions`。

对于已有表 `media_inputs` 中包含 inline 图片的 FILE 类型列 `image_file`，请配置 `dashscope_multimodal` 和支持图片向量的默认模型：

```sql
EXPLAIN SELECT ai_embed(image_file) FROM media_inputs;
```

如需选择已注册的 AI provider 而非 SYSTEM 配置，请使用 [ai_custom_embedding](ai_custom_functions.md)。设置默认 AI provider 不改变 `ai_embed` 的路由；显式 `model` 参数仍选择远端模型名。
