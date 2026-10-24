---
displayed_sidebar: docs
description: "通过使用 `openai` 协议的 `embedding` Provider 生成文本向量，返回可空的 `ARRAY<FLOAT>`。"
sidebar_position: 22
---

# ai_embed

通过使用 `openai` 协议的 `embedding` Provider 生成文本向量，返回可空的 `ARRAY<FLOAT>`。

调用同时需要 `USAGE ON AI FUNCTION <function_name>` 和所选 Provider 的 `USAGE ON AI PROVIDER <provider_name>`，包括默认 Provider。参见 [AI 函数权限](ai_functions.mdx#ai-function-privileges)。

## 语法

```sql
ai_embed(input)
ai_embed(input, options)
ai_embed(file)
ai_embed(file, options)
ai_embed(model, input)
ai_embed(model, input, options)
ai_embed(model, file)
ai_embed(model, file, options)
ai_embed(provider, model, input)
ai_embed(provider, model, input, options)
ai_embed(provider, model, file)
ai_embed(provider, model, file, options)
```

## 参数

省略 `provider` 时，使用对应类型的集群默认 Provider：补全和文本辅助函数使用 `chat`，向量函数使用 `embedding`。省略 `model` 时，使用 Provider 必填的默认模型。显式模型优先，并可逐行变化。`provider` 必须是非空白常量字符串，常量显式模型也不能为空白。显式指定 Provider 的调用不要求设置集群默认值。

文本、显式 model 或 FILE 输入为 NULL 时返回 NULL，不发送模型请求；常量和运行时列值遵循相同规则。NULL options MAP 表示不传选项，NULL 翻译源语言表示自动识别。Provider 名和辅助函数数组仍需满足各自的参数校验。options 内部嵌套 NULL 保持原有 JSON 兼容语义。若裸 NULL 在多个重载中匹配不同参数类型，须用 `CAST` 指定类型，例如 `CAST(NULL AS VARCHAR)` 或 `CAST(NULL AS MAP<VARCHAR, JSON>)`。

`input` 是 VARCHAR 表达式，URL 字符串按普通文本处理，不读取内容。FILE 重载保留类型和内容校验，但当前开放的 OpenAI embedding 协议仅接受文本：FILE 无法生成向量，不支持的输入遵循 `ai_function_on_error`；引用型 FILE 报 `NotSupported`。

`dimensions` 默认继承 Provider 配置，SQL options 可以覆盖；模型和服务必须支持所请求的维度。[ai_complete 的递归 MAP 规则](ai_complete.md#options-map-规则)同样适用，但保留键中的 `messages`、`stream` 替换为 `input`、`encoding_format`，连接、模型和协议相关键仍为保留键。

响应必须仅包含索引为 0 的一个向量，且为非空、所有元素均为有限 FLOAT 的数组。无效响应遵循 `ai_function_on_error`。[运行时控制](ai_complete.md#运行时限制和重试)和 [SQL 限制](ai_complete.md#限制和安全)同样适用，向量请求使用 `ai_function_rate_limit_qps_embedding`。

## 配置

创建 `protocol = "openai"` 的 Provider。无论是否配置 API key，端点均支持 HTTP 和 HTTPS。非空 key 生成 `Authorization: Bearer ...` 请求头。URL 格式、控制字符、DNS 和地址校验仍然生效。请使用可信端点和允许向外传输的数据。

使用 `SET <provider_name> AS DEFAULT AI PROVIDER` 设置默认值。若对应默认 Provider 未设置，隐式调用在 FE 报错，`EXPLAIN` 也会报错。每个执行计划固定所选 Provider 的配置快照；ALTER 和默认值切换影响重新规划的查询，预处理语句每次 `EXECUTE` 都根据当前元数据重新规划。

## 示例

```sql
CREATE AI PROVIDER text_embedding TYPE embedding
PROPERTIES ("endpoint" = "http://localhost:8000/v1/embeddings",
            "model" = "embedding-model", "protocol" = "openai", "dimensions" = "256");
SET text_embedding AS DEFAULT AI PROVIDER;

EXPLAIN SELECT ai_embed('A local test sentence.');
EXPLAIN SELECT ai_embed('another-model', 'A local test sentence.');
EXPLAIN SELECT ai_embed('text_embedding', 'another-model', 'A local test sentence.',
                       map{'dimensions': 128});
EXPLAIN SELECT ai_embed('text_embedding', 'another-model', CAST(NULL AS VARCHAR));
EXPLAIN SELECT ai_embed('another-model', 'text', CAST(NULL AS MAP<VARCHAR, JSON>));
```
