---
displayed_sidebar: docs
description: "使用 SYSTEM 聊天模型对文本进行分类、提取、翻译、摘要和转换。"
sidebar_position: 21
---

# AI 文本函数

这些函数使用 [ai_complete](ai_complete.md#配置) 的 SYSTEM 聊天端点和凭证，构造特定任务的提示词并返回相应类型的结果。使用这些新函数前，请先升级所有 FE 和 BE 节点。

:::warning
输入文本会发送给模型提供商，且可能产生费用。请仅使用已批准的端点和数据。特别是，`ai_redact` 会将原始、未脱敏的文本发送给提供商，并非本地隐私过滤器。
:::

## 语法

每个函数仅有两种形式：使用默认聊天模型，或者将显式模型作为第一个参数。这些辅助函数不接受 options MAP。

```sql
ai_sentiment(text)
ai_sentiment(model, text)
ai_classify(text, categories)
ai_classify(model, text, categories)
ai_extract(text, keys)
ai_extract(model, text, keys)
ai_fix_grammar(text)
ai_fix_grammar(model, text)
ai_redact(text, categories)
ai_redact(model, text, categories)
ai_translate(text, source_language, target_language)
ai_translate(model, text, source_language, target_language)
ai_similarity(text1, text2)
ai_similarity(model, text1, text2)
ai_summarize(text)
ai_summarize(model, text)
ai_filter(text, condition)
ai_filter(model, text, condition)
```

文本、模型、条件和语言参数均为 VARCHAR 表达式。显式模型可逐行变化；常量显式模型不能为空白。未显式指定模型时，必须配置 `ai_default_chat_model`。

`categories` 和 `keys` 必须为非空常量 `ARRAY<VARCHAR>`，且不能包含 NULL 或空白元素。包含 URL 的 VARCHAR 仍作为文本处理，StarRocks 不会读取 URL 内容。不支持 FILE、图像或多模态输入。

## 返回值

所有结果都可为 NULL，且具有非确定性。

| 函数 | 类型 | 含义 |
|------|------|------|
| `ai_sentiment` | VARCHAR | 归一化为 `positive`、`negative`、`neutral`、`mixed` 或 `unknown` 之一。 |
| `ai_classify` | JSON | 请求返回包含一个分类的对象，例如 `{"labels":["category"]}`。 |
| `ai_extract` | JSON | 请求返回 `{"response":{"key":"value"}}` 形式的对象，缺失值使用 NULL。 |
| `ai_fix_grammar` | VARCHAR | 纠正语法和拼写后的文本。 |
| `ai_redact` | VARCHAR | 将检测到的值替换为 `[EMAIL]` 等类别标记的文本。 |
| `ai_translate` | VARCHAR | 翻译后的文本。源语言为 NULL 或空字符串时请求自动识别；目标语言为 NULL 或空字符串时直接返回 NULL，不发送请求。 |
| `ai_similarity` | FLOAT | 模型生成的 0 到 1 之间的语义相似度评分，并非向量距离计算。 |
| `ai_summarize` | VARCHAR | 文本摘要。 |
| `ai_filter` | BOOLEAN | 模型是否认为文本符合指定条件。 |

分类和提取的响应必须是有效的 JSON 对象。StarRocks 不强制校验所请求的对象结构，也不验证模型输出的事实准确性。

文本为 NULL 或显式模型为 NULL 时，直接返回 NULL，不发送提供商请求。翻译的 NULL 源语言是上述例外。格式不正确的类型化响应及其他行级失败遵循 [ai_function_on_error](../../../administration/configuration/BE_parameters/query_loading.md#ai_function_on_error)：`ignore` 返回 NULL，`fail` 中止查询。配置错误、取消和截止时间到期不会转换为 NULL。

[ai_complete 的 SQL 使用位置限制、预处理语句重新规划及运行时限制](ai_complete.md#限制和安全)同样适用。

## 示例

以下示例仅规划查询。`EXPLAIN` 仍需有效的 SYSTEM 聊天配置，但不会发送提供商请求。

```sql
EXPLAIN SELECT ai_sentiment('The delivery was excellent.');
EXPLAIN SELECT ai_classify('Please reset my password.', ['support', 'sales']);
EXPLAIN SELECT ai_extract('Order 42 ships Friday.', ['order', 'ship_date']);
EXPLAIN SELECT ai_translate('Hello', NULL, 'Chinese');
EXPLAIN SELECT ai_similarity('A quick reply', 'A fast response');
EXPLAIN SELECT ai_filter('The package arrived damaged.', 'describes a damaged item');
EXPLAIN SELECT ai_summarize('approved-chat-model', 'A local test paragraph.');
```

文本向量化参见 [ai_embed](ai_embed.md)；按命名模型选择聊天或向量模型参见 [AI 模型](ai_model.md)。
