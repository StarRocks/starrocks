---
displayed_sidebar: docs
description: "通过 AI Provider 对文本进行分类、提取、翻译、摘要和转换。"
sidebar_position: 21
---

# AI 文本函数

文本辅助函数通过使用 `openai` 协议的 `chat` Provider 构造特定任务的提示词。每个函数支持默认模型、显式模型或显式 Provider 加模型三种形式，不接受 options MAP。

调用同时需要 `USAGE ON AI FUNCTION <function_name>` 和所选 Provider 的 `USAGE ON AI PROVIDER <provider_name>`，包括默认 Provider。参见 [AI 函数权限](ai_functions.mdx#ai-function-privileges)。

## 语法

```sql
ai_sentiment(text)
ai_sentiment(model, text)
ai_sentiment(provider, model, text)
ai_classify(text, categories)
ai_classify(model, text, categories)
ai_classify(provider, model, text, categories)
ai_extract(text, keys)
ai_extract(model, text, keys)
ai_extract(provider, model, text, keys)
ai_fix_grammar(text)
ai_fix_grammar(model, text)
ai_fix_grammar(provider, model, text)
ai_redact(text, categories)
ai_redact(model, text, categories)
ai_redact(provider, model, text, categories)
ai_translate(text, source_language, target_language)
ai_translate(model, text, source_language, target_language)
ai_translate(provider, model, text, source_language, target_language)
ai_similarity(text1, text2)
ai_similarity(model, text1, text2)
ai_similarity(provider, model, text1, text2)
ai_summarize(text)
ai_summarize(model, text)
ai_summarize(provider, model, text)
ai_filter(text, condition)
ai_filter(model, text, condition)
ai_filter(provider, model, text, condition)
```

## 参数

省略 `provider` 时，使用对应类型的集群默认 Provider：补全和文本辅助函数使用 `chat`，向量函数使用 `embedding`。省略 `model` 时，使用 Provider 必填的默认模型。显式模型优先，并可逐行变化。`provider` 必须是非空白常量字符串，常量显式模型也不能为空白。显式指定 Provider 的调用不要求设置集群默认值。

文本、模型、条件和语言参数均为 VARCHAR 表达式。`categories` 和 `keys` 必须是非空常量 `ARRAY<VARCHAR>`，元素不能为 NULL 或空白。不支持 FILE 输入。`ai_redact` 会把原始文本发送给模型，并非本地隐私过滤器。

文本、显式 model 或 FILE 输入为 NULL 时返回 NULL，不发送模型请求；常量和运行时列值遵循相同规则。NULL options MAP 表示不传选项，NULL 翻译源语言表示自动识别。Provider 名和辅助函数数组仍需满足各自的参数校验。options 内部嵌套 NULL 保持原有 JSON 兼容语义。若裸 NULL 在多个重载中匹配不同参数类型，须用 `CAST` 指定类型，例如 `CAST(NULL AS VARCHAR)` 或 `CAST(NULL AS MAP<VARCHAR, JSON>)`。

源语言和目标语言使用 `en`、`zh`、`fr` 等语言代码。源语言为 NULL 或空字符串（`''`）时自动识别。目标语言为 NULL 时返回 NULL，不发送请求；目标语言为空或仅含 ASCII 空白字符时也返回 NULL，不发送请求。不提供固定语言白名单或 `return_error_details` 参数。

## 返回值

| 函数 | 类型 | 含义 |
|------|------|------|
| `ai_sentiment` | VARCHAR | 归一化为 `positive`、`negative`、`neutral`、`mixed` 或 `unknown` 之一。 |
| `ai_classify` | JSON | 请求返回包含一个分类的对象，例如 `{"labels":["category"]}`。 |
| `ai_extract` | JSON | 请求返回 `{"response":{"key":"value"}}` 形式的对象，缺失值使用 NULL。 |
| `ai_fix_grammar` | VARCHAR | 纠正语法和拼写后的文本。 |
| `ai_redact` | VARCHAR | 将检测到的值替换为 `[EMAIL]` 等类别标记的文本。 |
| `ai_translate` | VARCHAR | 翻译后的文本，语言和 NULL 规则见下文。 |
| `ai_similarity` | FLOAT | 模型生成的 0 到 1 之间的语义相似度评分，并非向量距离计算。 |
| `ai_summarize` | VARCHAR | 文本摘要。 |
| `ai_filter` | BOOLEAN | 模型是否认为文本符合指定条件。 |

分类和提取结果必须是 JSON 对象，但不会验证所请求的对象结构或事实准确性。所有结果均可为 NULL，且具有非确定性。行级错误遵循 `ai_function_on_error`，[SQL 限制和运行时控制](ai_complete.md#限制和安全)同样适用。

## 示例

```sql
EXPLAIN SELECT ai_sentiment('The delivery was excellent.');
EXPLAIN SELECT ai_classify('Please reset my password.', ['support', 'sales']);
EXPLAIN SELECT ai_translate('Hello', '', 'zh');
EXPLAIN SELECT ai_translate('support_chat', 'chat-model', 'Bonjour', 'fr', 'en');
EXPLAIN SELECT ai_summarize('chat-model', 'A local test paragraph.');
```
