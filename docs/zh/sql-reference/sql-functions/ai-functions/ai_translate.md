---
displayed_sidebar: docs
description: "将文本从源语言翻译为目标语言。"
products: [oss, enterprise, cloud, anywhere]
---

# ai_translate

将文本从源语言翻译为目标语言。

## 语法

```sql
ai_translate(input, source_language, target_language[, provider[, model]])
```

## 参数

- `input`：VARCHAR 表达式，要翻译的文本。
- `source_language`：VARCHAR 表达式，源语言；使用空字符串 `''` 请求自动识别。
- `target_language`：VARCHAR 表达式，目标语言。

语言可写为 `en`、`zh`、`fr` 等代码，也可以使用模型识别的语言名称。可用语言取决于模型，StarRocks 没有固定的语言列表。

`provider`、`model` 的类型及默认行为见 [公共调用规则](ai_functions.mdx#公共调用规则)。

## 返回值

返回可空的 VARCHAR，内容为翻译后的文本。`input` 求值为 NULL 时，函数返回 NULL。`source_language` 求值为 NULL 时，模型自动识别源语言。`target_language` 求值为 NULL、空字符串或只含 ASCII 空白字符的字符串时，函数返回 NULL，不发送请求。

三个语义参数均为必填参数，不能直接传裸 NULL。自动识别源语言的调用应显式传空字符串。

## 示例

```sql
SELECT ai_translate('Hello', '', 'zh');

SELECT ai_translate(
    input => 'Bonjour',
    source_language => 'fr',
    target_language => 'en',
    provider => 'support_chat'
);
```
