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

语言可使用 `en`、`zh`、`fr` 等标识，或模型理解的语言名称；StarRocks 不提供固定语言白名单。

`provider`、`model` 的类型及默认行为见 [公共调用规则](ai_functions.mdx#公共调用规则)。

## 返回值

返回可空的 VARCHAR，内容为翻译后的文本。输入文本为运行时 NULL 时返回 NULL。源语言的运行时 NULL 按自动识别处理；目标语言的运行时 NULL、空字符串或仅含 ASCII 空白字符时返回 NULL，不发送请求。

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
