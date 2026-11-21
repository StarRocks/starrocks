---
displayed_sidebar: docs
description: "请求模型识别指定类别的敏感信息，并将其替换为类别标记。"
products: [oss, enterprise, cloud, anywhere]
---

# ai_redact

请求模型识别指定类别的敏感信息，并将其替换为类别标记。

## 语法

```sql
ai_redact(input, categories[, provider[, model]])
```

## 参数

- `input`：VARCHAR 表达式，要处理的原始文本。
- `categories`：非空常量 `ARRAY<VARCHAR>`，需要遮蔽的信息类别，例如 `name`、`email`、`phone`。

`provider`、`model` 的类型及默认行为见 [公共调用规则](ai_functions.mdx#公共调用规则)。

## 返回值

返回可空的 VARCHAR。StarRocks 要求模型用方括号括起的大写类别名替换识别到的敏感信息，例如用 `[EMAIL]` 替换邮箱地址。没有匹配项时，模型应返回原文。

原始文本会发送给模型服务。此函数不会在数据出站前执行本地脱敏，模型也可能遗漏敏感信息。

## 示例

```sql
SELECT ai_redact('Contact Alice at alice@example.com.', ['name', 'email']);

SELECT ai_redact(
    input => 'Contact Alice at alice@example.com.',
    categories => ['name', 'email'],
    provider => 'support_chat'
);
```
