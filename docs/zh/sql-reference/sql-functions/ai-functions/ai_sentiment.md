---
displayed_sidebar: docs
description: "分析文本的整体情感倾向。"
products: [oss, enterprise, cloud, anywhere]
---

# ai_sentiment

分析文本的整体情感倾向。

## 语法

```sql
ai_sentiment(input[, provider[, model]])
```

## 参数

- `input`：VARCHAR 表达式，要分析的文本。

`provider`、`model` 的类型及默认行为见 [公共调用规则](ai_functions.mdx#公共调用规则)。

## 返回值

返回可空的 VARCHAR。有效结果归一化为 `positive`、`negative`、`neutral`、`mixed` 或 `unknown` 之一；其他响应按行级错误处理。

## 示例

```sql
SELECT ai_sentiment('The delivery was excellent.');

SELECT ai_sentiment(input => 'The delivery was excellent.', provider => 'support_chat');
```
