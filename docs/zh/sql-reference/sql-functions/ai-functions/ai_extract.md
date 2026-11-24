---
displayed_sidebar: docs
description: "按给定字段名称从文本中提取信息。"
products: [oss, enterprise, cloud, anywhere]
---

# ai_extract

按给定字段名称从文本中提取信息。

## 语法

```sql
ai_extract(input, labels[, provider[, model]])
```

## 参数

- `input`：VARCHAR 表达式，要提取信息的文本。
- `labels`：非空常量 `ARRAY<VARCHAR>`，需要提取的字段名称。

`provider`、`model` 的类型及默认行为见 [公共调用规则](ai_functions.mdx#公共调用规则)。

## 返回值

返回可空的 JSON 对象。StarRocks 要求模型把提取的字段和值放在 `response` 对象中，找不到值的字段填 JSON `null`。例如，提取姓名和城市时，模型可能返回 `{"response":{"name":"Alice","city":"Paris"}}`。

StarRocks 只检查返回值是否为 JSON 对象，不检查 `response` 的字段结构或提取结果是否准确。

## 示例

```sql
SELECT ai_extract('Alice lives in Paris.', ['name', 'city']);

SELECT ai_extract(
    input => 'Alice lives in Paris.',
    labels => ['name', 'city'],
    provider => 'support_chat'
);
```
