---
displayed_sidebar: docs
description: "请求聊天模型评估两段文本的语义相似度。"
products: [oss, enterprise, cloud, anywhere]
---

# ai_similarity

请求聊天模型评估两段文本的语义相似度。

## 语法

```sql
ai_similarity(input, other[, provider[, model]])
```

## 参数

- `input`：VARCHAR 表达式，第一段文本。
- `other`：VARCHAR 表达式，第二段文本。

`provider`、`model` 的类型及默认行为见 [公共调用规则](ai_functions.mdx#公共调用规则)。

## 返回值

返回可空的 FLOAT，范围为 `[0, 1]`，数值越大表示模型判断的语义越相近。无法解析或超出范围的响应按行级错误处理。

该分数由聊天模型生成，不是对 embedding 向量进行距离计算。

## 示例

```sql
SELECT ai_similarity('A cat is sleeping.', 'A kitten is taking a nap.');

SELECT ai_similarity(
    input => 'A cat is sleeping.',
    other => 'A kitten is taking a nap.',
    provider => 'support_chat'
);
```
