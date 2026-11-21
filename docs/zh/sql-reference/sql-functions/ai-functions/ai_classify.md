---
displayed_sidebar: docs
description: "请求模型将文本归入给定类别中的一个类别。"
products: [oss, enterprise, cloud, anywhere]
---

# ai_classify

请求模型将文本归入给定类别中的一个类别。

## 语法

```sql
ai_classify(input, categories[, provider[, model]])
```

## 参数

- `input`：VARCHAR 表达式，要分类的文本。
- `categories`：非空常量 `ARRAY<VARCHAR>`，候选类别。

`provider`、`model` 的类型及默认行为见 [公共调用规则](ai_functions.mdx#公共调用规则)。

## 返回值

返回可空的 JSON 对象。请求的结构为 `{"labels":["选中的类别"]}`，例如 `{"labels":["support"]}`。

StarRocks 校验结果是否为 JSON 对象，但不验证 labels 的数量、类别成员关系或分类准确性。

## 示例

```sql
SELECT ai_classify('Please reset my password.', ['support', 'sales']);

SELECT ai_classify(
    input => 'Please reset my password.',
    categories => ['support', 'sales'],
    provider => 'support_chat'
);
```
