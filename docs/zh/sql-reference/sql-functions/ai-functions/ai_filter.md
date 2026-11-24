---
displayed_sidebar: docs
description: "根据自然语言条件判断文本是否符合要求。"
products: [oss, enterprise, cloud, anywhere]
---

# ai_filter

根据自然语言条件判断文本是否符合要求。

## 语法

```sql
ai_filter(input, condition[, provider[, model]])
```

## 参数

- `input`：VARCHAR 表达式，要判断的文本。
- `condition`：VARCHAR 表达式，用自然语言描述判断条件。

`provider`、`model` 的类型及默认行为见 [公共调用规则](ai_functions.mdx#公共调用规则)。

## 返回值

返回可空的 BOOLEAN。模型响应须能归一化为 `true` 或 `false`，其他响应按行级错误处理。

## 示例

```sql
SELECT ai_filter('The package arrived broken.', 'The customer reports damaged goods.');

SELECT ai_filter(
    input => 'The package arrived broken.',
    condition => 'The customer reports damaged goods.',
    provider => 'support_chat'
);
```
