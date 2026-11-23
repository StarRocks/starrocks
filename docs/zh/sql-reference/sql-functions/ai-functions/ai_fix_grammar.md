---
displayed_sidebar: docs
description: "纠正文本的语法和拼写，并请求模型保留原意和语气。"
products: [oss, enterprise, cloud, anywhere]
---

# ai_fix_grammar

纠正文本的语法和拼写，并请求模型保留原意和语气。

## 语法

```sql
ai_fix_grammar(input[, provider[, model]])
```

## 参数

- `input`：VARCHAR 表达式，需要纠正的文本。

`provider`、`model` 的类型及默认行为见 [公共调用规则](ai_functions.mdx#公共调用规则)。

## 返回值

返回可空的 VARCHAR，内容为纠正后的文本。

## 示例

```sql
SELECT ai_fix_grammar('She go to school every day.');

SELECT ai_fix_grammar(input => 'She go to school every day.', provider => 'support_chat');
```
