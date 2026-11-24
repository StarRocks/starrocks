---
displayed_sidebar: docs
description: "请求模型生成保留主要信息的简短摘要。"
products: [oss, enterprise, cloud, anywhere]
---

# ai_summarize

请求模型生成保留主要信息的简短摘要。

## 语法

```sql
ai_summarize(input[, provider[, model]])
```

## 参数

- `input`：VARCHAR 表达式，要生成摘要的文本。

`provider`、`model` 的类型及默认行为见 [公共调用规则](ai_functions.mdx#公共调用规则)。

## 返回值

返回可空的 VARCHAR，内容为模型生成的摘要。

## 示例

```sql
SELECT ai_summarize('会议决定先完成接口测试，再发布新版本。测试负责人将在周五汇报进度。');

SELECT ai_summarize(
    input => '会议决定先完成接口测试，再发布新版本。测试负责人将在周五汇报进度。',
    provider => 'support_chat'
);
```
