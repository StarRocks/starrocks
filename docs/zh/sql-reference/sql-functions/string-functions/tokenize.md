---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "根据指定的分词器将文本拆分解析为标记数组。"
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# tokenize

根据指定的分词器将文本拆分并解析为标记。

## 语法

```sql
ARRRY<VARCHAR> tokenize(VARCHAR tokenizer_name, VARCHAR content);
```

## 参数

- `tokenizer_name`：要使用的分词器。有效值：`english`、`standard` 和 `chinese`。

- `content`：要进行分词的文本。此项可以是常量字符串或列名。如果指定为列，则必须是 STRING 或 VARCHAR 类型。

## 返回值

返回一个 VARCHAR 数组。

## 示例

```sql
MYSQL > SELECT tokenize('english', 'Today is saturday');
+------------------------------------------+
| tokenize('english', 'Today is saturday') |
+------------------------------------------+
| ["today","is","saturday"]                |
+------------------------------------------+

```

## Keyword

TOKENIZE