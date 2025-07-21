---
displayed_sidebar: docs
---

# tokenize

tokenize 函数用于根据指定的分词器将文本拆分并解析为词元（token）。

## 语法

```sql
ARRRY<VARCHAR> tokenize(VARCHAR tokenizer_name, VARCHAR content);
```
## 参数
- `tokenizer_name`：指定要使用的分词器。目前仅支持以下几种: "english"、"standard"、"chinese"。
- `content`：要进行分词的文本。可以是常量字符串，也可以是列名。如果是列名，该列必须是 STRING 或 VARCHAR 类型。

## 返回值
返回一个VARCHAR类型的数组

## 示例

```sql
MYSQL > SELECT tokenize('english', 'Today is saturday');
+------------------------------------------+
| tokenize('english', 'Today is saturday') |
+------------------------------------------+
| ["today","is","saturday"]                |
+------------------------------------------+
```

## 关键字

TOKENIZE