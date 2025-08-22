---
displayed_sidebar: docs
---

# tokenize

指定されたトークナイザに基づいてテキストをトークンに分割し解析します。

## Syntax

```sql
ARRRY<VARCHAR> tokenize(VARCHAR tokenizer_name, VARCHAR content);
```

## Parameters

- `tokenizer_name`: 使用するトークナイザ。 有効な値は `english`、`standard`、`chinese` です。

- `content`: トークン化されるテキスト。この項目は定数文字列または列名にすることができます。列が指定されている場合、それは STRING または VARCHAR 型でなければなりません。

## Return value

VARCHAR の配列を返します。

## Examples

```sql
MYSQL > SELECT tokenize('english', 'Today is saturday');
+------------------------------------------+
| tokenize('english', 'Today is saturday') |
+------------------------------------------+
| ["today","is","saturday"]                |
+------------------------------------------+

```

## keyword

TOKENIZE