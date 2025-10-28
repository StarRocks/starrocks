---
displayed_sidebar: docs
---

# tokenize

Splits and parses text into tokens based on the specified tokenizer.

## Syntax

```sql
ARRRY<VARCHAR> tokenize(VARCHAR tokenizer_name, VARCHAR content);
```

## Parameters

- `tokenizer_name`: The tokenizer to use. Valid values: `english`, `standard`, and `chinese`.

- `content`: The text to be tokenized. This item can be a constant string or a column name. If a column is specified, it must be of the STRING or VARCHAR type.

## Return value

Returns a array of VARCHAR.

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