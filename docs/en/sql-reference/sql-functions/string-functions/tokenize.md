---
displayed_sidebar: docs
---

# tokenize

The tokenize function is used to split and analyze text into tokens based on the specified tokenizer.

## Syntax

```sql
ARRRY<VARCHAR> tokenize(VARCHAR tokenizer_name, VARCHAR content);
```
## Parameters
- `tokenizer_name`: Specifies the tokenizer to use. Currently, only the following are supported:
"english","standard","chinese"

- `content`: The text to be tokenized. This can be a constant string or a column name.
  If it is a column, the column must be of type STRING or VARCHAR.

## Return value
Returns a array of VARCHAR

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
