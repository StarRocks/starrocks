---
displayed_sidebar: "English"
---

# regexp_instr

## Description
This function returns the first matching index in the target value which matches the regular expression pattern. The pattern must completely match some parts of str so that the function can return the exactly index. Be careful, returned character index begin at 1. If no matches are found, it will return an 0. If str or pattern is NULL, the return value is NULL.

## Syntax

```Haskell
INT regexp_instr(VARCHAR str, VARCHAR pattern)
```

## Examples

```Plain Text
MySQL > SELECT regexp_instr('AbCdE', '([[:lower:]]+)C([[:lower:]]+)');
+--------------------------------------------------------+
| regexp_instr('AbCdE', '([[:lower:]]+)C([[:lower:]]+)') |
+--------------------------------------------------------+
| 2                                                      |
+--------------------------------------------------------+

MySQL > SELECT regexp_instr('abCdE', '([[:lower:]]+)C([[:lower:]]+)');
+--------------------------------------------------------+
| regexp_instr('abCdE', '([[:lower:]]+)C([[:lower:]]+)') |
+--------------------------------------------------------+
| 1                                                      |
+--------------------------------------------------------+

MySQL > SELECT regexp_instr('abCdE', 'ABC');
+--------------------------------------------------------+
| regexp_instr('abCdE', 'ABC');                          |
+--------------------------------------------------------+
| 0                                                      |
+--------------------------------------------------------+
```

## keyword

REGEXP_INSTR,REGEXP,INSTR