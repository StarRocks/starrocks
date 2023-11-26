---
displayed_sidebar: "English"
---

# regexp_extract

## Description

This function return all matching substrings in the target value which matches the regular expression pattern. It extracts the item in pos that matches the pattern. The pattern must completely match some parts of str so that the function can return parts needed to be matched in the pattern. If no matches are found, it will return an empty string.

## Syntax

```Haskell
ARRAY<VARCHAR> regexp_extract_all(VARCHAR str, VARCHAR pattern, int pos)
```

## Examples

```Plain Text
MySQL > SELECT regexp_extract_all('AbCdE', '([[:lower:]]+)C([[:lower:]]+)', 1);
+-------------------------------------------------------------------+
| regexp_extract_all('AbCdE', '([[:lower:]]+)C([[:lower:]]+)', 1)   |
+-------------------------------------------------------------------+
| ['b']                                                             |
+-------------------------------------------------------------------+

MySQL > SELECT regexp_extract_all('AbCdExCeF', '([[:lower:]]+)C([[:lower:]]+)', 2);
+---------------------------------------------------------------------+
| regexp_extract_all('AbCdExCeF', '([[:lower:]]+)C([[:lower:]]+)', 2) |
+---------------------------------------------------------------------+
| ['d','e']                                                           |
+---------------------------------------------------------------------+
```

## keyword

REGEXP_EXTRACT_ALL,REGEXP,EXTRACT
