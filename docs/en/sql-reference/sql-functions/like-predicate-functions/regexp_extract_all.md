---
displayed_sidebar: "English"
---

# regexp_extract_all

## Description

Extracts all substrings from the target string (`str`) that matches a regular expression pattern (`pattern`) and corresponds to the regex group index specified by `pos`. This function returns an array.

In regex, groups are enclosed within the parentheses () and numbered by counting their opening parentheses from left to right, starting from 1. For example, `([[:lower:]]+)C([[:lower:]]+)` is to match lowercase letters to the left or right side of the uppercase letter `C`. This pattern contains two groups: `([[:lower:]]+)` to the left of `C` is the first group and `([[:lower:]]+)` to the right of `C` is the second group.

The pattern must completely match some parts of `str`. If no matches are found, an empty string is returned.

This function is supported from v3.2.

## Syntax

```Haskell
ARRAY<VARCHAR> regexp_extract_all(VARCHAR str, VARCHAR pattern, BIGINT pos)
```

## Parameters

- `str`: the string to be matched.

- `pattern`: the regular expression pattern used to match substrings.

- `pos`: `pattern` may contain multiple groups. `pos` indicates which regex group to extract.

## Return value

Returns an ARRAY that consists of VARCHAR elements.

## Examples

```Plain Text
-- Return all the letters that match group 1 in the pattern.
MySQL > SELECT regexp_extract_all('AbCdE', '([[:lower:]]+)C([[:lower:]]+)', 1);
+-------------------------------------------------------------------+
| regexp_extract_all('AbCdE', '([[:lower:]]+)C([[:lower:]]+)', 1)   |
+-------------------------------------------------------------------+
| ['b']                                                             |
+-------------------------------------------------------------------+

-- Return all the letters that match group 2 in the pattern.
MySQL > SELECT regexp_extract_all('AbCdExCeF', '([[:lower:]]+)C([[:lower:]]+)', 2);
+---------------------------------------------------------------------+
| regexp_extract_all('AbCdExCeF', '([[:lower:]]+)C([[:lower:]]+)', 2) |
+---------------------------------------------------------------------+
| ['d','e']                                                           |
+---------------------------------------------------------------------+
```

## Keywords

REGEXP_EXTRACT_ALL,REGEXP,EXTRACT
