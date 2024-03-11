# regexp_extract

## Description

<<<<<<< HEAD
This function return all matching substrings in the target value which matches the regular expression pattern. It extracts the item in pos that matches the pattern. The pattern must completely match some parts of str so that the function can return parts needed to be matched in the pattern. If no matches are found, it will return an empty string.
=======
Extracts all substrings from the target string (`str`) that matches a regular expression pattern (`pattern`) and corresponds to the regex group index specified by `pos`. This function returns an array.

In regex, groups are enclosed within the parentheses () and numbered by counting their opening parentheses from left to right, starting from 1. For example, `([[:lower:]]+)C([[:lower:]]+)` is to match lowercase letters to the left or right side of the uppercase letter `C`. This pattern contains two groups: `([[:lower:]]+)` to the left of `C` is the first group and `([[:lower:]]+)` to the right of `C` is the second group.

The pattern must completely match some parts of `str`. If no matches are found, an empty string is returned.

This function is supported from v2.5.19.
>>>>>>> 8bf8324aef ([Doc] add milliseconds_diff func and update file external table according to v3.1.9 and 3.2.4 rn (backport #42437) (#42439))

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
