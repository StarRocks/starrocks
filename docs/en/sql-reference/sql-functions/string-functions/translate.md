---
displayed_sidebar: docs
---

# translate

## Description

Substitutes specified characters within a string. It works by taking a string (`source`) as the input and replaces the `from_string` characters in `source` with `to_string`.

This function is supported from v3.2.

## Syntax

```Haskell
TRANSLATE(source, from_string, to_string)
```

## Parameters

- `source`: supports the `VARCHAR` type. The source string to be translated. If a character in the `source` is not found in `from_string`, it is simply included in the result string.

- `from_string`: supports the `VARCHAR` type. Each character in `from_string` is either replaced by its corresponding character in the `to_string`, or if there is no corresponding character (i.e. if `to_string` has fewer characters than the `from_string`, the character is excluded from the result string). See Examples 2 and 3. If a character appears multiple times in `from_string`, only its first occurrence is effective. See Example 5.

- `to_string`: supports the `VARCHAR` type. The string used to replace characters. If more characters are specified in `to_string` than in the `from_string` argument, extra characters from `to_string` are ignored. See Example 4.

## Return value

Returns a value of the `VARCHAR` type.

Scenarios where the result is `NULL`:

- Any of the input parameters is `NULL`.

- The length of the result string after translation exceeds the maximum length (1048576) of `VARCHAR`.

## Examples

```plaintext
-- Replace 'ab' in the source string with '12'.
mysql > select translate('abcabc', 'ab', '12') as test;
+--------+
| test   |
+--------+
| 12c12c |
+--------+

-- Replace 'mf1' in the source string with 'to'. 'to' has less characters than 'mf1' and '1' is excluded from the result string.
mysql > select translate('s1m1a1rrfcks','mf1','to') as test;
+-----------+
| test      |
+-----------+
| starrocks |
+-----------+

-- Replace 'ab' in the source string with '1'. '1' has less characters than 'ab' and 'b' is excluded from the result string.
mysql > select translate('abcabc', 'ab', '1') as test;
+------+
| test |
+------+
| 1c1c |
+------+

-- Replace 'ab' in the source string with '123'. '123' has more characters than 'ab' and '3' is ignored.
mysql > select translate('abcabc', 'ab', '123') as test;
+--------+
| test   |
+--------+
| 12c12c |
+--------+

-- Replace 'aba' in the source string with '123'. 'a' appears twice and only the first occurrence of 'a' is replaced.
mysql > select translate('abcabc', 'aba', '123') as test;
+--------+
| test   |
+--------+
| 12c12c |
+--------+

-- Use this function with repeat() and concat(). The result string exceeds the maximum length of VARCHAR and NULL is returned.
mysql > select translate(concat('bcde', repeat('a', 1024*1024-3)), 'a', 'z') as test;
+--------+
| test   |
+--------+
| NULL   |
+--------+

-- Use this function with length(), repeat(), and concat() to calculate the length of the result string.
mysql > select length(translate(concat('bcd', repeat('a', 1024*1024-3)), 'a', 'z')) as test;
+---------+
| test    |
+---------+
| 1048576 |
+---------+
```

## See also

- [concat](./concat.md)
- [length](./length.md)
- [repeat](./repeat.md)
