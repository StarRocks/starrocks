# translate

## Description

The function `TRANSLATE()` is used to substitute characters within a string. It works by taking a string as input, along with a set of characters to be replaced, and the corresponding characters to replace them with. TRANSLATE() then performs the specified substitutions.

If the length of result string after translated exceed the maximum length (1048576) of [`VARCAHR`](../sql-statements/data-types/VARCHAR.md), the result string will be `NULL`.

## Syntax

```SQL
TRANSLATE( <source>, <from_string>, <to_string> )
```

## Parameters

- `source`: supports the `VARCAHR` type. The source string to be translated. When a character in the `source` is not found in the `from_string`, it is simply included in the result string.
- `from_string`: supports the `VARCAHR` type. Each character in the `from_string` is either replaced by its corresponding character in the `to_string`, or if there is no corresponding character (i.e. if the `to_string` has fewer characters than the `from_string`, the character is excluded from the resulting string.)
- `to_string`: supports the `VARCAHR` type. A string with the characters that are used to replace characters from the `from_string`. If more characters are specified in the `to_string` than in the `from_string` argument, the extra characters from the `to_string` argument are ignored.

## Return value

Returns a value of the `VARCHAR` type.

## Examples

```SQL
MySQL > select translate('s1m1a1rrfcks','mf1','to') as test;

+-----------+
| test      |
+-----------+
| starrocks |
+-----------+

MySQL > select translate('测abc试忽略', '测试a忽略', 'CS1') as test;
+-------+
| test  |
+-------+
| C1bcS |
+-------+

MySQL > select translate('abcabc', 'ab', '1') as test;
+------+
| test |
+------+
| 1c1c |
+------+

MySQL > select translate('abcabc', 'ab', '12') as test;
+--------+
| test   |
+--------+
| 12c12c |
+--------+

MySQL > select translate('abcabc', 'ab', '123') as test;
+--------+
| test   |
+--------+
| 12c12c |
+--------+

MySQL > select translate(concat('b', repeat('a', 1024*1024-3)), 'a', '膨') as test;
+--------+
| test   |
+--------+
| <null> |
+--------+

select length(translate(concat('b', repeat('a', 1024*1024-3)), 'b', '膨')) as test
+---------+
| test    |
+---------+
| 1048576 |
+---------+

```
