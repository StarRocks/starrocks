---
displayed_sidebar: "English"
---

# substring_index

## Description

Extracts a substring that precedes or follows the `count` occurrences of the delimiter.

- If `count` is positive, counting starts from the beginning of the string and this function returns the substring that precedes the `count`th delimiter. For example, `select substring_index('https://www.starrocks.io', '.', 2);` returns the substring before the second `.` delimiter, which is `https://www.starrocks`.

- If `count` is negative, counting starts from the end of the string and this function returns the substring that follows the `count`th delimiter. For example, `select substring_index('https://www.starrocks.io', '.', -2);` returns the substring after the second `.` delimiter, which is `starrocks.io`.

If any input parameter is null, NULL is returned.

This function is supported from v3.2.

## Syntax

```Haskell
VARCHAR substring_index(VARCHAR str, VARCHAR delimiter, INT count)
```

## Parameters

- `str`: required, the string you want to split.
- `delimiter`: required, the delimiter used to split the string.
- `count`: required, the position of the delimiter. The value cannot be 0. Otherwise, NULL is returned. If the value is greater than the actual number of delimiters in the string, the entire string is returned.

## Return value

Returns a VARCHAR value.

## Examples

```Plain Text
-- Return the substring that precedes the second "." delimiter.
mysql> select substring_index('https://www.starrocks.io', '.', 2);
+-----------------------------------------------------+
| substring_index('https://www.starrocks.io', '.', 2) |
+-----------------------------------------------------+
| https://www.starrocks                               |
+-----------------------------------------------------+

-- The count is negative.
mysql> select substring_index('https://www.starrocks.io', '.', -2);
+------------------------------------------------------+
| substring_index('https://www.starrocks.io', '.', -2) |
+------------------------------------------------------+
| starrocks.io                                         |
+------------------------------------------------------+

mysql> select substring_index("hello world", " ", 1);
+----------------------------------------+
| substring_index("hello world", " ", 1) |
+----------------------------------------+
| hello                                  |
+----------------------------------------+

mysql> select substring_index("hello world", " ", -1);
+-----------------------------------------+
| substring_index('hello world', ' ', -1) |
+-----------------------------------------+
| world                                   |
+-----------------------------------------+

-- Count is 0 and NULL is returned.
mysql> select substring_index("hello world", " ", 0);
+----------------------------------------+
| substring_index('hello world', ' ', 0) |
+----------------------------------------+
| NULL                                   |
+----------------------------------------+

-- Count is greater than the number of spaces in the string and the entire string is returned.
mysql> select substring_index("hello world", " ", 2);
+----------------------------------------+
| substring_index("hello world", " ", 2) |
+----------------------------------------+
| hello world                            |
+----------------------------------------+

-- Count is greater than the number of spaces in the string and the entire string is returned.
mysql> select substring_index("hello world", " ", -2);
+-----------------------------------------+
| substring_index("hello world", " ", -2) |
+-----------------------------------------+
| hello world                             |
+-----------------------------------------+
```

## keyword

substring_index
