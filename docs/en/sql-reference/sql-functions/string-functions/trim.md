---
displayed_sidebar: docs
description: "Removes consecutive spaces or specified characters from the beginning and end of the `str` argument."
---

# trim



Removes consecutive spaces or specified characters from the beginning and end of the `str` argument. Removing specified characters are supported from StarRocks 2.5.0.

## Syntax

```Haskell
VARCHAR trim(VARCHAR str[, VARCHAR characters])
```

## Parameters

`str`: required, the string to trim, which must evaluate to a VARCHAR value.

`characters`: optional, the characters to remove, which must be a VARCHAR value. If this parameter is not specified, spaces are removed from the string by default. If this parameter is set to an empty string, an error is returned.

## Return value

Returns a VARCHAR value.

## Examples

Example 1: Remove the five spaces from the beginning and end of the string.

```Plain Text
MySQL > SELECT trim("   ab c  ");
+-------------------+
| trim('   ab c  ') |
+-------------------+
| ab c              |
+-------------------+
1 row in set (0.00 sec)
```

Example 2: Remove specified characters from the beginning and end of the string.

```Plain Text
MySQL > SELECT trim("abcd", "ad");
+--------------------+
| trim('abcd', 'ad') |
+--------------------+
| bc                 |
+--------------------+

MySQL > SELECT trim("xxabcdxx", "x");
+-----------------------+
| trim('xxabcdxx', 'x') |
+-----------------------+
| abcd                  |
+-----------------------+
```

## MySQL-compatible `TRIM(... FROM ...)` syntax

In addition to `trim(str)` and `trim(str, characters)`, StarRocks supports the MySQL/SQL-standard form:

```SQL
TRIM([{BOTH | LEADING | TRAILING}] [remstr] FROM str)
```

- `BOTH` (assumed when the specifier is omitted) trims both ends; `LEADING` trims only the start; `TRAILING` trims only the end.
- `remstr` is an optional **string literal**. If omitted, spaces are trimmed.
- **`remstr` is removed as a whole substring, repeatedly** — it is NOT treated as a set of characters. This differs from `trim(str, characters)`, where the second argument is a character set.

```Plain Text
MySQL > SELECT TRIM(LEADING 'x' FROM 'xxxbarxxx');
+------------------------------------+
| TRIM(LEADING 'x' FROM 'xxxbarxxx') |
+------------------------------------+
| barxxx                             |
+------------------------------------+

MySQL > SELECT TRIM(TRAILING 'xyz' FROM 'barxxyzxyz');
+----------------------------------------+
| TRIM(TRAILING 'xyz' FROM 'barxxyzxyz') |
+----------------------------------------+
| barx                                   |
+----------------------------------------+

MySQL > SELECT TRIM(BOTH 'ab' FROM 'abfooab');
+--------------------------------+
| TRIM(BOTH 'ab' FROM 'abfooab') |
+--------------------------------+
| foo                            |
+--------------------------------+
```

Note the difference between the substring (`FROM`) form and the character-set (comma) form:

```Plain Text
-- substring: removes the whole 'xyz' once, leaving 'barx'
MySQL > SELECT TRIM(TRAILING 'xyz' FROM 'barxxyz');  -- 'barx'

-- character set: removes any trailing x/y/z, leaving 'bar'
MySQL > SELECT rtrim('barxxyz', 'xyz');              -- 'bar'
```

The `FROM` form is backed by the builtins `trim_string(str, remstr)`, `ltrim_string(str, remstr)`, and `rtrim_string(str, remstr)`, which can also be called directly. `remstr` must be a constant string literal.

> **NOTE**
>
> `remstr` must not be NULL. Passing a NULL `remstr` to these builtins (for example `trim_string('abc', CAST(NULL AS STRING))`) returns an error rather than NULL. This is consistent with the comma form `trim(str, characters)` and differs from MySQL, which returns NULL. A NULL `str` still propagates to NULL (for example `TRIM('x' FROM NULL)` returns NULL).

## References

- [ltrim](ltrim.md)
- [rtrim](rtrim.md)
