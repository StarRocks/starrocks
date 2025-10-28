---
displayed_sidebar: docs
---

import Tip from '../../../_assets/commonMarkdown/quickstart-shared-nothing-tip.mdx';

# regexp_count

Counts the number of times a pattern occurs in a string. It returns the number of occurrences of the regular expression pattern in the target string.

## Syntax

```Haskell
INT regexp_count(VARCHAR str, VARCHAR pattern)
```

## Parameters

- `str`: The string to search in. Supported data type is VARCHAR. If the input is NULL, NULL is returned.

- `pattern`: The regular expression pattern to search for. Supported data type is VARCHAR. If the pattern is NULL, NULL is returned.

## Return value

Returns an INT representing the count of occurrences. Returns 0 if no matches are found or if the input string is empty.

## Examples

<Tip />

```SQL
-- Count occurrences of digits
SELECT regexp_count('abc123def456', '[0-9]');
```

```plaintext
+---------------------------------------+
| regexp_count('abc123def456', '[0-9]') |
+---------------------------------------+
|                                     6 |
+---------------------------------------+
```

```SQL
-- Count occurrences of dots
SELECT regexp_count('test.com test.net test.org', '\\.');
```

```plaintext
+---------------------------------------------------+
| regexp_count('test.com test.net test.org', '\\.') |
+---------------------------------------------------+
|                                                 3 |
+---------------------------------------------------+
```

```SQL
-- Count occurrences of whitespace sequences
SELECT regexp_count('a b  c   d', '\\s+');
```

```plaintext
+----------------------------------------+
| regexp_count('a b  c   d', '\\s+')     |
+----------------------------------------+
|                                      3 |
+----------------------------------------+
```

```SQL
-- Count occurrences of a repeated pattern
SELECT regexp_count('ababababab', 'ab');
```

```plaintext
+------------------------------------+
| regexp_count('ababababab', 'ab')   |
+------------------------------------+
|                                  5 |
+------------------------------------+
```

```SQL
-- Using with NULL and empty values
SELECT 
  regexp_count('', '.') AS empty_str,
  regexp_count(NULL, '.') AS null_str,
  regexp_count('abc', NULL) AS null_pattern;
```

```plaintext
+------------+----------+--------------+
| empty_str  | null_str | null_pattern |
+------------+----------+--------------+
|          0 |     NULL |         NULL |
+------------+----------+--------------+
```

```SQL
-- Count occurrence of Unicode/Chinese characters
SELECT regexp_count('abc中文def', '[\\p{Han}]+');
```

```plaintext
+-----------------------------------------------+
| regexp_count('abc中文def', '[\\p{Han}]+')     |
+-----------------------------------------------+
|                                             1 |
+-----------------------------------------------+
```

### With table data

```SQL
CREATE TABLE sample_text (
  str VARCHAR(65533) NULL,
  regex VARCHAR(65533) NULL
);

INSERT INTO sample_text VALUES 
  ('abc123def456', '[0-9]'), 
  ('test.com test.net test.org', '\\.'), 
  ('a b  c   d', '\\s+'), 
  ('ababababab', 'ab');

SELECT str, regex, regexp_count(str, regex) AS count 
FROM sample_text 
ORDER BY str;
```

```plaintext
+-----------------------------+------+-------+
| str                         | regex| count |
+-----------------------------+------+-------+
| a b  c   d                  | \s+  |     3 |
| abc123def456                | [0-9]|     6 |
| ababababab                  | ab   |     5 |
| test.com test.net test.org  | \.   |     3 |
+-----------------------------+------+-------+
```

## keyword

REGEXP_COUNT,REGEXP,COUNT