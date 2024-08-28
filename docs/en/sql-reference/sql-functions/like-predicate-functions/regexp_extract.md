---
displayed_sidebar: docs
---

# regexp_extract

## Description

This function returns the first matching substring in the target value which matches the regular expression pattern. It extracts the item in pos that matches the pattern. The pattern must completely match some parts of str so that the function can return parts needed to be matched in the pattern. If no matches are found, it will return an empty string.

## Syntax

```Haskell
VARCHAR regexp_extract(VARCHAR str, VARCHAR pattern, int pos)
```

## Examples

:::tip
This example uses the Local Climatological Data(LCD) dataset featured in the [StarRocks Basics](../../../quick_start/shared-nothing.md) Quick Start. You can load the data and try the example yourself.
:::

Given this data:

```SQL
SELECT HourlySkyConditions FROM quickstart.weatherdata
WHERE HourlySkyConditions LIKE '%OVC%'
LIMIT 10;
```

```plaintext
+---------------------+
| HourlySkyConditions |
+---------------------+
| OVC:08 110          |
| OVC:08 120          |
| OVC:08 120          |
| OVC:08 30           |
| OVC:08 29           |
| OVC:08 27           |
| OVC:08 26           |
| OVC:08 22           |
| OVC:08 23           |
| OVC:08 22           |
+---------------------+
10 rows in set (0.03 sec)
```

### Return the two sets of digits following the string `OVC: `

```SQL
SELECT regexp_extract(HourlySkyConditions, 'OVC:(\\d+ \\d+)', 1) FROM quickstart.weatherdata
WHERE HourlySkyConditions LIKE '%OVC%'
LIMIT 10;
```

```plaintext
+-----------------------------------------------------------+
| regexp_extract(HourlySkyConditions, 'OVC:(\\d+ \\d+)', 1) |
+-----------------------------------------------------------+
| 08 110                                                    |
| 08 120                                                    |
| 08 120                                                    |
| 08 30                                                     |
| 08 29                                                     |
| 08 27                                                     |
| 08 26                                                     |
| 08 22                                                     |
| 08 23                                                     |
| 08 22                                                     |
+-----------------------------------------------------------+
10 rows in set (0.01 sec)
```

### Return only the second set of digits following the string `OVC: `

```SQL
SELECT regexp_extract(HourlySkyConditions, 'OVC:(\\d+) (\\d+)', 2) FROM quickstart.weatherdata WHERE HourlySkyConditions LIKE '%OVC%' LIMIT 10;
```

```plaintext
+-------------------------------------------------------------+
| regexp_extract(HourlySkyConditions, 'OVC:(\\d+) (\\d+)', 2) |
+-------------------------------------------------------------+
| 110                                                         |
| 120                                                         |
| 120                                                         |
| 30                                                          |
| 29                                                          |
| 27                                                          |
| 26                                                          |
| 22                                                          |
| 23                                                          |
| 22                                                          |
+-------------------------------------------------------------+
10 rows in set (0.01 sec)
```

:::tip
The same result could be returned by not wrapping the first set of digits in a matching group `()` and returning the first group:

```SQL
regexp_extract(HourlySkyConditions, 'OVC:\\d+ (\\d+)', 1)
```

:::

## keyword

REGEXP_EXTRACT,REGEXP,EXTRACT
