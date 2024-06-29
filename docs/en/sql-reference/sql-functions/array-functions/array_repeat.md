---
displayed_sidebar: "English"
---

# array_repeat

## Description

Returns an array containing a given element repeated a specified number of times.

## Syntax

```Haskell
array_repeat(element, count)
```

## Parameters

- `element`: The element to be repeated, of type BOOLEAN, TINYINT, SMALLINT, INT, BIGINT, LARGEINT, FLOAT, DOUBLE, DECIMALV2, VARCHAR, DATETIME, DATE, JSON, or an ARRAY of these types.

- `count`: The number of repetitions, of type INT.

## Return value

The data type of the return value is the ARRAY type of element.

## Usage notes

- When count is less than 1, an empty array is returned.
- When any argument is NULL, the result is NULL.

## Examples

Example 1: 

```plain text
mysql> select array_repeat(1,5) as res;
+-------------+
| res         |
+-------------+
| [1,1,1,1,1] |
+-------------+
```

Example 2: 

```plain text
mysql> select  array_repeat([1,2],3) as res;
+---------------------+
| res                 |
+---------------------+
| [[1,2],[1,2],[1,2]] |
+---------------------+
```

Example 3: 

```Plain
mysql> select array_repeat(1,-1) as res;
+------+
| res  |
+------+
| []   |
+------+
```

Example 4: 

```Plain
mysql> select  array_repeat(null,3) as res;
+------+
| res  |
+------+
| NULL |
+------+
```
