---
displayed_sidebar: docs
---

# array_repeat

## Description

Returns an array containing a given element repeated a specified number of times.

## Syntax

```Haskell
array_repeat(element, count)
```

## Parameters

- `element`: The element to be repeated can be any data type supported by StarRocks.

- `count`: The number of repetitions, of type INT.

## Return value

The data type of the return value is the ARRAY type of element.

## Usage notes

- When count is less than 1, an empty array is returned.
- When the element parameter is NULL, the result is an array consisting of count NULLs.
- When the count parameter is NULL, the result is NULL.

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

Example 5:

```Plain
mysql> CREATE TABLE IF NOT EXISTS test (COLA INT, COLB INT) PROPERTIES ("replication_num"="1");
mysql> INTO test (COLA, COLB) VALUES (1, 3), (NULL, 3), (2, NULL);
mysql> select array_repeat(COLA,COLB) from test;
+--------------------------+
| array_repeat(COLA, COLB) |
+--------------------------+
| [1,1,1]                  |
| [null,null,null]         |
| NULL                     |
+--------------------------+
```