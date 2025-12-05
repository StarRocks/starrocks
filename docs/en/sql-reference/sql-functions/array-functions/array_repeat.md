---
displayed_sidebar: docs
---

# array_repeat

`array_repeat` returns an array containing a given element repeated a specified number of times.

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

```sql
select array_repeat(1,5) as res;
```

```sql
+-------------+
| res         |
+-------------+
| [1,1,1,1,1] |
+-------------+
```

Example 2: 

```sql
select  array_repeat([1,2],3) as res;
```

```sql
+---------------------+
| res                 |
+---------------------+
| [[1,2],[1,2],[1,2]] |
+---------------------+
```

Example 3: 

```sql
select array_repeat(1,-1) as res;
```

```sql
+------+
| res  |
+------+
| []   |
+------+
```

Example 4: 

```sql
select  array_repeat(null,3) as res;
```

```sql
+------+
| res  |
+------+
| NULL |
+------+
```

Example 5:

```sql
CREATE TABLE IF NOT EXISTS test (COLA INT, COLB INT) PROPERTIES ("replication_num"="1");
INSERT INTO test (COLA, COLB) VALUES (1, 3), (NULL, 3), (2, NULL);
select array_repeat(COLA,COLB) from test;
```

```sql
+--------------------------+
| array_repeat(COLA, COLB) |
+--------------------------+
| [1,1,1]                  |
| [null,null,null]         |
| NULL                     |
+--------------------------+
```
