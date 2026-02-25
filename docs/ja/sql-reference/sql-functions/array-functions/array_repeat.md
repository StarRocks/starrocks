---
displayed_sidebar: docs
---

# array_repeat

## 説明

指定された要素を指定された回数だけ繰り返した配列を返します。

## 構文

```Haskell
array_repeat(element, count)
```

## パラメータ

- `element`: 繰り返される要素で、StarRocks がサポートする任意のデータ型を指定できます。

- `count`: 繰り返し回数で、INT 型です。

## 戻り値

戻り値のデータ型は、要素の ARRAY 型です。

## 使用上の注意

- count が 1 未満の場合、空の配列が返されます。
- element パラメータが NULL の場合、結果は count 個の NULL からなる配列です。
- count パラメータが NULL の場合、結果は NULL です。

## 例

例 1:

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

例 2:

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

例 3:

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

例 4:

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

例 5:

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
