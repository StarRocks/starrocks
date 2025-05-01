---
displayed_sidebar: docs
---

# group_concat

## Description

これは sum() に似た集計関数です。group_concat は非 NULL 値を 1 つの文字列に連結し、2 番目の引数 `sep` が区切り文字となります。2 番目の引数は省略可能です。この関数は通常、`group by` と共に使用する必要があります。

> 分散コンピューティングを使用しているため、文字列が順番に連結されない場合がありますのでご注意ください。

## Syntax

```Haskell
VARCHAR group_concat(VARCHAR str[, VARCHAR sep])
```

## Parameters

- `str`: 連結する値。VARCHAR に評価される必要があります。
- `sep`: 区切り文字、省略可能です。指定されていない場合、デフォルトでカンマとスペース (`, `) が使用されます。

## Return value

VARCHAR 値を返します。

## Examples

```sql
CREATE TABLE IF NOT EXISTS group_concat (
    id        tinyint(4)      NULL,
    value   varchar(65533)  NULL
) ENGINE=OLAP
DISTRIBUTED BY HASH(id);

INSERT INTO group_concat VALUES
(1,'fruit'),
(2,'drinks'),
(3,null),
(4,'fruit'),
(5,'meat'),
(6,'seafood');

select * from group_concat order by id;
+------+---------+
| id   | value   |
+------+---------+
|    1 | fruit   |
|    2 | drinks  |
|    3 | NULL    |
|    4 | fruit   |
|    5 | meat    |
|    6 | seafood |
```

```sql
select group_concat(value) from group_concat;
+-------------------------------------+
| group_concat(value)                 |
+-------------------------------------+
| meat, fruit, seafood, fruit, drinks |
+-------------------------------------+

MySQL > select group_concat(value, " ") from group_concat;
+---------------------------------+
| group_concat(value, ' ')        |
+---------------------------------+
| fruit meat fruit drinks seafood |
+---------------------------------+
```

## keyword

GROUP_CONCAT,GROUP,CONCAT