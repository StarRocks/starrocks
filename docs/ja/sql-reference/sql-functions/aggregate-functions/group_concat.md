---
displayed_sidebar: docs
---

# group_concat

## 説明

これは sum() に似た集計関数です。group_concat は、非 NULL 値を 1 つの文字列に連結し、2 番目の引数 `sep` がセパレーターとなります。2 番目の引数は省略可能です。この関数は通常、`group by` と一緒に使用する必要があります。

> 分散コンピューティングを使用しているため、文字列が順番に連結されない場合がありますのでご注意ください。

## 構文

```Haskell
VARCHAR group_concat(VARCHAR str[, VARCHAR sep])
```

## パラメータ

- `str`: 連結する値。VARCHAR に評価される必要があります。
- `sep`: セパレーター、省略可能です。指定されていない場合、デフォルトでカンマとスペース（`, `）が使用されます。

## 戻り値

VARCHAR 値を返します。

## 例

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

## キーワード

GROUP_CONCAT,GROUP,CONCAT