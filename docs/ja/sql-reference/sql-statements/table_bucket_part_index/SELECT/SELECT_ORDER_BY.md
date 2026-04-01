---
displayed_sidebar: docs
sidebar_label: "ORDER BY"
---

# ORDER BY

SELECTステートメントのORDER BY句は、1つまたは複数のカラムの値を比較して、結果セットをソートします。

ORDER BYは、すべての結果を1つのノードに送信してマージしてからソートする必要があるため、時間とリソースを消費する操作です。ソートは、ORDER BYなしのクエリよりも多くのメモリリソースを消費します。

したがって、ソートされた結果セットから最初の`N`個の結果のみが必要な場合は、LIMIT句を使用できます。これにより、メモリ使用量とネットワークオーバーヘッドが削減されます。LIMIT句が指定されていない場合、デフォルトで最初の65535個の結果が返されます。

## 構文

```sql
ORDER BY <column_name> 
    [ASC | DESC]
    [NULLS FIRST | NULLS LAST]
```

## パラメータ

- `ASC` は、結果を昇順で返すように指定します。
- `DESC` は、結果を降順で返すように指定します。順序が指定されていない場合、デフォルトは ASC （昇順）です。
- `NULLS FIRST` は、NULL 値を非 NULL 値よりも前に返すことを示します。
- `NULLS LAST` は、NULL 値を非 NULL 値よりも後に返すことを示します。

## 例

```sql
select * from big_table order by tiny_column, short_column desc;
select  *  from  sales_record  order by  employee_id  nulls first;
```
