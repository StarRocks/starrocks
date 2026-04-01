---
displayed_sidebar: docs
sidebar_label: "ORDER BY"
---

# ORDER BY

SELECT 语句的 ORDER BY 子句通过比较一列或多列的值对结果集进行排序。

ORDER BY 是一项非常消耗时间和资源的操作，因为所有结果都必须发送到一个节点进行合并，然后才能对结果进行排序。与没有 ORDER BY 的查询相比，排序会消耗更多的内存资源。

因此，如果您只需要排序结果集中的前 `N` 个结果，则可以使用 LIMIT 子句，这样可以减少内存使用和网络开销。如果未指定 LIMIT 子句，则默认返回前 65535 个结果。

## 语法

```sql
ORDER BY <column_name> 
    [ASC | DESC]
    [NULLS FIRST | NULLS LAST]
```

## 参数

- `ASC` 指定结果应按升序返回。
- `DESC` 指定结果应按降序返回。如果未指定顺序，则默认为 ASC（升序）。
- `NULLS FIRST` 表示 NULL 值应在非 NULL 值之前返回。
- `NULLS LAST` 表示 NULL 值应在非 NULL 值之后返回。

## 示例

```sql
select * from big_table order by tiny_column, short_column desc;
select  *  from  sales_record  order by  employee_id  nulls first;
```
