# bitmap_union

## description

聚合函数，用于计算分组后的 bitmap 并集。常见使用场景如：计算PV，UV。

### Syntax

```Haskell
BITMAP BITMAP_UNION(BITMAP value)
```

输入一组 bitmap 值，求这一组 bitmap 值的并集，并返回。

## example

```sql
select page_id, bitmap_union(user_id)
from table
group by page_id;
```

和 bitmap_count 函数组合使用可以求得网页的 PV 数据

```sql
select page_id, bitmap_count(bitmap_union(user_id))
from table
group by page_id;
```

当 user_id 字段为 int 时，上面查询语义等同于

```sql
select page_id, count(distinct user_id)
from table
group by page_id;
```

## keyword

BITMAP_UNION, BITMAP
