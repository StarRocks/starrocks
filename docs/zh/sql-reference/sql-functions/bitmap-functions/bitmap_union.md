---
displayed_sidebar: "Chinese"
---

# bitmap_union

## 功能

输入一组 bitmap 值，求这一组 bitmap 值的并集。

## 语法

```Haskell
BITMAP_UNION(value)
```

## 参数说明

`value`: 支持的数据类型为 BITMAP。

## 返回值说明

返回值的数据类型为 BITMAP。

## 示例

```sql
select page_id, bitmap_union(user_id)
from table
group by page_id;
```

和 bitmap_count 函数组合使用可以求得网页的 UV 数据。

```sql
select page_id, bitmap_count(bitmap_union(user_id))
from table
group by page_id;
```

当 `user_id` 字段为 INT 时，上面查询语义等同于如下语句：

```sql
select page_id, count(distinct user_id)
from table
group by page_id;
```
