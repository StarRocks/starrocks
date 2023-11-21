---
displayed_sidebar: "Chinese"
---

# bitmap_intersect

## 功能

输入一组 bitmap 值，求这一组 bitmap 值的交集，并返回。

## 语法

```Haskell
BITMAP_INTERSECT(value)
```

## 参数说明

`value`: 支持的数据类型为 BITMAP。

## 返回值说明

返回值的数据类型为 BITMAP。

## 示例

表结构

```yml
KeysType: AGG_KEY
Columns: tag varchar, date datetime, user_id bitmap bitmap_union
```

```SQL
-- 求今天和昨天不同 tag 下的用户留存。
select tag, bitmap_intersect(user_id)
from (
    select tag, date, bitmap_union(user_id) user_id
    from table
    where date in ('2020-05-18', '2020-05-19')
    group by tag, date) a
group by tag;
```

和 bitmap_to_string 函数组合使用可以获取交集的具体数据

```SQL
--求今天和昨天不同 tag 下留存的用户都是哪些。
select tag, bitmap_to_string(bitmap_intersect(user_id))
from (
    select tag, date, bitmap_union(user_id) user_id
    from table where date in ('2020-05-18', '2020-05-19')
    group by tag, date) a
group by tag;
```
