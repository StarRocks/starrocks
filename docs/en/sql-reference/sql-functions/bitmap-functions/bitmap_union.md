---
displayed_sidebar: docs
---

# bitmap_union

## Description

Calculates the bitmap union of a set of values after grouping. Common usage scenarios include calculating PV and UV.

## Syntax

```Haskell
BITMAP BITMAP_UNION(BITMAP value)
```

## Examples

```sql
select page_id, bitmap_union(user_id)
from table
group by page_id;
```

Use this function with bitmap_count() to obtain the UV of a web page.

```sql
select page_id, bitmap_count(bitmap_union(user_id))
from table
group by page_id;
```

If `user_id` is an integer, the above query statement is equivalent to the following:

```sql
select page_id, count(distinct user_id)
from table
group by page_id;
```

## keyword

BITMAP_UNION, BITMAP
