---
displayed_sidebar: "English"
---

# bitmap_intersect

## Description

Aggregation function, used to calculate the bitmap intersection after grouping. Common usage scenarios, such as calculating user retention rate.

## Syntax

```Haskell
BITMAP BITMAP_INTERSECT(BITMAP value)
```

Enter a set of bitmap values, find the intersection of this set of bitmap values, and return the result.

## Example

Table structure

```yml
KeysType: AGG_KEY
Columns: tag varchar, date datetime, user_id bitmap bitmap_union
```

```SQL
-- Calculate users retention under different tags today and yesterday. 
select tag, bitmap_intersect(user_id)
from (
    select tag, date, bitmap_union(user_id) user_id
    from table
    where date in ('2020-05-18', '2020-05-19')
    group by tag, date) a
group by tag;
```

Use with bitmap_to_string function to obtain the specific data of the intersection.

```SQL
--Find out users retained under different tags today and yesterday. 
select tag, bitmap_to_string(bitmap_intersect(user_id))
from (
    select tag, date, bitmap_union(user_id) user_id
    from table where date in ('2020-05-18', '2020-05-19')
    group by tag, date) a
group by tag;
```

## keyword

BITMAP_INTERSECT, BITMAP
