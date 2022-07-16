# bitmap_union

## description

Aggregation function, used to calculate the bitmap union after grouping. Common usage scenarios: calculating PV and UV.

### Syntax

```Haskell
BITMAP BITMAP_UNION(BITMAP value)
```

Enter a set of bitmap values, calculate the union of this set of bitmap values, and return the result.

## example

```sql
select page_id, bitmap_union(user_id)
from table
group by page_id;
```

Use with bitmap_count function to obtain the PV data of the web page:

```sql
select page_id, bitmap_count(bitmap_union(user_id))
from table
group by page_id;
```

When the user_id field is int, the above query semantics is equivalent to:

```sql
select page_id, count(distinct user_id)
from table
group by page_id;
```

## keyword

BITMAP_UNION, BITMAP
