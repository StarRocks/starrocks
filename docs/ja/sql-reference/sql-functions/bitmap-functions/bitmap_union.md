---
displayed_sidebar: docs
---

# bitmap_union

グループ化後の値のセットに対して、bitmap union を計算します。一般的な使用シナリオには、PV や UV の計算が含まれます。

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

この関数を bitmap_count() と組み合わせて使用することで、ウェブページの UV を取得できます。

```sql
select page_id, bitmap_count(bitmap_union(user_id))
from table
group by page_id;
```

`user_id` が整数の場合、上記のクエリ文は次のものと同等です。

```sql
select page_id, count(distinct user_id)
from table
group by page_id;
```

## keyword

BITMAP_UNION, BITMAP