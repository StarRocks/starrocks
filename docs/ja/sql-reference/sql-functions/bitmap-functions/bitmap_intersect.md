---
displayed_sidebar: docs
---

# bitmap_intersect

## 説明

グループ化後にビットマップの共通部分を計算するための集計関数です。一般的な使用シナリオとして、ユーザーリテンション率の計算があります。

## 構文

```Haskell
BITMAP BITMAP_INTERSECT(BITMAP value)
```

ビットマップ値のセットを入力し、このビットマップ値のセットの共通部分を見つけ、その結果を返します。

## 例

テーブル構造

```yml
KeysType: AGG_KEY
Columns: tag varchar, date datetime, user_id bitmap bitmap_union
```

```SQL
-- 今日と昨日の異なるタグの下でのユーザーリテンションを計算します。
select tag, bitmap_intersect(user_id)
from (
    select tag, date, bitmap_union(user_id) user_id
    from table
    where date in ('2020-05-18', '2020-05-19')
    group by tag, date) a
group by tag;
```

bitmap_to_string 関数と組み合わせて、共通部分の具体的なデータを取得します。

```SQL
-- 今日と昨日の異なるタグの下でリテンションされたユーザーを見つけます。
select tag, bitmap_to_string(bitmap_intersect(user_id))
from (
    select tag, date, bitmap_union(user_id) user_id
    from table where date in ('2020-05-18', '2020-05-19')
    group by tag, date) a
group by tag;
```

## キーワード

BITMAP_INTERSECT, BITMAP