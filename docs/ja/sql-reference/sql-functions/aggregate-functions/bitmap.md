---
displayed_sidebar: docs
---

# bitmap

ここでは、Bitmap でのいくつかの集計関数の使用例を示します。関数の詳細な定義や他の Bitmap 関数については、bitmap-functions を参照してください。

## テーブルの作成

テーブルを作成する際には、集計モデルが必要です。データ型は bitmap で、集計関数は bitmap_union です。

```SQL
CREATE TABLE `pv_bitmap` (
  `dt` int(11) NULL COMMENT "",
  `page` varchar(10) NULL COMMENT "",
  `user_id` bitmap BITMAP_UNION NULL COMMENT ""
) ENGINE=OLAP
AGGREGATE KEY(`dt`, `page`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`dt`);
```

注意: データ量が多い場合は、高頻度の bitmap_union に対応するロールアップテーブルを作成することをお勧めします。

```SQL
ALTER TABLE pv_bitmap ADD ROLLUP pv (page, user_id);
```

## データロード

`TO_BITMAP (expr)`: 0 ~ 18446744073709551615 の符号なし bigint を bitmap に変換

`BITMAP_EMPTY ()`: 空の bitmap 列を生成し、挿入または入力時に埋めるデフォルト値として使用

`BITMAP_HASH (expr)`: 任意の型の列をハッシュ化して bitmap に変換

### Stream Load

Stream Load を使用してデータを入力する際、データを Bitmap フィールドに変換することができます。

``` bash
cat data | curl --location-trusted -u user:passwd -T - \
    -H "columns: dt,page,user_id, user_id=to_bitmap(user_id)" \
    http://host:8410/api/test/testDb/_stream_load
```

``` bash
cat data | curl --location-trusted -u user:passwd -T - \
    -H "columns: dt,page,user_id, user_id=bitmap_hash(user_id)" \
    http://host:8410/api/test/testDb/_stream_load
```

``` bash
cat data | curl --location-trusted -u user:passwd -T - \
    -H "columns: dt,page,user_id, user_id=bitmap_empty()" \
    http://host:8410/api/test/testDb/_stream_load
```

### Insert Into

Insert Into を使用してデータを入力する際、ソーステーブルの列の型に基づいて対応するモードを選択する必要があります。

* ソーステーブルの id2 の列型が bitmap の場合

```SQL
insert into bitmap_table1
select id, id2 from bitmap_table2;
```

* ターゲットテーブルの id2 の列型が bitmap の場合

```SQL
insert into bitmap_table1 (id, id2)
values (1001, to_bitmap(1000))
, (1001, to_bitmap(2000));
```

* ソーステーブルの id2 の列型が bitmap で、bit_map_union() を使用した集計の結果である場合

```SQL
insert into bitmap_table1
select id, bitmap_union(id2) from bitmap_table2 group by id;
```

* ソーステーブルの id2 の列型が INT で、to_bitmap() によって bitmap 型が生成される場合

```SQL
insert into bitmap_table1
select id, to_bitmap(id2) from table;
```

* ソーステーブルの id2 の列型が STRING で、bitmap_hash() によって bitmap 型が生成される場合

```SQL
insert into bitmap_table1
select id, bitmap_hash(id2) from table;
```

## データクエリ

### 構文

`BITMAP_UNION (expr)`: 入力された Bitmap の和集合を計算し、新しい Bitmap を返します。

`BITMAP_UNION_COUNT (expr)`: 入力された Bitmap の和集合を計算し、その基数を返します。これは BITMAP_COUNT (BITMAP_UNION (expr)) に相当します。BITMAP_UNION_COUNT 関数を優先して使用することをお勧めします。これは BITMAP_COUNT (BITMAP_UNION (expr)) よりもパフォーマンスが優れているためです。

`BITMAP_UNION_INT (expr)`: TINYINT、SMALLINT、INT 型の列における異なる値の数を計算し、COUNT (DISTINCT expr) と同じ値を返します。

`INTERSECT_COUNT (bitmap_column_to_count, filter_column, filter_values ...)`: filter_column 条件を満たす複数の bitmap の交差の基数を計算します。bitmap_column_to_count は bitmap 型の列であり、filter_column は異なる次元の列であり、filter_values は次元値のリストです。

`BITMAP_INTERSECT(expr)`: このグループの bitmap 値の交差を計算し、新しい bitmap を返します。

### 例

以下の SQL は、上記の `pv_bitmap` テーブルを例として使用しています。

`user_id` の重複を排除した値を計算します。

```SQL
select bitmap_union_count(user_id)
from pv_bitmap;

select bitmap_count(bitmap_union(user_id))
from pv_bitmap;
```

`id` の重複を排除した値を計算します。

```SQL
select bitmap_union_int(id)
from pv_bitmap;
```

`user_id` のリテンションを計算します。

```SQL
select intersect_count(user_id, page, 'game') as game_uv,
    intersect_count(user_id, page, 'shopping') as shopping_uv,
    intersect_count(user_id, page, 'game', 'shopping') as retention -- 'game' と 'shopping' ページの両方にアクセスしたユーザーの数
from pv_bitmap
where page in ('game', 'shopping');
```

## キーワード

BITMAP,BITMAP_COUNT,BITMAP_EMPTY,BITMAP_UNION,BITMAP_UNION_INT,TO_BITMAP,BITMAP_UNION_COUNT,INTERSECT_COUNT,BITMAP_INTERSECT