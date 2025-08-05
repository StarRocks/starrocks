# ds_hll_combine

複数のシリアライズされた HyperLogLog スケッチを単一のシリアライズされたスケッチに結合します。この関数は DataSketches HLL 近似重複除去カウント関数ファミリーの一部です。

`ds_hll_combine` は、`ds_hll_accumulate` によって作成された複数の VARBINARY シリアライズされたスケッチを受け取り、すべての異なる値の和集合を表す単一のスケッチにマージします。

Apache DataSketches に基づいており、近似重複除去カウントに高精度を提供します。詳細については、[HyperLogLog スケッチ](https://datasketches.apache.org/docs/HLL/HllSketches.html) を参照してください。

## 構文

```Haskell
sketch ds_hll_combine(sketch)
```

### パラメータ

- `sketch`: シリアライズされた HyperLogLog スケッチを含む VARBINARY 列

## 戻り値の型

結合されたシリアライズされたスケッチを含む VARBINARY を返します。

## 例

```sql
-- テストテーブルを作成
CREATE TABLE t1 (
  id BIGINT,
  province VARCHAR(64),
  age SMALLINT,
  dt VARCHAR(10)
)
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 3;

-- テストデータを挿入
INSERT INTO t1 SELECT generate_series, generate_series, generate_series % 100, "2024-07-24" 
FROM table(generate_series(1, 1000));

-- スケッチを含むテーブルを作成
CREATE TABLE t2 (
  id BIGINT,
  dt VARCHAR(10),
  ds_id VARBINARY,
  ds_province VARBINARY,
  ds_age VARBINARY,
  ds_dt VARBINARY
)
DUPLICATE KEY(id, dt)
DISTRIBUTED BY HASH(id) BUCKETS 3;

-- スケッチデータを挿入
INSERT INTO t2 
SELECT id, dt,
       ds_hll_accumulate(id),
       ds_hll_accumulate(province, 20),
       ds_hll_accumulate(age, 12, "HLL_6"),
       ds_hll_accumulate(dt, 10, "HLL_8") 
FROM t1;

-- 日付でグループ化してスケッチを結合
SELECT dt, 
       ds_hll_combine(ds_id), 
       ds_hll_combine(ds_province), 
       ds_hll_combine(ds_age), 
       ds_hll_combine(ds_dt) 
FROM t2 
GROUP BY dt 
ORDER BY 1 
LIMIT 3;
```

## 関連関数

- `ds_hll_accumulate`: 値をシリアライズされたスケッチに蓄積
- `ds_hll_estimate`: シリアライズされたスケッチから重複除去カウントを推定
- `ds_hll_count_distinct`: 直接近似重複除去カウント関数

## キーワード

ds_HLL_COMBINE, HLL, HYPERLOGLOG, APPROXIMATE, DISTINCT, COUNT 