# ds_hll_accumulate

値を HyperLogLog スケッチに蓄積し、シリアライズされたスケッチを VARBINARY として返します。この関数は DataSketches HLL 近似重複除去カウント関数ファミリーの一部です。

`ds_hll_accumulate` は、`ds_hll_combine` で他のスケッチと結合し、`ds_hll_estimate` で推定できるシリアライズされた HyperLogLog スケッチを作成します。

Apache DataSketches に基づいており、近似重複除去カウントに高精度を提供します。詳細については、[HyperLogLog スケッチ](https://datasketches.apache.org/docs/HLL/HllSketches.html) を参照してください。

## 構文

```Haskell
sketch ds_hll_accumulate(expr)
sketch ds_hll_accumulate(expr, log_k)
sketch ds_hll_accumulate(expr, log_k, tgt_type)
```

### パラメータ

- `expr`: スケッチに蓄積する式。任意のデータ型にできます。
- `log_k`: 整数。範囲 [4, 21]。デフォルト：17。スケッチの精度とメモリ使用量を制御します。
- `tgt_type`: 有効な値は `HLL_4`、`HLL_6`（デフォルト）、`HLL_8` です。HyperLogLog スケッチのターゲットタイプを制御します。

## 戻り値の型

シリアライズされた HyperLogLog スケッチを含む VARBINARY を返します。

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

-- 単一引数での基本使用法
SELECT ds_hll_accumulate(id) FROM t1;

-- カスタム log_k パラメータを使用
SELECT ds_hll_accumulate(province, 20) FROM t1;

-- log_k と tgt_type の両方のパラメータを使用
SELECT ds_hll_accumulate(age, 12, "HLL_6") FROM t1;

-- グループ化使用法
SELECT dt, 
       ds_hll_accumulate(id), 
       ds_hll_accumulate(province, 20),  
       ds_hll_accumulate(age, 12, "HLL_6"), 
       ds_hll_accumulate(dt) 
FROM t1 
GROUP BY dt 
ORDER BY 1 
LIMIT 3;
```

## 関連関数

- `ds_hll_combine`: 複数のシリアライズされたスケッチを単一のスケッチに結合
- `ds_hll_estimate`: シリアライズされたスケッチから重複除去カウントを推定
- `ds_hll_count_distinct`: 直接近似重複除去カウント関数

## キーワード

ds_HLL_ACCUMULATE, HLL, HYPERLOGLOG, APPROXIMATE, DISTINCT, COUNT 