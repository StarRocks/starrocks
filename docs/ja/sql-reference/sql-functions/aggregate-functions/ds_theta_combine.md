# ds_theta_combine

シリアライズされた Apache DataSketches Theta スケッチ（`VARBINARY`）を和集合でマージして集計し、単一のシリアライズされたコンパクトスケッチを返します。集計レイヤーにおける [ds_theta_accumulate](./ds_theta_accumulate.md) の逆操作です。

ワイヤフォーマットは標準的な Apache DataSketches C++ コンパクト theta シリアライゼーションのため、規格に準拠する任意の Apache DataSketches 実装で生成されたスケッチを変換なしで結合できます。

## 構文

```Haskell
VARBINARY ds_theta_combine(sketch)
```

- `sketch`: コンパクト theta スケッチの `VARBINARY` 列。

## 例

```SQL
SELECT year(day), ds_theta_estimate(ds_theta_combine(daily_sk))
FROM daily_sketches
GROUP BY year(day);
```

## キーワード

DS_THETA_COMBINE, DS_THETA_ACCUMULATE, DS_THETA_ESTIMATE, DS_THETA_COUNT_DISTINCT
