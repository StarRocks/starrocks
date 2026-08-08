# ds_theta_accumulate

`expr` に対して Apache DataSketches Theta スケッチを構築し、シリアライズされたスケッチを `VARBINARY`（コンパクト形式）として返します。[ds_theta_combine](./ds_theta_combine.md) および [ds_theta_estimate](./ds_theta_estimate.md) と組み合わせてスケッチを永続化・再利用できます。

出力は標準的な Apache DataSketches C++ コンパクトシリアライゼーションを使用するため、StarRocks が書き込んだスケッチはデフォルトのハッシュシードを使用する任意の Apache DataSketches 実装で読み取ることができ、その逆も可能です。

## 構文

```Haskell
VARBINARY ds_theta_accumulate(expr)
```

- `expr`: 重複を除いた値を集計する対象の列。

## 例

```SQL
CREATE TABLE sketches AS
SELECT grp, ds_theta_accumulate(id) AS sk FROM t GROUP BY grp;

SELECT grp, ds_theta_estimate(sk) FROM sketches;
```

## キーワード

DS_THETA_ACCUMULATE, DS_THETA_COMBINE, DS_THETA_ESTIMATE, DS_THETA_COUNT_DISTINCT
