# ds_theta_estimate

シリアライズされた Apache DataSketches Theta スケッチ（`VARBINARY`、コンパクト形式）が要約する近似的な distinct count を返します。[ds_theta_accumulate](./ds_theta_accumulate.md) の逆操作です。

デフォルトのハッシュシードを用いた標準的な Apache DataSketches C++ コンパクト theta フォーマットで書き込まれた任意のスケッチを受け付けます。

## 構文

```Haskell
BIGINT ds_theta_estimate(sketch)
```

- `sketch`: `VARBINARY` コンパクト theta スケッチ。

## 例

```SQL
SELECT ds_theta_estimate(sk) FROM sketches;
```

## キーワード

DS_THETA_ESTIMATE, DS_THETA_ACCUMULATE, DS_THETA_COMBINE, DS_THETA_COUNT_DISTINCT
