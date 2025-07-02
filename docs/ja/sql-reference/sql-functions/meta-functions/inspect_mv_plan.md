---
displayed_sidebar: docs
---

# inspect_mv_plan

`inspect_mv_plan(mv_name)`
`inspect_mv_plan(mv_name, use_cache)`

これらの関数は、マテリアライズドビューの論理プランを返します。

## 引数

`mv_name`: マテリアライズドビューの名前 (VARCHAR)。
`use_cache`: (オプション) マテリアライズドビューのプランキャッシュを使用するかどうかを示すブール値。デフォルトは `TRUE` です。

## 戻り値

マテリアライズドビューの論理プランを含む VARCHAR 文字列を返します。

