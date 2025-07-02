---
displayed_sidebar: docs
---

# inspect_mv_plan

`inspect_mv_plan(mv_name)`
`inspect_mv_plan(mv_name, use_cache)`

这些函数返回物化视图的逻辑计划。

## 参数

`mv_name`: 物化视图的名称 (VARCHAR)。
`use_cache`: (可选) 一个布尔值，指示是否使用物化视图计划缓存。默认为 `TRUE`。

## 返回值

返回包含物化视图逻辑计划的 VARCHAR 字符串。

