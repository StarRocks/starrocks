# REFRESH MATERIALIZED VIEW

## 功能

手动刷新指定物化视图。

> 注意
>
> 您只能通过该命令手动刷新刷新方式为 ASYNC 或 MANUAL 的物化视图。

## 语法

```SQL
REFRESH MATERIALIZED VIEW [database.]mv_name
```

## 参数

| **参数** | **必选** | **说明**                     |
| -------- | -------- | ---------------------------- |
| mv_name  | 是       | 待手动刷新的物化视图的名称。 |

## 示例

### 示例一：手动刷新指定物化视图

```SQL
REFRESH MATERIALIZED VIEW lo_mv1;
```
