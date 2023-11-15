# ALTER MATERIALIZED VIEW

## 功能

修改指定物化视图的名称或异步刷新规则。

## 语法

```SQL
ALTER MATERIALIZED VIEW [database.]mv_name {REFRESH ASYNC new_refresh_scheme_desc | RENAME [database.]new_mv_name}
```

## 参数

| **参数**                | **必选** | **说明**                                                     |
| ----------------------- | -------- | ------------------------------------------------------------ |
| mv_name                 | 是       | 待修改属性的物化视图的名称。                                 |
| new_refresh_scheme_desc | 否       | 新的异步刷新规则，详细信息请见 [SQL 参考 - CREATE MATERIALIZED VIEW - 参数](../data-definition/CREATE_MATERIALIZED_VIEW.md#参数)。 |
| new_mv_name             | 否       | 新的物化视图的名称。                                           |

## 示例

### 示例一：修改物化视图名称

```SQL
ALTER MATERIALIZED VIEW lo_mv1 RENAME lo_mv1_new_name;
```

### 示例二：修改物化视图刷新间隔

```SQL
ALTER MATERIALIZED VIEW lo_mv2 REFRESH ASYNC EVERY(INTERVAL 1 DAY);
```
