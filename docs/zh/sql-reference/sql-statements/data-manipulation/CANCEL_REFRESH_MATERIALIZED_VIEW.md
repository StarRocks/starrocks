---
displayed_sidebar: "Chinese"
---

# CANCEL REFRESH MATERIALIZED VIEW

## 功能

取消异步物化视图的刷新任务。

## 语法

```SQL
CANCEL REFRESH MATERIALIZED VIEW [database_name.]materialized_view_name
```

## 参数说明

| **参数**               | **必选** | **说明**                                                     |
| ---------------------- | -------- | ------------------------------------------------------------ |
| database_name          | 否       | 待取消刷新的物化视图所属数据库名称。如果不指定该参数，则默认使用当前数据库。 |
| materialized_view_name | 是       | 待取消刷新的物化视图名称。                                   |

## 示例

示例一：取消异步物化视图 `lo_mv1` 的刷新任务。

```SQL
CANCEL REFRESH MATERIALIZED VIEW lo_mv1;
```
