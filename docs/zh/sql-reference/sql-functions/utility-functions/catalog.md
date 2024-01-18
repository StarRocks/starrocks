---
displayed_sidebar: "Chinese"
---

# catalog

## 功能

查询当前会话所在的 Catalog。可以是 Internal Catalog 或 External Catalog。有关 Catalog 的详细信息，参见 [](../../../data_source/catalog/catalog_overview.md)。

如果未选定 Catalog，默认显示 StarRocks 系统内 Internal Catalog `default_catalog`。

## 语法

```Haskell
catalog()
```

## 参数说明

该函数不需要传入参数。

## 返回值说明

返回当前会话所在的 Catalog 名称。

## 示例

当前 Catalog 为 StarRocks 系统内 Internal Catalog。

```sql
select catalog();
+-----------------+
| CATALOG()       |
+-----------------+
| default_catalog |
+-----------------+
1 row in set (0.01 sec)
```
