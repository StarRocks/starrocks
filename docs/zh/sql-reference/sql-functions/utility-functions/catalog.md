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

示例一：当前 Catalog 为 StarRocks 系统内 Internal Catalog。

```sql
select catalog();
+-----------------+
| CATALOG()       |
+-----------------+
| default_catalog |
+-----------------+
1 row in set (0.01 sec)
```

示例二：当前 Catalog 为 External Catalog `hudi_catalog`。

```sql
-- 切换到目标 External Catalog。
set catalog hudi_catalog;

-- 返回当前 Catalog 名称。
select catalog();
+--------------+
| CATALOG()    |
+--------------+
| hudi_catalog |
+--------------+
```

## 相关 SQL

[SET CATALOG](../../sql-statements/data-definition/SET_CATALOG.md)：切换到指定 Catalog。
