---
displayed_sidebar: docs
toc_max_heading_level: 4
---

# `native_query`

`native_query()` 是一种 JDBC 查询表函数。它会将 `SELECT` 语句直接下发到 JDBC Catalog 对应的远端数据库，根据远端查询结果的元数据推断返回 Schema，并将结果作为普通的 StarRocks 表关系暴露出来。

当针对 JDBC 数据源的查询无法表示为一次简单的外表扫描时，可以使用该函数，例如：需要在远端执行 Join、访问远端视图、使用数据库方言特有谓词，或者先在远端做子查询过滤。

## 语法

```SQL
TABLE(<catalog_name>.native_query('<sql>'))
```

## 参数说明

- `catalog_name`：已有 JDBC Catalog 的名称。
- `sql`：一个字符串字面量，包含一条远端执行的 `SELECT` 查询。如果远端 SQL 本身包含单引号，需要使用双单引号转义，例如 `''2026-01-01''`。

## 返回值

返回一张表，表的 Schema 根据远端查询结果的元数据自动推断。

`native_query()` 返回远端结果集后，StarRocks 仍然可以继续在其上执行本地过滤、Join、投影、聚合，以及 `INSERT INTO` 等处理。

## 使用说明

- `native_query()` 仅支持 JDBC Catalog。
- 调用 `native_query()` 时必须使用 `TABLE()` 包裹。
- `native_query()` 仅接受一个参数，并且该参数必须是字符串字面量。
- 不支持命名参数。
- 透传 SQL 必须以 `SELECT` 开头。不支持 `WITH` 查询，也不支持非 `SELECT` 语句。
- 支持为表函数结果指定表别名，但不支持在表函数后追加列别名列表。如果需要指定输出列名，请直接在透传 SQL 内部定义列别名。
- 输出列名应保持唯一。如果远端查询返回了重复列名，请在透传 SQL 中为这些列指定唯一别名。
- 当前用户必须拥有目标 JDBC Catalog 的 `USAGE` 权限。

## 示例

示例一：通过 JDBC Catalog 查询远端表。

```SQL
SELECT *
FROM TABLE(pg_jdbc.native_query(
    'SELECT c_custkey, c_name, c_nationkey FROM customer'
));
```

示例二：在透传 SQL 中使用数据库原生过滤能力，然后在 StarRocks 侧继续追加过滤。

```SQL
SELECT *
FROM TABLE(pg_jdbc.native_query(
    'SELECT id, email FROM app_user WHERE email ILIKE ''%starrocks.com'''
)) AS t
WHERE t.id < 100;
```

示例三：将远端查询结果与本地 StarRocks 表做 Join。

```SQL
SELECT local_ids.id, remote_users.name
FROM local_ids
JOIN TABLE(pg_jdbc.native_query(
    'SELECT id, name FROM dim_user'
)) AS remote_users
ON local_ids.id = remote_users.id;
```

示例四：将远端查询结果写入 StarRocks 内表。

```SQL
INSERT INTO sr_orders_stage
SELECT order_id, customer_id, amount
FROM TABLE(pg_jdbc.native_query(
    'SELECT order_id, customer_id, amount
     FROM orders
     WHERE order_date >= DATE ''2026-01-01'''
));
```

## 相关文档

- [JDBC Catalog](../../../data_source/catalog/jdbc_catalog.md)

## keywords

表函数，JDBC，native query，JDBC Catalog
