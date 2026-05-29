---
displayed_sidebar: docs
---

# native_query

`native_query` 是 JDBC Catalog 表函数。该函数通过 JDBC Catalog 在源数据库中执行原生 `SELECT` 语句，并将查询结果作为 StarRocks 中的普通关系返回。

当源数据库需要执行难以通过单张 JDBC 外部表表达的 SQL 时，可以使用该函数，例如预先过滤的子查询、在源数据库中执行的 Join，或特定数据库方言的 SQL 语法。`native_query` 返回关系后，您可以继续在 StarRocks 中对其执行过滤、Join、聚合、投影，也可以通过 [INSERT INTO](../../sql-statements/loading_unloading/INSERT.md) 导入结果。

该函数自 v4.1 起支持。

## 语法

```SQL
SELECT ...
FROM TABLE(<jdbc_catalog>.native_query('<select_sql>')) [AS] <alias>
[WHERE ...];
```

## 参数说明

### `jdbc_catalog`

[JDBC Catalog](../../../data_source/catalog/jdbc_catalog.md) 的名称。仅 JDBC Catalog 支持该函数。

### `select_sql`

字符串字面量，表示要在源数据库中执行的透传 SQL 语句。

StarRocks 会移除 `select_sql` 开头的注释和末尾的分号。移除后，该语句必须以 `SELECT` 开头。请按照源数据库的 SQL 方言、对象名、引用规则和函数语法编写该语句。

如果 `select_sql` 中包含单引号 (`'`)，需要在 StarRocks SQL 字符串中写成两个单引号 (`''`) 进行转义。

### `alias`

结果关系的可选表别名。

不支持在表别名后指定列别名，例如 `AS q(c1, c2)`。如需设置列别名，请在 `select_sql` 中定义，例如 `SELECT id AS id_alias FROM ...`。

## 返回值

`native_query` 返回一个关系，其列由 `select_sql` 的 JDBC 结果集元数据推断而来。StarRocks 使用 JDBC Catalog 的 Schema Resolver 将 JDBC 列类型映射为 StarRocks 列类型。

## 使用说明

- StarRocks 用户需要拥有 JDBC Catalog 的 `USAGE` 权限。源数据库对象权限由远端数据库使用 JDBC Catalog 中配置的用户进行校验。
- `select_sql` 必须是字符串字面量，并且必须是唯一参数。不支持命名参数。
- 移除开头注释后，`select_sql` 必须以 `SELECT` 开头。不支持以 `WITH`、`INSERT`、`UPDATE`、`DELETE` 或其他非 `SELECT` 关键字开头的语句。
- 不支持旧形式 `<catalog>.system.query(...)`。
- 如果源查询不返回任何列，StarRocks 会在分析阶段返回错误。

## 示例

以下示例假设源 MySQL 数据库中存在表 `app.orders`，并且 StarRocks 中已创建名为 `jdbc0` 的 JDBC Catalog。

示例一：执行源端子查询，并在 StarRocks 外层继续过滤。

```SQL
SELECT id, name, doubled_score
FROM TABLE(jdbc0.native_query(
    'SELECT id, name, score * 2 AS doubled_score
     FROM app.orders
     WHERE score >= 20'
)) q
WHERE doubled_score < 70
ORDER BY id;
```

示例二：对透传查询返回的结果执行聚合。

```SQL
SELECT category, SUM(score) AS total_score
FROM TABLE(jdbc0.native_query(
    'SELECT category, score
     FROM app.orders
     WHERE status = ''PAID'''
)) q
GROUP BY category
ORDER BY category;
```

示例三：将原生查询结果导入 StarRocks 表。

```SQL
INSERT INTO paid_order_summary
SELECT category, SUM(score) AS total_score
FROM TABLE(jdbc0.native_query(
    'SELECT category, score
     FROM app.orders
     WHERE status = ''PAID'''
)) q
GROUP BY category;
```

## 不支持的形式

```SQL
-- 不支持命名参数。
SELECT * FROM TABLE(jdbc0.native_query(query => 'SELECT id FROM app.orders'));

-- 不支持旧形式 system.query。
SELECT * FROM TABLE(jdbc0.system.query('SELECT id FROM app.orders'));

-- 不支持 WITH 查询，因为 SQL 必须以 SELECT 开头。
SELECT * FROM TABLE(jdbc0.native_query('WITH q AS (SELECT id FROM app.orders) SELECT * FROM q'));

-- 不支持在表别名后指定列别名。
SELECT * FROM TABLE(jdbc0.native_query('SELECT id FROM app.orders')) q(id_alias);
```
