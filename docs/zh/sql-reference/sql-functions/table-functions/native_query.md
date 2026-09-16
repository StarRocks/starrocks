---
displayed_sidebar: docs
description: "native_query 是一个 JDBC catalog 表函数。"
---

# native_query

`native_query` 是一个 JDBC catalog 表函数。它通过 JDBC catalog 执行数据库原生的 `SELECT` 语句，并将结果作为 StarRocks 关系暴露出来。

当源数据库需要执行难以通过单个 JDBC 外部表表达的 SQL 时，请使用此函数，例如预过滤子查询、在源数据库中执行连接，或使用特定于数据库厂商的 SQL 语法。`native_query` 返回关系后，您可以使用 StarRocks 对结果应用额外的过滤、连接、聚合、投影，或使用 `INSERT INTO` 加载结果。

此函数从 v4.1 版本开始支持。

## 语法

```SQL
SELECT ...
FROM TABLE(<jdbc_catalog>.native_query('<select_sql>')) [AS] <alias>
[WHERE ...];
```

## 参数

### `jdbc_catalog`

JDBC catalog 的名称。仅 JDBC catalog 支持此函数。

### `select_sql`

一个字符串字面量，包含由源数据库执行的透传 SQL 语句。

StarRocks 从 `select_sql` 中去除前导注释和末尾分号后，该语句必须以 `SELECT` 开头。请使用源数据库的 SQL 方言、对象名称、引用规则和函数。

如果 `select_sql` 包含单引号（`'`），请在 StarRocks SQL 字符串中将其转义为两个单引号（`''`）。

### `alias`

结果关系的可选表别名。

不支持在表别名之后使用列别名，例如 `AS q(c1, c2)`。请在 `select_sql` 内部定义列别名，例如 `SELECT id AS id_alias FROM ...`。

## 返回值

`native_query` 返回一个关系，其列从 `select_sql` 的 JDBC 结果集元数据中推断得出。StarRocks 使用 JDBC catalog 的 schema 解析器将 JDBC 列类型映射为 StarRocks 列类型。

## 使用说明

- StarRocks 用户需要拥有 JDBC catalog 的 `USAGE` 权限。源数据库对象的权限由远程数据库使用 JDBC catalog 中配置的用户进行检查。
- `select_sql` 必须是字符串字面量，且必须是唯一的参数。不支持命名参数。
- `select_sql` 在去除前导注释后必须以 `SELECT` 开头。以 `WITH`、`INSERT`、`UPDATE`、`DELETE` 或其他非 `SELECT` 关键字开头的语句不受支持。
- 不支持旧版 `<catalog>.system.query(...)` 形式。
- 如果源查询未返回任何列，StarRocks 将在分析阶段返回错误。

## 示例

以下示例假设源 MySQL 数据库中包含一个名为 `app.orders` 的表，并且已在 StarRocks 中创建了名为 `jdbc0` 的 JDBC catalog。

示例 1：运行源端子查询并应用外部 StarRocks 过滤器。

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

示例 2：对透传查询返回的结果进行聚合。

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

示例 3：将原生查询的结果加载到 StarRocks 表中。

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

-- 不支持旧版 system.query 别名。
SELECT * FROM TABLE(jdbc0.system.query('SELECT id FROM app.orders'));

-- 不支持 WITH 查询，因为 SQL 必须以 SELECT 开头。
SELECT * FROM TABLE(jdbc0.native_query('WITH q AS (SELECT id FROM app.orders) SELECT * FROM q'));

-- 不支持在表别名之后使用列别名。
SELECT * FROM TABLE(jdbc0.native_query('SELECT id FROM app.orders')) q(id_alias);
```
