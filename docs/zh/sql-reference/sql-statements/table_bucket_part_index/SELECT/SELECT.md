---
displayed_sidebar: docs
---

# SELECT

SELECT 语句用于从一个或多个表、视图或物化视图中查询数据。SELECT 可以作为一个独立的语句执行，也可以作为嵌套在其他语句中的子句使用。SELECT 子句的输出可以作为其他语句的输入。

StarRocks 的查询语句基本上符合 SQL92 标准。

:::note
要从 StarRocks 内表中的表、视图或物化视图查询数据，您必须拥有对这些对象的 SELECT 权限。要从外部数据源中的表、视图或物化视图查询数据，您必须拥有对相应 external catalog 的 USAGE 权限。
:::

## 语法

```SQL
SELECT select_list FROM table_reference [, ...]
```

SELECT 语句通常由以下子句组成：

- [Common Table Expression (CTE)](./SELECT_CTE.md)
- [Joins](./SELECT_JOIN.md)
- [ORDER BY](./SELECT_ORDER_BY.md)
- [GROUP BY](./SELECT_GROUP_BY.md)
- [HAVING](./SELECT_HAVING.md)
- [LIMIT](./SELECT_LIMIT.md)
- [OFFSET](./SELECT_OFFSET.md)
- [UNION](./SELECT_UNION.md)
- [INTERSECT](./SELECT_INTERSECT.md)
- [EXCEPT/MINUS](./SELECT_EXCEPT_MINUS.md)
- [DISTINCT](./SELECT_DISTINCT.md)
- [子查询](#子查询)
- [WHERE and operators](./SELECT_WHERE_operator.md)
- [别名](./SELECT_alias.md)
- [PIVOT](./SELECT_PIVOT.md)
- [EXCLUDE](./SELECT_EXCLUDE.md)

有关详细说明，请参阅相应的主题。
