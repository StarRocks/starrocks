---
displayed_sidebar: "Chinese"
---

# 管理黑名单

本文介绍如何管理 SQL 黑名单 (SQL Blacklist)。

您可以在 StarRocks 中维护一个 SQL 黑名单，以在某些场景下禁止特定类型的 SQL，避免此类 SQL 导致集群宕机或者其他预期之外的行为。

> 注意：您需要 ADMIN_PRIV 权限以使用黑名单功能。

## 开启黑名单功能

通过以下命令开启黑名单功能。

```sql
ADMIN SET FRONTEND CONFIG ("enable_sql_blacklist" = "true");
```

## 添加黑名单

通过以下命令添加 SQL 黑名单。

```sql
ADD SQLBLACKLIST "sql";
```

**"sql"**：某类 SQL 的正则表达式。由于 SQL 常用字符里面就包含 `(`、`)`、`*`、`.` 等字符，这些字符会和正则表达式中的语义混淆，因此在设置黑名单的时候需要通过转义符作出区分，鉴于 `(` 和 `)` 在SQL中使用频率过高，我们内部进行了处理，设置的时候不需要转义，其他特殊字符需要使用转义字符"\"作为前缀。

示例:

* 禁止 `count(\*)`。

    ```sql
    ADD SQLBLACKLIST "select count\\(\\*\\) from .+";
    ```

* 禁止 `count(distinct )`。

    ```sql
    ADD SQLBLACKLIST "select count\\(distinct .+\\) from .+";
    ```

* 禁止 `order by limit x, y，1 <= x <=7, 5 <=y <=7`。

    ```sql
    ADD SQLBLACKLIST "select id_int from test_all_type_select1 order by id_int limit [1-7], [5-7]";
    ```

* 禁止复杂 SQL（主要演示 `*` 和 `-` 的转义写法。）。

    ```sql
    ADD SQLBLACKLIST "select id_int \\* 4, id_tinyint, id_varchar from test_all_type_nullable except select id_int, id_tinyint, id_varchar from test_basic except select (id_int \\* 9 \\- 8) \\/ 2, id_tinyint, id_varchar from test_all_type_nullable2 except select id_int, id_tinyint, id_varchar from test_basic_nullable";
    ```

## 展示黑名单列表

```sql
SHOW SQLBLACKLIST;
```

示例：

```plain text
mysql> show sqlblacklist;
+-------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| Index | Forbidden SQL                                                                                                                                                                                                                                                                                          |
+-------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| 1     | select count\(\*\) from .+                                                                                                                                                                                                                                                                             |
| 2     | select id_int \* 4, id_tinyint, id_varchar from test_all_type_nullable except select id_int, id_tinyint, id_varchar from test_basic except select \(id_int \* 9 \- 8\) \/ 2, id_tinyint, id_varchar from test_all_type_nullable2 except select id_int, id_tinyint, id_varchar from test_basic_nullable |
| 3     | select id_int from test_all_type_select1 order by id_int limit [1-7], [5-7]                                                                                                                                                                                                                            |
| 4     | select count\(distinct .+\) from .+                                                                                                                                                                                                                                                                    |
+-------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+

```

返回结果包括 `Index` 和 `Forbidden SQL`。其中，`Index` 字段为被禁止的 SQL 黑名单序号，`Forbidden SQL` 字段展示了被禁止的 SQL，对于所有 SQL 语义的字符做了转义处理。

## 删除黑名单

您可以通过以下命令删除 SQL 黑名单。

```sql
DELETE SQLBLACKLIST index_no;
```

`index_no`：被禁止的 SQL 黑名单序号，您可以通过 `SHOW SQLBLACKLIST;` 命令查询。多个 `index_no` 以 `,` 分隔。

示例：

```plain text
mysql> delete sqlblacklist  3, 4;

show sqlblacklist;
+-------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| Index | Forbidden SQL                                                                                                                                                                                                                                                                                          |
+-------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| 1     | select count\(\*\) from .+                                                                                                                                                                                                                                                                             |
| 2     | select id_int \* 4, id_tinyint, id_varchar from test_all_type_nullable except select id_int, id_tinyint, id_varchar from test_basic except select \(id_int \* 9 \- 8\) \/ 2, id_tinyint, id_varchar from test_all_type_nullable2 except select id_int, id_tinyint, id_varchar from test_basic_nullable |
+-------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+

```
