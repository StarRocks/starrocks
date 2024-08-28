---
displayed_sidebar: docs
---

# role_edges

您可以通过查询 `role_edges` 视图查看角色的授予关系。

:::note

默认仅拥有 `user_admin` 角色的用户或角色可以查询此视图，或通过 [GRANT](../../sql-reference/sql-statements/account-management/GRANT.md) 语句将此视图的权限赋予给其他用户。

:::

`role_edges` 提供以下字段：

| **字段**  | **描述**                                                     |
| --------- | ------------------------------------------------------------ |
| FROM_ROLE | 授予的角色。因为一个角色可以授予多个角色和用户，所以 `role_edges` 视图可以返回多条具有相同 `FROM_ROLE` 的记录。 |
| TO_ROLE   | 当前 `FROM_ROLE` 被授予的角色。如果当前 `FROM_ROLE` 被授予用户，则返回 `NULL`。 |
| TO_USER   | 当前 `FROM_ROLE` 被授予的用户。如果当前 `FROM_ROLE` 被授予角色，则返回 `NULL`。 |

示例：

```Plain
MySQL > SELECT * FROM sys.role_edges;
+------------+------------+---------------+
| FROM_ROLE  | TO_ROLE    | TO_USER       |
+------------+------------+---------------+
| dba_leader | director   | NULL          |
| dba        | dba_leader | NULL          |
| dba        | NULL       | 'aneesh'@'%'  |
| dba        | NULL       | 'chelsea'@'%' |
| dba_leader | NULL       | 'albert'@'%'  |
| director   | NULL       | 'stan'@'%'    |
| root       | NULL       | 'root'@'%'    |
+------------+------------+---------------+
```

下图说明了以上示例中角色授予的嵌套关系：

![role_edges](../../_assets/role_edges.png)