---
displayed_sidebar: docs
---

# column_privileges

:::note

该视图不适用于 StarRocks 当前支持的功能。

:::

`column_privileges` 用于识别当前启用的角色被授予的或由当前启用的角色授予的所有列权限。

`column_privileges` 提供以下字段：

| 字段           | 描述                                                         |
| -------------- | ------------------------------------------------------------ |
| GRANTEE        | 被授予权限的用户的名称。                                     |
| TABLE_CATALOG  | 包含列的表所属的目录的名称。该值始终为 `def`。               |
| TABLE_SCHEMA   | 包含列的表所属的模式（数据库）的名称。                       |
| TABLE_NAME     | 包含列的表的名称。                                           |
| COLUMN_NAME    | 列的名称。                                                   |
| PRIVILEGE_TYPE | 被授予的权限。该值可以是在列级别授予的任何权限。每一行列出一个权限，因此对于被授予者拥有的每个列权限，都有一行。 |
| IS_GRANTABLE   | 如果用户具有 `GRANT OPTION` 权限，则为 `YES`，否则为 `NO`。输出不将 `GRANT OPTION` 列为单独的行，其 `PRIVILEGE_TYPE` 为 `'GRANT OPTION'`。 |
