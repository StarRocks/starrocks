---
displayed_sidebar: docs
---

# table_privileges

:::note

该视图不适用于 StarRocks 当前支持的功能。

:::

`table_privileges` 提供有关表权限的信息。

`table_privileges` 提供以下字段：

| 字段           | 描述                                                         |
| -------------- | ------------------------------------------------------------ |
| GRANTEE        | 授予权限的用户的名称。                                       |
| TABLE_CATALOG  | 表所属的目录的名称。该值始终为 def。                         |
| TABLE_SCHEMA   | 表所属的数据库的名称。                                       |
| TABLE_NAME     | 表的名称。                                                   |
| PRIVILEGE_TYPE | 所授予的权限。该值可以是可在表级别授予的任何权限。           |
| IS_GRANTABLE   | 如果用户具有 GRANT OPTION 权限，则为 YES；否则为 NO。输出不会将 GRANT OPTION 作为单独的行列出，而是使用 PRIVILEGE_TYPE='GRANT OPTION'。 |

