---
displayed_sidebar: "Chinese"
---

# schema_privileges

`schema_privileges` 提供有关数据库权限的信息。

`schema_privileges` 提供以下字段：

| 字段           | 描述                                                         |
| -------------- | ------------------------------------------------------------ |
| GRANTEE        | 授予权限的用户的名称。                                       |
| TABLE_CATALOG  | 模式所属的目录的名称。该值始终为 def。                       |
| TABLE_SCHEMA   | 模式的名称。                                                 |
| PRIVILEGE_TYPE | 所授予的权限。每一行列出一个权限，因此每个由授予权限的用户拥有的模式权限都有一行。 |
| IS_GRANTABLE   | 如果用户具有 GRANT OPTION 权限，则为 YES；否则为 NO。输出不会将 GRANT OPTION 作为单独的行列出，而是使用 PRIVILEGE_TYPE='GRANT OPTION'。 |
