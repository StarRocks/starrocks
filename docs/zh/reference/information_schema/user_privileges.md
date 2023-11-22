---
displayed_sidebar: "Chinese"
---

# user_privileges

`user_privileges` 提供有关用户权限的信息。

`user_privileges` 提供以下字段：

| 字段           | 描述                                                         |
| -------------- | ------------------------------------------------------------ |
| GRANTEE        | 被授予权限的用户的名称。                                     |
| TABLE_CATALOG  | 目录的名称。该值始终为 def。                                 |
| PRIVILEGE_TYPE | 授予的权限。该值可以是可在全局级别授予的任何权限。           |
| IS_GRANTABLE   | 如果用户具有 GRANT OPTION 权限，则为 YES，否则为 NO。输出不会将 GRANT OPTION 作为 PRIVILEGE_TYPE='GRANT OPTION' 的单独行列出。 |
