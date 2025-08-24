---
displayed_sidebar: docs
---

# applicable_roles

`applicable_roles` 提供有关适用于当前用户的角色的信息。

`applicable_roles` 提供以下字段：

| **字段**       | **描述**                                         |
| -------------- | ------------------------------------------------ |
| USER           | 适用角色的用户。                                 |
| HOST           | 用户连接的主机。                                 |
| GRANTEE        | 授予角色的用户或角色。                           |
| GRANTEE_HOST   | 授权者连接的主机。                               |
| ROLE_NAME      | 角色的名称。                                     |
| ROLE_HOST      | 与角色关联的主机。                               |
| IS_GRANTABLE   | 角色是否可以授予给其他人（`YES` 或 `NO`）。      |
| IS_DEFAULT     | 角色是否为默认角色（`YES` 或 `NO`）。            |
| IS_MANDATORY   | 角色是否为强制角色（`YES` 或 `NO`）。            |
