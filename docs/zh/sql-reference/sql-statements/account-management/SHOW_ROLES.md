---
displayed_sidebar: "Chinese"
---

# SHOW ROLES

import UserManagementPriv from '../../../assets/commonMarkdown/userManagementPriv.md'

## 功能

查看当前系统中的所有角色。如果要查看角色的权限信息，可以使用 `SHOW GRANTS FOR ROLE <role_name>;`，具体参见 [SHOW GRANTS](SHOW_GRANTS.md)。

该命令从 3.0 版本开始支持。

<UserManagementPriv />

## 语法

```SQL
SHOW ROLES
```

返回字段说明：

| **字段** | **描述**   |
| -------- | ---------- |
| Name     | 角色名称。 |

## 示例

查看当前系统中的所有角色。

```Plain
mysql> SHOW ROLES;
+---------------+
| Name          |
+---------------+
| root          |
| db_admin      |
| cluster_admin |
| user_admin    |
| public        |
| testrole      |
+---------------+
```

## 相关参考

- [CREATE ROLE](CREATE_ROLE.md)
- [ALTER USER](ALTER_USER.md)
- [SET ROLE](SET_ROLE.md)
- [DROP ROLE](DROP_ROLE.md)
