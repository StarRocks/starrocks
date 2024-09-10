---
displayed_sidebar: docs
---

# 权限系统 FAQ

## 为什么已经赋予角色给用户，但用户执行 SQL 时仍旧报错没有权限？

有可能是因为用户没有激活该角色。每个用户都可以通过 `select current_role();` 命令来查看当前会话下已经激活的角色。

如果想要手动激活角色，可以在当前会话下使用 [SET ROLE](../../sql-reference/sql-statements/account-management/SET_ROLE.md) 命令，激活该角色进行相关操作。

如果希望登录时自动激活角色，user_admin 管理员可以通过 [SET DEFAULT ROLE](../../sql-reference/sql-statements/account-management/SET_DEFAULT_ROLE.md) 或者 [ALTER USER DEFAULT ROLE](../../sql-reference/sql-statements/account-management/ALTER_USER.md) 为每个用户设置默认角色。设置成功后，用户重新登录即可自动激活默认角色。

如果希望系统内部所有用户都能够在登录时默认激活所有拥有的角色，可以执行如下操作。该操作需要 System 层的 OPERATE 权限。

```SQL
SET GLOBAL activate_all_roles_on_login = TRUE;
```

不过，我们还是建议您能够使用最小权限原则，为用户设置拥有相对较小权限的默认角色来规避潜在的误操作。例如对于普通用户，将仅有 SELECT 权限的 read_only 角色设置为默认角色，不将拥有 ALTER、DROP、INSERT 等权限的角色设置为默认角色；对于管理员，将 db_admin 设置为默认角色，不将可以上线和下线节点的 node_admin 设置为默认角色。

您可以执行 [GRANT](../../sql-reference/sql-statements/account-management/GRANT.md) 命令进行授权。

## 为什么赋予了用户数据库下所有表的权限，`GRANT ALL ON ALL TABLES IN DATABASE <db_name> TO USER <user_identity>;`，用户仍旧不能在数据库里创建表？

在数据库下创建表是数据库级别的权限，您需要赋权：

```SQL
GRANT CREATE TABLE ON DATABASE <db_name> TO USER <user_identity>;;
```

## 为什么赋予了用户数据库的所有权限，`GRANT ALL ON DATABASE <db_name> TO USER <user_identity>;`，在数据库下 `SHOW TABLES;` 什么也不返回？

`SHOW TABLES;` 只会返回当前用户有任意权限的表。以 SELECT 权限为例，您可以进行如下赋权：

```SQL
GRANT SELECT ON ALL TABLES IN DATABASE <db_name> TO USER <user_identity>;
```

这个语句与 3.0 之前版本的 `GRANT select_priv ON db.* TO <user_identity>;` 是等价的。
