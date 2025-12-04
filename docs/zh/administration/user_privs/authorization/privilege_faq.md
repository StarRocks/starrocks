---
displayed_sidebar: docs
sidebar_position: 50
---

# 权限系统 FAQ

## 为什么已经赋予角色给用户，但用户执行 SQL 时仍旧报错没有权限？

有可能是因为用户没有激活该角色。每个用户都可以通过 `select current_role();` 命令来查看当前会话下已经激活的角色。

如果想要手动激活角色，可以在当前会话下使用 [SET ROLE](../../../sql-reference/sql-statements/account-management/SET_ROLE.md) 命令，激活该角色进行相关操作。

如果希望登录时自动激活角色，user_admin 管理员可以通过 [SET DEFAULT ROLE](../../../sql-reference/sql-statements/account-management/SET_DEFAULT_ROLE.md) 或者 [ALTER USER DEFAULT ROLE](../../../sql-reference/sql-statements/account-management/ALTER_USER.md) 为每个用户设置默认角色。设置成功后，用户重新登录即可自动激活默认角色。

如果希望系统内部所有用户都能够在登录时默认激活所有拥有的角色，可以执行如下操作。该操作需要 System 层的 OPERATE 权限。

```SQL
SET GLOBAL activate_all_roles_on_login = TRUE;
```

不过，我们还是建议您能够使用最小权限原则，为用户设置拥有相对较小权限的默认角色来规避潜在的误操作。例如对于普通用户，将仅有 SELECT 权限的 read_only 角色设置为默认角色，不将拥有 ALTER、DROP、INSERT 等权限的角色设置为默认角色；对于管理员，将 db_admin 设置为默认角色，不将可以上线和下线节点的 node_admin 设置为默认角色。

您可以执行 [GRANT](../../../sql-reference/sql-statements/account-management/GRANT.md) 命令进行授权。

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

## 访问 StarRocks Web 控制台 `http://<fe_ip>:<fe_http_port>` 需要哪些权限？

用户必须具有 `cluster_admin` 角色。

## StarRocks v3.0 前后的权限保留机制有何变化？

在 v3.0 之前，用户在表上被授予权限后，即使表被删除并重新创建，权限仍然会保留。从 v3.0 开始，删除并重新创建表后，权限将不再保留。

## 如何在 StarRocks 中查询用户和授予的权限？

您可以通过查询系统视图 `sys.grants_to_users` 或执行 SHOW USERS 获取完整的用户列表，然后使用 `SHOW GRANTS FOR <user_identity>` 单独查询每个用户。

## 查询大量用户和表的权限元数据的系统视图时，对 FE 资源有何影响？

当用户或表的数量非常大时，查询系统视图 `sys.grants_to_users`、`sys.grants_to_roles` 和 `sys.role_edges` 可能需要很长时间。这些视图是实时计算的，会消耗一定比例的 FE 资源。因此，不建议在大规模上频繁运行此类操作。

## 重新创建一个 catalog 会导致权限丢失吗？应如何备份和恢复权限？

会的。重新创建一个 catalog 会导致其相关权限丢失。您应先备份所有用户权限，并在重新创建 catalog 后恢复它们。

## 是否有支持自动权限迁移的工具？

目前没有。用户必须手动使用 SHOW GRANTS 为每个用户备份和恢复权限。

## 使用 KILL 命令是否有限制？可以限制为仅终止用户自己的查询吗？

是的。KILL 命令现在需要 OPERATE 权限，并且用户只能终止自己发起的查询。

## 为什么在重命名或删除表后授予的权限会发生变化？系统能否在添加重命名表的权限时保留旧权限？

对于内表，权限与表 ID 绑定，而不是表名。这确保了数据安全，因为表名可以随意更改。如果权限跟随表名，可能会导致数据泄漏。同样，当表被删除时，其权限也会被移除，因为对象不再存在。

对于外部表，历史版本的行为与内表相同。然而，由于外部表元数据不由 StarRocks 管理，可能会出现延迟或权限丢失。为了解决这个问题，未来版本将对外部表使用基于表名的权限管理，这与预期行为一致。

## 如何备份用户权限？

以下是备份集群中用户权限信息的示例脚本。

```Bash
#!/bin/bash

# MySQL 连接信息
HOST=""
PORT="9030"
USER="root"
PASSWORD=""  
OUTPUT_FILE="user_privileges.txt"

# 清空输出文件
> $OUTPUT_FILE

# 获取用户列表
users=$(mysql -h$HOST -P$PORT -u$USER -p$PASSWORD -e "SHOW USERS;" | sed -e '1d' -e '/^+/d')

# 遍历用户并获取权限
for user in $users; do
    echo "Privileges for $user:" >> $OUTPUT_FILE
    mysql -h$HOST -P$PORT -u$USER -p$PASSWORD -e "SHOW GRANTS FOR $user;" >> $OUTPUT_FILE
    echo "" >> $OUTPUT_FILE
done

echo "All user privileges have been written to $OUTPUT_FILE"
```

## 为什么在普通函数上授予 USAGE 时会出现错误“Unexpected input 'IN', the most similar input is {'TO'}.”？在函数上授予权限的正确方法是什么？

普通函数不能在所有数据库中使用 IN 授予权限；它们只能在当前数据库中授予权限。而全局函数是在所有数据库范围内授予的。