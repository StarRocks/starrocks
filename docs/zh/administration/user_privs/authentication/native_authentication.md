---
displayed_sidebar: docs
sidebar_position: 10
---

# 本地身份验证

通过 SQL 命令在 StarRocks 中使用本地身份验证创建和管理用户。

StarRocks 本地身份验证是一种基于密码的身份验证方法。此外，StarRocks 还支持与外部身份验证系统集成，如 LDAP、OpenID Connect 和 OAuth 2.0。更多说明，请参见[Authenticate with Security Integration](./security_integration.md)。

:::note

具有系统定义角色 `user_admin` 的用户可以在 StarRocks 中创建用户、修改用户和删除用户。

:::

## 创建用户

您可以通过指定用户身份、身份验证方法以及可选的默认角色来创建用户。要为用户启用本地身份验证，您需要明确指定明文或密文密码。

以下示例创建用户 `jack`，仅允许其从 IP 地址 `172.10.1.10` 连接，启用本地身份验证，将密码设置为明文 `12345`，并将角色 `example_role` 作为其默认角色分配给它：

```SQL
CREATE USER jack@'172.10.1.10' IDENTIFIED BY '12345' DEFAULT ROLE 'example_role';
```

:::note
- StarRocks 在存储用户密码之前会对其进行加密。您可以使用 password() 函数获取加密后的密码。
- 如果在创建用户时未指定默认角色，则会为用户分配系统定义的默认角色 `PUBLIC`。
:::

用户的默认角色在连接到 StarRocks 时会自动激活。有关如何在连接后为用户启用所有（默认和授予的）角色的说明，请参见[启用所有角色](../authorization/User_privilege.md#启用所有角色)。

有关创建用户的更多信息和高级说明，请参见[CREATE USER](../../../sql-reference/sql-statements/account-management/CREATE_USER.md)。

## 变更用户

您可以更改用户的密码、默认角色或属性。

有关如何更改用户默认角色的说明，请参见[更改用户的默认角色](../authorization/User_privilege.md#更改用户的默认角色)。

### 变更用户属性

您可以使用 [ALTER USER](../../../sql-reference/sql-statements/account-management/ALTER_USER.md) 设置用户的属性。

以下示例将用户 `jack` 的最大连接数设置为 `1000`。具有相同用户名的用户身份共享相同的属性。

因此，您只需为 `jack` 设置属性，此设置将对所有具有用户名 `jack` 的用户身份生效。

```SQL
ALTER USER 'jack' SET PROPERTIES ("max_user_connections" = "1000");
```

### 重置用户密码

您可以使用 [SET PASSWORD](../../../sql-reference/sql-statements/account-management/SET_PASSWORD.md) 或 [ALTER USER](../../../sql-reference/sql-statements/account-management/ALTER_USER.md) 重置用户的密码。

> **NOTE**
>
> - 任何用户都可以在不需要任何权限的情况下重置自己的密码。
> - 只有 `root` 用户本身可以设置其密码。如果您丢失了其密码并且无法连接到 StarRocks，请参见[重置遗失的 root 用户密码](#重置遗失的-root-用户密码) 获取更多说明。

以下两个示例都将 `jack` 的密码重置为 `54321`：

- 使用 SET PASSWORD 重置密码：

  ```SQL
  SET PASSWORD FOR jack@'172.10.1.10' = PASSWORD('54321');
  ```

- 使用 ALTER USER 重置密码：

  ```SQL
  ALTER USER jack@'172.10.1.10' IDENTIFIED BY '54321';
  ```

#### 重置遗失的 root 用户密码

如果您丢失了 `root` 用户的密码并且无法连接到 StarRocks，可以通过以下步骤重置：

1. 在 **所有 FE 节点** 的配置文件 **fe/conf/fe.conf** 中添加以下配置项以禁用用户身份验证：

   ```YAML
   enable_auth_check = false
   ```

2. 重启 **所有 FE 节点** 以使配置生效。

   ```Bash
   ./fe/bin/stop_fe.sh
   ./fe/bin/start_fe.sh
   ```

3. 从 MySQL 客户端通过 `root` 用户连接到 StarRocks。禁用用户身份验证时，无需指定密码。

   ```Bash
   mysql -h <fe_ip_or_fqdn> -P<fe_query_port> -uroot
   ```

4. 重置 `root` 用户的密码。

   ```SQL
   SET PASSWORD for root = PASSWORD('xxxxxx');
   ```

5. 通过在 **所有 FE 节点** 的配置文件 **fe/conf/fe.conf** 中将配置项 `enable_auth_check` 设置为 `true` 来重新启用用户身份验证。

   ```YAML
   enable_auth_check = true
   ```

6. 重启 **所有 FE 节点** 以使配置生效。

   ```Bash
   ./fe/bin/stop_fe.sh
   ./fe/bin/start_fe.sh
   ```

7. 从 MySQL 客户端使用 `root` 用户和新密码连接到 StarRocks，以验证密码是否重置成功。

   ```Bash
   mysql -h <fe_ip_or_fqdn> -P<fe_query_port> -uroot -p<xxxxxx>
   ```

## 删除用户

您可以使用 [DROP USER](../../../sql-reference/sql-statements/account-management/DROP_USER.md) 删除用户。

以下示例删除用户 `jack`：

```SQL
DROP USER jack@'172.10.1.10';
```

## 查看用户

您可以使用 SHOW USERS 查看 StarRocks 集群中的所有用户。

```SQL
SHOW USERS;
```

## 查看用户属性

您可以使用 [SHOW PROPERTY](../../../sql-reference/sql-statements/account-management/SHOW_PROPERTY.md) 查看用户的属性。

以下示例显示用户 `jack` 的属性：

```SQL
SHOW PROPERTY FOR 'jack';
```

或者查看特定属性：

```SQL
SHOW PROPERTY FOR 'jack' LIKE 'max_user_connections';
```
