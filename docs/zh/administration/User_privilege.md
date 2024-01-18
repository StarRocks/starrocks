---
displayed_sidebar: "Chinese"
---

# 管理用户权限

本文介绍如何管理 StarRocks 集群中的用户权限。

StarRocks 的权限管理系统参照了 MySQL 的权限管理机制，支持表级别细粒度的权限控制、基于角色的权限访问控制，以及白名单机制。

## 创建用户

通过以下命令创建 StarRocks 用户。

> 注意
>
> 拥有 ADMIN 权限，或任意层级的 GRANT 权限的用户才可以创建新用户。

```sql
CREATE USER user_identity [auth_option] [DEFAULT ROLE 'role_name'];
```

参数：

* `user_identity`：用户标识。以 `username@'userhost'` 或 `username@['domain']` 的形式标明。
* `auth_option`：认证方式。可选方式包括：
  * `IDENTIFIED BY 'auth_string'`
  * `IDENTIFIED WITH auth_plugin`
  * `IDENTIFIED WITH auth_plugin BY 'auth_string'`
  * `IDENTIFIED WITH auth_plugin AS 'auth_string'`
* `DEFAULT ROLE`：当前用户的默认角色。

示例：

```sql
-- 创建一个无密码用户，且不指定 host。
CREATE USER 'jack';
-- 使用明文密码创建用户，允许其从 '172.16.1.10' 登录。
CREATE USER jack@'172.16.1.10' IDENTIFIED WITH mysql_native_password BY '123456';
-- 使用暗文密码创建用户。
CREATE USER jack@'172.16.1.10' IDENTIFIED BY PASSWORD '*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9';
-- 创建一个 LDAP 认证的用户，并指定用户在 LDAP 中的 Distinguished Name (DN)。
CREATE USER jack@'172.16.1.10' IDENTIFIED WITH authentication_ldap_simple AS 'uid=jack,ou=company,dc=example,dc=com';
-- 创建一个允许从 '192.168' 子网登录的用户，同时指定其角色为 example_role。
CREATE USER 'jack'@'192.168.%' DEFAULT ROLE 'example_role';
-- 创建一个允许从域名 'example_domain' 登录的用户。
CREATE USER 'jack'@['example_domain'] IDENTIFIED BY '12345';
```

> 说明
> 您可以通过 `PASSWORD()` 方法获得密文密码。

## 修改用户密码

通过以下命令修改用户登录密码。

> **注意**
>
> * 拥有 ADMIN 权限，或者 GLOBAL 层级 GRANT 权限的用户，可以设置任意用户的密码。
> * 普通用户可以设置自己对应的 User Identity 的密码。自己对应的 User Identity 可以通过 SELECT CURRENT_USER(); 命令查看。
> * 拥有非 GLOBAL 层级 GRANT 权限的用户，不可以设置已存在用户的密码，仅能在创建用户时指定密码。
> * 除了 root 用户自身，任何用户都不能重置 root 用户的密码。

```sql
SET PASSWORD [FOR user_identity] = [PASSWORD('plain password')]|['hashed password'];
```

示例：

```sql
-- 修改当前用户的密码。
SET PASSWORD = PASSWORD('123456')
SET PASSWORD = '*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9'
-- 修改指定用户密码。
SET PASSWORD FOR 'jack'@'192.%' = PASSWORD('123456')
SET PASSWORD FOR 'jack'@['domain'] = '*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9'
```

> **说明**
>
> 您可以通过 `PASSWORD()` 方法获得暗文密码。

### 修改 root 用户密码

如果您忘记 `root` 用户密码，您可以通过以下步骤重置密码：

1. 在**所有 FE 节点**的配置文件 **fe/conf/fe.conf** 中添加以下配置项以关闭 FE 鉴权：

    ```plain
    enable_auth_check = false
    ```

2. 重启**所有 FE 节点**。

    ```shell
    ./fe/bin/stop_fe.sh
    ./fe/bin/start_fe.sh
    ```

3. 通过 MySQL 客户端登录 `root` 用户，无需密码。

    ```shell
    mysql -h <fe_ip> -P<fe_query_port> -uroot
    ```

4. 修改 `root` 用户密码。

    ```sql
    SET PASSWORD for root = PASSWORD('xxxxxx');
    ```

5. 在**所有 FE 节点**的配置文件 **fe/conf/fe.conf** 中修改配置项 `enable_auth_check` 为 `true` 以重新开启 FE 鉴权。

    ```plain
    enable_auth_check = true
    ```

6. 重启**所有 FE 节点**。

    ```shell
    ./fe/bin/stop_fe.sh
    ./fe/bin/start_fe.sh
    ```

7. 通过 MySQL 客户端登录 `root` 用户验证密码是否修改成功。

    ```shell
    mysql -h <fe_ip> -P<fe_query_port> -uroot -p<xxxxxx>
    ```

## 删除用户

通过以下命令删除 StarRocks 用户。

> **注意**
>
> 拥有 ADMIN 权限的用户可以删除用户。

```sql
DROP USER 'user_identity';
```

## 授予权限

通过以下命令授予指定用户或角色指定的权限。

> 注意
>
> * 拥有 ADMIN 权限，或者 GLOBAL 层级 GRANT 权限的用户，可以授予任意**用户**的权限。
> * 拥有 DATABASE 层级 GRANT 权限的用户，可以授予任意用户对指定**数据库**的权限。
> * 拥有 TABLE 层级 GRANT 权限的用户，可以授予任意用户对指定数据库中**指定表**的权限。
> * ADMIN_PRIV 权限只能在 GLOBAL 层级授予或撤销。
> * 拥有 GLOBAL 层级 GRANT_PRIV 实际等同于拥有 ADMIN_PRIV，因为该层级的 GRANT_PRIV 有授予任意权限的权限，请谨慎授予该权限。

```sql
-- 授予指定用户数据库级或表级权限。
GRANT privilege_list ON db_name.tbl_name TO user_identity [ROLE role_name];
-- 授予指定用户指定资源权限。
GRANT privilege_list ON RESOURCE resource_name TO user_identity [ROLE role_name];
```

参数：

* `privilege_list`：需要赋予的权限列表，以逗号分隔。
  * NODE_PRIV：节点变更权限。包括 FE、BE、BROKER 节点的添加、删除、下线等操作。目前该权限只能授予 root 用户。
  * GRANT_PRIV：权限变更权限。允许执行包括授权、撤权、添加/删除/变更 用户/角色等操作。
  * SELECT_PRIV：对数据库、表的只读权限。
  * LOAD_PRIV：对数据库、表的写权限。包括 LOAD、INSERT、DELETE 等。
  * ALTER_PRIV：对数据库、表的更改权限。包括重命名库/表、添加/删除/变更 列、添加/删除分区等操作。
  * CREATE_PRIV：创建数据库、表、视图的权限。
  * DROP_PRIV：删除数据库、表、视图的权限。
  * USAGE_PRIV：资源的使用权限。
* `db_name`：数据库名。
* `tbl_name`：表名。`db_name.tbl_name` 支持以下格式：
  * `*.*`：所有数据库及库中所有表，即全局权限。
  * `db.*`：指定数据库及库中所有表。
  * `db.tbl`：指定数据库下的指定表。
* `user_identity`：用户标识，将权限赋予给指定用户。指定的用户必须存在。
* `role_name`：角色名称，将权限赋予给指定角色。指定的角色必须存在。

示例：

```sql
-- 授予所有库和表的查询权限给用户。
GRANT SELECT_PRIV ON *.* TO 'jack'@'%';
-- 授予指定库表的查询、修改、导入权限给用户。
GRANT SELECT_PRIV,ALTER_PRIV,LOAD_PRIV ON db1.tbl1 TO 'jack'@'192.8.%';
-- 授予指定数据库下所有表的导入权限给角色。
GRANT LOAD_PRIV ON db1.* TO ROLE 'my_role';
-- 授予所有资源的使用权限给用户。
GRANT USAGE_PRIV ON RESOURCE * TO 'jack'@'%';
-- 授予指定资源的使用权限给用户。
GRANT USAGE_PRIV ON RESOURCE 'spark_resource' TO 'jack'@'%';
-- 授予指定资源的使用权限给角色。
GRANT USAGE_PRIV ON RESOURCE 'spark_resource' TO ROLE 'my_role';
```

## 撤销权限

通过以下命令撤销用户或角色指定权限。

> 注意
>
> * 拥有 ADMIN 权限，或者 GLOBAL 层级 GRANT 权限的用户，可以撤销任意**用户**的权限。
> * 拥有 DATABASE 层级 GRANT 权限的用户，可以撤销任意用户对指定**数据库**的权限。
> * 拥有 TABLE 层级 GRANT 权限的用户，可以撤销任意用户对指定数据库中**指定表**的权限。

```sql
-- 撤销指定用户数据库级或表级权限。
REVOKE privilege_list ON db_name.tbl_name FROM user_identity [ROLE role_name];
-- 撤销指定用户指定资源权限。
REVOKE privilege_list ON RESOURCE resource_name FROM user_identity [ROLE role_name];
```

## 创建角色

您可以对创建好的角色可以进行授权操作，拥有该角色的用户会拥有角色被赋予的权限。

通过以下命令创建指定角色。

> 注意
>
> 拥有 ADMIN 权限的用户才可以创建角色。

```sql
CREATE ROLE role_name;
```

## 查看角色

通过以下命令查看已创建的角色。

```sql
SHOW ROLES;
```

## 删除角色

> 注意
>
> 拥有 GRANT_PRIV 或 ADMIN_PRIV 权限的用户可以删除角色。

通过以下命令删除指定角色。

```sql
DROP ROLE role_name;
```

## 查看用户权限

您可以查看所有用户或指定用户的权限。

* 查看所有用户的权限。

```sql
SHOW ALL GRANTS;
```

* 查看指定用户的权限。

```sql
SHOW GRANTS FOR user_identity;
```

## 查看用户属性

通过以下命令查看用户属性。

```sql
SHOW PROPERTY [FOR user] [LIKE key];
```

参数：

* `user`：用户名。
* `LIKE`：相关属性关键字。

示例：

```plain text
-- 查看指定用户的属性。
SHOW PROPERTY FOR 'jack';
+------------------------+-------+
| Key                    | Value |
+------------------------+-------+
| default_load_cluster   |       |
| max_user_connections   | 100   |
| quota.high             | 800   |
| quota.low              | 100   |
| quota.normal           | 400   |
| resource.cpu_share     | 1000  |
| resource.hdd_read_iops | 80    |
| resource.hdd_read_mbps | 30    |
| resource.io_share      | 1000  |
| resource.ssd_read_iops | 1000  |
| resource.ssd_read_mbps | 30    |
+------------------------+-------+
-- 看指定用户导入 cluster 相关属性。
SHOW PROPERTY FOR 'jack' LIKE '%load_cluster%';
```

## 名词与概念

### 名词解释

* 用户标识 User Identity

在 StarRocks 权限系统中，一个用户被识别为一个用户标识（User Identity）。用户标识由 `username` 和 `userhost` 共同组成。其中 `username` 为用户名，由英文大小写组成。`userhost` 表示该用户链接来自的 IP。用户标识以 `username@'userhost'` 的方式呈现，表示来自 `userhost` 的 `username`。

用户标识的另一种表现方式为 `username@['domain']`，其中 `domain` 为域名，可以通过 DNS 解析为一组 IP。最终表现为一组 `username@'userhost'`。

以下示例统一使用 `username@'userhost'` 表示用户标识。

* 权限 Privilege

不同的权限代表不同的操作许可。权限作用的对象是节点、数据库或表。

* 角色 Role

StarRocks 可以创建自定义命名的角色。角色可以被看做是一组权限的集合。新创建的用户可以被赋予某一角色，从而自动被赋予该角色所拥有的权限。后续对角色的权限变更，也会体现在所有属于该角色的用户权限上。

* 用户属性 User Property

用户属性直接附属于某一用户，而非用户标识。即 `user1@'192.%'` 和 `user1@['domain']` 都拥有同一组用户属性，该属性属于用户 `user1`，而非 `user1@'192.%'` 或 `user1@['domain']`。用户属性包括但不限于：用户最大连接数、导入集群配置等等。

### 相关命令

* 创建用户：`CREATE USER`
* 删除用户：`DROP USER`
* 授权：`GRANT`
* 撤权：`REVOKE`
* 创建角色：`CREATE ROLE`
* 删除角色：`DROP ROLE`
* 查看当前用户权限：`SHOW GRANTS`
* 查看所有用户权限：`SHOW ALL GRANTS`
* 查看已创建的角色：`SHOW ROLES`
* 查看用户属性：`SHOW PROPERTY`

### 权限类型

StarRocks 目前支持以下几种权限：

* NODE_PRIV：节点变更权限。包括 FE、BE、BROKER 节点的添加、删除、下线等操作。目前该权限只能授予 Root 用户。
* GRANT_PRIV：权限变更权限。允许执行包括授权、撤权、添加/删除/变更 用户/角色等操作。
* SELECT_PRIV：对数据库、表的只读权限。
* LOAD_PRIV：对数据库、表的写权限。包括 Load、Insert、Delete 等。
* ALTER_PRIV：对数据库、表的更改权限。包括重命名库/表、添加/删除/变更列、添加/删除分区等操作。
* CREATE_PRIV：创建数据库、表、视图的权限。
* DROP_PRIV：删除数据库、表、视图的权限。
* USAGE_PRIV：资源的使用权限。

### 权限层级

根据权限适用范围的不同，StarRocks 将库表的权限分为以下三个层级：

* GLOBAL LEVEL：全局权限。即通过 GRANT 语句授予的 *.* 上的权限。被授予的权限适用于任意数据库中的任意表。
* DATABASE LEVEL：数据库级权限。即通过 GRANT 语句授予的 db.* 上的权限。被授予的权限适用于指定数据库中的任意表。
* TABLE LEVEL：表级权限。即通过 GRANT 语句授予的 db.tbl 上的权限。被授予的权限适用于指定数据库中的指定表。

将资源的权限分为以下两个层级：

* GLOBAL LEVEL：全局权限。即通过 GRANT 语句授予的 * 上的权限。被授予的权限适用于资源。
* RESOURCE LEVEL: 资源级权限。即通过 GRANT 语句授予的 resource_name 上的权限。被授予的权限适用于指定资源。

### 权限说明

ADMIN_PRIV 和 GRANT_PRIV 权限同时拥有授予权限的权限，较为特殊。这里对和这两个权限相关的操作逐一说明。

* `CREATE USER`

  * 拥有 ADMIN 权限，或任意层级的 GRANT 权限的用户可以创建新用户。

* `DROP USER`

  * 只有 ADMIN 权限可以删除用户。

* `CREATE ROLE`/`DROP ROLE`

  * 只有拥有 GRANT_PRIV 或 ADMIN_PRIV 权限可以创建角色。

* `GRANT/REVOKE`

  * 拥有 ADMIN 权限，或者 GLOBAL 层级 GRANT 权限的用户，可以授予或撤销任意**用户**的权限。
  * 拥有 DATABASE 层级 GRANT 权限的用户，可以授予或撤销任意用户对指定**数据库**的权限。
  * 拥有 TABLE 层级 GRANT 权限的用户，可以授予或撤销任意用户对指定数据库中**指定表**的权限。

* `SET PASSWORD`

  * 拥有 ADMIN 权限，或者 GLOBAL 层级 GRANT 权限的用户，可以设置任意用户的密码。
  * 普通用户可以设置自己对应的 User Identity 的密码。自己对应的 User Identity 可以通过 SELECT CURRENT_USER(); 命令查看。
  * 拥有非 GLOBAL 层级 GRANT 权限的用户，不可以设置已存在用户的密码，仅能在创建用户时指定密码。

### 其他说明

StarRocks 初始化时，会自动创建如下角色和用户：

* **角色**
  * operator 角色：该角色的用户有且只有一个，拥有 NODE_PRIV 和 ADMIN_PRIV，即对 StarRocks 的所有权限。后续某个升级版本中，可能会将该角色的权限限制为 NODE_PRIV，即仅授予节点变更权限。以满足某些云上部署需求。
  * admin 角色：该角色拥有 ADMIN_PRIV，即除节点变更以外的所有权限。您可以创建多个 admin 角色。
* **用户**
  * root@'%'：root 用户，允许从任意节点登录，角色为 operator。
  * admin@'%'：admin 用户，允许从任意节点登录，角色为 admin。

> 说明
> StarRocks 不支持删除或更改默认创建的角色或用户的权限。

一些可能产生冲突的操作说明：

* **域名与 IP 冲突**

假设创建了如下用户 `CREATE USER user1@['domain'];`，并且授权 `GRANT SELECT_PRIV ON` `*.*` `TO user1@['domain']`。此时该 `domain` 被解析为两个 IP：`ip1` 和 `ip2`。

假设之后我们对 `user1@'ip1'` 进行一次单独授权 `GRANT ALTER_PRIV ON` `*.*` `TO user1@'ip1';`，则 `user1@'ip1'` 的权限会被修改为 SELECT_PRIV， ALTER_PRIV，而且当我们再次变更 `user1@['domain']` 的权限时，`user1@'ip1'` 也不会跟随改变。

* **重复IP冲突**

假设创建了如下用户 `CREATE USER user1@'%' IDENTIFIED BY "12345";` 以及 `CREATE USER user1@'192.%' IDENTIFIED BY "abcde";`。

在优先级上，`'192.%'` 优先于 `'%'`，因此，当用户 `user1` 从 `192.168.1.1` 这台机器尝试使用密码 `'12345'` 登录 StarRocks 时会被拒绝。

* **忘记密码**

如果忘记密码无法登录 StarRocks，您可以在 StarRocks FE 节点所在机器，使用如下命令无密码登录 StarRocks：`mysql-client -h 127.0.0.1 -P query_port -uroot`。登录后，您可以通过 `SET PASSWORD` 命令重置密码。

关于 `current_user()` 和 `user()`：

用户可以通过 `SELECT current_user();` 和 `SELECT user();` 分别查看 `current_user` 和 `user`。其中 `current_user` 表示当前用户是以哪种身份通过认证系统的，而 `user` 则是用户当前实际的用户标识。

例如，假设创建了 `user1@'192.%'` 用户，然后来自 `192.168.10.1` 的用户 `user1` 登录了系统，则此时的 `current_user` 为 `user1@'192.%'`，而 `user` 为 `user1@'192.168.10.1'`。

所有权限都是赋予某个 `current_user` 的，真实用户拥有对应的 `current_user` 的所有权限。

## 最佳实践

这里列举一些 StarRocks 权限系统的使用场景。

### 场景一：权限分配

StarRocks 集群的使用者分为管理员（Admin）、开发工程师（RD）和用户（Client）。其中管理员拥有整个集群的所有权限，主要负责集群的搭建、节点管理等。开发工程师负责业务建模，包括建库建表、数据的导入和修改等。用户访问不同的数据库和表来获取数据。

在这种场景下，可以为管理员赋予 ADMIN 权限或 GRANT 权限。对 RD 赋予对任意或指定数据库表的 CREATE、DROP、ALTER、LOAD、SELECT 权限。对 Client 赋予对任意或指定数据库表 SELECT 权限。同时，也可以通过创建不同的角色，来简化对多个用户的授权操作。

### 场景二：多业务线

一个集群内有多个业务，每个业务可能使用一个或多个数据。每个业务需要管理自己的用户。在这种场景下，管理员用户可以为每个数据库创建一个拥有 DATABASE 层级 GRANT 权限的用户。该用户仅可以对用户进行指定的数据库的授权。

### 场景三：黑名单

StarRocks 本身不支持黑名单，只有白名单功能，但我们可以通过某些方式来模拟黑名单。假设先创建了名为 `user@'192.%'` 的用户，表示允许来自 `192.*` 的用户登录。此时如果想禁止来自 `192.168.10.1` 的用户登录。则可以再创建一个用户 `user1@'192.168.10.1'` 的用户，并设置一个新的密码。因为 `192.168.10.1` 的优先级高于 `192.%`，所以来自 `192.168.10.1` 将不能再使用旧密码进行登录。
