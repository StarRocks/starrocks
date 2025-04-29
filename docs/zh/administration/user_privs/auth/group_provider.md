---
displayed_sidebar: docs
sidebar_position: 60
---

# 认证用户组

本文介绍如何在 StarRocks 中创建、认证和授权用户组。

从 v3.5.0 开始，StarRocks 支持创建和管理用户组。

## 概述

为了更好地与外部用户认证和授权系统（如 LDAP、OpenID Connect、OAuth 2.0 和 Apache Ranger）集成，StarRocks 支持用户组，以提升集体用户管理的体验。

:::note

**StarRocks 中用户组与角色的区别**

- **用户组是用户的集合**。可以将角色和权限授予用户组，使其对用户组的所有成员生效。
- **角色是权限的集合**。角色中的权限只有在授予特定用户或用户组后才会生效。

例如，角色 `analyst_role` 包含对 `table_a` 的 INSERT 和 SELECT 权限，而用户组 `analyst_group` 包含三个用户，`tom`、`chelsea` 和 `sophie`。您可以将 `analyst_role` 授予 `analyst_group`，这样 `tom`、`chelsea` 和 `sophie` 都将拥有对 `table_a` 的 INSERT 和 SELECT 权限。

:::

您可以通过组提供者从外部用户系统获取组信息，在 StarRocks 中创建用户组。组信息是独立的，可以灵活地集成到认证、授权或其他流程中，而不需要与任何特定工作流紧密耦合。

组提供者本质上是用户与组之间的映射。任何需要组信息的过程都可以根据需要查询此映射。

### 工作流程

以下流程图以 LDAP 和 Apache Ranger 为例，解释了组提供者的工作流程。

![Group Provider](../../../_assets/group_provider.png)

## 创建组提供者

StarRocks 支持三种类型的组提供者：
- **LDAP 组提供者**：在您的 LDAP 服务中搜索并匹配用户与组
- **Unix 组提供者**：在您的操作系统中搜索并匹配用户与组
- **文件组提供者**：通过文件指定的用户与组进行搜索和匹配

### 语法

```SQL
-- LDAP 组提供者
CREATE GROUP PROVIDER <group_provider_name> 
PROPERTIES (
    "type" = "ldap",
    ldap_info,
    ldap_search_group_arg,
    ldap_search_attr,
    [ldap_cache_attr]
)

ldap_info ::=
    "ldap_conn_url" = "",
    "ldap_bind_root_dn" = "",
    "ldap_bind_root_pwd" = "",
    "ldap_bind_base_dn" = "",
    ["ldap_conn_timeout" = "",]
    ["ldap_conn_read_timeout" = ""]

ldap_search_group_arg ::= 
    { "ldap_group_dn" = "" 
    | "ldap_group_filter" = "" }, 
    "ldap_group_identifier_attr" = ""

ldap_search_user_arg ::=
    "ldap_group_member_attr" = "",
    "ldap_user_search_attr" = ""

ldap_cache_arg ::= 
    "ldap_cache_refresh_interval" = ""

-- Unix 组提供者
CREATE GROUP PROVIDER <group_provider_name> 
PROPERTIES (
    "type" = "unix"
)

-- 文件组提供者
CREATE GROUP PROVIDER <group_provider_name> 
PROPERTIES (
    "type" = "file",
    "group_file_url" = ""
)
```

### 参数

#### `type`

要创建的组提供者类型。有效值：
- `ldap`：创建一个 LDAP 组提供者。当设置此值时，需要指定 `ldap_info`、`ldap_search_group_arg`、`ldap_search_user_arg`，以及可选的 `ldap_cache_arg`。
- `unix`：创建一个 Unix 组提供者。
- `file`：创建一个文件组提供者。当设置此值时，需要指定 `group_file_url`。

#### `ldap_info`

用于连接到您的 LDAP 服务的信息。

##### `ldap_conn_url`

您的 LDAP 服务器的 URL。格式：`ldap://<ldap_server_host>:<ldap_server_port>)`。

##### `ldap_bind_root_dn`

您的 LDAP 服务的管理员 DN。

##### `ldap_bind_root_pwd`

您的 LDAP 服务的管理员密码。

##### `ldap_bind_base_dn`

集群搜索的 LDAP 用户的基本 DN。

##### `ldap_conn_timeout`

连接到您的 LDAP 服务的超时时间。

##### `ldap_conn_read_timeout`

可选。连接到您的 LDAP 服务的读取操作的超时时间。

#### `ldap_search_group_arg`

控制 StarRocks 如何搜索组的参数。

:::note
您只能指定 `ldap_group_dn` 或 `ldap_group_filter` 之一。不支持同时指定两者。
:::

##### `ldap_group_dn`

要搜索的组的 DN。将直接使用此 DN 查询组。示例：`"cn=ldapgroup1,ou=Group,dc=starrocks,dc=com;cn=ldapgroup2,ou=Group,dc=starrocks,dc=com"`。

##### `ldap_group_filter`

LDAP 服务器可识别的自定义组过滤器。将直接发送到您的 LDAP 服务器以搜索组。示例：`(&(objectClass=groupOfNames)(cn=testgroup))`。

##### `ldap_group_identifier_attr`

用作组名称标识符的属性。

#### `ldap_search_user_arg`

控制 StarRocks 如何识别组中用户的参数。

##### `ldap_group_member_attr`

表示组成员的属性。有效值：`member` 和 `memberUid`。

##### `ldap_user_search_attr`

指定如何从成员属性值中提取用户标识符。您可以显式定义一个属性（例如，`cn` 或 `uid`）或使用正则表达式。

#### `ldap_cache_arg`

定义 LDAP 组信息缓存行为的参数。

##### `ldap_cache_refresh_interval`

可选。StarRocks 自动刷新缓存的 LDAP 组信息的间隔时间。单位：秒。默认值：`900`。

#### `group_file_url`

定义用户组的文件的 URL 或相对路径（在 `fe/conf` 下）。

:::note

组文件包含组及其成员的列表。您可以在每行中定义一个组，用冒号分隔。多个用户用逗号分隔。示例：`group_name:user_1,user_2,user_3`。

:::

### 示例

假设一个 LDAP 服务器包含以下组和成员信息。

```Plain
-- 组信息
# testgroup, Group, starrocks.com
dn: cn=testgroup,ou=Group,dc=starrocks,dc=com
objectClass: groupOfNames
cn: testgroup
member: uid=test,ou=people,dc=starrocks,dc=com
member: uid=tom,ou=people,dc=starrocks,dc=com

-- 用户信息
# test, People, starrocks.com
dn: cn=test,ou=People,dc=starrocks,dc=com
objectClass: inetOrgPerson
cn: test
uid: test
sn: FTE
userPassword:: 
```

为 `testgroup` 中的成员创建一个组提供者 `ldap_group_provider`：

```SQL
CREATE GROUP PROVIDER ldap_group_provider 
PROPERTIES(
    "type"="ldap", 
    "ldap_conn_url"="ldap://xxxx:xxx",
    "ldap_bind_root_dn"="cn=admin,dc=starrocks,dc=com",
    "ldap_bind_root_pwd"="123456",
    "ldap_bind_base_dn"="dc=starrocks,dc=com",
    "ldap_group_filter"="(&(objectClass=groupOfNames)(cn=testgroup))",
    "ldap_group_identifier_attr"="cn",
    "ldap_group_member_attr"="member",
    "ldap_user_search_attr"="uid=([^,]+)"
)
```

上述示例使用 `ldap_group_filter` 搜索具有 `groupOfNames` objectClass 和 `cn` 为 `testgroup` 的组。因此，在 `ldap_group_identifier_attr` 中指定 `cn` 以标识组。`ldap_group_member_attr` 设置为 `member`，以便在 `groupOfNames` objectClass 中使用 `member` 属性标识成员。`ldap_user_search_attr` 设置为表达式 `uid=([^,]+)`，用于在 `member` 属性中识别用户。

## 将组提供者与安全集成结合

创建组提供者后，您可以将其与安全集成结合，以允许组提供者指定的用户登录 StarRocks。有关创建安全集成的更多信息，请参见 [Authenticate with Security Integration](./security_integration.md)。

### 语法

```SQL
ALTER SECURITY INTEGRATION <security_integration_name> SET
PROPERTIES(
        "group_provider" = "",
        "authenticated_group_list" = ""
)
```

### 参数

#### `group_provider`

要与安全集成结合的组提供者名称。多个组提供者用逗号分隔。设置后，StarRocks 将在用户登录时记录每个指定提供者下的用户组信息。

#### `authenticated_group_list`

可选。允许其成员登录 StarRocks 的组名称。多个组用逗号分隔。确保指定的组可以通过结合的组提供者检索到。

### 示例

```SQL
ALTER SECURITY INTEGRATION LDAP SET
PROPERTIES(
        "group_provider"="ldap_group_provider",
        "authenticated_group_list"="testgroup"
);
```

## 将组提供者与外部授权系统（Apache Ranger）结合

一旦在安全集成中配置了相关的组提供者，StarRocks 将在用户登录时记录用户的组信息。此组信息将自动包含在与 Ranger 的授权过程中，无需额外配置。

有关将 StarRocks 与 Ranger 集成的更多说明，请参见 [Manage permissions with Apache Ranger](../ranger_plugin.md)。