---
displayed_sidebar: docs
sidebar_position: 50
---

# 使用安全集成认证

本文介绍如何使用安全集成将StarRocks与外部身份验证系统集成。

通过在StarRocks集群中创建安全集成，可以允许外部身份验证服务访问StarRocks。StarRocks会缓存身份验证服务的组成员信息，并在需要时刷新缓存。

## 创建安全集成

目前，StarRocks的安全集成支持以下身份验证系统：
- LDAP
- OpenID Connect (OIDC)
- OAuth 2.0

> **注意**
>
> 创建安全集成时，StarRocks不提供连接性检查。

### 使用LDAP创建安全集成

语法：

```SQL
CREATE SECURITY INTEGRATION <security_integration_name> 
PROPERTIES (
    "type" = "ldap",
    "ldap_server_host" = "",
    "ldap_server_port" = "",
    "ldap_bind_base_dn" = "",
    "ldap_user_search_attr" = "",
    "ldap_user_group_match_attr" = "",
    "ldap_bind_root_dn" = "",
    "ldap_bind_root_pwd" = "",
    "ldap_cache_refresh_interval" = "",
    "ldap_ssl_conn_allow_insecure" = "{true | false}",
    "ldap_ssl_conn_trust_store_path" = "",
    "ldap_ssl_conn_trust_store_pwd" = "",
    "comment" = ""
)
```

参数：

| **参数**                    | **必需** | **描述**                                                      |
| --------------------------- | -------- | ------------------------------------------------------------ |
| security_integration_name   | 是       | 安全集成的名称。<br />**注意**<br />安全集成名称是全局唯一的。不能将此参数指定为`native`。 |
| type                        | 是       | 安全集成的类型。指定为`ldap`。                               |
| ldap_server_host            | 否       | LDAP服务的IP地址。默认值：`127.0.0.1`。                      |
| ldap_server_port            | 否       | LDAP服务的端口。默认值：`389`。                              |
| ldap_bind_base_dn           | 是       | 集群搜索的LDAP用户的基本专有名称（DN）。                     |
| ldap_user_search_attr       | 是       | 用于登录LDAP服务的用户属性，例如，`uid`。                    |
| ldap_user_group_match_attr  | 否       | 如果作为组成员的用户属性与用户的DN不同，则必须指定此参数。例如，如果用户的DN是`uid=bob,ou=people,o=starrocks,dc=com`，但其作为组成员的属性是`memberUid=bob,ou=people,o=starrocks,dc=com`，则需要将`ldap_user_search_attr`指定为`uid`，`ldap_user_group_match_attr`指定为`memberUid`。如果未指定此参数，则使用`ldap_user_search_attr`中指定的值。还可以指定正则表达式来匹配组中的成员。正则表达式必须以`regex:`为前缀。假设一个组有一个成员`CN=Poornima K Hebbar (phebbar),OU=User Policy 0,OU=All Users,DC=SEA,DC=CORP,DC=EXPECN,DC=com`。如果将此属性指定为`regex:CN=.*\\(([^)]+)\\)`，则会匹配成员`phebbar`。 |
| ldap_bind_root_dn           | 是       | LDAP服务的管理员DN。                                         |
| ldap_bind_root_pwd          | 是       | LDAP服务的管理员密码。                                       |
| ldap_cache_refresh_interval | 否       | 集群自动刷新缓存的LDAP组信息的间隔。单位：秒。默认值：`900`。 |
| ldap_ssl_conn_allow_insecure | 否      | 是否使用非SSL连接到LDAP服务器。默认值：`true`。将此值设置为`false`表示启用LDAP over SSL。有关启用SSL的详细说明，请参阅[SSL 认证](./ssl_authentication.md)。 |
| ldap_ssl_conn_trust_store_path | 否    | 存储LDAP SSL证书的本地路径。                                 |
| ldap_ssl_conn_trust_store_pwd | 否     | 访问本地存储的LDAP SSL证书的密码。                           |
| group_provider              | 否       | 要与安全集成结合使用的 Group Provider 名称。多个 Group Provider 用逗号分隔。设置后，StarRocks将在用户登录时记录每个指定提供者下的用户组信息。从v3.5开始支持。有关启用 Group Provider 的详细说明，请参阅[认证用户组](./group_provider.md)。 |
| authenticated_group_list    | 否       | 允许其成员登录StarRocks的组名称。多个组用逗号分隔。确保指定的组可以通过组合的 Group Provider 检索。从v3.5开始支持。 |
| comment                     | 否       | 安全集成的描述。                                             |

### 使用OIDC创建安全集成

语法：

```SQL
CREATE SECURITY INTEGRATION <security_integration_name> 
PROPERTIES (
    "type" = "oidc",
    "jwks_url" = "",
    "principal_field" = "",
    "required_issuer" = "",
    "required_audience" = ""
    "comment" = ""
)
```

参数：

| **参数**                    | **必需** | **描述**                                                      |
| --------------------------- | -------- | ------------------------------------------------------------ |
| security_integration_name   | 是       | 安全集成的名称。<br />**注意**<br />安全集成名称是全局唯一的。不能将此参数指定为`native`。 |
| type                        | 是       | 安全集成的类型。指定为`oidc`。                               |
| jwks_url                    | 是       | JSON Web Key Set (JWKS) 服务的URL或`fe/conf`目录下本地文件的路径。 |
| principal_field             | 是       | 用于标识JWT中主体（`sub`）字段的字符串。默认值为`sub`。此字段的值必须与登录StarRocks的用户名相同。 |
| required_issuer             | 否       | 用于标识JWT中发行者（`iss`）的字符串列表。只有当列表中的一个值与JWT发行者匹配时，JWT才被认为是有效的。 |
| required_audience           | 否       | 用于标识JWT中受众（`aud`）的字符串列表。只有当列表中的一个值与JWT受众匹配时，JWT才被认为是有效的。 |
| comment                     | 否       | 安全集成的描述。                                             |

### 使用OAuth 2.0创建安全集成

语法：

```SQL
CREATE SECURITY INTEGRATION <security_integration_name> 
PROPERTIES (
    "type" = "oauth2",
    "auth_server_url" = "",
    "token_server_url" = "",
    "client_id" = "",
    "client_secret" = "",
    "redirect_url" = "",
    "jwks_url" = "",
    "principal_field" = "",
    "required_issuer" = "",
    "required_audience" = ""
    "comment" = ""
)
```

参数：

| **参数**                    | **必需** | **描述**                                                      |
| --------------------------- | -------- | ------------------------------------------------------------ |
| security_integration_name   | 是       | 安全集成的名称。<br />**注意**<br />安全集成名称是全局唯一的。不能将此参数指定为`native`。 |
| auth_server_url             | 是       | 授权URL。用户浏览器将被重定向到此URL以开始OAuth 2.0授权过程。 |
| token_server_url            | 是       | 授权服务器上StarRocks获取访问令牌的端点URL。                 |
| client_id                   | 是       | StarRocks客户端的公共标识符。                                 |
| client_secret               | 是       | 用于授权StarRocks客户端与授权服务器的密钥。                   |
| redirect_url                | 是       | OAuth 2.0身份验证成功后，用户浏览器将被重定向到此URL。授权代码将发送到此URL。在大多数情况下，需要配置为`http://<starrocks_fe_url>:<fe_http_port>/api/oauth2`。 |
| type                        | 是       | 安全集成的类型。指定为`oauth2`。                             |
| jwks_url                    | 是       | JSON Web Key Set (JWKS) 服务的URL或`fe/conf`目录下本地文件的路径。 |
| principal_field             | 是       | 用于标识JWT中主体（`sub`）字段的字符串。默认值为`sub`。此字段的值必须与登录StarRocks的用户名相同。 |
| required_issuer             | 否       | 用于标识JWT中发行者（`iss`）的字符串列表。只有当列表中的一个值与JWT发行者匹配时，JWT才被认为是有效的。 |
| required_audience           | 否       | 用于标识JWT中受众（`aud`）的字符串列表。只有当列表中的一个值与JWT受众匹配时，JWT才被认为是有效的。 |
| comment                     | 否       | 安全集成的描述。                                             |

## 配置身份验证链

创建安全集成后，它将作为新的身份验证方法添加到StarRocks集群中。必须通过FE动态配置项`authentication_chain`设置身份验证方法的顺序来启用安全集成。在这种情况下，需要将安全集成设置为首选身份验证方法，然后是StarRocks集群的本地身份验证。

```SQL
ADMIN SET FRONTEND CONFIG (
    "authentication_chain" = "<security_integration_name>, native"
);
```

> **注意**
>
> - 如果未指定`authentication_chain`，则仅启用本地身份验证。
> - 一旦设置了`authentication_chain`，StarRocks首先使用首选身份验证方法验证用户登录。如果使用首选身份验证方法登录失败，集群将按照指定顺序尝试下一个身份验证方法。

可以使用以下语句检查`authentication_chain`的值：

```SQL
ADMIN SHOW FRONTEND CONFIG LIKE 'authentication_chain';
```

## 管理安全集成

可以使用以下语句更改现有安全集成的配置：

```SQL
ALTER SECURITY INTEGRATION <security_integration_name> SET
PROPERTIES (
    "key"="value"[, ...]
)
```

> **注意**
>
> 无法更改安全集成的`type`。

可以使用以下语句删除现有安全集成：

```SQL
DROP SECURITY INTEGRATION <security_integration_name>
```

可以使用以下语句查看集群中的所有安全集成：

```SQL
SHOW SECURITY INTEGRATIONS;
```

示例：

```Plain
SHOW SECURITY INTEGRATIONS;
+--------+--------+---------+
| Name   | Type   | Comment |
+--------+--------+---------+
| LDAP1  | LDAP   | NULL    |
+--------+--------+---------+
```

| **参数** | **描述**                                                      |
| -------- | ------------------------------------------------------------ |
| Name     | 安全集成的名称。                                             |
| Type     | 安全集成的类型。                                             |
| Comment  | 安全集成的描述。当未为安全集成指定描述时，返回`NULL`。       |

可以使用以下语句检查安全集成的详细信息：

```SQL
SHOW CREATE SECURITY INTEGRATION <integration_name>
```

示例：

```Plain
SHOW CREATE SECURITY INTEGRATION LDAP1；

+----------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| Security Integration  | Create Security Integration                                                                                                                                                                                                                                                                                                                                                                              |
+----------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| LDAP1                | CREATE SECURITY INTEGRATION LDAP1
    PROPERTIES (
    "type" = "ldap",
    "ldap_server_host"="",
    "ldap_server_port"="",
    "ldap_bind_base_dn"="",
    "ldap_user_search_attr"="",
    "ldap_bind_root_dn"="",
    "ldap_bind_root_pwd"="*****",
    "ldap_cache_refresh_interval"="",
    "comment"=""
)|
+----------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
```

> **注意**
>
> 执行SHOW CREATE SECURITY INTEGRATION时，`ldap_bind_root_pwd`会被隐藏。

## 另请参阅

- 有关如何在StarRocks中通过LDAP手动验证用户的说明，请参阅[LDAP Authentication](./ldap_authentication.md)。
- 有关如何在StarRocks中通过OpenID Connect手动验证用户的说明，请参阅[OpenID Connect Authentication](./oidc_authentication.md)。
- 有关如何在StarRocks中通过OAuth 2.0手动验证用户的说明，请参阅[OAuth 2.0 Authentication](./oauth2_authentication.md)。