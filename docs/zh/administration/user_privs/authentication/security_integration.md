---
displayed_sidebar: docs
sidebar_position: 20
---

# 通过安全集成认证用户

使用安全集成将 StarRocks 与外部身份验证系统集成。

通过在 StarRocks 集群中创建安全集成，您可以允许外部身份验证服务访问 StarRocks。借助安全集成，您无需在 StarRocks 中手动创建用户。当用户尝试使用外部身份登录时，StarRocks 将根据 `authentication_chain` 中的配置使用相应的安全集成来验证用户身份。身份验证成功后，用户被允许登录，StarRocks 会在会话中为用户创建一个虚拟用户以执行后续操作。

请注意，如果您使用安全集成配置外部身份验证方法，您还必须 [将 StarRocks 与 Apache Ranger 集成](../authorization/ranger_plugin.md) 以启用外部授权。目前，不支持将安全集成与 StarRocks 本地授权集成。

您还可以为 StarRocks 启用 [Group Provider](../group_provider.md)，以访问外部身份验证系统中的组信息，从而允许在 StarRocks 中创建、验证和授权用户组。

在特定情况下，也支持使用外部身份验证服务手动创建和管理用户。有关更多说明，请参阅 [另见](#另见)。

## 创建安全集成

目前，StarRocks 的安全集成支持以下身份验证系统：
- LDAP
- JSON Web Token（JWT）
- OAuth 2.0

:::note
创建安全集成时，StarRocks 不提供连接性检查。
:::

### 使用 LDAP 创建安全集成

#### 语法

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

#### 参数

##### security_integration_name

- 必需：是
- 描述：安全集成的名称。<br />**注意**<br />安全集成名称是全局唯一的。您不能将此参数指定为 `native`。

##### type

- 必需：是
- 描述：安全集成的类型。指定为 `ldap`。

##### ldap_server_host

- 必需：否
- 描述：LDAP 服务的 IP 地址。默认值：`127.0.0.1`。

##### ldap_server_port

- 必需：否
- 描述：LDAP 服务的端口。默认值：`389`。

##### ldap_bind_base_dn

- 必需：是
- 描述：集群搜索的 LDAP 用户的基本专有名称 (DN)。

##### ldap_user_search_attr

- 必需：是
- 描述：用于登录 LDAP 服务的用户属性，例如 `uid`。

##### ldap_user_group_match_attr

- 必需：否
- 描述：如果用户作为组成员的属性与用户的 DN 不同，则必须指定此参数。例如，如果用户的 DN 是 `uid=bob,ou=people,o=starrocks,dc=com`，但其作为组成员的属性是 `memberUid=bob,ou=people,o=starrocks,dc=com`，则需要将 `ldap_user_search_attr` 指定为 `uid`，`ldap_user_group_match_attr` 指定为 `memberUid`。如果未指定此参数，则使用您在 `ldap_user_search_attr` 中指定的值。您还可以指定正则表达式来匹配组中的成员。正则表达式必须以 `regex:` 为前缀。假设一个组有一个成员 `CN=Poornima K Hebbar (phebbar),OU=User Policy 0,OU=All Users,DC=SEA,DC=CORP,DC=EXPECN,DC=com`。如果您将此属性指定为 `regex:CN=.*\\(([^)]+)\\)`，它将匹配成员 `phebbar`。

##### ldap_bind_root_dn

- 必需：是
- 描述：LDAP 服务的管理员 DN。

##### ldap_bind_root_pwd

- 必需：是
- 描述：LDAP 服务的管理员密码。

##### ldap_cache_refresh_interval

- 必需：否
- 描述：集群自动刷新缓存的 LDAP 组信息的间隔。单位：秒。默认值：`900`。

##### ldap_ssl_conn_allow_insecure

- 必需：否
- 描述：是否使用非 SSL 连接到 LDAP 服务器。默认值：`true`。将此值设置为 `false` 表示启用 LDAP over SSL。有关启用 SSL 的详细说明，请参阅 [SSL Authentication](../ssl_authentication.md)。

##### ldap_ssl_conn_trust_store_path

- 必需：否
- 描述：存储 LDAP SSL 证书的本地路径。

##### ldap_ssl_conn_trust_store_pwd

- 必需：否
- 描述：访问本地存储的 LDAP SSL 证书所用的密码。

##### group_provider

- 必需：否
- 描述：与安全集成结合使用的 Group Provider 名称。多个 Group Provider 用逗号分隔。设置后，StarRocks 将在用户登录时记录每个指定提供者下的用户组信息。从 v3.5 开始支持。有关启用 Group Provider 的详细说明，请参阅 [Authenticate User Groups](../group_provider.md)。

##### authenticated_group_list

- 必需：否
- 描述：允许登录到 StarRocks 的组名称。多个组用逗号分隔。确保指定的组可以通过组合的 Group Provider 检索。从 v3.5 开始支持。

##### comment

- 必需：否
- 描述：安全集成的描述。

### 使用 JWT 认证创建安全集成

#### 语法

```SQL
CREATE SECURITY INTEGRATION <security_integration_name> 
PROPERTIES (
    "type" = "jwt",
    "jwks_url" = "",
    "principal_field" = "",
    "required_issuer" = "",
    "required_audience" = ""
    "comment" = ""
)
```

#### 参数

##### security_integration_name

- 必需：是
- 描述：安全集成的名称。<br />**注意**<br />安全集成名称是全局唯一的。您不能将此参数指定为 `native`。

##### type

- 必需：是
- 描述：安全集成的类型。指定为 `jwt`。

##### jwks_url

- 必需：是
- 描述：JSON Web Key Set (JWKS) 服务的 URL 或 `fe/conf` 目录下本地文件的路径。

##### principal_field

- 必需：是
- 描述：用于标识 JWT 中主题 (`sub`) 的字段的字符串。默认值为 `sub`。此字段的值必须与登录 StarRocks 的用户名相同。

##### required_issuer

- 必需：否
- 描述：用于标识 JWT 中发行者 (`iss`) 的字符串列表。仅当列表中的某个值与 JWT 发行者匹配时，JWT 才被视为有效。

##### required_audience

- 必需：否
- 描述：用于标识 JWT 中受众 (`aud`) 的字符串列表。仅当列表中的某个值与 JWT 受众匹配时，JWT 才被视为有效。

##### comment

- 必需：否
- 描述：安全集成的描述。

### 使用 OAuth 2.0 创建安全集成

#### 语法

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

#### 参数

##### security_integration_name

- 必需：是
- 描述：安全集成的名称。<br />**注意**<br />安全集成名称是全局唯一的。您不能将此参数指定为 `native`。

##### auth_server_url

- 必需：是
- 描述：授权 URL。用户浏览器将被重定向到此 URL 以开始 OAuth 2.0 授权过程。

##### token_server_url

- 必需：是
- 描述：StarRocks 从中获取访问令牌的授权服务器端点的 URL。

##### client_id

- 必需：是
- 描述：StarRocks 客户端的公共标识符。

##### client_secret

- 必需：是
- 描述：用于授权 StarRocks 客户端与授权服务器通信的密钥。

##### redirect_url

- 必需：是
- 描述：OAuth 2.0 身份验证成功后，用户浏览器将被重定向到的 URL。授权代码将发送到此 URL。在大多数情况下，需要将其配置为 `http://<starrocks_fe_url>:<fe_http_port>/api/oauth2`。

##### type

- 必需：是
- 描述：安全集成的类型。指定为 `oauth2`。

##### jwks_url

- 必需：是
- 描述：JSON Web Key Set (JWKS) 服务的 URL 或 `fe/conf` 目录下本地文件的路径。

##### principal_field

- 必需：是
- 描述：用于标识 JWT 中主题 (`sub`) 的字段的字符串。默认值为 `sub`。此字段的值必须与登录 StarRocks 的用户名相同。

##### required_issuer

- 必需：否
- 描述：用于标识 JWT 中发行者 (`iss`) 的字符串列表。仅当列表中的某个值与 JWT 发行者匹配时，JWT 才被视为有效。

##### required_audience

- 必需：否
- 描述：用于标识 JWT 中受众 (`aud`) 的字符串列表。仅当列表中的某个值与 JWT 受众匹配时，JWT 才被视为有效。

##### comment

- 必需：否
- 描述：安全集成的描述。

## 配置身份验证链

创建安全集成后，它将作为新的身份验证方法添加到您的 StarRocks 集群中。您必须通过设置 FE 动态配置项 `authentication_chain` 来启用安全集成。

```SQL
ADMIN SET FRONTEND CONFIG (
    "authentication_chain" = "<security_integration_name>[... ,]"
);
```

:::note
- StarRocks 会优先使用本地用户进行验证。如本地不存在同名用户，则按照`authentication_chain`的配置顺序进行认证。如果使用身份验证方法登录失败，集群将按照指定的顺序尝试下一个身份验证方法。
- 您可以在 `authentication_chain` 中指定多个安全集成，但不能指定多个 OAuth 2.0 安全集成或将其与其他安全集成一起指定。
:::

您可以使用以下语句检查 `authentication_chain` 的值：

```SQL
ADMIN SHOW FRONTEND CONFIG LIKE 'authentication_chain';
```

## 管理安全集成

### 修改安全集成

您可以使用以下语句修改现有安全集成的配置：

```SQL
ALTER SECURITY INTEGRATION <security_integration_name> SET
(
    "key"="value"[, ...]
)
```

:::note
您不能更改安全集成的 `type`。
:::

### 删除安全集成

您可以使用以下语句删除现有的安全集成：

```SQL
DROP SECURITY INTEGRATION <security_integration_name>
```

### 查看安全集成

您可以使用以下语句查看集群中的所有安全集成：

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

| **参数** | **描述**                                              |
| ------------- | ------------------------------------------------------------ |
| Name          | 安全集成的名称。                        |
| Type          | 安全集成的类型。                        |
| Comment       | 安全集成的描述。当未为安全集成指定描述时，返回 `NULL`。 |

您可以使用以下语句检查安全集成的详细信息：

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

:::note
执行 SHOW CREATE SECURITY INTEGRATION 时，`ldap_bind_root_pwd` 会被隐藏。
:::

## 另见

- 有关如何在 StarRocks 中通过 LDAP 手动验证用户的说明，请参阅 [LDAP 认证](./ldap_authentication.md)。
- 有关如何在 StarRocks 中通过 SON Web Token 认证手动验证用户的说明，请参阅 [JSON Web Token 认证](./jwt_authentication.md)。
- 有关如何在 StarRocks 中通过 OAuth 2.0 手动验证用户的说明，请参阅 [OAuth 2.0 认证](./oauth2_authentication.md)。
- 有关如何验证用户组的说明，请参阅 [认证用户组](../group_provider.md)。
