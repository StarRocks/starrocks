---
displayed_sidebar: docs
description: "Integrate StarRocks with external authentication systems using security integration to enable seamless user authentication without manual user creation."
sidebar_position: 20
---

# 通过安全集成认证用户

import SecurityIntegrationRangerLink from '../../../_assets/user_priv/security_integration_ranger_link.mdx'
import SecurityIntegrationIntro from '../../../_assets/user_priv/security_integration_intro.mdx'
import SecurityIntegrationJWT from '../../../_assets/user_priv/security_integration_jwt.mdx'
import SecurityIntegrationOAuth from '../../../_assets/user_priv/security_integration_oauth.mdx'
import SecurityIntegrationConnectSeeAlso from '../../../_assets/user_priv/security_integration_connect_see_also.mdx'

使用安全集成将 StarRocks 与外部身份验证系统集成。

通过在 StarRocks 集群中创建安全集成，您可以允许外部身份验证服务访问 StarRocks。借助安全集成，您无需在 StarRocks 中手动创建用户。当用户尝试使用外部身份登录时，StarRocks 将根据 `authentication_chain` 中的配置使用相应的安全集成来验证用户身份。身份验证成功后，用户被允许登录，StarRocks 会在会话中为用户创建一个虚拟用户以执行后续操作。

<SecurityIntegrationRangerLink />

您还可以为 StarRocks 启用 [Group Provider](../group_provider.md)，以访问外部身份验证系统中的组信息，从而允许在 StarRocks 中创建、验证和授权用户组。

在特定情况下，也支持使用外部身份验证服务手动创建和管理用户。有关更多说明，请参阅 [另见](#另见)。

## 创建安全集成

<SecurityIntegrationIntro />

:::note
创建安全集成时，StarRocks 不提供连接性检查。
:::

### 使用 LDAP 创建安全集成

#### 语法

```SQL
CREATE SECURITY INTEGRATION <security_integration_name> 
PROPERTIES (
    "type" = "authentication_ldap_simple",
    "authentication_ldap_simple_server_host" = "",
    "authentication_ldap_simple_server_port" = "",
    "authentication_ldap_simple_bind_base_dn" = "",
    "authentication_ldap_simple_user_search_attr" = "",
    "authentication_ldap_simple_bind_root_dn" = "",
    "authentication_ldap_simple_bind_root_pwd" = "",
    "authentication_ldap_simple_bind_dn_pattern" = "",
    "authentication_ldap_simple_ssl_conn_allow_insecure" = "{true | false}",
    "authentication_ldap_simple_ssl_conn_trust_store_path" = "",
    "authentication_ldap_simple_ssl_conn_trust_store_pwd" = "",
    "comment" = ""
)
```

#### 参数

##### security_integration_name

- 必需：是
- 描述：安全集成的名称。<br />**注意**<br />安全集成名称是全局唯一的。您不能将此参数指定为 `native`。

##### type

- 必需：是
- 描述：安全集成的类型。指定为 `authentication_ldap_simple`。

##### authentication_ldap_simple_server_host

- 必需：否
- 描述：LDAP 服务的 IP 地址。默认值：`127.0.0.1`。

##### authentication_ldap_simple_server_port

- 必需：否
- 描述：LDAP 服务的端口。默认值：`389`。

##### authentication_ldap_simple_bind_base_dn

- 必需：否
- 描述：集群搜索的 LDAP 用户的基本专有名称 (DN)。使用搜索绑定模式时必需。使用直接绑定模式（配置 `authentication_ldap_simple_bind_dn_pattern`）时不需要。

##### authentication_ldap_simple_user_search_attr

- 必需：否
- 描述：用户条目上承载登录名的属性。它会被拼进搜索绑定模式的过滤器，所以必须是用户登录时**实际输入**的那个值所在的属性。默认值：`uid`，这是 OpenLDAP 的惯例，那里它通常同时也是条目的 RDN（`uid=alice,ou=People,dc=example,dc=com`）。直接绑定模式下不使用该属性。

:::note Active Directory 上请设为 `sAMAccountName`

AD 条目的 RDN 是显示名（`CN=Dana Scully,OU=People,DC=company,DC=com`），而用户登录时输入的是它的 `sAMAccountName`（`dscully`）。AD 的 schema 里确实**有** `uid` 这个属性，但除非管理员专门填过，否则它没有值，因此用默认值谁也搜不到，所有登录都会失败。选择该属性会连带决定另外两处配置：

- **只能用搜索绑定，不能用直接绑定。** `sAMAccountName` 不是 AD 条目 DN 的组成部分，任何 `authentication_ldap_simple_bind_dn_pattern` 都拼不出一个存在的 DN。请不要设置该属性，改为设置 `authentication_ldap_simple_bind_base_dn`、`authentication_ldap_simple_bind_root_dn` 与 `authentication_ldap_simple_bind_root_pwd`。
- **Group Provider 同样无法按 `sAMAccountName` 匹配成员。** AD 的组是按 DN 列出成员的（`member: CN=Dana Scully,OU=People,...`），而这个 DN 里不含 `sAMAccountName`，所以给 group provider 配 `"ldap_user_search_attr" = "sAMAccountName"` 会一个人都匹配不上。要么把 group provider 自己的 `ldap_user_search_attr` **留空**（改为按 DN 匹配），要么用 `authentication_ldap_simple_group_source = memberof` 从用户自身条目读取用户组。

:::

:::note

**DN 传递机制**：LDAP 安全集成支持 DN 传递功能。

- 认证成功后，系统会同时记录用户的登录名和完整 DN。
- 当与 Group Provider 结合使用时，DN 信息会自动传递给 Group Provider。
- 如果 Group Provider 未配置 `ldap_user_search_attr` 参数，将使用 DN 进行组匹配。
- 这种机制特别适用于 Microsoft AD 等复杂 LDAP 环境。

有关详细信息，请参见[认证用户组](../group_provider.md)中的 DN 匹配机制说明。

:::

##### authentication_ldap_simple_bind_root_dn

- 必需：否
- 描述：LDAP 服务的管理员 DN。使用搜索绑定模式时必需。

##### authentication_ldap_simple_bind_root_pwd

- 必需：否
- 描述：LDAP 服务的管理员密码。使用搜索绑定模式时必需。

##### authentication_ldap_simple_bind_dn_pattern

- 必需：否
- 描述：直接绑定认证的 DN 模式。使用 `${USER}` 作为用户名的占位符。模式必须生成合法的 LDAP Distinguished Name（DN），不支持 UPN 格式（如 `${USER}@domain`）。例如 `uid=${USER},ou=People,dc=example,dc=com`。多个模式之间用分号分隔，系统将按顺序尝试每个模式直到成功。设置此参数后，系统将跳过搜索步骤直接使用构造的 DN 进行绑定，因此不需要配置 `authentication_ldap_simple_bind_base_dn`、`authentication_ldap_simple_user_search_attr`、`authentication_ldap_simple_bind_root_dn` 和 `authentication_ldap_simple_bind_root_pwd`。

##### authentication_ldap_simple_group_source

- 必需：否
- 描述：经 LDAP 认证的用户，其用户组从哪里来。取值：
  - `group_provider`（默认）：只使用 `group_provider` 中配置的 Group Provider，与低版本行为一致。
  - `memberof`：只使用用户自身 LDAP 条目上的组成员属性。`group_provider` 中已配置的 Group Provider 被忽略，但配置保留，因此改回来只需要一条 `ALTER SECURITY INTEGRATION`。
  - `both`：两者取并集。

  取值为 `memberof` 或 `both` 时，目录中新建的用户组会在用户下次登录时生效，无需修改这里的任何配置。解析出的用户组与 Group Provider 解析出的用户组同等参与 `GRANT ... TO EXTERNAL GROUP` 的角色映射、`permitted_groups` 登录校验、`current_group()` 返回值以及 Apache Ranger 鉴权。集群级默认值为同名 FE 配置项。自 v4.2 起支持。

##### authentication_ldap_simple_memberof_attr

- 必需：否
- 描述：用户条目上承载组成员关系的属性名，在 `authentication_ldap_simple_group_source` 取 `memberof` 或 `both` 时使用。与所有 LDAP 属性名一样，匹配时忽略大小写。默认值：`memberOf`，适用于 Active Directory 以及安装了 `memberof` overlay 的 OpenLDAP。Oracle Directory Server 与 389 Directory Server 使用 `isMemberOf`。集群级默认值为同名 FE 配置项。自 v4.2 起支持。

:::note 从用户条目读取组成员关系

- 只解析直接成员关系。嵌套组、Active Directory 的 primary group（默认为 `Domain Users`）以及来自其他域或林的用户组都不会包含在内，因为它们不出现在该属性中。
- 用户组名取每个组 DN 的第一个 RDN 的值。例如 `CN=SR Analysts,OU=Groups,DC=company,DC=com` 得到 `SR Analysts`，并保持目录中的原始大小写。
- 该属性在认证已经建立的连接上读取，不新增 LDAP 连接。搜索绑定模式下请求数完全不变；直接绑定模式（`authentication_ldap_simple_bind_dn_pattern`）下会多一个读请求，因为 bind 响应无法携带属性。
- 直接绑定模式下由用户自己读取该属性。如果目录不允许用户读取自己的该属性，而 `authentication_ldap_simple_bind_root_dn` 与 `authentication_ldap_simple_bind_root_pwd` 已配置，FE 会自动用该账号重试一次，无需改变认证模式。两者都读不到时用户组为空，登录仍然成功。
- 用户组为空且配置了 `permitted_groups` 时该用户被拒绝登录，因为交集必然为空。
- 用户组在登录时计算一次并保存在会话中。目录中的成员关系变更在下次登录时生效，不会影响正在运行的会话。
- 旧的单用户写法 `CREATE USER ... IDENTIFIED WITH authentication_ldap_simple AS '<dn>'` 不支持该功能，请改用 Security Integration。
- `EXECUTE AS` 解析的是**目标身份**的用户组，依据目标身份自己的配置：native 密码用户没有 LDAP 身份，只会拿到它的 group provider 组；`EXECUTE AS EXTERNAL USER` 用 `authentication_chain` 里第一个 `authentication_ldap_simple` 集成。impersonate 全程不出示目标用户的口令，所以该属性是用 `authentication_ldap_simple_bind_root_dn` 与 `authentication_ldap_simple_bind_root_pwd` 读的；没有配服务账号时只有 group provider 生效。

:::

##### authentication_ldap_simple_ssl_conn_allow_insecure

- 必需：否
- 描述：是否允许使用非加密方式连接到 LDAP 服务器。默认值：`true`。将此值设置为 `false` 表示访问 LDAP 需要使用 SSL 加密。

##### authentication_ldap_simple_ssl_conn_trust_store_path

- 必需：否
- 描述：存储 LDAP 服务器的 SSL CA 证书的本地路径。支持 pem 和 jks 格式。如果证书是由受信机构颁发的，则无需配置。

##### authentication_ldap_simple_ssl_conn_trust_store_pwd

- 必需：否
- 描述：访问本地存储的 LDAP 服务器的 SSL CA 证书所用的密码。pem 格式证书不需要密码，只有 jsk 格式证书需要。

##### group_provider

- 必需：否
- 描述：与安全集成结合使用的 Group Provider 名称。多个 Group Provider 用逗号分隔。设置后，StarRocks 将在用户登录时记录每个指定提供者下的用户组信息。从 v3.5 开始支持。有关启用 Group Provider 的详细说明，请参阅 [Authenticate User Groups](../group_provider.md)。

##### permitted_groups

- 必需：否
- 描述：允许登录到 StarRocks 的组名称。多个组用逗号分隔。确保指定的组可以通过组合的 Group Provider 检索。从 v3.5 开始支持。

##### comment

- 必需：否
- 描述：安全集成的描述。

<SecurityIntegrationJWT />

<SecurityIntegrationOAuth />

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
      "type" = "authentication_ldap_simple",
      "authentication_ldap_simple_server_host" = "",
      "authentication_ldap_simple_server_port" = "",
      "authentication_ldap_simple_bind_base_dn" = "",
      "authentication_ldap_simple_user_search_attr" = ""
      "authentication_ldap_simple_bind_root_dn" = "",
      "authentication_ldap_simple_bind_root_pwd" = "",
      "authentication_ldap_simple_ssl_conn_allow_insecure" = "{true | false}",
      "authentication_ldap_simple_ssl_conn_trust_store_path" = "",
      "authentication_ldap_simple_ssl_conn_trust_store_pwd" = "",
      "comment" = ""
)|
+----------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
```

:::note
执行 SHOW CREATE SECURITY INTEGRATION 时，`ldap_bind_root_pwd` 会被隐藏。
:::

<SecurityIntegrationConnectSeeAlso />
