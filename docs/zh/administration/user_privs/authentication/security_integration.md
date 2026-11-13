---
displayed_sidebar: docs
description: "Integrate StarRocks with external authentication systems using security integration to enable seamless user authentication without manual user creation."
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

### 使用 JWT 认证创建安全集成

#### 语法

```SQL
CREATE SECURITY INTEGRATION <security_integration_name> 
PROPERTIES (
    "type" = "authentication_jwt",
    "jwks_url" = "",
    "principal_field" = "",
    "required_issuer" = "",
    "required_audience" = "",
    "group_source" = "",
    "group_claim" = "",
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

##### group_source

- 必需：否
- 描述：经该安全集成认证的用户，其用户组从哪里来。取值：
  - `group_provider`（默认）：只使用 `group_provider` 中配置的 Group Provider，与低版本行为一致。
  - `claim`：只使用已验签 token 中由 `group_claim` 指定的 claim。`group_provider` 中已配置的 Group Provider 被忽略，但配置保留，因此改回来只需要一条 `ALTER SECURITY INTEGRATION`。
  - `both`：两者取并集。

  从 token 里取用户组，是让没有 LDAP 目录的身份提供方也能授权的前提：解析出的用户组与 Group Provider 解析出的用户组同等参与 `GRANT ... TO EXTERNAL GROUP` 的角色映射、`permitted_groups` 登录校验、`current_group()` 返回值以及 Apache Ranger 鉴权。自 v4.2 起支持。

##### group_claim

- 必需：`group_source` 为 `claim` 或 `both` 时必需。未设置 `group_source` 而单独设置该参数会被拒绝，因为它不会起任何作用；与显式设置的 `group_source` = `group_provider` 一起设置则会被接受并保留，这样可以先配置好 claim，再用 `ALTER SECURITY INTEGRATION` 切换来源。
- 描述：token 中存放用户组的 claim。嵌套在其他对象里的 claim 用点号分隔逐层书写，例如顶层 claim 写 `groups`，Keycloak 的 realm role 写 `realm_access.roles`，其 client role 写 `resource_access.<client_id>.roles`。该 claim 应当是字符串数组；单个字符串也接受，视为一个用户组。每个组名会去掉首尾空白；除此之外不做任何改写，大小写和前导斜杠都保留，因为组名会原样交给 Apache Ranger，而它按精确字符串匹配。组名与 `GRANT ... TO EXTERNAL GROUP` 或 `permitted_groups` 中仅大小写不同的条目是否算匹配，由 FE 配置项 `authentication_group_case_insensitive` 决定（默认忽略大小写；如果你的身份提供方把大小写不同的组当作不同的组，请设为 `false`）。

  该参数故意没有默认值：各身份提供方签发的 claim 并不相同，猜错的结果是用户能登录但没有任何权限，而不是明确报错。claim 只在签名、`required_issuer` 和 `required_audience` 都校验通过之后才读取，因此持有 token 的人无法自己指定用户组。如果 token 中没有该 claim，登录仍然成功、用户组为空，原因记录在日志里，而不会被当成认证失败——用户所属组超过 200 个时 Microsoft Entra ID 会直接丢掉整个 claim，所以 claim 缺失是正确配置下也会出现的状态。另外注意，多数身份提供方默认不会把用户组放进 token：Keycloak 需要在 client 上加一个 group membership mapper，Entra ID 需要在 token 配置中加上 groups claim。自 v4.2 起支持。

##### comment

- 必需：否
- 描述：安全集成的描述。

### 使用 OAuth 2.0 创建安全集成

#### 语法

```SQL
CREATE SECURITY INTEGRATION <security_integration_name> 
PROPERTIES (
    "type" = "authentication_oauth2",
    "auth_server_url" = "",
    "token_server_url" = "",
    "client_id" = "",
    "client_secret" = "",
    "redirect_url" = "",
    "jwks_url" = "",
    "principal_field" = "",
    "required_issuer" = "",
    "required_audience" = "",
    "group_source" = "",
    "group_claim" = "",
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
- 描述：安全集成的类型。指定为 `authentication_oauth2`。

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

##### group_source

- 必需：否
- 描述：经该安全集成认证的用户，其用户组从哪里来。取值：
  - `group_provider`（默认）：只使用 `group_provider` 中配置的 Group Provider，与低版本行为一致。
  - `claim`：只使用已验签 token 中由 `group_claim` 指定的 claim。`group_provider` 中已配置的 Group Provider 被忽略，但配置保留，因此改回来只需要一条 `ALTER SECURITY INTEGRATION`。
  - `both`：两者取并集。

  从 token 里取用户组，是让没有 LDAP 目录的身份提供方也能授权的前提：解析出的用户组与 Group Provider 解析出的用户组同等参与 `GRANT ... TO EXTERNAL GROUP` 的角色映射、`permitted_groups` 登录校验、`current_group()` 返回值以及 Apache Ranger 鉴权。对 OAuth 2.0 安全集成而言，这同样适用于经 MySQL 协议的浏览器登录：这条路上 token 要等用户在浏览器里登录之后才到达 StarRocks，所以用户组在 ID token 到达时读取，`permitted_groups` 也在浏览器登录后的第一条语句上校验，而不是在建立连接时。`group_source` 保持 `group_provider` 时行为不变：用户组和校验仍在建立连接时决定。自 v4.2 起支持。

##### group_claim

- 必需：`group_source` 为 `claim` 或 `both` 时必需；`group_source` 为其他取值时设置该参数会被拒绝，因为它不会起任何作用。
- 描述：token 中存放用户组的 claim。嵌套在其他对象里的 claim 用点号分隔逐层书写，例如顶层 claim 写 `groups`，Keycloak 的 realm role 写 `realm_access.roles`，其 client role 写 `resource_access.<client_id>.roles`。该 claim 应当是字符串数组；单个字符串也接受，视为一个用户组。每个组名会去掉首尾空白；除此之外不做任何改写，大小写和前导斜杠都保留，因为组名会原样交给 Apache Ranger，而它按精确字符串匹配。组名与 `GRANT ... TO EXTERNAL GROUP` 或 `permitted_groups` 中仅大小写不同的条目是否算匹配，由 FE 配置项 `authentication_group_case_insensitive` 决定（默认忽略大小写；如果你的身份提供方把大小写不同的组当作不同的组，请设为 `false`）。

  该参数故意没有默认值：各身份提供方签发的 claim 并不相同，猜错的结果是用户能登录但没有任何权限，而不是明确报错。claim 只在签名、`required_issuer` 和 `required_audience` 都校验通过之后才读取，因此持有 token 的人无法自己指定用户组。如果 token 中没有该 claim，登录仍然成功、用户组为空，原因记录在日志里，而不会被当成认证失败——用户所属组超过 200 个时 Microsoft Entra ID 会直接丢掉整个 claim，所以 claim 缺失是正确配置下也会出现的状态。另外注意，多数身份提供方默认不会把用户组放进 token：Keycloak 需要在 client 上加一个 group membership mapper，Entra ID 需要在 token 配置中加上 groups claim。自 v4.2 起支持。

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
- `authentication_chain` 可以同时包含 LDAP 和 JWT 安全集成，但在 MySQL 协议下只是部分支持，见[在一条认证链中混用 LDAP 与 JWT 安全集成](#在一条认证链中混用-ldap-与-jwt-安全集成)。
:::

您可以使用以下语句检查 `authentication_chain` 的值：

```SQL
ADMIN SHOW FRONTEND CONFIG LIKE 'authentication_chain';
```

### 在一条认证链中混用 LDAP 与 JWT 安全集成

在 HTTP 和 Arrow Flight 上，同一条 `authentication_chain` 里的 LDAP 和 JWT 安全集成可以并存，因为凭据本身就说明了自己是什么：`Basic`（用户名和口令）交给 LDAP 安全集成验证，`Bearer`（token）交给 JWT 和 OAuth 2.0 安全集成验证。参见[通过 HTTP 和 Arrow Flight 连接](#通过-http-和-arrow-flight-连接)。

在 MySQL 协议上只是部分支持。StarRocks 必须在看到任何凭据之前为连接选定一个客户端认证插件，而两类安全集成需要的插件不同：LDAP 用 `mysql_clear_password`，JWT 用 `authentication_openid_connect_client`。StarRocks 按客户端在握手时声明的插件选择：

| 客户端声明的插件 | 使用的安全集成 |
| --- | --- |
| `authentication_openid_connect_client` | 链上第一个 JWT 安全集成 |
| `mysql_clear_password` | 链上第一个 LDAP 安全集成 |
| 都没有声明（多数客户端默认发送 `mysql_native_password`） | 链上第一个安全集成 |

因此：

- 把大多数客户端不做任何额外设置就要用的安全集成放在最前面。
- 另一类客户端需要声明自己的插件：MySQL 客户端用 `--default-auth=<plugin>`，MySQL Connector/J 用 `defaultAuthenticationPlugin=<plugin>`。MySQL 客户端从 9.1 起自带 `authentication_openid_connect_client`，社区版也包含；更早的 MySQL 客户端和 MariaDB 客户端没有这个插件。
- 无法声明插件的客户端（例如 PyMySQL）总是使用链上第一个安全集成。

```Bash
# JWT 安全集成用户，链上 LDAP 安全集成在前
mysql -h <fe_host> -P <query_port> --default-auth=authentication_openid_connect_client \
    --authentication-openid-connect-client-id-token-file=<path_to_token_file> -u <username>

# LDAP 安全集成用户，链上 JWT 安全集成在前
mysql -h <fe_host> -P <query_port> --default-auth=mysql_clear_password --enable-cleartext-plugin -u <username> -p
```

如果 JWT 客户端无法加载 OpenID Connect 插件，可以把 FE 配置项 `authentication_chain_accept_jwt_as_password` 设为 `true`，并把 LDAP 安全集成放在最前面。JWT 客户端随后把 token 当口令传入：MySQL 登录的口令形状像 JWT 时，StarRocks 先用 JWT 和 OAuth 2.0 安全集成验证它，没有任何一个接受时才把它当口令交给 LDAP 安全集成。token 和明文口令一样在线路上传输，请使用 SSL 连接。该配置项只作用于 MySQL 协议；在 HTTP 和 Arrow Flight 上请以 `Bearer` 发送 token。

```Bash
mysql -h <fe_host> -P <query_port> --ssl-mode=REQUIRED --enable-cleartext-plugin -u <username> -p"$(cat <path_to_token_file>)"
```

## 通过 HTTP 和 Arrow Flight 连接

加入 `authentication_chain` 的安全集成用户可以从 StarRocks 的所有入口登录，而不仅是 MySQL 协议：HTTP SQL API、Stream Load（经 FE 或直接访问 BE）、FE Web UI 以及 Arrow Flight SQL。客户端需要发送的凭据取决于安全集成的类型。

| 安全集成类型 | HTTP | Arrow Flight SQL |
| --- | --- | --- |
| LDAP（`authentication_ldap_simple`） | HTTP Basic：`-u <username>:<password>` | 用户名和密码 |
| JWT（`authentication_jwt`）、<br />OAuth 2.0（`authentication_oauth2`） | `Authorization: Bearer <token>` | `Authorization: Bearer <token>` 请求头，<br />例如 ADBC 的 `adbc.flight.sql.authorization_header` 选项 |

使用 Bearer token 时不需要提供用户名。StarRocks 从 `principal_field` 指定的 claim 中读取用户名，用 `jwks_url` 校验签名，并把 token 交给链上每一个 `required_issuer` 和 `required_audience`（如设置）与之匹配的 JWT 或 OAuth 2.0 安全集成。多个安全集成共用同一个 issuer 时，请为每个安全集成设置不同的 `required_audience`：token 的 `aud` claim 决定命中哪个安全集成，进而决定使用哪个 Group Provider 和哪些角色。强烈建议为每个安全集成都设置 `required_issuer`。

示例：

```Bash
# 以 LDAP 安全集成用户调用 SQL API
curl -u alice:alice_password -H 'Content-Type: application/json' -XPOST \
    http://<fe_host>:<fe_http_port>/api/v1/catalogs/default_catalog/databases/<db>/sql \
    -d '{"query": "select current_user()"}'

# 使用 JWT 直接向 BE 发起 Stream Load（BE 需开启 enable_http_auth = true）
curl -H "Authorization: Bearer ${TOKEN}" -H "label:load_1" -H "column_separator:," \
    -T data.csv -XPUT http://<be_host>:<be_http_port>/api/<db>/<table>/_stream_load
```

请注意以下几点：

- 安全集成用户的权限只能通过 [Group Provider](../group_provider.md) 和 `GRANT ... TO EXTERNAL GROUP` 获得；用户解析出的组在所有入口上都生效。
- 在 Arrow Flight SQL 上，JWT 的认证方式与用户名密码完全一致：服务端返回自己颁发的 session token，客户端在该连接后续的请求中使用它。该 session 不会比 JWT 活得更久；token 的 `exp` 一过，连接即被拒绝，客户端需要用新的 JWT 重新认证。
- FE 配置项 `enable_rest_bearer_authentication`（默认 `true`）用于关闭 HTTP 和 Arrow Flight 入口的 Bearer 认证。它不影响 MySQL 协议；MySQL 协议下的 token 登录仍使用 `authentication_openid_connect_client` 和 `authentication_oauth2_client` 插件。
- MySQL 协议同一时刻只能向客户端提供一种插件：客户端声明的插件决定走哪个安全集成，没有声明的客户端走 `authentication_chain` 中的第一个安全集成（见[在一条认证链中混用 LDAP 与 JWT 安全集成](#在一条认证链中混用-ldap-与-jwt-安全集成)）。HTTP 和 Arrow Flight 没有这个限制，因为 token 本身会按 issuer 和 audience 路由。
- 所有 HTTP 认证失败都返回 `401`，`WWW-Authenticate` 中同时带 `Basic` 和 `Bearer` 两个 challenge，且不说明是哪个安全集成拒绝、为何拒绝。排查时请使用 FE 的 `authentication_failure_total` 和 `authentication_failure_reason_total` 指标。

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

## 通过安全集成连接到 StarRocks

- 有关如何通过 LDAP 连接到 StarRocks 的说明，请参阅 [LDAP 认证 - 连接到 StarRocks](./ldap_authentication.md#从-mysql-客户端连接-ldap)。
- 有关通过 JWT 连接 StarRocks 的说明，请参阅 [JSON Web Token 认证 - 连接 StarRocks](./jwt_authentication.md#使用-jwt-从-mysql-客户端连接)。
- 有关通过 OAuth 2.0 连接 StarRocks 的说明，请参阅 [OAuth 2.0 认证 - 连接 StarRocks](./oauth2_authentication.md#使用-oauth-20-从-jdbc-客户端连接)。

## 另见

- 有关如何在 StarRocks 中通过 LDAP 手动验证用户的说明，请参阅 [LDAP 认证](./ldap_authentication.md)。
- 有关如何在 StarRocks 中通过 SON Web Token 认证手动验证用户的说明，请参阅 [JSON Web Token 认证](./jwt_authentication.md)。
- 有关如何在 StarRocks 中通过 OAuth 2.0 手动验证用户的说明，请参阅 [OAuth 2.0 认证](./oauth2_authentication.md)。
- 有关如何验证用户组的说明，请参阅 [认证用户组](../group_provider.md)。
