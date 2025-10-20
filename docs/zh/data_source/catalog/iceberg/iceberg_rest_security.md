---
displayed_sidebar: docs
---

# Iceberg REST Catalog 的安全设置

在多个节点协同访问同一数据湖的场景中，核心挑战是**如何实现安全、统一且可审计的权限管理**。在传统模式中，用户需要为每个节点单独配置存储凭证并进行本地权限控制，这不仅增加了维护成本，还带来了凭证泄露和权限不一致的风险。

通过将 Iceberg REST Catalog 与 StarRocks 集成，可以通过 JWT（JSON Web Token）认证和 Vended Credentials（临时凭证）的组合实现安全访问和统一权限管理。这种设置有助于：

- **降低凭证风险**：无需在 StarRocks 中存储高权限账户信息。凭证由 Catalog 临时发放，避免泄露。
- **统一和简化权限**：所有数据库、表和视图的访问控制由 Catalog 集中管理，确保不同节点间的一致性，避免冗余配置。
- **合规和简化操作**：用户操作可追溯，便于审计。同时减少了在 StarRocks 中维护权限和存储凭证的成本。

## 安全机制

- **JWT 认证**
  - 当用户登录到 StarRocks 时，获取的 JWT Token 可以传递给 Iceberg REST Session Catalog。
  - Catalog 将根据 JWT Token 认证用户，并以真实用户身份执行查询。
  - 这样做的好处是 StarRocks 不需要存储高权限账户，从而显著降低安全风险。
- **Vended Credentials**
  - 认证后，Catalog 可以为用户生成临时存储访问凭证。
  - 用户无需在 StarRocks 中配置存储层的凭证。
  - 每次访问对象存储时，StarRocks 将使用 Catalog 发放的临时凭证。
  - 这增强了安全性并简化了凭证管理。

## 使用方法

### 第一步：设置 JWT 认证

在 StarRocks 中，配置一个 **[基于 JWT 的安全集成](../../../administration/user_privs/authentication/security_integration.md#create-a-security-integration-with-jwt)** 或 [创建一个使用 JWT 认证的用户](https://docs.starrocks.io/en/docs/administration/user_privs/authentication/jwt_authentication/)。

### 第二步：创建 Iceberg REST Catalog 并配置安全设置

```SQL
CREATE EXTERNAL CATALOG iceberg_rest_catalog
PROPERTIES (
  "iceberg.catalog.type" = "rest",
  "iceberg.catalog.uri" = "<rest_server_api_endpoint>",
  "iceberg.catalog.security" = "jwt",
  "iceberg.catalog.warehouse" = "<identifier_or_path_to_warehouse>",
  "iceberg.catalog.vended-credentials-enabled" = "true"
);
```

属性：

- `iceberg.catalog.type`：将此属性设置为 `rest`，表示使用 REST Catalog。
- `iceberg.catalog.uri`：REST Catalog 服务的 API 端点。
- `iceberg.catalog.security`：将此属性设置为 `jwt` 以启用 JWT 认证。StarRocks 将当前用户的认证信息传递给 Catalog。
- `iceberg.catalog.warehouse`：指定 Iceberg 数据仓库路径或标识符。
- `iceberg.catalog.vended-credentials-enabled`：将此属性设置为 `true` 以启用 Vended Credentials 并允许临时凭证发放。

### 第三步：授予权限

当用户通过 StarRocks 查询 Catalog 时，权限在两个层面上处理：

1. **StarRocks 内部对象权限**

   用户是否有权限查看 StarRocks 内的 Catalog 对象，并使用 `SET CATALOG` 切换到该 Catalog。

2. **Catalog 内部权限**

   用户在访问 Catalog 内的数据库、表或视图时是否具有适当的访问权限。

在理想的工作流程中，StarRocks 负责管理用户登录到 StarRocks 集群后访问 Catalog 对象所需的基本权限。Catalog 内的细粒度数据访问权限完全由 Catalog 自己管理。这意味着：

- 用户必须拥有 StarRocks 中 Catalog 对象的 `USAGE` 权限，以便可以使用 `SET CATALOG` 切换到 Catalog 或使用 `SHOW CATALOGS` 查看 Catalog 信息。
- 数据级别的权限检查完全由 Catalog 处理。

#### StarRocks 内部 Catalog 对象权限

要在 StarRocks 中执行 `SHOW CATALOGS` 或 `SET CATALOG`，用户需要对相应的 Catalog 对象拥有 `USAGE` 权限。

##### 选项 1：授予所有用户访问权限

如果允许所有用户查看并切换到指定的 Catalog，可以将 Catalog 的 `USAGE` 权限授予 `public` 角色。

```SQL
GRANT USAGE ON CATALOG <catalog_name> TO ROLE public;
```

这将自动授予所有用户在登录到 StarRocks 时 `public` 角色，从而获得该 Catalog 的 `USAGE` 权限。

##### 选项 2：细粒度权限管理

如果只希望特定用户或组查看并切换到 Catalog，可以使用 **Group Provider** 和 **Role** 进行更细粒度的管理：

1. 使用 [Group Provider](../../../administration/user_privs/group_provider.md) 将外部用户组信息同步到 StarRocks。
2. 创建相应的 StarRocks 角色。

    ```SQL
    CREATE ROLE <role_name>;
    ```

3. 将角色绑定到外部用户组：

    ```SQL
    GRANT <role_name> TO EXTERNAL GROUP <group_name>;
    ```

4. 授予角色 Catalog 的 `USAGE` 权限：

    ```SQL
    GRANT USAGE ON CATALOG <catalog_name> TO ROLE <role_name>;
    ```

在这种情况下，外部组的成员在登录到 StarRocks 时将自动继承分配的角色，并获得该 Catalog 的 `USAGE` 权限。

#### Catalog 内部数据权限

通过配置 Catalog 的属性，Catalog 内所有对象的权限检查都委托给 Catalog 自己。Catalog 将集中管理认证和权限，简化了 StarRocks 中的权限配置，避免了冗余授权，并确保不同引擎间的一致权限规则。

```SQL
ALTER CATALOG iceberg_rest_catalog
SET PROPERTIES (
  "catalog.access.control" = "allowall"
);
```

属性：

- `catalog.access.control`：将此属性设置为 `allowall`。StarRocks 将不再对 Catalog 内的对象执行任何进一步的权限检查。相反，所有权限管理将由 Catalog 自己处理。