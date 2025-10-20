---
displayed_sidebar: docs
---

# Security Setup for Iceberg REST Catalog

In scenarios where multiple nodes collaboratively access the same data lake, the core challenge is **how to achieve secure, unified, and auditable permission management**. In traditional models, users need to configure storage credentials and perform local permission control individually for each node, which not only increases maintenance costs but also exposes risks of credential leakage and inconsistent permissions.

By integrating Iceberg REST Catalog with StarRocks, secure access and unified permission management can be achieved through a combination of JWT (JSON Web Token) authentication and Vended Credentials (temporary credentials). This setup helps to:

- **Reduce credential risks**: No need to store high-privilege account information within StarRocks. Credentials are issued temporarily by the Catalog, avoiding leakage.
- **Unified and simplified permissions**: Access control for all databases, tables, and views is centrally managed by the Catalog, ensuring consistency across different nodes and avoiding redundant configuration.
- **Compliance and simplified operations**: User actions are traceable, facilitating audits. It also reduces the cost of maintaining permissions and storage credentials within StarRocks.

## Security mechanisms

- **JWT authentication**
  - When a user logs into StarRocks, the JWT Token obtained can be passed through to the Iceberg REST Session Catalog.
  - The Catalog will authenticate the user based on the JWT Token and execute queries under the real user’s identity.
  - The benefit is that StarRocks doesn’t need to store high-privilege accounts, which significantly lowers security risks.
- **Vended credentials**
  - After authentication, the Catalog can generate temporary storage access credentials for the user.
  - Users do not need to configure credentials for the storage layer in StarRocks.
  - Each time object storage is accessed, StarRocks will use the temporary credentials issued by the Catalog.
  - This enhances security and simplifies credential management.

## Usage

### Step 1. Sett up JWT authentication

In StarRocks, configure a **[JWT-based Security Integration](../../../administration/user_privs/authentication/security_integration.md#create-a-security-integration-with-jwt)** or [create a user with JWT authentication](https://docs.starrocks.io/en/docs/administration/user_privs/authentication/jwt_authentication/).

### Step 2. Create Iceberg REST Catalog and configure security settings

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

Properties:

- `iceberg.catalog.type`: Set this property to `rest`, indicating to use the REST Catalog.
- `iceberg.catalog.uri`: API endpoint for the REST Catalog service.
- `iceberg.catalog.security`: Set this property to `jwt` to enable JWT authentication. StarRocks will pass the current user’s authentication information to the Catalog.
- `iceberg.catalog.warehouse`: Specifies the Iceberg data warehouse path or identifier.
- `iceberg.catalog.vended-credentials-enabled`: Set this property to `true` to enable Vended Credentials and allow temporary credentials issuance.

### Step 3. Grant permissions

When a user queries the Catalog via StarRocks, permissions are handled at two levels:

1. **StarRocks Internal Object Permissions**

   Whether the user has permission to view the Catalog object within StarRocks and switch the session to that Catalog using `SET CATALOG`.

2. **Catalog Internal Permissions**

   Whether the user has appropriate access permissions when accessing databases, tables, or views within the Catalog.

In the ideal workflow, StarRocks is responsible for managing the basic permissions required for a user to access Catalog objects after logging into the StarRocks cluster. Fine-grained data access permissions within the Catalog are completely managed by the Catalog itself. This means:

- Users must have the `USAGE` permission to the Catalog object in StarRocks so that they can switch to the catalog using `SET CATALOG` or view the catalog information using `SHOW CATALOGS`.
- Data-level permission checks are entirely handled by the Catalog.

#### StarRocks Internal Catalog Object Permissions

To execute `SHOW CATALOGS` or `SET CATALOG` in StarRocks, the user needs `USAGE` permission on the corresponding Catalog object.

##### Option 1: Grant Access to All Users

If you allow all users to view and switch to a specified Catalog, you can grant the `USAGE` permission of the Catalog to the `public` role.

```SQL
GRANT USAGE ON CATALOG <catalog_name> TO ROLE public;
```

This will automatically grant all users, upon logging into StarRocks, the `public` role and thus the `USAGE` permission of that Catalog.

##### Option 2: Granular Permission Management

If you want only specific users or groups to view and switch to a Catalog, you can use **Group Provider** and **Role** for finer-grained management:

1. Synchronize the external user group information to StarRocks using [Group Provider](../../../administration/user_privs/group_provider.md).
2. Create the corresponding StarRocks role.

    ```SQL
    CREATE ROLE <role_name>;
    ```

3. Bind the role to the external user group:

    ```SQL
    GRANT <role_name> TO EXTERNAL GROUP <group_name>;
    ```

4. Grant the role `USAGE` permission of the Catalog:

    ```SQL
    GRANT USAGE ON CATALOG <catalog_name> TO ROLE <role_name>;
    ```

In this case, members of the external group will automatically inherit the assigned role upon logging into StarRocks and gain the `USAGE` permission of the Catalog.

#### Catalog Internal Data Permissions

By configuring the Catalog’s properties, permission checks for all objects within the Catalog are delegated to the Catalog itself. The Catalog will manage authentication and permissions centrally, simplifying permission configuration in StarRocks, avoiding redundant authorizations, and ensuring consistent permission rules across different engines.

```SQL
ALTER CATALOG iceberg_rest_catalog
SET PROPERTIES (
  "catalog.access.control" = "allowall"
);
```

Properties:

- `catalog.access.control`: Set this property to `allowall`. StarRocks will not perform any further permission checks on objects within the Catalog. Instead, all permission management will be handled by the Catalog itself.

