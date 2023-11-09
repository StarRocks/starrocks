---
displayed_sidebar: "English"
---

# Manage user privileges

This topic describes how to manage users, roles, and privileges in StarRocks.

StarRocks employs both role-based access control (RBAC) and identity-based access control (IBAC) to manage privileges within a StarRocks cluster, allowing cluster administrators to easily restrict privileges within the cluster on different granular levels.

Within a StarRocks cluster, privileges can be granted to users or roles. A role is a collection of privileges that can be assigned to users or other roles in the cluster as needed. A user can be granted one or more roles, which determine their permissions on different objects.

## View user and role information

Users with the system-defined role `user_admin` can view all the user and role information within the StarRocks cluster.

### View privilege information

You can view the privileges granted to a user or a role using [SHOW GRANTS](../../sql-reference/sql-statements/account-management/SHOW_GRANTS.md).

- View the privileges of the current user.

  ```SQL
  SHOW GRANTS;
  ```

  > **NOTE**
  >
  > Any user can view their own privileges without needing any privileges.

- View the privileges of a specific user.

  The following example shows the privileges of the user `jack`:

  ```SQL
  SHOW GRANTS FOR jack@'172.10.1.10';
  ```

- View the privileges of a specific role.

  The following example shows the privileges of the role `example_role`:

  ```SQL
  SHOW GRANTS FOR ROLE example_role;
  ```

### View user property

You can view the property of a user using [SHOW PROPERTY](../../sql-reference/sql-statements/account-management/SHOW_PROPERTY.md).

The following example shows the property of the user `jack`:

```SQL
SHOW PROPERTY FOR jack@'172.10.1.10';
```

### View roles

You can view all the roles within the StarRocks cluster using [SHOW ROLES](../../sql-reference/sql-statements/account-management/SHOW_ROLES.md).

```SQL
SHOW ROLES;
```

### View users

You can view all the users within the StarRocks cluster using SHOW USERS.

```SQL
SHOW USERS;
```

## Manage users

Users with the system-defined role `user_admin` can create users, alter users, and drop users in StarRocks.

### Create a user

You can create a user by specifying the user identity, authentication method, and default role.

StarRocks supports user authentication with login credentials or LDAP authentication. For more information about StarRocks' authentication, see [Authentication](../../administration/Authentication.md). For more information and advanced instructions on creating a user, see [CREATE USER](../../sql-reference/sql-statements/account-management/CREATE_USER.md).

The following example creates the user `jack`, allows it to connect only from the IP address `172.10.1.10`, sets the password to `12345` for it, and assigns the role `example_role` to it as its default role:

```SQL
CREATE USER jack@'172.10.1.10' IDENTIFIED BY '12345' DEFAULT ROLE 'example_role';
```

> **NOTE**
>
> - StarRocks encrypts users' passwords before storing them. You can get the encrypted password using the password() function.
> - A system-defined default role `PUBLIC` is assigned to a user if no default role is specified during user creation.

### Alter a user

You can alter the password, default role, or property for a user.

The default role of a user is automatically activated when the user connects to StarRocks. For instructions on how to enable all (default and granted) roles for a user after connection, see [Enable all roles](#enable-all-roles).

#### Alter the default role of a user

You can set the default role of a user using [SET DEFAULT ROLE](../../sql-reference/sql-statements/account-management/SET_DEFAULT_ROLE.md) or [ALTER USER](../../sql-reference/sql-statements/account-management/ALTER_USER.md).

Both of the following examples set the default role of `jack` to `db1_admin`. Note that `db1_admin` must have been assigned to `jack`.

- Set the default role using SET DEFAULT ROLE:

  ```SQL
  SET DEFAULT ROLE 'db1_admin' TO jack@'172.10.1.10';
  ```

- Set the default role using ALTER USER:

  ```SQL
  ALTER USER jack@'172.10.1.10' DEFAULT ROLE 'db1_admin';
  ```

#### Alter the property of a user

You can set the property of a user using [SET PROPERTY](../../sql-reference/sql-statements/account-management/SET_PROPERTY.md).

The following example sets the maximum number of connections for user `jack` to `1000`. User identities that have the same user name share the same property.

Therefore, you only need to set the property for `jack` and this setting takes effect for all the user identities with the user name `jack`.

```SQL
SET PROPERTY FOR jack 'max_user_connections' = '1000';
```

#### Reset password for a user

You can reset the password for a user using [SET PASSWORD](../../sql-reference/sql-statements/account-management/SET_PASSWORD.md) or [ALTER USER](../../sql-reference/sql-statements/account-management/ALTER_USER.md).

> **NOTE**
>
> - Any user can reset their own passwords without needing any privileges.
> - Only the `root` user itself can set its password. If you have lost its password and cannot connect to StarRocks, see [Reset lost root password](#reset-lost-root-password) for more instructions.

Both the following examples reset the password of `jack` to `54321`:

- Reset the password using SET PASSWORD:

  ```SQL
  SET PASSWORD FOR jack@'172.10.1.10' = PASSWORD('54321');
  ```

- Reset the password using ALTER USER:

  ```SQL
  ALTER USER jack@'172.10.1.10' IDENTIFIED BY '54321';
  ```

#### Reset lost root password

If you have lost the password of the `root` user and cannot connect to StarRocks, you can reset it by following these procedures:

1. Add the following configuration item to the configuration files **fe/conf/fe.conf** of **all FE nodes** to disable user authentication:

   ```YAML
   enable_auth_check = false
   ```

2. Restart **all FE nodes** to allow the configuration to take effect.

   ```Bash
   ./fe/bin/stop_fe.sh
   ./fe/bin/start_fe.sh
   ```

3. Connect from a MySQL client to StarRocks via the `root` user. You do not need to specify the password when user authentication is disabled.

   ```Bash
   mysql -h <fe_ip_or_fqdn> -P<fe_query_port> -uroot
   ```

4. Reset the password for the `root` user.

   ```SQL
   SET PASSWORD for root = PASSWORD('xxxxxx');
   ```

5. Re-enable user authentication by setting the configuration item `enable_auth_check` to `true` in the configuration files **fe/conf/fe.conf** of **all FE nodes**.

   ```YAML
   enable_auth_check = true
   ```

6. Restart **all FE nodes** to allow the configuration to take effect.

   ```Bash
   ./fe/bin/stop_fe.sh
   ./fe/bin/start_fe.sh
   ```

7. Connect from a MySQL client to StarRocks using the `root` user and the new password to verify whether the password is reset successfully.

   ```Bash
   mysql -h <fe_ip_or_fqdn> -P<fe_query_port> -uroot -p<xxxxxx>
   ```

### Drop a user

You can drop a user using [DROP USER](../../sql-reference/sql-statements/account-management/DROP_USER.md).

The following example drops the user `jack`:

```SQL
DROP USER jack@'172.10.1.10';
```

## Manage roles

Users with the system-defined role `user_admin` can create, grant, revoke, or drop roles in StarRocks.

### Create a role

You can create a role using [CREATE ROLE](../../sql-reference/sql-statements/account-management/CREATE_ROLE.md).

The following example creates the role `example_role`:

```SQL
CREATE ROLE example_role;
```

### Grant a role

You can grant roles to a user or another role using [GRANT](../../sql-reference/sql-statements/account-management/GRANT.md).

- Grant a role to a user.

  The following example grants the role `example_role` to the user `jack`:

  ```SQL
  GRANT example_role TO USER jack@'172.10.1.10';
  ```

- Grant a role to another role.

  The following example grants the role `example_role` to the role `test_role`:

  ```SQL
  GRANT example_role TO ROLE test_role;
  ```

### Revoke a role

You can revoke roles from a user or another role using [REVOKE](../../sql-reference/sql-statements/account-management/REVOKE.md).

> **NOTE**
>
> You cannot revoke the system-defined default role `PUBLIC` from a user.

- Revoke a role from a user.

  The following example revokes the role `example_role` from the user `jack`:

  ```SQL
  REVOKE example_role FROM USER jack@'172.10.1.10';
  ```

- Revoke a role from another role.

  The following example revokes the role `example_role` from the role `test_role`:

  ```SQL
  REVOKE example_role FROM ROLE test_role;
  ```

### Drop a role

You can drop a role using [DROP ROLE](../../sql-reference/sql-statements/account-management/DROP_ROLE.md).

The following example drops the role `example_role`:

```SQL
DROP ROLE example_role;
```

> **CAUTION**
>
> System-defined roles cannot be dropped.

### Enable all roles

The default roles of a user are roles that are automatically activated each time the user connects to the StarRocks cluster.

If you want to enable all the roles (default and granted roles) for all StarRocks users when they connect to the StarRocks cluster, you can perform the following operation.

This operation requires the system privilege OPERATE.

```SQL
SET GLOBAL activate_all_roles_on_login = TRUE;
```

You can also use SET ROLE to activate the roles assigned to you. For example, user `jack@'172.10.1.10'` has roles `db_admin` and `user_admin` but they are not default roles of the user and are not automatically activated when the user connects to StarRocks. If jack@'172.10.1.10' needs to activate `db_admin` and `user_admin`, he can run `SET ROLE db_admin, user_admin;`. Note that SET ROLE overwrites original roles. If you want to enable all your roles, run SET ROLE ALL.

## Manage privileges

Users with the system-defined role `user_admin` can grant or revoke privileges in StarRocks.

### Grant privileges

You can grant privileges to a user or a role using [GRANT](../../sql-reference/sql-statements/account-management/GRANT.md).

- Grant a privilege to a user.

  The following example grants the SELECT privilege on the table `sr_member` to the user `jack`, and allows `jack` to grant this privilege to other users or roles (by specifying WITH GRANT OPTION in the SQL):

  ```SQL
  GRANT SELECT ON TABLE sr_member TO USER jack@'172.10.1.10' WITH GRANT OPTION;
  ```

- Grant a privilege to a role.

  The following example grants the SELECT privilege on the table `sr_member` to the role `example_role`:

  ```SQL
  GRANT SELECT ON TABLE sr_member TO ROLE example_role;
  ```

### Revoke privileges

You can revoke privileges from a user or a role using [REVOKE](../../sql-reference/sql-statements/account-management/REVOKE.md).

- Revoke a privilege from a user.

  The following example revokes the SELECT privilege on the table `sr_member` from the user `jack`, and disallows `jack` to grant this privilege to other users or roles):

  ```SQL
  REVOKE SELECT ON TABLE sr_member FROM USER jack@'172.10.1.10';
  ```

- Revoke a privilege from a role.

  The following example revokes the SELECT privilege on the table `sr_member` from the role `example_role`:

  ```SQL
  REVOKE SELECT ON TABLE sr_member FROM ROLE example_role;
  ```

## Best practices

### Multi-service access control

Usually, a company-owned StarRocks cluster is managed by a sole service provider and maintains multiple lines of business (LOBs), each of which uses one or more databases.

As shown below, a StarRocks cluster's users include members from the service provider and two LOBs (A and B). Each LOB is operated by two roles - analysts and executives. Analysts generate and analyze business statements, and executives query the statements.

![User Privileges](../../assets/user_privilege_1.png)

LOB A independently manages the database `DB_A`, and LOB B the database `DB_B`. LOB A and LOB B use different tables in `DB_C`. `DB_PUBLIC` can be accessed by all members of both LOBs.

![User Privileges](../../assets/user_privilege_2.png)

Because different members perform different operations on different databases and tables, we recommend you create roles in accordance with their services and positions, apply only the necessary privileges to each role, and assign these roles to corresponding members. As shown below:

![User Privileges](../../assets/user_privilege_3.png)

1. Assign the system-defined roles `db_admin`, `user_admin`, and `cluster_admin` to cluster maintainers, set `db_admin` and `user_admin` as their default roles for daily maintenance, and manually activate the role `cluster_admin` when they need to operate the nodes of the cluster.

   Example:

   ```SQL
   GRANT db_admin, user_admin, cluster_admin TO USER user_platform;
   ALTER USER user_platform DEFAULT ROLE db_admin, user_admin;
   ```

2. Create users for each member within the LOBs, and set complex passwords for each user.
3. Create roles for each position within the LOBs, and apply the corresponding privileges to each role.

   For the director of each LOB, grant their role the maximum collection of the privileges their LOBs need, and the corresponding GRANT privileges (by specifying WITH GRANT OPTION in the statement). Therefore, they can assign these privileges to the members of their LOB. Set the role as their default role if their daily work requires it.

   Example:

   ```SQL
   GRANT SELECT, ALTER, INSERT, UPDATE, DELETE ON ALL TABLES IN DATABASE DB_A TO ROLE linea_admin WITH GRANT OPTION;
   GRANT SELECT, ALTER, INSERT, UPDATE, DELETE ON TABLE TABLE_C1, TABLE_C2, TABLE_C3 TO ROLE linea_admin WITH GRANT OPTION;
   GRANT linea_admin TO USER user_linea_admin;
   ALTER USER user_linea_admin DEFAULT ROLE linea_admin;
   ```

   For analysts and executives, assign them the role with the corresponding privileges.

   Example:

   ```SQL
   GRANT SELECT ON ALL TABLES IN DATABASE DB_A TO ROLE linea_query;
   GRANT SELECT ON TABLE TABLE_C1, TABLE_C2, TABLE_C3 TO ROLE linea_query;
   GRANT linea_query TO USER user_linea_salesa;
   GRANT linea_query TO USER user_linea_salesb;
   ALTER USER user_linea_salesa DEFAULT ROLE linea_query;
   ALTER USER user_linea_salesb DEFAULT ROLE linea_query;
   ```

4. For the database `DB_PUBLIC`, which can be accessed by all cluster users, grant the SELECT privilege on `DB_PUBLIC` to the system-defined role `public`.

   Example:

   ```SQL
   GRANT SELECT ON ALL TABLES IN DATABASE DB_PUBLIC TO ROLE public;
   ```

You can assign roles to others to achieve role inheritance in complicated scenarios.

For example, if analysts require privileges to write into and query tables in `DB_PUBLIC`, and executives can only query these tables, you can create roles `public_analysis` and `public_sales`, apply relevant privileges to the roles, and assign them to the original roles of analysts and executives respectively.

Example:

```SQL
CREATE ROLE public_analysis;
CREATE ROLE public_sales;
GRANT SELECT, ALTER, INSERT, UPDATE, DELETE ON ALL TABLES IN DATABASE DB_PUBLIC TO ROLE public_analysis;
GRANT SELECT ON ALL TABLES IN DATABASE DB_PUBLIC TO ROLE public_sales;
GRANT public_analysis TO ROLE linea_analysis;
GRANT public_analysis TO ROLE lineb_analysis;
GRANT public_sales TO ROLE linea_query;
GRANT public_sales TO ROLE lineb_query;
```

### Customize roles based on scenarios

We recommend you customize roles to manage privileges and users. The following examples classify a few combinations of privileges for some common scenarios.

1. Grant global read-only privileges on StarRocks tables:

   ```SQL
   --Create a role.
   CREATE ROLE read_only;
   --Grant the USAGE privilege on all catalogs to the role.
   GRANT USAGE ON ALL CATALOGS TO ROLE read_only;
   --Grant the SELECT privilege on all tables to the role.
   GRANT SELECT ON ALL TABLES IN ALL DATABASES TO ROLE read_only;
   --Grant the SELECT privilege on all views to the role.
   GRANT SELECT ON ALL VIEWS IN ALL DATABASES TO ROLE read_only;
   --Grant the SELECT privilege on all materialized views and the privilege to accelerate queries with them to the role.
   GRANT SELECT ON ALL MATERIALIZED VIEWS IN ALL DATABASES TO ROLE read_only;
   ```

   And you can further grant the privilege to use UDFs in queries:

   ```SQL
   --Grant the USAGE privilege on all database-level UDF to the role.
   GRANT USAGE ON ALL FUNCTIONS IN ALL DATABASES TO ROLE read_only;
   --Grant the USAGE privilege on global UDF to the role.
   GRANT USAGE ON ALL GLOBAL FUNCTIONS TO ROLE read_only;
   ```

2. Grant global write privileges on StarRocks tables:

   ```SQL
   --Create a role.
   CREATE ROLE write_only;
   --Grant the USAGE privilege on all catalogs to the role.
   GRANT USAGE ON ALL CATALOGS TO ROLE write_only;
   --Grant INSERT and UPDATE privileges on all tables to the role.
   GRANT INSERT, UPDATE ON ALL TABLES IN ALL DATABASES TO ROLE write_only;
   --Grant REFRESH privileges on all materialized views to the role.
   GRANT REFRESH ON ALL MATERIALIZED VIEWS IN ALL DATABASES TO ROLE write_only;
   ```

3. Grant the read-only privilege on a specific external catalog:

   ```SQL
   --Create a role.
   CREATE ROLE read_catalog_only;
   -- Grant the USAGE privilege on the destination catalog to the role.
   GRANT USAGE ON CATALOG hive_catalog TO ROLE read_catalog_only;
   --Switch to the corresponding catalog.
   SET CATALOG hive_catalog;
   --Grant the SELECT privilege on all tables and views in all databases.
   GRANT SELECT ON ALL TABLES IN ALL DATABASES TO ROLE read_catalog_only;
   GRANT SELECT ON ALL VIEWS IN ALL DATABASES TO ROLE read_catalog_only;
   ```

   Note: You can query only Hive table views (since v3.1).

4. Grant write-only privileges on a specific external catalog

   You can only write data into Iceberg tables (since v3.1).

   ```SQL
   -- Create a role.
   CREATE ROLE write_catalog_only;
   -- Grant the USAGE privilege on the destination catalog to the role.
   GRANT USAGE ON CATALOG iceberg_catalog TO ROLE read_catalog_only;
   -- Switch to the corresponding catalog.
   SET CATALOG iceberg_catalog;
   -- Grant the privilege to write data into Iceberg tables.
   GRANT INSERT ON ALL TABLES IN ALL DATABASES TO ROLE write_catalog_only;
   ```

5. Grant privileges to perform backup and restore operations on global, database, table, and partition levels.

   - Grant privileges to perform global backup and restore operations:

     The privileges to perform global backup and restore operations allow the role to back up and restore any database, table, or partition. It requires the REPOSITORY privilege on the SYSTEM level, the privileges to create databases in the default catalog, to create tables in any database, and to load and export data on any table.

     ```SQL
     --Create a role.
     CREATE ROLE recover;
     --Grant the REPOSITORY privilege on the SYSTEM level.
     GRANT REPOSITORY ON SYSTEM TO ROLE recover;
     --Grant the privilege to create databases in the default catalog.
     GRANT CREATE DATABASE ON CATALOG default_catalog TO ROLE recover;
     --Grant the privilege to create tables in any database.
     GRANT CREATE TABLE ON ALL DATABASE TO ROLE recover;
     --Grant the privilege to load and export data on any table.
     GRANT INSERT, EXPORT ON ALL TABLES IN ALL DATABASES TO ROLE recover;
     ```

   - Grant the privileges to perform database-level backup and restore operations:

     The privileges to perform database-level backup and restore operations require the REPOSITORY privilege on the SYSTEM level, the privilege to create databases in the default catalog, the privilege to create tables in any database, the privilege to load data into any table, and the privilege export data from any table in the database to be backed up.

     ```SQL
     --Create a role.
     CREATE ROLE recover_db;
     --Grant the REPOSITORY privilege on the SYSTEM level.
     GRANT REPOSITORY ON SYSTEM TO ROLE recover_db;
     --Grant the privilege to create databases.
     GRANT CREATE DATABASE ON CATALOG default_catalog TO ROLE recover_db;
     --Grant the privilege to create tables.
     GRANT CREATE TABLE ON ALL DATABASE TO ROLE recover_db;
     --Grant the privilege to load data into any table.
     GRANT INSERT ON ALL TABLES IN ALL DATABASES TO ROLE recover_db;
     --Grant the privilege to export data from any table in the database to be backed up.
     GRANT EXPORT ON ALL TABLES IN DATABASE <db_name> TO ROLE recover_db;
     ```

   - Grant the privileges to perform table-level backup and restore operations:

     The privileges to perform table-level backup and restore operations require the REPOSITORY privilege on the SYSTEM level, the privilege to create tables in corresponding databases, the privilege to load data into any table in the database, and the privilege to export data from the table to be backed up.

     ```SQL
     --Create a role.
     CREATE ROLE recover_tbl;
     --Grant the REPOSITORY privilege on the SYSTEM level.
     GRANT REPOSITORY ON SYSTEM TO ROLE recover_tbl;
     --Grant the privilege to create tables in corresponding databases.
     GRANT CREATE TABLE ON DATABASE <db_name> TO ROLE recover_tbl;
     --Grant the privilege to load data into any table in a database.
     GRANT INSERT ON ALL TABLES IN DATABASE <db_name> TO ROLE recover_db;
     -- Grant the privilege to export data from the table you want to back up.
     GRANT EXPORT ON TABLE <table_name> TO ROLE recover_tbl;     
     ```

   - Grant the privileges to perform partition-level backup and restore operations:

     The privileges to perform partition-level backup and restore operations require the REPOSITORY privilege on the SYSTEM level, and the privilege to load and export data on the corresponding table.

     ```SQL
     --Create a role.
     CREATE ROLE recover_par;
     --Grant the REPOSITORY privilege on the SYSTEM level.
     GRANT REPOSITORY ON SYSTEM TO ROLE recover_par;
     --Grant the privilege to load and export data on the corresponding table.
     GRANT INSERT, EXPORT ON TABLE <tbl_name> TO ROLE recover_par;
     ```
