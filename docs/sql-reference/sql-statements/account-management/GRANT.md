# GRANT

## Description

You can use the GRANT statement to perform the following operations:

- Grant specific privileges to a user or a role.
- Grant a role to a user. This feature is supported only in StarRock 2.4 and later versions.
- Grant user `a` the privilege to impersonate user `b`. Then user `a` can perform operations as user `b` by using the [EXECUTE AS](../account-management/EXECUTE%20AS.md) statement. This feature is supported only in StarRock 2.4 and later versions.

## Syntax

- Grant specific privileges on a database and a table to a user or a role. If the role that is granted these privileges does not exist, the system automatically creates the role when you execute this statement.

    ```SQL
    GRANT privilege_list ON db_name[.tbl_name] TO {user_identity | ROLE 'role_name'}
    ```

- Grant specific privileges on a resource to a user or a role. If the role that is granted these privileges does not exist, the system automatically creates the role when you execute this statement.

    ```SQL
    GRANT privilege_list ON RESOURCE 'resource_name' TO {user_identity | ROLE 'role_name'};
    ```

- Grant user `a` the privilege to impersonate user `b` to perform operations.

    ```SQL
    GRANT IMPERSONATE ON user_identity_b TO user_identity_a;
    ```

- Grant a role to a user. The role to be granted must exist.

    ```SQL
    GRANT 'role_name' TO user_identity;
    ```

## Parameters

### privilege_list

The privileges that can be granted to a user or a role. If you want to grant multiple privileges at a time, separate the privileges with commas (`,`). The following privileges are supported:

- `NODE_PRIV`: the privilege to manage cluster nodes such as enabling nodes and disabling nodes.
- `ADMIN_PRIV`: all privileges except `NODE_PRIV`.
- `GRANT_PRIV`: the privilege of performing operations such as creating users and roles, deleting users and roles, granting privileges, revoking privileges, and setting passwords for accounts.
- `SELECT_PRIV`: the read privilege on databases and tables.
- `LOAD_PRIV`: the privilege to load data into databases and tables.
- `ALTER_PRIV`: the privilege to change schemas of databases and tables.
- `CREATE_PRIV`: the privilege to create databases and tables.
- `DROP_PRIV`: the privilege to delete databases and tables.
- `USAGE_PRIV`: the privilege to use resources.

The preceding privileges can be classified into the following three categories:

- Node privilege: `NODE_PRIV`
- Database and table privilege: `SELECT_PRIV`, `LOAD_PRIV`, `ALTER_PRIV`, `CREATE_PRIV`, and `DROP_PRIV`
- Resource privilege: `USAGE_PRIV`

### db_name[.tbl_name]

The database and table. This parameter supports the following three formats:

- `*.*`: indicates all databases and tables. If this format is specified, the global privilege is granted.
- `db.*`: indicates a specific database and all tables in this database.
- `db.tbl`: indicates a specific table in a specific database.

> Note: When you use the `db.*` or `db.tbl` format, you can specify a database or a table that does not exist.

### resource_name

The resource name. This parameter supports the following two formats:

- `*`: indicates all the resources.
- `resource`: indicates a specific resource.

> Note: When you use the `resource` format, you can specify a resource that does not exist.

### user_identity

This parameter contains two parts: `user_name` and `host`. `user_name` indicates the user name. `host` indicates the IP address of the user. You can leave `host` unspecified or you can specify a domain for `host`. If you leave `host` unspecified, `host` defaults to `%`, which means you can access StarRocks from any host. If you specify a domain for `host`, it may take one minute for the privilege to take effect. The `user_identity` parameter must be created by the CREATE USER statement.

### role_name

The role name.

## Examples

Example 1: Grant the read privilege on all databases and tables to user `jack`.

```SQL
GRANT SELECT_PRIV ON *.* TO 'jack'@'%';
```

Example 2: Grant the data loading privilege on `db1` and all tables in this database to `my_role`.

```SQL
GRANT LOAD_PRIV ON db1.* TO ROLE 'my_role';
```

Example 3: Grant the read privilege, schema change privilege, and data loading privilege on `db1` and `tbl1` to user `jack`.

```SQL
GRANT SELECT_PRIV,ALTER_PRIV,LOAD_PRIV ON db1.tbl1 TO 'jack'@'192.8.%';
```

Example 4: Grant the privilege to use all the resources to user `jack`.

```SQL
GRANT USAGE_PRIV ON RESOURCE * TO 'jack'@'%';
```

Example 5: Grant the privilege to use spark_resource to user `jack`.

```SQL
GRANT USAGE_PRIV ON RESOURCE 'spark_resource' TO 'jack'@'%';
```

Example 6: Grant the privilege to use spark_resource to the `my_role`.

```SQL
GRANT USAGE_PRIV ON RESOURCE 'spark_resource' TO ROLE 'my_role';
```

Example 7: Grant `my_role` to user `jack`.

```SQL
GRANT 'my_role' TO 'jack'@'%';
```

Example 8: Grant user `jack` the privilege to impersonate user `rose` to perform operations.

```SQL
GRANT IMPERSONATE ON 'rose'@'%' TO 'jack'@'%';
```
<<<<<<< HEAD
=======

## Best practices - Customize roles based on scenarios

We recommend you customize roles to manage privileges and users. The following examples classify a few combinations of privileges for some common scenarios.

### Grant global read-only privileges on StarRocks tables

   ```SQL
   -- Create a role.
   CREATE ROLE read_only;
   -- Grant the USAGE privilege on all catalogs to the role.
   GRANT USAGE ON ALL CATALOGS TO ROLE read_only;
   -- Grant the privilege to query all tables to the role.
   GRANT SELECT ON ALL TABLES IN ALL DATABASES TO ROLE read_only;
   -- Grant the privilege to query all views to the role.
   GRANT SELECT ON ALL VIEWS IN ALL DATABASES TO ROLE read_only;
   -- Grant the privilege to query all materialized views and the privilege to accelerate queries with them to the role.
   GRANT SELECT ON ALL MATERIALIZED VIEWS IN ALL DATABASES TO ROLE read_only;
   ```

   And you can further grant the privilege to use UDFs in queries:

   ```SQL
   -- Grant the USAGE privilege on all database-level UDF to the role.
   GRANT USAGE ON ALL FUNCTIONS IN ALL DATABASES TO ROLE read_only;
   -- Grant the USAGE privilege on global UDF to the role.
   GRANT USAGE ON ALL GLOBAL FUNCTIONS TO ROLE read_only;
   ```

### Grant global write privileges on StarRocks tables

   ```SQL
   -- Create a role.
   CREATE ROLE write_only;
   -- Grant the USAGE privilege on all catalogs to the role.
   GRANT USAGE ON ALL CATALOGS TO ROLE write_only;
   -- Grant the INSERT and UPDATE privileges on all tables to the role.
   GRANT INSERT, UPDATE ON ALL TABLES IN ALL DATABASES TO ROLE write_only;
   -- Grant the REFRESH privilege on all materialized views to the role.
   GRANT REFRESH ON ALL MATERIALIZED VIEWS IN ALL DATABASES TO ROLE write_only;
   ```

### Grant read-only privileges on a specific external catalog

   ```SQL
   -- Create a role.
   CREATE ROLE read_catalog_only;
   -- Switch to the corresponding catalog.
   SET CATALOG hive_catalog;
   -- Grant the privileges to query all tables and all views in all databases.
   GRANT SELECT ON ALL TABLES IN ALL DATABASES TO ROLE read_catalog_only;
   GRANT SELECT ON ALL VIEWS IN ALL DATABASES TO ROLE read_catalog_only;
   ```

   Note: You can query only Hive table views (since v3.1).

### Grant write-only privileges on a specific external catalog

You can only write data into Iceberg tables (since v3.1).

   ```SQL
   -- Create a role.
   CREATE ROLE write_catalog_only;
   -- Switch to the corresponding catalog.
   SET CATALOG iceberg_catalog;
   -- Grant the privilege to write data into Iceberg tables.
   GRANT INSERT ON ALL TABLES IN ALL DATABASES TO ROLE write_catalog_only;
   ```

### Grant privileges to perform backup and restore operations on global, database, table, and partition levels

- Grant privileges to perform global backup and restore operations:

     The privileges to perform global backup and restore operations allow the role to back up and restore any database, table, or partition. It requires the REPOSITORY privilege on the SYSTEM level, the privileges to create databases in the default catalog, to create tables in any database, and to load and export data on any table.

     ```SQL
     -- Create a role.
     CREATE ROLE recover;
     -- Grant the REPOSITORY privilege on the SYSTEM level.
     GRANT REPOSITORY ON SYSTEM TO ROLE recover;
     -- Grant the privilege to create databases in the default catalog.
     GRANT CREATE DATABASE ON CATALOG default_catalog TO ROLE recover;
     -- Grant the privilege to create tables in any database.
     GRANT CREATE TABLE ON ALL DATABASE TO ROLE recover;
     -- Grant the privilege to load and export data on any table.
     GRANT INSERT, EXPORT ON ALL TABLES IN ALL DATABASES TO ROLE recover;
     ```

- Grant the privileges to perform database-level backup and restore operations:

     The privileges to perform database-level backup and restore operations require the REPOSITORY privilege on the SYSTEM level, the privilege to create databases in the default catalog, the privilege to create tables in any database, the privilege to load data into any table, and the privilege export data from any table in the database to be backed up.

     ```SQL
     -- Create a role.
     CREATE ROLE recover_db;
     -- Grant the REPOSITORY privilege on the SYSTEM level.
     GRANT REPOSITORY ON SYSTEM TO ROLE recover_db;
     -- Grant the privilege to create databases.
     GRANT CREATE DATABASE ON CATALOG default_catalog TO ROLE recover_db;
     -- Grant the privilege to create tables.
     GRANT CREATE TABLE ON ALL DATABASE TO ROLE recover_db;
     -- Grant the privilege to load data into any table.
     GRANT INSERT ON ALL TABLES IN ALL DATABASES TO ROLE recover_db;
     -- Grant the privilege to export data from any table in the database to be backed up.
     GRANT EXPORT ON ALL TABLES IN DATABASE <db_name> TO ROLE recover_db;
     ```

- Grant the privileges to perform table-level backup and restore operations:

     The privileges to perform table-level backup and restore operations require the REPOSITORY privilege on the SYSTEM level, the privilege to create tables in corresponding databases, the privilege to load data into any table in the database, and the privilege to export data from the table to be backed up.

     ```SQL
     -- Create a role.
     CREATE ROLE recover_tbl;
     -- Grant the REPOSITORY privilege on the SYSTEM level.
     GRANT REPOSITORY ON SYSTEM TO ROLE recover_tbl;
     -- Grant the privilege to create tables in corresponding databases.
     GRANT CREATE TABLE ON DATABASE <db_name> TO ROLE recover_tbl;
     -- Grant the privilege to load data into any table in a database.
     GRANT INSERT ON ALL TABLES IN DATABASE <db_name> TO ROLE recover_db;
     -- Grant the privilege to export data from the table you want to back up.
     GRANT EXPORT ON TABLE <table_name> TO ROLE recover_tbl;     
     ```

- Grant the privileges to perform partition-level backup and restore operations:

     The privileges to perform partition-level backup and restore operations require the REPOSITORY privilege on the SYSTEM level, and the privilege to load and export data on the corresponding table.

     ```SQL
     -- Create a role.
     CREATE ROLE recover_par;
     -- Grant the REPOSITORY privilege on the SYSTEM level.
     GRANT REPOSITORY ON SYSTEM TO ROLE recover_par;
     -- Grant the privilege to load and export data on the corresponding table.
     GRANT INSERT, EXPORT ON TABLE <table_name> TO ROLE recover_par;
     ```

For the best practices of multi-service access control, see [Multi-service access control](../../../administration/User_privilege.md#multi-service-access-control).
>>>>>>> 4cf65a8b45 ([Doc] Add link to bitmap value and update sys variables (#28604))
