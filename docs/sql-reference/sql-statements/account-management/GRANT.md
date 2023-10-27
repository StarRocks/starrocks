# GRANT

## Description

Grants one or more privileges on specific objects to a user or a role.

Grants roles to users or other roles.

For more information about the privileges that can be granted, see [Privilege items](../../../administration/privilege_item.md).

After a GRANT operation is performed, you can run [SHOW GRANTS](./SHOW_GRANTS.md) to view detailed privilege information or run [REVOKE](REVOKE.md) to revoke a privilege or role.

Before a GRANT operation is performed, make sure that the related user or role has been created. For more information, see [CREATE USER](./CREATE_USER.md) and [CREATE ROLE](./CREATE_ROLE.md).

> **NOTE**
>
> Only users with the `user_admin` role can grant any privilege to other users and roles.
> Other users can only grant privileges with the WITH GRANT OPTION keyword to other users and roles.

## Syntax

### Grant privileges to roles or users

#### System

```SQL
GRANT
    { CREATE RESOURCE GROUP | CREATE RESOURCE | CREATE EXTERNAL CATALOG | REPOSITORY | BLACKLIST | FILE | OPERATE } 
    ON SYSTEM
    TO { ROLE | USER} {<role_name>|<user_identity>} [ WITH GRANT OPTION ]
```

#### Resource group

```SQL
GRANT
    { ALTER | DROP | ALL [PRIVILEGES] } 
    ON { RESOURCE GROUP <resource_group_name> [, <resource_group_name >,...] ｜ ALL RESOURCE GROUPS} 
    TO { ROLE | USER} {<role_name>|<user_identity>} [ WITH GRANT OPTION ]
```

#### Resource

```SQL
GRANT
    { USAGE | ALTER | DROP | ALL [PRIVILEGES] } 
    ON { RESOURCE <resource_name> [, < resource_name >,...] ｜ ALL RESOURCES} 
    TO { ROLE | USER} {<role_name>|<user_identity>} [ WITH GRANT OPTION ]
```

#### Global UDF

```SQL
GRANT
    { USAGE | DROP | ALL [PRIVILEGES]} 
    ON { GLOBAL FUNCTION <function_name> [, < function_name >,...]    
       | ALL GLOBAL FUNCTIONS }
    TO { ROLE | USER} {<role_name>|<user_identity>} [ WITH GRANT OPTION ]
```

#### Internal catalog

```SQL
GRANT
    { USAGE | CREATE DATABASE | ALL [PRIVILEGES]} 
    ON CATALOG default_catalog
    TO { ROLE | USER} {<role_name>|<user_identity>} [ WITH GRANT OPTION ]
```

#### External catalog

```SQL
GRANT
   { USAGE | DROP | ALL [PRIVILEGES] } 
   ON { CATALOG <catalog_name> [, <catalog_name>,...] | ALL CATALOGS}
   TO { ROLE | USER} {<role_name>|<user_identity>} [ WITH GRANT OPTION ]
```

#### Database

```SQL
GRANT
    { ALTER | DROP | CREATE TABLE | CREATE VIEW | CREATE FUNCTION | CREATE MATERIALIZED VIEW | ALL [PRIVILEGES] } 
    ON { {DATABASE <database_name> [, <database_name>,...]} | ALL DATABASES }
    TO { ROLE | USER} {<role_name>|<user_identity>} [ WITH GRANT OPTION ]
```

* You must first run SET CATALOG before you run this command.

#### Table

```SQL
GRANT
    { ALTER | DROP | SELECT | INSERT | EXPORT | UPDATE | DELETE | ALL [PRIVILEGES]} 
    ON { TABLE <table_name> [, < table_name >,...]
       | ALL TABLES} IN 
           { { DATABASE <database_name> [, <database_name>,...] } | ALL DATABASES }
    TO { ROLE | USER} {<role_name>|<user_identity>} [ WITH GRANT OPTION ]
```

* You must first run SET CATALOG before you run this command.
* You can also use `<db_name>.<table_name>` to represent a table.

  ```SQL
  GRANT <priv> ON TABLE <db_name>.<table_name> TO {ROLE <role_name> | USER <user_name>}
  ```

#### View

```SQL
GRANT  
    { ALTER | DROP | SELECT | ALL [PRIVILEGES]} 
    ON { VIEW <view_name> [, < view_name >,...]
       ｜ ALL VIEWS} IN 
           { { DATABASE <database_name> [, <database_name>,...] } | ALL DATABASES }
    TO { ROLE | USER} {<role_name>|<user_identity>} [ WITH GRANT OPTION ]
```

* You must first run SET CATALOG before you run this command.
* You can also use `<db_name>.<view_name>` to represent a view.

  ```SQL
  GRANT <priv> ON VIEW <db_name>.<view_name> TO {ROLE <role_name> | USER <user_name>}
  ```

#### Materialized view

```SQL
GRANT
    { SELECT | ALTER | REFRESH | DROP | ALL [PRIVILEGES]} 
    ON { MATERIALIZED VIEW <mv_name> [, < mv_name >,...]
       ｜ ALL MATERIALIZED VIEWS} IN 
           { { DATABASE <database_name> [, <database_name>,...] } | ALL DATABASES }
    TO { ROLE | USER} {<role_name>|<user_identity>} [ WITH GRANT OPTION ]
```

* You must first run SET CATALOG before you run this command.
* You can also use `<db_name>.<mv_name>` to represent an mv.

  ```SQL
  GRANT <priv> ON MATERIALIZED_VIEW <db_name>.<mv_name> TO {ROLE <role_name> | USER <user_name>}
  ```

#### Function

```SQL
GRANT
    { USAGE | DROP | ALL [PRIVILEGES]} 
    ON { FUNCTION <function_name> [, < function_name >,...]
       ｜ ALL FUNCTIONS} IN 
           { { DATABASE <database_name> [, <database_name>,...] } | ALL DATABASES }
    TO { ROLE | USER} {<role_name>|<user_identity>} [ WITH GRANT OPTION ]
```

* You must first run SET CATALOG before you run this command.
* You can also use `<db_name>.<function_name>` to represent a function.

  ```SQL
  GRANT <priv> ON FUNCTION <db_name>.<function_name> TO {ROLE <role_name> | USER <user_name>}
  ```

#### User

```SQL
GRANT IMPERSONATE
ON USER <user_identity>
TO USER <user_identity> [ WITH GRANT OPTION ]
```

#### Storage volume

```SQL
GRANT
    CREATE STORAGE VOLUME
    ON SYSTEM
    TO { ROLE | USER} {<role_name>|<user_identity>} [ WITH GRANT OPTION ]

GRANT  
    { USAGE | ALTER | DROP | ALL [PRIVILEGES] } 
    ON { STORAGE VOLUME < name > [, < name >,...] ｜ ALL STORAGE VOLUME} 
    TO { ROLE | USER} {<role_name>|<user_identity>} [ WITH GRANT OPTION ]
```

### Grant roles to roles or users

```SQL
GRANT <role_name> [,<role_name>, ...] TO ROLE <role_name>
GRANT <role_name> [,<role_name>, ...] TO USER <user_identity>
```

## Examples

Example 1: Grant the privilege to read data from all tables in all databases to user `jack`.

```SQL
GRANT SELECT ON *.* TO 'jack'@'%';
```

Example 2: Grant the privilege to load data into all tables of database `db1` to role `my_role`.

```SQL
GRANT INSERT ON db1.* TO ROLE 'my_role';
```

Example 3: Grant the privileges to read, update, and load data into table `tbl1` of database `db1` to user `jack`.

```SQL
GRANT SELECT,ALTER,INSERT ON db1.tbl1 TO 'jack'@'192.8.%';
```

Example 4: Grant the privilege to use all the resources to user `jack`.

```SQL
GRANT USAGE ON RESOURCE * TO 'jack'@'%';
```

Example 5: Grant the privilege to use resource `spark_resource` to user `jack`.

```SQL
GRANT USAGE ON RESOURCE 'spark_resource' TO 'jack'@'%';
```

Example 6: Grant the privilege to use resource `spark_resource` to role `my_role`.

```SQL
GRANT USAGE ON RESOURCE 'spark_resource' TO ROLE 'my_role';
```

Example 7: Grant the privilege to read data from table `sr_member` to user `jack` and allow user `jack` to grant this privilege to other users or roles (by specifying WITH GRANT OPTION).

```SQL
GRANT SELECT ON TABLE sr_member TO USER jack@'172.10.1.10' WITH GRANT OPTION;
```

Example 8: Grant system-defined roles `db_admin`, `user_admin`, and `cluster_admin` to user `user_platform`.

```SQL
GRANT db_admin, user_admin, cluster_admin TO USER user_platform;
```

Example 9: Allow user `jack` to perform operations as user `rose`.

```SQL
GRANT IMPERSONATE ON 'rose'@'%' TO 'jack'@'%';
```

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
   -- Grant the USAGE privilege on the destination catalog to the role.
   GRANT USAGE ON CATALOG hive_catalog TO ROLE read_catalog_only;
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
   -- Grant the USAGE privilege on the destination catalog to the role.
   GRANT USAGE ON CATALOG iceberg_catalog TO ROLE read_catalog_only;
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
