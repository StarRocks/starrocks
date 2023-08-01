# Privileges supported by StarRocks

Privileges granted to a user or role determine which operations the user or role can perform on certain objects. Privileges can be used to implement fine-grained access control to safeguard data security.

This topic describes privileges provided by StarRocks on different objects and their meanings. Privileges are granted and revoked by using [GRANT](../sql-reference/sql-statements/account-management/GRANT.md) and [REVOKE](../sql-reference/sql-statements/account-management/REVOKE.md). The privileges that can be granted on an object are specific to the object type. For example, table privileges are different from database privileges.

> NOTE: The privileges described in this topic are available only from v3.0. The privilege framework and syntax in v3.0 are not backward compatible with those in earlier versions. After an upgrade to v3.0, most of your original privileges are still retained except those for specific operations. For the detailed differences, see [Upgrade notes](#upgrade-notes) at the end of this topic.

## Privilege list

This section describes privileges that are available on different objects.

### SYSTEM

| Privilege               | Description                                                  |
| ----------------------- | ------------------------------------------------------------ |
| NODE                    | Operates nodes, such as adding, deleting, or decommissioning nodes. To ensure cluster security, this privilege cannot be directly granted to users or roles. The `cluster_admin` role has this privilege. |
| GRANT                   | Creates a user or role, alters a user or role, or grants privileges to a user or role. This privilege cannot be directly granted to users or roles. The `user_admin` role has this privilege. |
| CREATE RESOURCE GROUP   | Creates a resource group.                                    |
| CREATE RESOURCE         | Creates resources for Spark Load jobs or external tables.    |
| CREATE EXTERNAL CATALOG | Creates an External Catalog.                                 |
| PLUGIN                  | Installs or uninstalls a plugin.                             |
| REPOSITORY              | Creates, deletes, or views repositories.                     |
| BLACKLIST               | Creates, deletes, or displays SQL blacklists.                |
| FILE                    | Creates, deletes, or views files.                            |
| OPERATE                 | Manages replicas, configuration items, variables, and transactions. |
| CREATE GLOBAL FUNCTION  | Creates a global UDF.                                        |

### RESOURCE GROUP

| Privilege | Description                                       |
| --------- | ------------------------------------------------- |
| ALTER     | Adds or deletes classifiers for a resource group. |
| DROP      | Deletes a resource group.                         |
| ALL       | Has all the above privileges on a resource grup.  |

### RESOURCE

| Privilege | Description                                |
| --------- | ------------------------------------------ |
| USAGE     | Uses a resource.                           |
| ALTER     | Alters a resource.                         |
| DROP      | Deletes a resource.                        |
| ALL       | Has all the above privileges on a resoure. |

### USER

| Privilege   | Description                                    |
| ----------- | ---------------------------------------------- |
| IMPERSONATE | Allows user A to perform operations as user B. |

### GLOBAL FUNCTION (Global UDFs)

| Privilege | Description                                 |
| --------- | ------------------------------------------- |
| USAGE     | Uses a function in a query.                 |
| DROP      | Deletes a function.                         |
| ALL       | Has all the above privileges on a function. |

### CATALOG

| Object                      | Privilege                                             | Description                                    |
| --------------------------- | ----------------------------------------------------- | ---------------------------------------------- |
| CATALOG (internal catalog) | USAGE                                                 | Uses the internal catalog (default_catalog).   |
| CATALOG (internal catalog)             | CREATE DATABASE           |        Creates databases in the internal catalog.                                   |
| CATALOG (internal catalog)                        | ALL  |               Has all the above privileges on the internal catalog.                           |
| CATALOG (external catalog)  | USAGE                                                 | Uses an external catalog to view tables in it. |
| CATALOG (external catalog)                        |   DROP                       |    Deletes an external catalog.                                   |
| CATALOG (external catalog)                         |  ALL   |              Has all the above privileges on the external catalog.                                |

> Notes: StarRocks internal catalog cannot be deleted.

### DATABASE

| Privilege                | Description                                                  |
| ------------------------ | ------------------------------------------------------------ |
| ALTER                    | Sets properties for a database, rename a database, or sets quotas for a database. |
| DROP                     | Deletes a database.                                          |
| CREATE TABLE             | Creates tables in a database.                                |
| CREATE VIEW              | Creates a view.                                              |
| CREATE FUNCTION          | Creates a function.                                          |
| CREATE MATERIALIZED VIEW | Creates a materialized view.                                 |
| ALL                      | Has all the above privileges on a database.                  |

### TABLE

| Privilege | Description                                                  |
| --------- | ------------------------------------------------------------ |
| ALTER     | Modifies a table or refreshes metadata in an external table. |
| DROP      | Drops a table.                                               |
| SELECT    | Queries data in a table.                                     |
| INSERT    | Inserts data into a table.                                   |
| UPDATE    | Updates data in a table.                                     |
| EXPORT    | Exports data from a StarRocks table.                         |
| DELETE    | Deletes data from a table based on the specified condition or deletes all the data from a table. |
| ALL       | Has all the above privileges on a table.                     |

### VIEW

| Privilege | Description                             |
| --------- | --------------------------------------- |
| SELECT    | Queries data in a view.                 |
| ALTER     | Modifies the definition of a view.      |
| DROP      | Deletes a logical view.                 |
| ALL       | Has all the above privileges on a view. |

### MATERIALIZED VIEW

| Privilege | Description                                          |
| --------- | ---------------------------------------------------- |
| SELECT    | Queries a materialized view to accelerate queries.   |
| ALTER     | Changes a materialized view.                         |
| REFRESH   | Refreshes a materialized view.                       |
| DROP      | Deletes a materialized view.                         |
| ALL       | Has all the above privileges on a materialized view. |

### FUNCTION (Database-level UDFs)

| USAGE | Uses a function.                            |
| ----- | ------------------------------------------- |
| DROP  | Deletes a function.                         |
| ALL   | Has all the above privileges on a function. |

## Upgrade notes

During an upgrade from v2.x to v3.0, some of your operations may be unable to perform due to the introduction of the new privilege system. The following table describes the changes before and after the upgrade.

| **Operation**               | **Commands involved**           | **Before**                                                   | **After**                                                    |
| --------------------------- | ------------------------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| Change table                | ALTER TABLE, CANCEL ALTER TABLE | Users who have the `LOAD_PRIV` privilege on a table or the database to which the table belongs can perform the `ALTER TABLE` and `CANCEL ALTER TABLE` operations. | You must have the ALTER privilege on the table to perform these two operations. |
| Refresh external table      | REFRESH EXTERNAL TABLE          | Users who have the `LOAD_PRIV` privilege on an external table can refresh the external table. | You must have the ALTER privilege on the external table to perform this operation. |
| Backup and restore          | BACKUP, RESTORE                 | Users who have the `LOAD_PRIV` privilege on a database can back up and restore the database or any table in the database. | The administrator must grant backup and restore privileges to users again after the upgrade. |
| Recover after deletion      | RECOVER                         | Users who have the `ALTER_PRIV`, `CREATE_PRIV`, and `DROP_PRIV` privileges on the database and table can recover the database and table. | You must have the CREATE DATABASE privilege on the default_catalog to recover the database. You must have the CREATE TABLE privilege on the database and the DROP privilege on the table. |
| Create and change users     | CREATE USER, ALTER USER         | Users who have the `GRANT_PRIV` privilege on the database can create and change users. | You must have the `user_admin` role to create and change users. |
| Grant and revoke privileges | GRANT, REVOKE                   | Users who have the `GRANT_PRIV` privilege on an object can grant privileges on the object to other users or roles. | After the upgrade, you can still grant the privileges you already have on that object to other users or roles after the upgrade.<br>In the new privilege system: <ul><li>You must have the `user_admin` role to grant privileges to other users or roles.</li><li>If your GRANT statement includes `WITH GRANT OPTION`, you can grant the privileges involved in the statement to other users or roles. </li></ul>|

In v2.x, StarRocks does not fully implement role-based access control (RBAC). When you assign a role to a user, StarRocks directly grants all the privileges of the role to the user, instead of the role itself. Therefore, the user does not actually own the role.

In v3.0, StarRocks renovates its privilege system. After an upgrade to v3.0, your original roles are retained but there is still no ownership between users and roles. If you want to use the new RBAC system, perform the GRANT operation to assign roles and privileges.
