# User Privilege

Based on MySQL's permission management mechanism, StarRocks permission management system supports fine-grained permission control at table level, role-based access control, and whitelisting.

## Terminology

* User Identity

In the privilege system, a user is identified as a User Identity. The user identity consists of two parts: username and userhost. `user_identity` is presented as `username@'userhost'`. The username is made of English characters. The userhost is the IP from which the user is linked.

`user_identity` can also be presented as `username@['domain'],` where `domain` is a domain name that can be resolved into a set of IPs by DNS. The final expression is a set of username@'userhost', so  we use username@'userhost' to represent it throughout this chapter.

* Privilege

<<<<<<< HEAD
Users can set privileges on nodes, databases, or tables. Different privileges represent different permissions to operate.
=======
You can view the privileges granted to a user or a role using [SHOW GRANTS](../sql-reference/sql-statements/account-management/SHOW_GRANTS.md).
>>>>>>> ad1d16540e ([Doc] Fix filename spaces (#32525))

* Role

StarRocks can create roles with customized names. A role is a collection of permissions. A newly created user can be given a role with corresponding permissions. Subsequent changes to the permissions apply to all users granted with the role.

* User Property

User properties are attached directly to a user, not to a user identity That is, both `abc@'192.%'` and `abc@['domain']` have the same set of user properties that belong to the user `abc`, not `abc@'192.%'` or `abc@['domain']`. User attributes include, but are not limited to maximum number of user connections, import cluster configuration, and so on.

## Supported operations

* Create user: CREATE USER
* Delete user: DROP USER
* Authorization: GRANT
* Revoke authority: REVOKE
* Create Role: CREATE ROLE
* Delete Role: DROP ROLE
* View current user permissions: SHOW GRANTS
* View all user permissions: SHOW ALL GRANTS
* View created roles: SHOW ROLES
* View user properties: SHOW PROPERTY

## Privilege Types

StarRocks currently supports the following types of  privileges.

* Node\_priv  
    Privileges for Node changes. This allows you to add, delete, and disable FE, BE, and BROKER nodes. Currently, this privilege can only be granted to Root users.
* Grant\_priv  
    Privileges for privilege changes. This allows you to grant, revoke, and modify users/roles.
* Select\_priv  
    Read-only access to databases and tables.
* Load\_priv  
    Write access to databases and tables. This allows you to  Load, Insert, and Delete databases or tables.
* Alter\_priv  
     privileges to change databases and tables. This allows you to  rename, add/remove/change columns, add/remove partitions.
* Create\_priv  
    Privileges to create databases, tables, and views.
* Drop\_priv  
    Privileges to delete databases, tables, and views.
* Usage\_priv  
Privileges to use resources.

## Privilege Hierarchy

<<<<<<< HEAD
StarRocks classifies database and table privileges into three levels.
=======
You can view the property of a user using [SHOW PROPERTY](../sql-reference/sql-statements/account-management/SET_PROPERTY.md).
>>>>>>> ad1d16540e ([Doc] Fix filename spaces (#32525))

* GLOBAL LEVEL: Global privileges. This privilege is granted on `.` by the GRANT statement. The privilege granted applies to any table in any database.
* DATABASE LEVEL: Database level privileges. This  privilege is granted on `db.*` by the `GRANT` statement. The privilege granted applies to any table in a specific  database.
* TABLE LEVEL: Table level privileges. This privilege is granted on  `db.tbl` by the `GRANT` statement. The privilege granted applies to a specific table in a specific database.

StarRocks classifies the resource privileges into two levels.

* GLOBAL LEVEL: Global privileges. This privilege is granted on `*` by the `GRANT` statement. The privilege granted applies to all resources.
* RESOURCE LEVEL: Resource level privileges. This privilege is granted  on `resource_name` by the `GRANT` statement. The privilege granted applies to a specific resource.

<<<<<<< HEAD
## Privilege Description
=======
You can view all the roles within the StarRocks cluster using [SHOW ROLES](../sql-reference/sql-statements/account-management/SHOW_ROLES.md).
>>>>>>> ad1d16540e ([Doc] Fix filename spaces (#32525))

The `ADMIN\PRIV` and `GRANT_PRIV` privileges are special because they both have the privilege to grant privileges.

* CREATE USER

* Users with ADMIN privilege or GRANT privilege at any level can create new users.

* DROP USER

* Users with ADMIN privilege can delete users.

* CREATE/DROP ROLE

* Users with  ADMIN privilege can create roles.

* GRANT/REVOKE

<<<<<<< HEAD
* Users with ADMIN privilege or GLOBAL level GRANT privilege can grant or revoke privileges to any user.
* Users with DATABASE level GRANT privilege can grant or revoke privileges to any user for any table in a specific database.
* Users with TABLE level GRANT privilege can grant or revoke privileges to any user for a specific table in a specific database.
=======
StarRocks supports user authentication with login credentials or LDAP authentication. For more information about StarRocks' authentication, see [Authentication](../administration/Authentication.md). For more information and advanced instructions on creating a user, see [CREATE USER](../sql-reference/sql-statements/account-management/CREATE_USER.md).
>>>>>>> ad1d16540e ([Doc] Fix filename spaces (#32525))

* SET PASSWORD

* Users with ADMIN privilege or GLOBAL level GRANT privilege can set the user password.
* Regular users can set passwords for their User Identity. Use the `SELECT CURRENT_USER();` command to view User Identity.
* Users with TABLE or DATABASE level GRANT privileges cannot set passwords for existing users, but can set passwords for new users.

## Other notes

* When StarRocks is initialized, the following users and roles are automatically created.

* operator role: This role has `Node_priv` and `Admin_priv`, i.e. all privileges. In a later upgrade, this role may be restricted to `Node_priv` (privileges for node changes). This is to meet certain deployment requirements on the cloud.
* admin role: This role has `Admin_priv`, i.e., all privileges except for the one for node changes.
* root@'%': Root user, allowed to log in from any node as `operator`.
* admin@'%': Admin user, allowed to log in from any node as `admin`.

* It is not supported to delete or change  default privileges set for roles or users.
* Only one user can be granted the operator role. Multiple users can be granted the admin role.
* Description of some potentially conflicting operations:

* Domain name and IP conflict

<<<<<<< HEAD
Suppose the following user is created.  
   `CREATE USER abc@['domain'];`  
and authorized:  
   `GRANT SELECT_PRIV ON` `*. *` `TO abc@['domain']`  
The domain is resolved to two IPs: ip1 and ip2. Suppose we then authorize abc@'ip1' separately:
   `GRANT ALTER_PRIV ON` `*. *` `TO abc@'ip1';`  
Then the privileges of abc@'ip1' will be changed to `SELECT_PRIV`, `ALTER_PRIV`.Any future privileges changes to abc@\['domain'\] will not apply to abc@'ip1'.
=======
You can set the default role of a user using [SET DEFAULT ROLE](../sql-reference/sql-statements/account-management/SET_DEFAULT_ROLE.md) or [ALTER USER](../sql-reference/sql-statements/account-management/ALTER_USER.md).
>>>>>>> ad1d16540e ([Doc] Fix filename spaces (#32525))

* Duplicate IP conflict

Suppose the following user is created.  
   `CREATE USER abc@'%' IDENTIFIED BY "12345";`  
   `CREATE USER abc@'192.%' IDENTIFIED BY "abcde";`  
'192.%' takes precedence over '%'. When the user requests to log in to StarRocks from 192.168.1.1 with the password '12345', the request will be denied.

* Forgotten password  
    If you forget your password, you can log in to StarRocks using the following command.  
    `mysql-client -h 127.0.0.1 -P query_port -uroot`  
    After logging in, you can reset the password with the `SET PASSWORD` command.
* Only root users can reset their password, other users cannot reset root user’s  password.
* The ADMIN\_PRIV privilege can only be granted or revoked at the GLOBAL level.
* `GRANT_PRIV` at GLOBAL level is similar to`ADMIN_PRIV`.`GRANT_PRIV` at GLOBAL level has the privilege to grant arbitrary privileges, so please use it carefully.

* current\_user() and user()

Users can use `SELECT current_user();` and `SELECT user();` to see `current_user` and `user` respectively. `current_user` indicates the current user’s identity under the authentication system, and `user` is the actual `user_identity` of the user.

For example, suppose user1@'192.%' is created, and then user1 logs into the system from 192.168.10.1. In this case, `current_user` is user1@'192.%', and `user` is user1@'192.168.10.1'.

<<<<<<< HEAD
All privileges are granted to `current_user`, and the real user has all the privileges of the corresponding `current_user`.
=======
You can set the property of a user using [SET PROPERTY](../sql-reference/sql-statements/account-management/SET_PROPERTY.md).
>>>>>>> ad1d16540e ([Doc] Fix filename spaces (#32525))

## Best Practices

Here are some scenarios for using the StarRocks permission management system.

### Scenario 1 Permissions Assignment

StarRocks users are divided into Administrator (Admin), Development Engineer (RD) and User (Client). The administrators have all the privileges and are mainly responsible for cluster construction, node management, and so on. The development engineers are responsible for business modeling, including building databases and tables, importing and modifying data,and so on. Users access databases and tables to get data.

<<<<<<< HEAD
In this scenario, Admins are granted ADMIN privileges or GRANT privileges. RDs are granted privileges to CREATE, DROP, ALTER, LOAD, and SELECT any or specific database tables. Clients are granted privileges to SELECT any or specific database tables. When there are multiple users, it is also possible to simplify the authorization by creating different roles.
=======
You can reset the password for a user using [SET PASSWORD](../sql-reference/sql-statements/account-management/SET_PASSWORD.md) or [ALTER USER](../sql-reference/sql-statements/account-management/ALTER_USER.md).
>>>>>>> ad1d16540e ([Doc] Fix filename spaces (#32525))

### Scenario 2 Multiple lines of business

There may be multiple use cases within a cluster, each of which may use one or more databases. Each use case needs to manage its own users. In this scenario, an ADMIN can grant one user with DATABASE level GRANT privilege for each database. This user can only authorize users for this specific database.

### Scenario 3: Blacklisting

StarRocks does not support blacklisting (only whitelisting is supported), but we can emulate blacklisting in some way. Suppose that the user named user@'192.%' is created first, indicating that the user f is allowed to log in from 192.\*. At this point, if you want to prevent the user from logging in from 192.168.10.1, you can create another user abc@'192.168.10.1' and set a new password. Because 192.168.10.1 has a higher priority than 192.%, the user will no longer be able to log in from 192.168.10.1 with the old password.

## Troubleshooting

## Reset password for root user

If you have lost the password for the user `root`, you can reset it by following these procedures:

1. Add the following configuration item to the configuration files **fe/conf/fe.conf** of **all FE nodes** to disable authentication:

    ```plain
    enable_auth_check = false
    ```

2. Restart **all FE nodes**.

    ```shell
    ./fe/bin/stop_fe.sh
    ./fe/bin/start_fe.sh
    ```

3. Launch a MySQL client, and connect to StarRocks using the user `root` without the password.

    ```shell
    mysql -h <fe_ip> -P<fe_query_port> -uroot
    ```

4. Reset the password for the user `root`.

    ```sql
    SET PASSWORD for root = PASSWORD('xxxxxx');
    ```

5. Re-enable authentication by configuring the configuration item `enable_auth_check` to `true` in the configuration files **fe/conf/fe.conf** of **all FE nodes**.

    ```plain
    enable_auth_check = true
    ```

6. Restart **all FE nodes**.

    ```shell
    ./fe/bin/stop_fe.sh
    ./fe/bin/start_fe.sh
    ```

7. Launch a MySQL client, and connect to StarRocks using the user `root` and the new password to verify whether the password is reset successfully.

<<<<<<< HEAD
    ```shell
    mysql -h <fe_ip> -P<fe_query_port> -uroot -p<xxxxxx>
    ```
=======
7. Connect from a MySQL client to StarRocks using the `root` user and the new password to verify whether the password is reset successfully.

   ```Bash
   mysql -h <fe_ip_or_fqdn> -P<fe_query_port> -uroot -p<xxxxxx>
   ```

### Drop a user

You can drop a user using [DROP USER](../sql-reference/sql-statements/account-management/DROP_USER.md).

The following example drops the user `jack`:

```SQL
DROP USER jack@'172.10.1.10';
```

## Manage roles

Users with the system-defined role `user_admin` can create, grant, revoke, or drop roles in StarRocks.

### Create a role

You can create a role using [CREATE ROLE](../sql-reference/sql-statements/account-management/CREATE_ROLE.md).

The following example creates the role `example_role`:

```SQL
CREATE ROLE example_role;
```

### Grant a role

You can grant roles to a user or another role using [GRANT](../sql-reference/sql-statements/account-management/GRANT.md).

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

You can revoke roles from a user or another role using [REVOKE](../sql-reference/sql-statements/account-management/REVOKE.md).

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

You can drop a role using [DROP ROLE](../sql-reference/sql-statements/account-management/DROP_ROLE.md).

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

You can grant privileges to a user or a role using [GRANT](../sql-reference/sql-statements/account-management/GRANT.md).

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

You can revoke privileges from a user or a role using [REVOKE](../sql-reference/sql-statements/account-management/REVOKE.md).

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

![User Privileges](../assets/user_privilege_1.png)

LOB A independently manages the database `DB_A`, and LOB B the database `DB_B`. LOB A and LOB B use different tables in `DB_C`. `DB_PUBLIC` can be accessed by all members of both LOBs.

![User Privileges](../assets/user_privilege_2.png)

Because different members perform different operations on different databases and tables, we recommend you create roles in accordance with their services and positions, apply only the necessary privileges to each role, and assign these roles to corresponding members. As shown below:

![User Privileges](../assets/user_privilege_3.png)

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
>>>>>>> ad1d16540e ([Doc] Fix filename spaces (#32525))
