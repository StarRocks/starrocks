# User Privilege

Based on MySQL's permission management mechanism, StarRocks permission management system supports fine-grained permission control at table level, role-based access control, and whitelisting.

## Terminology

* User Identity

In the privilege system, a user is identified as a User Identity. The user identity consists of two parts: username and userhost. `user_identity` is presented as `username@'userhost'`. The username is made of English characters. The userhost is the IP from which the user is linked.

`user_identity` can also be presented as `username@['domain'],` where `domain` is a domain name that can be resolved into a set of IPs by DNS. The final expression is a set of username@'userhost', so  we use username@'userhost' to represent it throughout this chapter.

* Privilege

Users can set privileges on nodes, databases, or tables. Different privileges represent different permissions to operate.

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

StarRocks classifies database and table privileges into three levels.

* GLOBAL LEVEL: Global privileges. This privilege is granted on `.` by the GRANT statement. The privilege granted applies to any table in any database.
* DATABASE LEVEL: Database level privileges. This  privilege is granted on `db.*` by the `GRANT` statement. The privilege granted applies to any table in a specific  database.
* TABLE LEVEL: Table level privileges. This privilege is granted on  `db.tbl` by the `GRANT` statement. The privilege granted applies to a specific table in a specific database.

StarRocks classifies the resource privileges into two levels.

* GLOBAL LEVEL: Global privileges. This privilege is granted on `*` by the `GRANT` statement. The privilege granted applies to all resources.
* RESOURCE LEVEL: Resource level privileges. This privilege is granted  on `resource_name` by the `GRANT` statement. The privilege granted applies to a specific resource.

## Privilege Description

The `ADMIN\PRIV` and `GRANT_PRIV` privileges are special because they both have the privilege to grant privileges.

* CREATE USER

* Users with ADMIN privilege or GRANT privilege at any level can create new users.

* DROP USER

* Users with ADMIN privilege can delete users.

* CREATE/DROP ROLE

* Users with  ADMIN privilege can create roles.

* GRANT/REVOKE

* Users with ADMIN privilege or GLOBAL level GRANT privilege can grant or revoke privileges to any user.
* Users with DATABASE level GRANT privilege can grant or revoke privileges to any user for any table in a specific database.
* Users with TABLE level GRANT privilege can grant or revoke privileges to any user for a specific table in a specific database.

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

Suppose the following user is created.  
   `CREATE USER abc@['domain'];`  
and authorized:  
   `GRANT SELECT_PRIV ON` `*. *` `TO abc@['domain']`  
The domain is resolved to two IPs: ip1 and ip2. Suppose we then authorize abc@'ip1' separately:
   `GRANT ALTER_PRIV ON` `*. *` `TO abc@'ip1';`  
Then the privileges of abc@'ip1' will be changed to `SELECT_PRIV`, `ALTER_PRIV`.Any future privileges changes to abc@\['domain'\] will not apply to abc@'ip1'.

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

All privileges are granted to `current_user`, and the real user has all the privileges of the corresponding `current_user`.

## Best Practices

Here are some scenarios for using the StarRocks permission management system.

### Scenario 1 Permissions Assignment

StarRocks users are divided into Administrator (Admin), Development Engineer (RD) and User (Client). The administrators have all the privileges and are mainly responsible for cluster construction, node management, and so on. The development engineers are responsible for business modeling, including building databases and tables, importing and modifying data,and so on. Users access databases and tables to get data.

In this scenario, Admins are granted ADMIN privileges or GRANT privileges. RDs are granted privileges to CREATE, DROP, ALTER, LOAD, and SELECT any or specific database tables. Clients are granted privileges to SELECT any or specific database tables. When there are multiple users, it is also possible to simplify the authorization by creating different roles.

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

    ```shell
    mysql -h <fe_ip> -P<fe_query_port> -uroot -p<xxxxxx>
    ```
