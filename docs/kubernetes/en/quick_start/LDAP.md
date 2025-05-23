# ldap-demo

## Goal

- Integrate StarRocks with an LDAP server for user authentication and authorization.
- Assign LDAP groups to StarRocks database roles.
- Verify the role assignments.

## LDAP server

The LDAP system used for this guide contains these users and groups:

- `developers`
  - `john`
  - `jane`
- `sr_admin`
  - `sr_fte`
  - `contractor`

```
ldapsearch -x -b dc=example,dc=org -D "cn=admin,dc=example,dc=org" -w admin -H ldap://34.30.114.160 -L
```

```
version: 1

#
# LDAPv3
# base <dc=example,dc=org> with scope subtree
# filter: (objectclass=*)
# requesting: ALL
#

# example.org
dn: dc=example,dc=org
objectClass: top
objectClass: dcObject
objectClass: organization
o: Example Inc.
dc: example

# People, example.org
dn: ou=People,dc=example,dc=org
objectClass: organizationalUnit
ou: People

# jane, People, example.org
dn: cn=jane,ou=People,dc=example,dc=org
objectClass: person
objectClass: inetOrgPerson
sn: doe
cn: jane
uid: jane
mail: janedoe@example.com
userPassword:: Zm9v

# john, People, example.org
dn: cn=john,ou=People,dc=example,dc=org
objectClass: person
objectClass: inetOrgPerson
sn: doe
cn: john
uid: john
mail: johndoe@example.com
userPassword:: YmFy

# sr_fte, People, example.org
dn: uid=sr_fte,ou=People,dc=example,dc=org
objectClass: inetOrgPerson
cn: sr_fte
uid: sr_fte
sn: FTE
givenName: SR
userPassword:: YmFy

# sr_contractors, People, example.org
dn: uid=sr_contractors,ou=People,dc=example,dc=org
objectClass: inetOrgPerson
cn: sr_contractors
uid: sr_contractors
sn: Contractors
givenName: SR
userPassword:: YmFy

# Groups, example.org
dn: ou=Groups,dc=example,dc=org
objectClass: organizationalUnit
ou: Groups

# developers, Groups, example.org
dn: cn=developers,ou=Groups,dc=example,dc=org
objectClass: groupOfNames
cn: developers
member: uid=jane,ou=People,dc=example,dc=org
member: uid=john,ou=People,dc=example,dc=org

# sr_admin, Groups, example.org
dn: cn=sr_admin,ou=Groups,dc=example,dc=org
objectClass: groupOfNames
cn: sr_admin
member: uid=sr_fte,ou=People,dc=example,dc=org
member: uid=sr_contractors,ou=People,dc=example,dc=org

# search result

# numResponses: 10
# numEntries: 9
```

```
CREATE SECURITY INTEGRATION test_security_integration
        PROPERTIES (
        "type" = "ldap",
        "ldap_server_host"="%s",
        "ldap_server_port"="%s",
        "ldap_bind_base_dn"="ou=People,dc=starrocks,dc=com",
        "ldap_user_search_attr"="uid",
        "ldap_bind_root_dn"="cn=Manager,dc=starrocks,dc=com",
        "ldap_bind_root_pwd"="admin@123",
        "ldap_cache_refresh_interval"="60",
        "comment"=""
        );
```

### Configure the security integration

Use SQL to integrate the StarRocks cluster with LDAP and create the security integration.

You will need:
- LDAP server host and port
- LDAP base dn
- LDAP attribute to match against (`uid` in this example)
- LDAP admin credentials (`cn=admin,dc=example,dc=org"`, and `admin` in this example)

```
CREATE SECURITY INTEGRATION test_security_integration
    PROPERTIES (
               "type" = "ldap",
               "ldap_server_host"="35.203.69.235",
               "ldap_server_port"="389",
               "ldap_bind_base_dn"="ou=People,dc=example,dc=org",
               "ldap_user_search_attr"="uid",
               "ldap_bind_root_dn"="cn=admin,dc=example,dc=org",
               "ldap_bind_root_pwd"="admin",
               "ldap_cache_refresh_interval"="60",
               "comment"=""
               );
```

### Set the authentication chain

```
ADMIN SET FRONTEND CONFIG ("authentication_chain" = "test_security_integration, native");
```

### Verify the authentication chain

```
ADMIN SHOW FRONTEND CONFIG LIKE "authentication_chain"\G
```

```
*************************** 1. row ***************************
       Key: authentication_chain
AliasNames: []
     Value: [test_security_integration, native]
      Type: String[]
 IsMutable: true
   Comment:
1 row in set (0.00 sec)
```

### View the built-in roles

These roles are created when the StarRocks cluster is deployed.

```
SHOW ROLES\G
```

```
*************************** 1. row ***************************
   Name: root
Builtin: true
Comment: built-in root role which has all privileges on all objects
*************************** 2. row ***************************
-- highlight-start
   Name: db_admin
Builtin: true
Comment: built-in database administration role
-- highlight-end
*************************** 3. row ***************************
   Name: cluster_admin
Builtin: true
Comment: built-in cluster administration role
*************************** 4. row ***************************
   Name: user_admin
Builtin: true
Comment: built-in user administration role
*************************** 5. row ***************************
   Name: public
Builtin: true
Comment: built-in public role which is owned by any user
*************************** 6. row ***************************
   Name: security_admin
Builtin: true
Comment: built-in security administration role
6 rows in set (0.02 sec)
```

### Assign the `db_admin` role to an LDAP group

The role `db_admin` is a built-in role. Map the role to the LDAP group with `dn` `cn=sr_admin,ou=Groups,dc=example,dc=org`:

```
CREATE ROLE MAPPING test_role_mapping_db_admin
    PROPERTIES (
       "integration_name" = "test_security_integration",
       "role" = "db_admin",
       "ldap_group_list" = "cn=sr_admin,ou=Groups,dc=example,dc=org"
       );
```

### Assign the `public` role to an LDAP group

The role `public` is a built-in role, and this is the default role for users. Map the role to the LDAP group with `dn` `cn=developers,ou=Groups,dc=example,dc=org`:

```
CREATE ROLE MAPPING test_role_mapping_public
    PROPERTIES (
       "integration_name" = "test_security_integration",
       "role" = "public",
       "ldap_group_list" = "cn=developers,ou=Groups,dc=example,dc=org"
       );
```

### Refresh the role mappings

```
REFRESH ALL ROLE MAPPINGS;
```

### View the role mappings

```
SHOW ROLE MAPPINGS\G
```

```
*************************** 1. row ***************************
                   Name: test_role_mapping_db_admin
        IntegrationName: test_security_integration
                   Role: db_admin
          LdapGroupList: cn=sr_admin,ou=Groups,dc=example,dc=org
LastRefreshCompleteTime: 2024-05-10 23:33:55
*************************** 2. row ***************************
                   Name: test_role_mapping_public
        IntegrationName: test_security_integration
                   Role: public
          LdapGroupList: cn=developers,ou=Groups,dc=example,dc=org
LastRefreshCompleteTime: 2024-05-10 23:33:55
2 rows in set (0.01 sec)
```

### Exit MySQL client

```
exit
```

### Query using LDAP credentials

```
mysql -u sr_fte \
      -P 9030 \
      -h 127.0.0.1 \
      -p \
      --default-auth mysql_clear_password \
      --enable-cleartext-plugin \
      -e "show databases"
```

```
Enter password:
+--------------------+
| Database           |
+--------------------+
| _statistics_       |
| information_schema |
| sys                |
+--------------------+
```

## Log in as a `developer` user

The `developer` LDAP group is assigned the StarRocks role `public`. This group
cannot create databases by default, and cannot access the databases other than
`information_schema`.

```
mysql -u john \
      -P 9030 \
      -h 127.0.0.1 \
      -p \
      --default-auth mysql_clear_password \
      --enable-cleartext-plugin
```

```
show databases;
```

```
+--------------------+
| Database           |
+--------------------+
| information_schema |
+--------------------+
1 row in set (0.01 sec)
```

```
CREATE DATABASE john;
```

```
ERROR 1252 (42000): Access denied; you need (at least one of) the CREATE DATABASE privilege(s)
on CATALOG default_catalog for this operation. Please ask the admin to grant permission(s) or
try activating existing roles using <set [default] role>. Current role(s): [public].
Inactivated role(s): NONE.
```
