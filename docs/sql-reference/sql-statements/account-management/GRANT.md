# GRANT

## Description

Grants specific privileges to a user or a role.

Grants roles to users or other roles.

## Syntax

### Grant privileges to roles or users

```SQL
# System

GRANT
    { CREATE RESOURCE GROUP | CREATE RESOURCE | CREATE EXTERNAL CATALOG | REPOSITORY | BLACKLIST | FILE | OPERATE | ALL [PRIVILEGES]} 
    ON SYSTEM
    TO { ROLE | USER} {<role_name>|<user_identity>} [ WITH GRANT OPTION ]

# Resource group

GRANT
    { ALTER | DROP | ALL [PRIVILEGES] } 
    ON { RESOURCE GROUP <resource_name> [, < resource_name >,...] ｜ ALL RESOURCE GROUPS} 
    TO { ROLE | USER} {<role_name>|<user_identity>} [ WITH GRANT OPTION ]

# Resource

GRANT
    { USAGE | ALTER | DROP | ALL [PRIVILEGES] } 
    ON { RESOURCE <resource_name> [, < resource_name >,...] ｜ ALL RESOURCES} 
    TO { ROLE | USER} {<role_name>|<user_identity>} [ WITH GRANT OPTION ]

# Global UDF

GRANT
    { USAGE | DROP | ALL [PRIVILEGES]} 
    ON { GLOBAL FUNCTION <function_name> [, < function_name >,...]    
       | ALL GLOBAL FUNCTIONS }
    TO { ROLE | USER} {<role_name>|<user_identity>} [ WITH GRANT OPTION ]

# Internal catalog

GRANT
    { USAGE | CREATE DATABASE | ALL [PRIVILEGES]} 
    ON CATALOG default_catalog
    TO { ROLE | USER} {<role_name>|<user_identity>} [ WITH GRANT OPTION ]

# External catalog

GRANT
   { USAGE | DROP | ALL [PRIVILEGES] } 
   ON { CATALOG <catalog_name> [, <catalog_name>,...] | ALL CATALOGS}
   TO { ROLE | USER} {<role_name>|<user_identity>} [ WITH GRANT OPTION ]

# Database

GRANT
    { ALTER | DROP | CREATE TABLE | CREATE VIEW | CREATE FUNCTION | CREATE MATERIALIZED VIEW | ALL [PRIVILEGES] } 
    ON { DATABASE <database_name> [, <database_name>,...] | ALL DATABASES }
    TO { ROLE | USER} {<role_name>|<user_identity>} [ WITH GRANT OPTION ]
  
* You must first run SET CATALOG before you run this command.

# Table

GRANT
    { ALTER | DROP | SELECT | INSERT | EXPORT | UPDATE | DELETE | ALL [PRIVILEGES]} 
    ON { TABLE <table_name> [, < table_name >,...]
       | ALL TABLES} IN 
           { DATABASE <database_name> } | ALL DATABASES }
    TO { ROLE | USER} {<role_name>|<user_identity>} [ WITH GRANT OPTION ]

* You must first run SET CATALOG before you run this command. 
* You can also use db.tbl to represent a table.
GRANT <priv> ON TABLE db.tbl TO {ROLE <rolename> | USER <username>}

# View

GRANT  
    { ALTER | DROP | SELECT | ALL [PRIVILEGES]} 
    ON { VIEW <view_name> [, < view_name >,...]
       ｜ ALL VIEWS} IN 
           {  DATABASE <database_name> | ALL DATABASES }
    TO { ROLE | USER} {<role_name>|<user_identity>} [ WITH GRANT OPTION ]
    
* You must first run SET CATALOG before you run this command. 
* You can also use db.view to represent a view.
GRANT <priv> ON VIEW db.view TO {ROLE <rolename> | USER <username>}

# Materialized view

GRANT
    { SELECT | ALTER | REFRESH | DROP | ALL [PRIVILEGES]} 
    ON { MATERIALIZED VIEW <mv_name> [, < mv_name >,...]
       ｜ ALL MATERIALIZED VIEWS} IN 
           { DATABASE <database_name> | ALL DATABASES }
    TO { ROLE | USER} {<role_name>|<user_identity>} [ WITH GRANT OPTION ]
    
* You must first run SET CATALOG before you run this command. 
* You can also use db.mv to represent an mv.
GRANT <priv> ON MATERIALIZED_VIEW db.mv TO {ROLE <rolename> | USER <username>}

# Function

GRANT
    { USAGE | DROP | ALL [PRIVILEGES]} 
    ON { FUNCTION <function_name> [, < function_name >,...]
       ｜ ALL FUNCTIONS} IN 
           {  DATABASE <database_name>  | ALL DATABASES }
    TO { ROLE | USER} {<role_name>|<user_identity>} [ WITH GRANT OPTION ]
    
* You must first run SET CATALOG before you run this command. 
* You can also use db.function to represent a function.
GRANT <priv> ON FUNCTION db.function TO {ROLE <rolename> | USER <username>}

# User

GRANT IMPERSONATE
ON USER <user_identity>
TO USER <user_identity> [ WITH GRANT OPTION ]

# Storage volume

GRANT
    CREATE STORAGE VOLUME 
    ON SYSTEM
    TO { ROLE | USER} {<role_name>|<user_identity>} [ WITH GRANT OPTION ]

GRANT  
    { USAGE | ALTER | DROP | ALL [PRIVILEGES] } 
    ON { STORAGE VOLUME < name > [, < name >,...] ｜ ALL STORAGE VOLUME} 
    TO { ROLE | USER} {<role_name>|<user_identity>} [ WITH GRANT OPTION ]
```

### Grant roles  to roles or users

```SQL
GRANT <role_name> [,<role_name>, ...] TO ROLE <role_name>
GRANT <role_name> [,<role_name>, ...] TO USER <user_identity>
```

## References

- [SHOW GRANTS](SHOW%20GRANTS.md)

- [REVOKE](REVOKE.md)
