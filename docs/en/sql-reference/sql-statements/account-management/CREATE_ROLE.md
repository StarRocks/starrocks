---
displayed_sidebar: "English"
---

# CREATE ROLE

import UserManagementPriv from '../../../assets/commonMarkdown/userManagementPriv.md'

## Description

Creates a role. After a role is created, you can grant privileges to the role and then assign this role to a user or another role. This way, the privileges associated with this role are passed on to users or roles.

<UserManagementPriv />

## Syntax

```sql
CREATE ROLE <role_name>
```

## Parameters

`role_name`: the name of the role. For the naming conventions, see [System limits](../../../reference/System_limit.md).

Note that the created role name cannot be the same as [system-defined roles](../../../administration/privilege_overview.md#system-defined-roles): `root`, `cluster_admin`, `db_admin`, `user_admin`, and `public`.

## Examples

 Create a role.

  ```sql
  CREATE ROLE role1;
  ```

## References

- [GRANT](GRANT.md)
- [SHOW ROLES](SHOW_ROLES.md)
- [DROP ROLE](DROP_ROLE.md)
