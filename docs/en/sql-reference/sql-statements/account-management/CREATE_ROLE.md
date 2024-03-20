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

`role_name`: the name of the role. Naming conventions:

<<<<<<< HEAD
- It can only contain digits (0-9), letters, or underscores (_) and must start with a letter.
- It cannot exceed 64 characters in length.

Note that the created role name cannot be the same as [system-defined roles](../../../administration/privilege_overview.md#system-defined-roles).
=======
Note that the created role name cannot be the same as [system-defined roles](../../../administration/user_privs/privilege_overview.md#system-defined-roles): `root`, `cluster_admin`, `db_admin`, `user_admin`, and `public`.
>>>>>>> 27c574aa60 ([Doc] Remove excess doc links (#42813))

## Limits

By default, a user can have a maximum of 64 roles. You can adjust this setting by using the FE dynamic parameter `privilege_max_total_roles_per_user`. A role can have a maximum of 16 inheritance levels. You can adjust this setting by using the FE dynamic parameter `privilege_max_role_depth`.

## Examples

 Create a role.

  ```sql
  CREATE ROLE role1;
  ```

## References

- [GRANT](GRANT.md)
- [SHOW ROLES](SHOW_ROLES.md)
- [DROP ROLE](DROP_ROLE.md)
