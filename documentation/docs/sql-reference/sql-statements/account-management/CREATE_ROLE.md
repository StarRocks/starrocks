---
displayed_sidebar: "English"
---

# CREATE ROLE

## Description

Creates a role. After a role is created, you can grant privileges to the role and then assign this role to a user or another role. This way, the privileges associated with this role are passed on to users or roles.

Only users with the `user_admin` role or the `GRANT` privilege can create a role.

## Syntax

```sql
CREATE ROLE <role_name>
```

## Parameters

`role_name`: the name of the role. Naming conventions:

- It can only contain digits (0-9), letters, or underscores (_) and must start with a letter.
- It cannot exceed 64 characters in length.

Note that the created role name cannot be the same as [system-defined roles](../../../administration/privilege_overview.md#system-defined-roles).

## Examples

 Create a role.

  ```sql
  CREATE ROLE role1;
  ```

## References

- [GRANT](GRANT.md)
- [SHOW ROLES](SHOW_ROLES.md)
- [DROP ROLE](DROP_ROLE.md)
