---
displayed_sidebar: docs
---

# DROP ROLE

DROP ROLE drops a role. If a role has been granted to a user, the user will lose the privileges associated with this role after the role is dropped.

:::tip

- Only users with the `user_admin` role can drop a role.
- [StarRocks system-defined roles](../../../administration/user_privs/authorization/user_privs.md#system-defined-roles) cannot be dropped.

:::

## Syntax

```sql
DROP ROLE <role_name>
```

## Examples

Drop a role.

  ```sql
  DROP ROLE role1;
  ```
