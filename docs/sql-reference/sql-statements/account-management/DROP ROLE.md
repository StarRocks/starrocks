# DROP ROLE

## Description

This statement allows users to delete a role.

## Syntax

```sql
DROP ROLE <role_name>
```

 Deleting a role does not affect permissions of users who previously belonged to this role. It only decouples the role from the user without changing permissions that user has already obtained from the role.

## Examples

Drop a role.

  ```sql
  DROP ROLE role1;
  ```
