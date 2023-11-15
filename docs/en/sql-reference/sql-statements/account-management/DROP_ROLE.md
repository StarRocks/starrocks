# DROP ROLE

## description

This statement allows users to delete a role.

Syntax:

```sql
DROP ROLE role1;
```

 Deleting a role does not affect permissions of users who previously belonged to this role. It only decouples the role from the user without changing permissions that user has already obtained from the role.

## example

1. Drop a role

  ```sql
  DROP ROLE role1;
  ```

## keyword

   DROP, ROLE
