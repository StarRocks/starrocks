# is_role_in_session

## Description

Verifies whether a role (or a nested role) is active in the current session.

This function is supported from v3.1.4 onwards.

## Syntax

```Haskell
BOOLEAN is_role_in_session(VARCHAR role_name);
```

## Parameters

`role_name`: the role you want to verify (can also be a nested role). Supported data type is VARCHAR.

## Return value

Returns a BOOLEAN value. `1` indicates the role is active in the current session. `0` indicates the opposite.

## Examples

1. Create roles and a user.

   ```sql
   -- Create three roles.
   create role r1;
   create role r2;
   create role r3;

   -- Create user u1.
   create user u1;

   -- Pass roles r2 and r3 to r1, and grant r1 to user u1. This way, user u1 has three roles: r1, r2, and r3.
   grant r3 to role r2;
   grant r2 to role r1;
   grant r1 to user u1;

   -- Switch to user u1 and perform operations as u1.
   execute as u1 with no revert;
   ```

2. Verify whether `r1` is active. The result shows this role is not active.

   ```plaintext
   select is_role_in_session("r1");
   +--------------------------+
   | is_role_in_session('r1') |
   +--------------------------+
   |                        0 |
   +--------------------------+
   ```

3. Run the [SET ROLE](../../sql-statements/account-management/SET_ROLE.md) command to activate `r1` and use `is_role_in_session` to verify whether the role is active. The result shows `r1` is active and roles `r2` and `r3` that are nested in `r1` are also active.

   ```sql
   set role "r1";

   select is_role_in_session("r1");
   +--------------------------+
   | is_role_in_session('r1') |
   +--------------------------+
   |                        1 |
   +--------------------------+

   select is_role_in_session("r2");
   +--------------------------+
   | is_role_in_session('r2') |
   +--------------------------+
   |                        1 |
   +--------------------------+

   select is_role_in_session("r3");
   +--------------------------+
   | is_role_in_session('r3') |
   +--------------------------+
   |                        1 |
   +--------------------------+
   ```
