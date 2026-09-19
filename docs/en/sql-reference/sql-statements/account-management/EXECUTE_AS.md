---
displayed_sidebar: docs
description: "Use the IMPERSONATE privilege with EXECUTE AS statements to switch the execution context of the current session to the impersonated user."
---

# EXECUTE AS

Use the IMPERSONATE privilege with EXECUTE AS statements to switch the execution context of the current session to the impersonated user.

This command is supported from v2.4.

## Syntax

```SQL
EXECUTE AS user WITH NO REVERT
```

## Parameters

`user`: The user must already exist, unless it is a member of a group listed in `execute_as_external_user_allowed_groups`. See [Impersonating a user that was not created](#impersonating-a-user-that-was-not-created).

## Usage notes

- The current login user (who calls the EXECUTE AS statement) must be granted the privilege to impersonate another user. For more information, see [GRANT](./GRANT.md).
- The EXECUTE AS statement must contain the WITH NO REVERT clause, which means the execution context of the current session cannot be switched back to the original login user before the current session ends.

## Impersonating a user that was not created

From v4.2.0, a target that was never created with [CREATE USER](./CREATE_USER.md) can still be impersonated if it belongs to one of the groups listed in the FE configuration item `execute_as_external_user_allowed_groups`. This is useful when a service such as a notebook or a scheduler runs each query as the person who submitted it: those people no longer need an account in StarRocks before they can be impersonated.

The group membership is read from the [group providers](../../../administration/user_privs/group_provider.md) that apply to the session. A target admitted this way becomes an *ephemeral* user identity: it owns no privileges of its own, so its authorization comes entirely from the roles mapped to its groups, and per-user settings such as `max_user_connections` do not apply to it.

The configuration item is empty by default, which disables the feature. EXECUTE AS then fails with `cannot find user` for any target that was not created, and setting the item back to an empty value turns the feature off again at runtime.

```SQL
-- 1. Let the service account impersonate users that do not exist yet. Only the ALL USERS form can
--    cover such a target: a per-user grant cannot name a user that has not been created.
GRANT IMPERSONATE ON ALL USERS TO USER 'notebook_service'@'%';

-- 2. Allow members of the group to be impersonated.
ADMIN SET FRONTEND CONFIG ("execute_as_external_user_allowed_groups" = "analysts");

-- 3. With native access control, map the group to the roles it should have. This step is not
--    needed with Apache Ranger, which evaluates the session's groups directly.
GRANT analyst_role TO EXTERNAL GROUP 'analysts';
```

Two restrictions apply to a target that has no account:

- It must be written as `user` or `'user'@'%'`. An explicit host or the `'user'@['domain']` form is refused, because the host of the identity is also what Apache Ranger uses as the request's client IP.
- A grant that names the user does not cover it, even if one exists because a user of the same name was created and later dropped. Impersonating a name that has no account always requires `IMPERSONATE ON ALL USERS`.

Whether the target is a member of an allowed group is decided only after the caller's IMPERSONATE privilege has been verified, and a refusal is worded exactly like an unknown user. A caller without that privilege therefore cannot use EXECUTE AS to find out who exists or who belongs to which group; the reason for a refusal is written to the FE log instead.

## Examples

Switch the execution context of the current session to the user `test2`.

```SQL
EXECUTE AS test2 WITH NO REVERT;
```

After the switch succeeds, you can run the `select current_user()` command to obtain the current user.

```SQL
select current_user();
+-----------------------------+
| CURRENT_USER()              |
+-----------------------------+
| 'default_cluster:test2'@'%' |
+-----------------------------+
```
