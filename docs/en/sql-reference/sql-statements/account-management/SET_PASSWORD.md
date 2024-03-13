---
displayed_sidebar: "English"
---

# SET PASSWORD

## Description

Changes login password for users. The [ALTER USER](ALTER_USER.md) command can also be used to change a password.

:::tip

- All users can reset their own password.
- Only users with the `user_admin` role can change the password of other users.
- Only the `root` user itself can change its password. For more information, see [Reset root password](../../../administration/User_privilege.md#reset-lost-root-password).

:::

### Syntax

```SQL
SET PASSWORD [FOR user_identity] =
[PASSWORD('plain password')]|['hashed password']
```

The `user_identity` must match exactly the `user_identity` specified when creating a user by using CREATE USER. Otherwise, the user will be reported as non-existent. If `user_identity` is not specified, the password of the current user will be changed. The current user can be viewed through [SHOW GRANTS](./SHOW_GRANTS.md).

`PASSWORD()` inputs a plaintext password. If you directly input a string without using `PASSWORD()`, the string must be encrypted.

## Examples

1. Set the password for the current user.

    ```SQL
    SET PASSWORD = PASSWORD('123456')
    SET PASSWORD = '*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9'
    ```

2. Set the password for a specified user.

    ```SQL
    SET PASSWORD FOR 'jack'@'192.%' = PASSWORD('123456')
    SET PASSWORD FOR 'jack'@['domain'] = '*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9'
    ```
