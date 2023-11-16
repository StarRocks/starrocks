---
displayed_sidebar: "English"
---

# SET PASSWORD

## Description

### Syntax

```SQL
SET PASSWORD [FOR user_identity] =
[PASSWORD('plain password')]|['hashed password']
```

SET PASSWORD command can be used to change user's login password. If the field, [FOR user_identity], does not exist, the current user's password shall be modified.

```plain text
Please note that the user_identity must match exactly the user_identity specified when creating a user by using CREATE USER. Otherwise, the user will be reported as non-existent. If user_identity is not specified, the current user is 'username'@'ip, which may not match any user_identity. The current user can be viewed through SHOW GRANTS. 
```

PASSWORD() inputs plaintext passwords, while the direct usage of strings requires the transmission of encrypted password.

Modifying passwords of other users requires administrator privileges.

## Examples

1. Modify the password of the current user

    ```SQL
    SET PASSWORD = PASSWORD('123456')
    SET PASSWORD = '*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9'
    ```

2. Modify the password of the specified user

    ```SQL
    SET PASSWORD FOR 'jack'@'192.%' = PASSWORD('123456')
    SET PASSWORD FOR 'jack'@['domain'] = '*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9'
    ```
