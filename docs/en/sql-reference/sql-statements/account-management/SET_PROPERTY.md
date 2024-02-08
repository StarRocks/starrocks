---
displayed_sidebar: "English"
---

# SET PROPERTY

## Description

Sets user properties. Currently, only the number of user connections (`max_user_connections`) can be set using this command.

:::tip

- The user properties mean the properties of users, not user_identity. For example, if two users, 'jack'@'%' and 'jack'@'192.%', are created using CREATE USER, the SET PROPERTY statement works only on user `jack``, not 'jack'@'%' or 'jack'@'192.%'.
- Only users with the `user_admin` role can perform this operation.
:::

### Syntax

```SQL
SET PROPERTY [FOR 'user'] 'key' = 'value' [, 'key' = 'value']
```

## Examples

1. Modify the maximum number of connections to `1000` for user `jack`.

    ```SQL
    SET PROPERTY FOR 'jack' 'max_user_connections' = '1000';
    ```

<!--
2. Modify cpu_share to 1000 for the user jack

    ```SQL
    SET PROPERTY FOR 'jack' 'resource.cpu_share' = '1000';
    ```

3. Modify the weight of the normal level for the user jack

    ```SQL
    SET PROPERTY FOR 'jack' 'quota.normal' = '400';
    ```
-->
