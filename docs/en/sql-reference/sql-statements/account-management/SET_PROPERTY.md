---
displayed_sidebar: "English"
---

# SET PROPERTY

## Description

### Syntax

```SQL
SET PROPERTY [FOR 'user'] 'key' = 'value' [, 'key' = 'value']
```

Set user attributes, including resources allocated to users and etc. The user properties here means the attributes for users, not for user_identity. That is to say, if two users, 'jack'@'%' and 'jack'@'192.%', are created through CREATE USER statement, then SET PROPERTY statement can only be used for user jack, not 'jack'@'%' or 'jack'@'192.%'.

key:

Super user permission:

```plain text
max_user_connections: Maximum number of connections
resource.cpu_share: cpu resource assignment
```

Ordinary user permission:

```plain text
quota.normal: Resource allocation at the normal level
quota.high: Resource allocation at the high level 
quota.low: Resource allocation at the low level
```

## Examples

1. Modify the maximum number of connections to 1000 for the user jack

    ```SQL
    SET PROPERTY FOR 'jack' 'max_user_connections' = '1000';
    ```

2. Modify cpu_share to 1000 for the user jack

    ```SQL
    SET PROPERTY FOR 'jack' 'resource.cpu_share' = '1000';
    ```

3. Modify the weight of the normal level for the user jack

    ```SQL
    SET PROPERTY FOR 'jack' 'quota.normal' = '400';
    ```
