# DROP USER

## Description

### Syntax

```sql
 DROP USER 'user_identity'

`user_identity`:

 user@'host'
user@['domain']
```

 Delete a specified user identity

## Examples

1. Delete user jack@'192.%'

    ```sql
    DROP USER 'jack'@'192.%'
    ```
