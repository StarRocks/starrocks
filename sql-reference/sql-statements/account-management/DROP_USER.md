# DROP USER

## description

### Syntax

```sql
 DROP USER 'user_identity'

`user_identity`:

 user@'host'
user@['domain']
```

 删除指定的 user identitiy.

## example

1. 删除用户 jack@'192.%'

    ```sql
    DROP USER 'jack'@'192.%'
    ```

## keyword

DROP, USER
