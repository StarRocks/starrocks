# DROP USER

## 功能

删除用户。

## 语法

注：方括号 [] 中内容可省略不写。

```sql
 -- 命令
 DROP USER 'user_identity';

 --参数说明
user_identity:user@'host'
```

 删除指定的 `user identitiy`。`user identitiy`由`user_name` 和 `host`两部分组成，其中 `user_name` 为用户名。`host` 标识用户端连接所在的主机地址。

## 示例

1. 删除用户 jack@'192.%'

    ```sql
    DROP USER 'jack'@'192.%'
    ```

## 关键字(keywords)

DROP, USER
