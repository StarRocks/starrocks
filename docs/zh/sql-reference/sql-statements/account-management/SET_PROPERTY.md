# SET PROPERTY

## 描述

设置用户的属性，包括分配给用户的资源等。

## 语法

注：方括号 [] 中内容可省略不写。

```SQL
SET PROPERTY [FOR 'user'] 'key' = 'value' [, 'key' = 'value'];
```

这里设置的用户属性，是针对 user 的，而不是 user_identity。即假设通过 CREATE USER 语句创建了两个用户 `'jack'@'%'` 和 `'jack'@'192.%'`，则使用 `SET PROPERTY` 语句，只能针对 jack 这个用户，而不是 `'jack'@'%'` 或 `'jack'@'192.%'`。

**key:**

超级用户权限:

```plain text
max_user_connections: 最大连接数。
resource.cpu_share: cpu资源分配。
```

普通用户权限：

```plain text
quota.normal: normal级别的资源分配。
quota.high: high级别的资源分配。
quota.low: low级别的资源分配。
```

## 示例

1. 修改用户 jack 最大连接数为 1000。

    ```SQL
    SET PROPERTY FOR 'jack' 'max_user_connections' = '1000';
    ```

2. 修改用户 jack 的 cpu_share 为 1000。

    ```SQL
    SET PROPERTY FOR 'jack' 'resource.cpu_share' = '1000';
    ```

3. 修改 jack 用户的 normal 组的权重。

    ```SQL
    SET PROPERTY FOR 'jack' 'quota.normal' = '400';
    ```

## 关键字(keywords)

SET, PROPERTY
