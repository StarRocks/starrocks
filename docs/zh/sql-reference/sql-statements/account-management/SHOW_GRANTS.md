# SHOW GRANTS

## 功能

该语句用于查看用户权限。

## 语法

```sql
SHOW [ALL] GRANTS [FOR user_identity]
```

说明：

1. SHOW ALL GRANTS 可以查看所有用户的权限。
2. 如果指定 user_identity，则查看该指定用户的权限。这里的 `user_identity` 语法与 [CREATE USER](../account-management/CREATE_USER.md) 章节中的相同。且必须为使用 `CREATE USER` 创建过的 `user_identity`。
3. 如果不指定 user_identity，则查看当前用户的权限。

## 示例

1. 查看所有用户权限信息

    ```sql
    SHOW ALL GRANTS; 
    ```

2. 查看指定 user 的权限

    ```sql
    SHOW GRANTS FOR jack@'%';
    ```

3. 查看当前用户的权限

    ```sql
    SHOW GRANTS;
    ```
