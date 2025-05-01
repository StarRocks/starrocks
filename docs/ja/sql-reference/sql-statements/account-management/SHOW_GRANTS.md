---
displayed_sidebar: docs
---

# SHOW GRANTS

## 説明

このステートメントは、ユーザーの権限を表示するために使用されます。

構文:

```sql
SHOW [ALL] GRANTS [FOR user_identity]
```

注意:

```plain text
1. SHOW ALL GRANTS は、すべてのユーザーの権限を表示できます。
2. user_identity が指定されている場合、指定されたユーザーの権限が表示されます。この user_identity は CREATE USER コマンドを通じて作成されている必要があります。
3. user_identity が指定されていない場合、現在のユーザーの権限が表示されます。
```

## 例

1. すべてのユーザーの権限を表示します。

    ```sql
    SHOW ALL GRANTS; 
    ```

2. 指定されたユーザーの権限を表示します。

    ```sql
    SHOW GRANTS FOR jack@'%';
    ```

3. 現在のユーザーの権限を表示します。

    ```sql
    SHOW GRANTS;
    ```