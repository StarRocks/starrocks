---
displayed_sidebar: docs
---

# DROP FUNCTION

## 説明

カスタム関数を削除します。関数は、その名前とパラメーターの型が一致している場合にのみ削除できます。

カスタム関数の所有者のみが、その関数を削除する権限を持っています。

### 構文

```sql
DROP FUNCTION function_name(arg_type [, ...])
```

### パラメーター

`function_name`: 削除する関数の名前。

`arg_type`: 削除する関数の引数の型。

## 例

1. 関数を削除します。

    ```sql
    DROP FUNCTION my_add(INT, INT)
    ```