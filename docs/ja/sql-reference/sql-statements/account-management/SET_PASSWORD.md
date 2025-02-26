---
displayed_sidebar: docs
---

# SET PASSWORD

## 説明

ユーザーのログインパスワードを変更します。[ALTER USER](ALTER_USER.md) コマンドもパスワードの変更に使用できます。

:::tip

- すべてのユーザーは自分のパスワードをリセットできます。
- `user_admin` ロールを持つユーザーのみが他のユーザーのパスワードを変更できます。
- `root` ユーザー自身のみがそのパスワードを変更できます。詳細については、[権限の概要](../../../administration/user_privs/privilege_overview.md)を参照してください。

:::

### 構文

```SQL
SET PASSWORD [FOR user_identity] =
[PASSWORD('plain password')]|['hashed password']
```

`user_identity` は、CREATE USER を使用してユーザーを作成する際に指定した `user_identity` と正確に一致している必要があります。そうでない場合、ユーザーは存在しないと報告されます。`user_identity` が指定されていない場合、現在のユーザーのパスワードが変更されます。現在のユーザーは [SHOW GRANTS](./SHOW_GRANTS.md) を通じて確認できます。

`PASSWORD()` はプレーンテキストのパスワードを入力します。`PASSWORD()` を使用せずに文字列を直接入力する場合、その文字列は暗号化されている必要があります。

## 例

1. 現在のユーザーのパスワードを設定します。

    ```SQL
    SET PASSWORD = PASSWORD('123456')
    SET PASSWORD = '*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9'
    ```

2. 指定されたユーザーのパスワードを設定します。

    ```SQL
    SET PASSWORD FOR 'jack'@'192.%' = PASSWORD('123456')
    SET PASSWORD FOR 'jack'@['domain'] = '*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9'
    ```