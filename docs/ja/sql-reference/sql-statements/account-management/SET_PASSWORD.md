---
displayed_sidebar: docs
---

# SET PASSWORD

## 説明

### 構文

```SQL
SET PASSWORD [FOR user_identity] =
[PASSWORD('plain password')]|['hashed password']
```

SET PASSWORD コマンドは、ユーザーのログインパスワードを変更するために使用できます。[FOR user_identity] フィールドが存在しない場合、現在のユーザーのパスワードが変更されます。

```plain text
user_identity は、CREATE USER を使用してユーザーを作成する際に指定した user_identity と正確に一致する必要があります。そうでない場合、ユーザーは存在しないと報告されます。user_identity が指定されていない場合、現在のユーザーは 'username'@'ip' であり、これは user_identity と一致しない可能性があります。現在のユーザーは SHOW GRANTS を通じて確認できます。
```

PASSWORD() はプレーンテキストのパスワードを入力し、文字列を直接使用する場合は暗号化されたパスワードの送信が必要です。

他のユーザーのパスワードを変更するには、管理者権限が必要です。

## 例

1. 現在のユーザーのパスワードを変更する

    ```SQL
    SET PASSWORD = PASSWORD('123456')
    SET PASSWORD = '*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9'
    ```

2. 指定されたユーザーのパスワードを変更する

    ```SQL
    SET PASSWORD FOR 'jack'@'192.%' = PASSWORD('123456')
    SET PASSWORD FOR 'jack'@['domain'] = '*6BB4837EB74329105EE4568DDA7DC67ED2CA2AD9'
    ```