---
displayed_sidebar: docs
---

# ADMIN SET CONFIG

## 説明

このステートメントは、クラスターの設定項目を設定するために使用されます（現在、このコマンドを使用して設定できるのは FE の動的設定項目のみです）。これらの設定項目は [ADMIN SHOW FRONTEND CONFIG](ADMIN_SET_CONFIG.md) コマンドを使用して表示できます。

設定は FE が再起動すると `fe.conf` ファイルのデフォルト値に戻ります。そのため、変更が失われないように `fe.conf` ファイル内の設定項目も変更することをお勧めします。

## 構文

```sql
ADMIN SET FRONTEND CONFIG ("key" = "value")
```

## 例

1. `disable_balance` を `true` に設定します。

    ```sql
    ADMIN SET FRONTEND CONFIG ("disable_balance" = "true");
    ```