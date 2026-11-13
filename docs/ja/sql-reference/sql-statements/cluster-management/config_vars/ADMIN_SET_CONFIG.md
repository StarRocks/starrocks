---
displayed_sidebar: docs
description: "クラスタの FE 動的設定項目を実行時に設定します。"
products: [oss, enterprise, cloud, anywhere]
---

# ADMIN SET CONFIG

## 説明

このステートメントは、クラスタの設定項目を設定するために使用されます（現在、このコマンドで設定できるのは FE の動的設定項目のみです）。これらの設定項目は、[ADMIN SHOW FRONTEND CONFIG](ADMIN_SHOW_CONFIG.md) コマンドを使用して表示できます。

設定は FE が再起動すると `fe.conf` ファイルのデフォルト値に戻ります。そのため、変更が失われないように `fe.conf` の設定項目も変更することをお勧めします。v3.5.0 以降では、ステートメントに `WITH PERSISTENT` を追加すると、新しい値が各 FE の `fe.conf` ファイルにも書き込まれ、FE の再起動後も保持されます。`fe.conf` が書き込み不可である場合、または FE がコンテナ内で実行されている場合、`WITH PERSISTENT` を指定したステートメントは失敗します。

このステートメントは、まず接続先の FE に変更を適用し、その後、他の各 FE に変更を送信します。v3.5.2 以降では、オフラインの FE はスキップされるため、ステートメントが成功してもそれらの FE には古い値が残ります。それより前のバージョンでは、オフラインの FE があるとステートメントはエラーを報告します。一部の FE で変更に失敗した場合、ステートメントはそれらの FE を示すエラーを報告しますが、すでに変更が適用された FE ではロールバックされません。スキップされた FE や失敗した FE にそれぞれ接続し、ADMIN SHOW FRONTEND CONFIG で値を確認して、必要に応じて再設定してください。

:::tip

この操作には SYSTEM レベルの OPERATE 権限が必要です。この権限を付与するには、[GRANT](../../account-management/GRANT.md) の指示に従ってください。

:::

## 構文

```sql
ADMIN SET FRONTEND CONFIG ("key" = "value") [WITH PERSISTENT]
```

## 例

1. `disable_balance` を `true` に設定します。

    ```sql
    ADMIN SET FRONTEND CONFIG ("disable_balance" = "true");
    ```