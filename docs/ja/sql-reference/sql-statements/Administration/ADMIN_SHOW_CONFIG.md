---
displayed_sidebar: docs
---

# ADMIN SHOW CONFIG

## 説明

現在のクラスターの設定を表示します（現在はFEの設定項目のみ表示可能です）。これらの設定項目の詳細については、 [Configuration](../../../administration/Configuration.md#fe-configuration-items) を参照してください。

設定項目を設定または変更したい場合は、 [ADMIN SET CONFIG](ADMIN_SET_CONFIG.md) を使用してください。

## 構文

```sql
ADMIN SHOW FRONTEND CONFIG [LIKE "pattern"]
```

注意:

返り値のパラメータの説明:

```plain text
1. Key:        設定項目名
2. Value:      設定項目の値
3. Type:       設定項目のタイプ
4. IsMutable:  ADMIN SET CONFIG コマンドで設定可能かどうか
5. MasterOnly: Leader FE のみに適用されるかどうか
6. Comment:    設定項目の説明
```

## 例

1. 現在のFEノードの設定を表示します。

    ```sql
    ADMIN SHOW FRONTEND CONFIG;
    ```

2. `like` 述語を使用して現在のFEノードの設定を検索します。

    ```plain text
    mysql> ADMIN SHOW FRONTEND CONFIG LIKE '%check_java_version%';
    +--------------------+-------+---------+-----------+------------+---------+
    | Key                | Value | Type    | IsMutable | MasterOnly | Comment |
    +--------------------+-------+---------+-----------+------------+---------+
    | check_java_version | true  | boolean | false     | false      |         |
    +--------------------+-------+---------+-----------+------------+---------+
    1 row in set (0.00 sec)
    ```