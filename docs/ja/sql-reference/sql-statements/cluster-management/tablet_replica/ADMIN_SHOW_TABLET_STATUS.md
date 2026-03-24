---
displayed_sidebar: docs
---

# ADMIN SHOW TABLET STATUS

## 説明

共有データクラスター内のクラウドネイティブテーブルまたはクラウドネイティブマテリアライズドビューの Tablet ステータスを表示します。メタデータファイルまたはデータファイルが欠落しているかどうかを確認できます。

:::tip

この操作には、SYSTEM レベルの OPERATE 権限が必要です。[GRANT](../../account-management/GRANT.md) の指示に従って、この権限を付与することができます。

:::

## 構文

```sql
ADMIN SHOW TABLET STATUS FROM [<db_name>.]<table_name>
[PARTITION (<partition_name> [, <partition_name>, ...])]
[WHERE STATUS [=|!=] {'NORMAL'|'MISSING_META'|'MISSING_DATA'}]
[PROPERTIES ("max_missing_data_files_to_show" = "<num>")]
```

## パラメータ

| パラメータ                          | 説明                                                                                                                                                                       |
| ----------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| db_name                       | データベース名。省略した場合は、現在のデータベースが使用されます。                                                                                                                          |
| table_name                    | テーブル名またはマテリアライズドビュー名。共有データクラスターのクラウドネイティブテーブルおよびクラウドネイティブマテリアライズドビューのみサポートします。                                                              |
| PARTITION                     | 確認するパーティション。省略した場合は、テーブル内のすべてのパーティションの Tablet ステータスが確認されます。                                                                                         |
| WHERE STATUS [=\|!=]          | ステータスでフィルタリングします。`=`（等しい）および `!=`（等しくない）の 2 つの演算子をサポートします。有効なステータス値については**返される列**を参照してください。                                               |
| max_missing_data_files_to_show | 各 Tablet に表示する欠落データファイルの最大数。デフォルト: `5`。`-1` に設定するとすべての欠落ファイルを表示します。`0` に設定するとファイルリストは表示されませんが、欠落ファイル数はカウントされます。 |

## 返される列

| 列名                   | 説明                                                                                                                                              |
| -------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------- |
| TabletId             | Tablet ID。                                                                                                                                       |
| PartitionId          | Tablet が属するパーティションの物理パーティション ID。                                                                                                    |
| Version              | Tablet の現在の可視バージョン。                                                                                                                       |
| Status               | Tablet ステータス：`NORMAL`（メタデータとデータファイルはすべて正常）、`MISSING_META`（メタデータファイルが欠落）、`MISSING_DATA`（データファイルが欠落）。 |
| MissingDataFileCount | 欠落しているデータファイルの数。`Status` が `MISSING_DATA` の場合のみ値が設定されます。                                                                     |
| MissingDataFiles     | 欠落しているデータファイルパスのリスト。`max_missing_data_files_to_show` で制限されます。`Status` が `MISSING_DATA` の場合のみ値が設定されます。                   |

## 例

1. テーブル内のすべての Tablet のステータスを表示する。

    ```sql
    ADMIN SHOW TABLET STATUS FROM my_cloud_table;
    ```

2. 指定したパーティションの Tablet ステータスを表示する。

    ```sql
    ADMIN SHOW TABLET STATUS FROM my_cloud_table PARTITION (p20250101, p20250102);
    ```

3. テーブル内のすべての異常（NORMAL 以外）な Tablet を表示する。

    ```sql
    ADMIN SHOW TABLET STATUS FROM my_cloud_table WHERE STATUS != "NORMAL";
    ```

4. メタデータファイルが欠落している Tablet を表示する。

    ```sql
    ADMIN SHOW TABLET STATUS FROM my_cloud_table WHERE STATUS = "MISSING_META";
    ```

5. データファイルが欠落している Tablet を表示し、すべての欠落ファイルパスを表示する。

    ```sql
    ADMIN SHOW TABLET STATUS FROM my_cloud_table WHERE STATUS = "MISSING_DATA"
    PROPERTIES ("max_missing_data_files_to_show" = "-1");
    ```
