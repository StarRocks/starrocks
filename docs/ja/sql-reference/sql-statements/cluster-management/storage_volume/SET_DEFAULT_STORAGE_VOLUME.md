---
displayed_sidebar: docs
---

# SET DEFAULT STORAGE VOLUME

## 説明

ストレージボリュームをデフォルトのストレージボリュームとして設定します。外部データソース用のストレージボリュームを作成した後、それを StarRocks クラスターのデフォルトストレージボリュームとして設定できます。この機能は v3.1 からサポートされています。

> **注意**
>
> - 特定のストレージボリュームに対する USAGE 権限を持つユーザーのみがこの操作を実行できます。
> - デフォルトのストレージボリュームは削除または無効化できません。
> - StarRocks はシステム統計情報をデフォルトストレージボリュームに保存するため、共有データ StarRocks クラスターにはデフォルトストレージボリュームを設定する必要があります。

## 構文

```SQL
SET <storage_volume_name> AS DEFAULT STORAGE VOLUME
```

## パラメータ

| **パラメータ**      | **説明**                                                      |
| ------------------- | ------------------------------------------------------------ |
| storage_volume_name | デフォルトストレージボリュームとして設定するストレージボリュームの名前。 |

## 例

例 1: ストレージボリューム `my_s3_volume` をデフォルトストレージボリュームとして設定します。

```SQL
MySQL > SET my_s3_volume AS DEFAULT STORAGE VOLUME;
Query OK, 0 rows affected (0.01 sec)
```

## 関連する SQL ステートメント

- [CREATE STORAGE VOLUME](CREATE_STORAGE_VOLUME.md)
- [ALTER STORAGE VOLUME](ALTER_STORAGE_VOLUME.md)
- [DROP STORAGE VOLUME](DROP_STORAGE_VOLUME.md)
- [DESC STORAGE VOLUME](DESC_STORAGE_VOLUME.md)
- [SHOW STORAGE VOLUMES](SHOW_STORAGE_VOLUMES.md)