---
displayed_sidebar: docs
---

# ALTER STORAGE VOLUME

## 説明

ストレージボリュームの認証プロパティ、コメント、またはステータス（`enabled`）を変更します。ストレージボリュームのプロパティについて詳しくは、 [CREATE STORAGE VOLUME](CREATE_STORAGE_VOLUME.md) を参照してください。この機能は v3.1 からサポートされています。

> **注意**
>
> - 特定のストレージボリュームに対する ALTER 権限を持つユーザーのみがこの操作を実行できます。
> - 既存のストレージボリュームの `TYPE`、`LOCATIONS`、およびその他のパス関連のプロパティは変更できません。変更できるのは認証関連のプロパティのみです。パス関連の設定項目を変更した場合、変更前に作成したデータベースとテーブルは読み取り専用になり、データをロードすることはできません。
> - `enabled` が `false` の場合、対応するストレージボリュームは参照できません。

## 構文

```SQL
ALTER STORAGE VOLUME [ IF EXISTS ] <storage_volume_name>
{ COMMENT = '<comment_string>'
| SET ("key" = "value"[,...]) }
```

## パラメーター

| **パラメーター**       | **説明**                              |
| ------------------- | ---------------------------------------- |
| storage_volume_name | 変更するストレージボリュームの名前。     |
| COMMENT             | ストレージボリュームに対するコメント。   |

変更または追加できるプロパティの詳細については、 [CREATE STORAGE VOLUME - PROPERTIES](CREATE_STORAGE_VOLUME.md#properties) を参照してください。

## 例

例 1: ストレージボリューム `my_s3_volume` を無効にします。

```Plain
MySQL > ALTER STORAGE VOLUME my_s3_volume
    -> SET ("enabled" = "false");
Query OK, 0 rows affected (0.01 sec)
```

例 2: ストレージボリューム `my_s3_volume` の認証情報を変更します。

```Plain
MySQL > ALTER STORAGE VOLUME my_s3_volume
    -> SET (
    ->     "aws.s3.use_instance_profile" = "true"
    -> );
Query OK, 0 rows affected (0.00 sec)
```

## 関連する SQL ステートメント

- [CREATE STORAGE VOLUME](CREATE_STORAGE_VOLUME.md)
- [DROP STORAGE VOLUME](DROP_STORAGE_VOLUME.md)
- [SET DEFAULT STORAGE VOLUME](SET_DEFAULT_STORAGE_VOLUME.md)
- [DESC STORAGE VOLUME](DESC_STORAGE_VOLUME.md)
- [SHOW STORAGE VOLUMES](SHOW_STORAGE_VOLUMES.md)