---
displayed_sidebar: docs
---

# DROP STORAGE VOLUME

## Description

ストレージボリュームを削除します。削除されたストレージボリュームは、もう参照できません。この機能は v3.1 からサポートされています。

> **注意**
>
> - 特定のストレージボリュームに対する DROP 権限を持つユーザーのみがこの操作を実行できます。
> - デフォルトのストレージボリュームおよび組み込みのストレージボリューム `builtin_storage_volume` は削除できません。[DESC STORAGE VOLUME](DESC_STORAGE_VOLUME.md) を使用して、ストレージボリュームがデフォルトのストレージボリュームかどうかを確認できます。
> - 既存のデータベースやクラウドネイティブテーブルによって参照されているストレージボリュームは削除できません。

## Syntax

```SQL
DROP STORAGE VOLUME [ IF EXISTS ] <storage_volume_name>
```

## Parameters

| **Parameter**       | **Description**                         |
| ------------------- | --------------------------------------- |
| storage_volume_name | 削除するストレージボリュームの名前。     |

## Examples

例 1: ストレージボリューム `my_s3_volume` を削除します。

```Plain
MySQL > DROP STORAGE VOLUME my_s3_volume;
Query OK, 0 rows affected (0.01 sec)
```

## Relevant SQL statements

- [CREATE STORAGE VOLUME](CREATE_STORAGE_VOLUME.md)
- [ALTER STORAGE VOLUME](ALTER_STORAGE_VOLUME.md)
- [SET DEFAULT STORAGE VOLUME](SET_DEFAULT_STORAGE_VOLUME.md)
- [DESC STORAGE VOLUME](DESC_STORAGE_VOLUME.md)
- [SHOW STORAGE VOLUMES](SHOW_STORAGE_VOLUMES.md)