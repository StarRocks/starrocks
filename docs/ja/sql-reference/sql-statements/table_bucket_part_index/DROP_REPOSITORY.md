---
displayed_sidebar: docs
---

# DROP REPOSITORY

## Description

リポジトリを削除します。リポジトリは、[データのバックアップと復元](../../../administration/management/Backup_and_restore.md) のためにデータスナップショットを保存するために使用されます。

> **注意**
>
> - リポジトリを削除できるのは、root ユーザーとスーパーユーザーのみです。
> - この操作は、StarRocks 内のリポジトリのマッピングのみを削除し、実際のデータは削除しません。リモートストレージシステムで手動で削除する必要があります。削除後、同じリモートストレージシステムのパスを指定することで、そのリポジトリに再度マッピングできます。

## Syntax

```SQL
DROP REPOSITORY <repository_name>
```

## Parameters

| **Parameter**   | **Description**                       |
| --------------- | ------------------------------------- |
| repository_name | 削除するリポジトリの名前。             |

## Example

例 1: `oss_repo` という名前のリポジトリを削除します。

```SQL
DROP REPOSITORY `oss_repo`;
```