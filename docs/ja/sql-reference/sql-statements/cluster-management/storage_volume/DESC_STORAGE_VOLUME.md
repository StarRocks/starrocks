---
displayed_sidebar: docs
---

# DESC STORAGE VOLUME

## Description

ストレージボリュームを説明します。この機能は v3.1 からサポートされています。

> **注意**
>
> 特定のストレージボリュームに対して USAGE 権限を持つユーザーのみがこの操作を実行できます。

## Syntax

```SQL
DESC[RIBE] STORAGE VOLUME <storage_volume_name>
```

## Parameters

| **Parameter**       | **Description**                             |
| ------------------- | ------------------------------------------- |
| storage_volume_name | 説明するストレージボリュームの名前。         |

## Return value

| **Return** | **Description**                                              |
| ---------- | ------------------------------------------------------------ |
| Name       | ストレージボリュームの名前。                                 |
| Type       | リモートストレージシステムのタイプ。有効な値: `S3` および `AZBLOB`。 |
| IsDefault  | ストレージボリュームがデフォルトのストレージボリュームかどうか。 |
| Location   | リモートストレージシステムの場所。                           |
| Params     | リモートストレージシステムにアクセスするための認証情報。     |
| Enabled    | ストレージボリュームが有効かどうか。                         |
| Comment    | ストレージボリュームに関するコメント。                       |

## Examples

例 1: ストレージボリューム `my_s3_volume` を説明します。

```Plain
MySQL > DESCRIBE STORAGE VOLUME my_s3_volume\G
*************************** 1. row ***************************
     Name: my_s3_volume
     Type: S3
IsDefault: false
 Location: s3://defaultbucket/test/
   Params: {"aws.s3.access_key":"xxxxxxxxxx","aws.s3.secret_key":"yyyyyyyyyy","aws.s3.endpoint":"https://s3.us-west-2.amazonaws.com","aws.s3.region":"us-west-2","aws.s3.use_instance_profile":"true","aws.s3.use_aws_sdk_default_behavior":"false"}
  Enabled: false
  Comment: 
1 row in set (0.00 sec)
```

## Relevant SQL statements

- [CREATE STORAGE VOLUME](CREATE_STORAGE_VOLUME.md)
- [ALTER STORAGE VOLUME](ALTER_STORAGE_VOLUME.md)
- [DROP STORAGE VOLUME](DROP_STORAGE_VOLUME.md)
- [SET DEFAULT STORAGE VOLUME](SET_DEFAULT_STORAGE_VOLUME.md)
- [SHOW STORAGE VOLUMES](SHOW_STORAGE_VOLUMES.md)