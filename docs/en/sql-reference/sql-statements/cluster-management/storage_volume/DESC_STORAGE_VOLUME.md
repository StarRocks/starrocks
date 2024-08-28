---
displayed_sidebar: docs
---

# DESC STORAGE VOLUME

## Description

Describes a storage volume. This feature is supported from v3.1.

> **CAUTION**
>
> Only users with the USAGE privilege on a specific storage volume can perform this operation.

## Syntax

```SQL
DESC[RIBE] STORAGE VOLUME <storage_volume_name>
```

## Parameters

| **Parameter**       | **Description**                             |
| ------------------- | ------------------------------------------- |
| storage_volume_name | The name of the storage volume to describe. |

## Return value

| **Return** | **Description**                                              |
| ---------- | ------------------------------------------------------------ |
| Name       | The name of the storage volume.                              |
| Type       | The type of the remote storage system. Valid values: `S3` and `AZBLOB`. |
| IsDefault  | Whether the storage volume is the default storage volume.    |
| Location   | The location of the remote storage system.                   |
| Params     | The credential information used to access the remote storage system. |
| Enabled    | Whether the storage volume is enabled.                       |
| Comment    | The comment on the storage volume.                           |

## Examples

Example 1: Describe the storage volume `my_s3_volume`.

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
