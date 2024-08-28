---
displayed_sidebar: docs
---

# SHOW BACKUP

## 功能

查看指定数据库中的备份任务。更多信息，请见 备份和恢复。

> **说明**
>
> StarRocks 中仅保存最近一次备份任务信息。

## 语法

```SQL
SHOW BACKUP [FROM <db_name>]
```

## 参数说明

| **参数** | **说明**               |
| -------- | ---------------------- |
| db_name  | 备份任务所属数据库名。 |

## 返回

| **返回**             | **说明**                                                     |
| -------------------- | ------------------------------------------------------------ |
| JobId                | 本次备份作业的 ID。                                          |
| SnapshotName         | 用户指定的本次备份作业的名称。                               |
| DbName               | 备份作业对应的数据库。                                       |
| State                | 备份作业当前所在阶段：<ul><li>PENDING：作业初始状态。</li><li>SNAPSHOTING：正在进行快照操作。</li><li>UPLOAD_SNAPSHOT：快照结束，准备上传。</li><li>UPLOADING：正在上传快照。</li><li>SAVE_META：正在本地生成元数据文件。</li><li>UPLOAD_INFO：上传元数据文件和本次备份作业的信息。</li><li>FINISHED：备份完成。</li><li>CANCELLED：备份失败或被取消。</li></ul> |
| BackupObjs           | 本次备份涉及的对象。                                         |
| CreateTime           | 作业创建时间。                                               |
| SnapshotFinishedTime | 快照完成时间。                                               |
| UploadFinishedTime   | 快照上传完成时间。                                           |
| FinishedTime         | 本次作业完成时间。                                           |
| UnfinishedTasks      | 在 SNAPSHOTTING，UPLOADING 等阶段，会有多个子任务在同时进行，这里展示的当前段，未完成子任务的 Task ID。 |
| Progress             | 快照上传任务进展。|
| TaskErrMsg           | 如果有子任务执行出错，这里会显示对应子任务的错误信息。       |
| Status               | 用于记录在整个作业过程中，可能出现的一些状态信息。           |
| Timeout              | 作业的超时时间，单位为秒。                                   |

## 示例

示例一：查看数据库 `example_db` 下最后一次备份任务。

```SQL
SHOW BACKUP FROM example_db;
```
