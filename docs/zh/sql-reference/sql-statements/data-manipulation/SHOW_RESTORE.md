# SHOW RESTORE

## 功能

查看指定数据库中的恢复任务。更多信息，请见 [备份和恢复](../../../administration/Backup_and_restore.md)。

> **说明**
>
> StarRocks 中仅保存最近一次恢复任务信息。

## 语法

```SQL
SHOW RESTORE [FROM <db_name>]
```

## 参数说明

| **参数** | **说明**               |
| -------- | ---------------------- |
| db_name  | 恢复任务所属数据库名。 |

## 返回

| **返回**             | **说明**                                                     |
| -------------------- | ------------------------------------------------------------ |
| JobId                | 本次恢复作业的 ID。                                          |
| Label                | 要恢复的备份名称。                                           |
| Timestamp            | 备份时间戳。                                                 |
| DbName               | 恢复作业对应的数据库。                                       |
| State                | 恢复作业当前所在阶段：<ul><li>PENDING：作业初始状态。</li><li>SNAPSHOTING：正在进行本地新建表的快照操作。</li><li>DOWNLOAD：正在发送下载快照任务。</li><li>DOWNLOADING：快照正在下载。</li><li>COMMIT：准备生效已下载的快照。</li><li>COMMITTING：正在生效已下载的快照。</li><li>FINISHED：恢复完成。</li><li>CANCELLED：恢复失败或被取消。</li></ul> |
| AllowLoad            | 恢复期间是否允许导入。                                       |
| ReplicationNum       | 恢复指定的副本数。                                           |
| RestoreObjs          | 被恢复的对象（表和分区）。                                   |
| CreateTime           | 作业创建时间。                                               |
| MetaPreparedTime     | 本地元数据生成完成时间。                                     |
| SnapshotFinishedTime | 快照完成时间。                                               |
| DownloadFinishedTime | 远端快照下载完成时间。                                       |
| FinishedTime         | 本次作业完成时间。                                           |
| UnfinishedTasks      | 在 SNAPSHOTTING、DOWNLOADING、COMMITTING 等阶段，会有多个子任务在同时进行，这里展示的当前段，未完成子任务的 Task ID。 |
| Progress             | 快照下载任务进展。                                          |
| TaskErrMsg           | 如果有子任务执行出错，这里会显示对应子任务的错误信息。       |
| Status               | 用于记录在整个作业过程中，可能出现的一些状态信息。           |
| Timeout              | 作业的超时时间，单位为秒。                                   |

## 示例

示例一：查看数据库 `example_db` 下最后一次恢复任务。

```SQL
SHOW RESTORE FROM example_db;
```
