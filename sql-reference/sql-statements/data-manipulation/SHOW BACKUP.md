# SHOW BACKUP

## 功能

该语句用于查看 BACKUP 任务。

## 语法

```sql
SHOW BACKUP [FROM db_name];
```

说明：

```plain text
1. StarRocks 中仅保存最近一次 BACKUP 任务。
2. 各列含义如下：
JobId：                  唯一任务 id
SnapshotName：           备份的名称
DbName：                 所属数据库
State：                  备份任务当前所在阶段
    PENDING：                提交作业后的初始状态
    SNAPSHOTING：            正在进行快照操作
    UPLOAD_SNAPSHOT：        快照完成，准备上传
    UPLOADING：              快照上传中
    SAVE_META：              将作业元信息保存为本地文件
    UPLOAD_INFO：            上传作业元信息
    FINISHED：               备份完成，任务成功
    CANCELLED：              作业失败
BackupObjs：             备份的表和分区
CreateTime：             任务提交时间
SnapshotFinishedTime：   快照完成时间
UploadFinishedTime：     快照上传完成时间
FinishedTime：           作业结束时间
UnfinishedTasks：        在 SNAPSHOTING 和 UPLOADING 阶段会显示还未完成的子任务 id，该信息在备份任务工作时会不断更新
Status：                 如果任务失败，显示失败信息
Timeout：                作业超时时间，单位秒
```

## 示例

1. 查看数据库 example_db 下最后一次 BACKUP 任务。

```sql
SHOW BACKUP FROM example_db;
```
