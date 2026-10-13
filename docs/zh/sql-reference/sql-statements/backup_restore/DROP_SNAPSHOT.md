---
displayed_sidebar: docs
description: "从仓库中删除数据快照及其数据文件。"
---

# DROP SNAPSHOT

从仓库中删除数据快照，同时删除该快照在远端存储系统中的数据文件。与仅解除 StarRocks 中映射关系的 [DROP REPOSITORY](./DROP_REPOSITORY.md) 不同，该语句会删除备份数据本身。

删除不可撤销。快照被删除后，无法再通过 [RESTORE](./RESTORE.md) 恢复。

该语句自 v4.2.0 起支持。

:::caution

- 当有 BACKUP 作业正在写入该快照，或有 RESTORE 作业正在读取该快照时，无法删除。
- 当快照所在仓库为只读时，无法删除。

:::

## 权限要求

用户需拥有 System 级别的 REPOSITORY 权限。

```SQL
GRANT REPOSITORY ON SYSTEM TO ROLE <role_name>;
```

## 语法

```SQL
DROP SNAPSHOT <snapshot_name> ON <repository_name> [FORCE]
```

## 参数说明

| **参数**        | **说明**                                                     |
| --------------- | ------------------------------------------------------------ |
| snapshot_name   | 待删除快照的名称。                                           |
| repository_name | 快照所属仓库的名称。                                         |
| FORCE           | 跳过创建集群的校验直接删除。参见下文。                       |

## 快照归属

每个快照都记录了创建它的集群 ID。不带 `FORCE` 时，StarRocks 仅在该 ID 与当前集群一致时才执行删除。当前集群的 ID 可通过 [SHOW SNAPSHOT](./SHOW_SNAPSHOT.md) 的 `ClusterId` 列以及 [SHOW FRONTENDS](../cluster-management/nodes_processes/SHOW_FRONTENDS.md) 的 `ClusterId` 列查看。

以下情况该语句会报错：

- 快照由共用同一仓库的其他集群创建；
- 快照在 v4.2.0 之前创建，因而未记录集群 ID；
- 快照元数据不可读，包括备份仍在进行中，以及备份中断后残留的情况。

如需删除上述快照，请使用 `FORCE`。删除非本集群创建的快照会移除其他集群可能仍在依赖的数据，使用前请确认该数据确实可以删除。

## 示例

示例一：从仓库 `example_repo` 中删除快照 `backup1`。

```SQL
DROP SNAPSHOT backup1 ON example_repo;
```

示例二：从仓库 `example_repo` 中删除并非由本集群创建的快照 `legacy_backup`。

```SQL
DROP SNAPSHOT legacy_backup ON example_repo FORCE;
```

## 相关文档

- [BACKUP](./BACKUP.md)
- [SHOW SNAPSHOT](./SHOW_SNAPSHOT.md)
- [RESTORE](./RESTORE.md)
