---
displayed_sidebar: "Chinese"
---

# STOP ROUTINE LOAD

import RoutineLoadPrivNote from '../../../assets/commonMarkdown/RoutineLoadPrivNote.md'

## 功能

停止 Routine Load 导入作业。

<RoutineLoadPrivNote />

::: warning

导入作业停止且无法恢复。因此请谨慎执行该语句。

如果仅仅需要暂停导入作业，则可以执行 [PAUSE ROUTINE LOAD](./PAUSE_ROUTINE_LOAD.md)。

:::

## 语法

```SQL
STOP ROUTINE LOAD FOR [db_name.]<job_name>
```

## 参数说明

| 参数名称 | 是否必填 | 说明                        |
| -------- | -------- | --------------------------- |
| db_name  |          | Routine Load 导入作业所属数据库名称。         |
| job_name | ✅        | Routine Load 导入作业名称。 |

## 示例

停止 `example_db` 数据库中名称为 `example_tbl1_ordertest1` 的 Routine Load 导入作业。

```SQL
STOP ROUTINE LOAD FOR example_db.example_tbl1_ordertest1;
```
