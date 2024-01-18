---
displayed_sidebar: "Chinese"
---

# PAUSE ROUTINE LOAD

import RoutineLoadPrivNote from '../../../assets/commonMarkdown/RoutineLoadPrivNote.md'

## 功能

暂停 Routine Load 导入作业，导入作业会进入 PAUSED 状态，但是未结束，您可以执行 [RESUME ROUTINE LOAD](./RESUME_ROUTINE_LOAD.md) 语句重启导入作业。

Routine Load 导入作业暂停后，您可以执行 [SHOW ROUTINE LOAD](./SHOW_ROUTINE_LOAD.md) 、[ALTER ROUTINE LOAD](./ALTER_ROUTINE_LOAD.md) 语句查看和修改已暂停的导入作业的信息。

<RoutineLoadPrivNote />

## 语法

```SQL
PAUSE ROUTINE LOAD FOR [db_name.]<job_name>
```

## 参数说明

| 参数名称 | 是否必填 | 说明                                                         |
| -------- | -------- | ------------------------------------------------------------ |
| db_name  |          | Routine Load 导入作业所属数据库名称。                                           |
| job_name | ✅        | Routine Load 导入作业名称。|

## 示例

暂停 `example_db` 数据库中名称为 `example_tbl1_ordertest1` 的 Routine Load 导入作业。

```SQL
PAUSE ROUTINE LOAD FOR example_db.example_tbl1_ordertest1;
```
