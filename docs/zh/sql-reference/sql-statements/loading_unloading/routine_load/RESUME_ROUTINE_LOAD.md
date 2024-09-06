---
displayed_sidebar: docs
---

# RESUME ROUTINE LOAD

import RoutineLoadPrivNote from '../../../../_assets/commonMarkdown/RoutineLoadPrivNote.md'

## 功能

恢复 Routine Load 导入作业。导入作业会先短暂地进入 **NEED_SCHEDULE** 状态，表示正在重新调度导入作业，一段时间后会恢复至 **RUNNING** 状态，继续消费数据源的消息并且导入数据。您可以执行 [SHOW ROUTINE LOAD](SHOW_ROUTINE_LOAD.md) 语句查看已恢复的导入作业。

<RoutineLoadPrivNote />

## 语法

```SQL
RESUME ROUTINE LOAD FOR [db_name.]<job_name>
```

## 参数说明

| 参数名称 | 是否必填 | 说明                        |
| -------- | -------- | --------------------------- |
| db_name  |          | Routine Load 导入作业所属数据库名称。         |
| job_name | ✅        | Routine Load 导入作业名称。 |

## 示例

恢复 `example_db` 数据库中名称为 `example_tbl1_ordertest1` 的 Routine Load 导入作业。

```SQL
RESUME ROUTINE LOAD FOR example_db.example_tbl1_ordertest1;
```
