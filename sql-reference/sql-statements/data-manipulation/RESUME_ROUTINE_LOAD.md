# RESUME ROUTINE LOAD

## 功能

恢复已暂停 routine load 导入任务，通过 [PASUME](../data-manipulation/PAUSE_ROUTINE_LOAD.md) 命令可以暂停导入的任务，并进行 routine load 任务属性的修改，详细操作请参考 [alter routine load](../data-manipulation/alter-routine-load.md)。

## 示例

1. 恢复名称为 test1 的例行导入作业。

```sql
    RESUME ROUTINE LOAD FOR test1;
```

## 关键字(keywords)

RESUME, ROUTINE, LOAD
