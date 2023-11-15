# PAUSE ROUTINE LOAD

## 功能

暂停 routine load 导入任务，暂停任务后，可对 routine load 任务属性的修改。详细操作请参考 [alter routine load](../data-manipulation/alter-routine-load.md)，通过 [RESUME](../data-manipulation/RESUME_ROUTINE_LOAD.md) 命令可以恢复，。

## 示例

1. 暂停名称为 test1 的例行导入作业。

    ```sql
    PAUSE ROUTINE LOAD FOR test1;
    ```
