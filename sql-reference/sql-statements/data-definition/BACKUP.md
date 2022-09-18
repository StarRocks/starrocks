# BACKUP

## 功能

该语句用于备份指定数据库下的数据。该命令为 **异步** 操作。提交成功后，需通过 `SHOW BACKUP` 命令查看进度。当前，仅支持备份 OLAP 类型表，且表的数据模型需为明细模型、聚合模型或更新模型，暂不支持备份数据模型为主键模型的表。

## 语法

```sql
BACKUP SNAPSHOT [db_name].{snapshot_name}
TO `repository_name`
ON (
`table_name` [PARTITION (`p1`, ...)],
...
)
PROPERTIES ("key"="value", ...);
```

说明：

1. 同一数据库下只能有一个正在执行的 `BACKUP` 或 `RESTORE` 任务。
2. ON 子句中标识需要备份的表和分区。如果不指定分区，则默认备份该表的所有分区。
3. `PROPERTIES` 目前支持以下属性：

    ```plain text
    "type" = "full"：表示这是一次全量更新（默认）。
    "timeout" = "3600"：任务超时时间，默认为一天。单位秒。
    ```

## 示例

1. 全量备份 example_db 下的表 example_tbl 到仓库 example_repo 中。

    ```sql
    BACKUP SNAPSHOT example_db.snapshot_label1
    TO example_repo
    ON (example_tbl)
    PROPERTIES ("type" = "full");
    ```

2. 全量备份 example_db 下，表 example_tbl 的 p1, p2 分区，以及表 example_tbl2 到仓库 example_repo 中。

    ```sql
    BACKUP SNAPSHOT example_db.snapshot_label2
    TO example_repo
    ON
    (
    example_tbl PARTITION (p1,p2),
    example_tbl2
    );
    ```
