# RECOVER

## 功能

恢复之前删除的 database、table 或者 partition。通过 [TRUNCATE TABLE](/sql-reference/sql-statements/data-definition/TRUNCATE_TABLE.md) 命令删除数据无法恢复。

## 语法

### 恢复 database

```sql
RECOVER DATABASE db_name;
```

### 恢复 table

```sql
RECOVER TABLE [db_name.]table_name;
```

### 恢复 partition

```sql
RECOVER PARTITION partition_name FROM [db_name.]table_name;
```

注：方括号 [] 中内容可省略不写。

说明：

1. 该操作仅能恢复之前一段时间内删除的元信息。默认为 1 天。（可通过 fe.conf 中 `catalog_trash_expire_second` 参数配置）

2. 如果删除元信息后新建立了同名同类型的元信息，则之前删除的元信息不能被恢复。

## 示例

1. 恢复名为 example_db 的 database。

    ```sql
    RECOVER DATABASE example_db;
    ```

2. 恢复名为 example_tbl 的 table。

    ```sql
    RECOVER TABLE example_db.example_tbl;
    ```

3. 恢复表 example_tbl 中名为 p1 的 partition。

    ```sql
    RECOVER PARTITION p1 FROM example_tbl;
    ```

## 关键字(keywords)

RECOVER
