# CANCEL BACKUP

## 功能

该语句用于取消一个正在进行的 [BACKUP](../data-definition/BACKUP.md) 任务。

## 语法

```sql
CANCEL BACKUP FROM db_name;
```

## 示例

1. 取消 example_db 下的 BACKUP 任务。

    ```sql
    CANCEL BACKUP FROM example_db;
    ```

## 关键字(keywords)

CANCEL， BACKUP
