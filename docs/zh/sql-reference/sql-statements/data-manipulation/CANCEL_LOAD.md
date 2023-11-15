# CANCEL LOAD

## 功能

该语句用于撤销指定 load label 的批次的导入作业。

这是一个 **异步** 操作，任务提交成功则返回。执行后可使用 SHOW LOAD 命令查看进度。

## 语法

注：方括号 [] 中内容如无需指定可省略不写。

```sql
CANCEL LOAD
[FROM db_name]
WHERE LABEL = "load_label";
```

## 示例

1. 撤销数据库 example_db 中 label 为 example_db_test_load_label 的导入作业。

    ```sql
    CANCEL LOAD
    FROM example_db
    WHERE LABEL = "example_db_test_load_label";
    ```

## 关键字(keywords)

CANCEL, LOAD
