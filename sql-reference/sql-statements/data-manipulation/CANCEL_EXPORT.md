# CANCEL EXPORT

## 功能

该语句用于取消指定 query id 的导出作业。

## 语法

注：方括号 [] 中内容可省略不写。

```sql
CANCEL EXPORT
[FROM db_name]
WHERE QUERYID = "your_query_id";
```

## 示例

1. 取消数据库 example_db 中，query id 为 “921d8f80-7c9d-11eb-9342-acde48001122” 的导出作业

    ```sql
    CANCEL EXPORT FROM example_db WHERE queryid = "921d8f80-7c9d-11eb-9342-acde48001122";
    ```

## 关键字(keywords)

CANCEL, EXPORT
