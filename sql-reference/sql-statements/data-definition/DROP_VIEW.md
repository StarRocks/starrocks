# DROP VIEW

## 功能

该语句用于删除一个逻辑视图 VIEW。

创建 VIEW 操作请参考 [CREATE VIEW](../data-definition/CREATE_VIEW.md) 章节。

## 语法

```sql
DROP VIEW [IF EXISTS] [db_name.]view_name;
```

注：方括号 [] 中内容可省略不写。

## 示例

1. 如果存在，删除 example_db 上的视图 example_view。

    ```sql
    DROP VIEW IF EXISTS example_db.example_view;
    ```

## 关键字(keywords)

DROP，VIEW
