# SHOW TABLET

## 功能

该语句用于显示 tablet 相关的信息（仅管理员使用）。

## 语法

注：方括号 [] 中内容可省略不写。

```sql
SHOW TABLET
[FROM [db_name.]table_name | tablet_id] [partition(partition_name_1, partition_name_1)]
[where [version=1] [and backendid=10000] [and state="NORMAL|ROLLUP|CLONE|DECOMMISSION"]]
[order by order_column]
[limit [offset,]size]
```

`show tablet` 命令支持按照按照以下字段进行过滤：partition, index name, version, backendid,
state，同时支持按照任意字段进行排序，并且提供 limit 限制返回条数。

## 示例

1. 显示指定 db 的下指定表所有 tablet 信息。

    ```sql
    SHOW TABLET FROM example_db.table_name;
    ```

2. 显示指定 tablet id 为 10000 的 tablet 的父层级 id 信息。

    ```sql
    SHOW TABLET 10000;
    ```

3. 获取 partition p1 和 p2 的 tablet 信息

    ```sql
        SHOW TABLET FROM example_db.table_name partition(p1, p2);
    ```

4. 获取 10 个结果

    ```sql
        SHOW TABLET FROM example_db.table_name limit 10;
    ```

5. 从偏移 5 开始获取 10 个结果

    ```sql
        SHOW TABLET FROM example_db.table_name limit 5,10;
    ```

6. 按照 backendid/version/state 字段进行过滤

    ```sql
        SHOW TABLET FROM example_db.table_name where backendid=10000 and version=1 and state="NORMAL";
    ```

7. 按照 version 字段进行排序

    ```sql
        SHOW TABLET FROM example_db.table_name where backendid=10000 order by version;
    ```

8. 获取 index 名字为 t1_rollup 的 tablet 相关信息

    ```sql
        SHOW TABLET FROM example_db.table_name where indexname="t1_rollup";
    ```

## 关键字(keywords)

SHOW, TABLET, LIMIT
