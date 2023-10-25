# DROP RESOURCE

## description

该语句用于删除一个已有的资源。仅 root 或 admin 用户可以删除资源。

语法：

```sql
DROP RESOURCE 'resource_name'
```

## example

1. 删除名为 spark0 的 Spark 资源：

    ```SQL
    DROP RESOURCE 'spark0';
    ```

2. 删除名为 hive0 的 Hive 资源：

    ```SQL
    DROP RESOURCE 'hive0';
    ```

## keyword

DROP RESOURCE
