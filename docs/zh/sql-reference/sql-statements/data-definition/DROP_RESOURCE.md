# DROP RESOURCE

## 功能

该语句用于删除一个已有的资源。需要拥有资源的 DROP 权限才可以删除资源。

创建 RESOURCE 操作请参考 [CREATE RESOURCE](../data-definition/CREATE_RESOURCE.md) 章节。

## 语法

```sql
DROP RESOURCE 'resource_name'
```

## 示例

1. 删除名为 spark0 的 Spark 资源。

    ```SQL
    DROP RESOURCE 'spark0';
    ```

2. 删除名为 hive0 的 Hive 资源。

    ```SQL
    DROP RESOURCE 'hive0';
    ```
