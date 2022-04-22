# SHOW PROPERTY

## 功能

该语句用于查看用户的属性。

语法：

```sql
SHOW PROPERTY [FOR user] [LIKE key];
```

## 示例

1. 查看 jack 用户的属性。

    ```sql
    SHOW PROPERTY FOR 'jack';
    +------------------------+-------+
    | Key                    | Value |
    +------------------------+-------+
    | default_load_cluster   |       |
    | max_user_connections   | 100   |
    | quota.high             | 800   |
    | quota.low              | 100   |
    | quota.normal           | 400   |
    | resource.cpu_share     | 1000  |
    | resource.hdd_read_iops | 80    |
    | resource.hdd_read_mbps | 30    |
    | resource.io_share      | 1000  |
    | resource.ssd_read_iops | 1000  |
    | resource.ssd_read_mbps | 30    |
    +------------------------+-------+
    ```

2. 查看 jack 用户导入 cluster 相关属性。

    ```sql
    SHOW PROPERTY FOR 'jack' LIKE '%load_cluster%';
    ```

## 关键字(keywords)

SHOW, PROPERTY
