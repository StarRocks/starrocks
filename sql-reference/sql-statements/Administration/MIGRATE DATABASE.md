# MIGRATE DATABASE

## 功能

（已废弃！！！）
该语句用于迁移一个逻辑集群的数据库到另外一个逻辑集群，执行此操作前数据库必须已经处于链接状态, 需要管理员权限

## 语法

```sql
MIGRATE DATABASE src_cluster_name.src_db_name des_cluster_name.des_db_name
```

## 示例

1. 迁移test_clusterA中的test_db到test_clusterB

    ```sql
    MIGRATE DATABASE test_clusterA.test_db test_clusterB.link_test_db;
    ```
