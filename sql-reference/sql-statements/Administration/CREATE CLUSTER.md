# CREATE CLUSTER

## 功能

该语句用于新建逻辑集群 (cluster), 需要管理员权限。如果不使用多租户，直接创建一个名称为 default_cluster 的 cluster。否则创建一个自定义名称的 cluster。

## 语法

```sql
CREATE CLUSTER [IF NOT EXISTS] cluster_name

PROPERTIES ("key"="value", ...)

IDENTIFIED BY 'password'
```

注：方括号 [] 中内容可省略不写。

**PROPERTIES**

指定逻辑集群的属性

```sql
-- 设置instance_num 逻辑集群节点树为3
PROPERTIES ("instance_num" = "3")
```

**identified by ‘password'**

每个逻辑集群含有一个 superuser，创建逻辑集群时必须指定其密码

## 示例

1. 新建一个含有 3 个 be 节点逻辑集群 test_cluster, 并指定其 superuser 用户密码。

    ```sql
    CREATE CLUSTER test_cluster PROPERTIES("instance_num"="3") IDENTIFIED BY 'test';
    ```

2. 新建一个含有 3 个 be 节点逻辑集群 default_cluster(不使用多租户), 并指定其 superuser 用户密码。

    ```sql
    CREATE CLUSTER default_cluster PROPERTIES("instance_num"="3") IDENTIFIED BY 'test';
    ```

## 关键字(keywords)

CREATE，CLUSTER
