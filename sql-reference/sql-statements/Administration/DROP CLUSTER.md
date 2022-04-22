# DROP CLUSTER

## 功能

该语句用于删除逻辑集群，成功删除逻辑集群需要首先删除集群内的 db，需要管理员权限。

## 语法

```sql
DROP CLUSTER [IF EXISTS] cluster_name;
```

注：方括号 [] 中内容可省略不写。

## 示例

删除逻辑集群 test_cluster。

```sql
DROP CLUSTER test_cluster;
```

## 关键字(keywords)

DROP，CLUSTER
