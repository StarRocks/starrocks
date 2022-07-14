# CREATE RESOURCE GROUP

## 功能

该语句用于创建资源组。

## 语法

```sql
CREAE RESOURCE GROUP <name> 
TO CLASSIFIER[,...]
WITH ("key" = "value", ...);
```

说明：

|参数|描述|
|----|----|
|cpu_core_limit|资源组所分配的 CPU 核数（按比例[弹性伸缩](../../../administration/Resource_Group.md#基本概念)）|
|mem_limit|为该资源组指定的内存百分比上限|
|type|资源组的类型，固定取值为 `normal`|

## 示例

依据多个 CLASSIFIER 创建 `type` 为 `normal` 的资源组 `rg1`，并为其分配 10 核 CPU 以及 20% 的内存资源。

```sql
CREATE RESOURCE GROUP rg1
to 
    (user='rg1_user1', role='rg1_role1', query_type in ('select'), source_ip='192.168.2.1/24'),
    (user='rg1_user2', query_type in ('select'), source_ip='192.168.3.1/24'),
    (user='rg1_user3', source_ip='192.168.4.1/24'),
    (user='rg1_user4')
with (
    'cpu_core_limit' = '10',
    'mem_limit' = '20%',
    'type' = 'normal'
);
```
