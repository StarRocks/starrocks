---
displayed_sidebar: docs
keywords: ['fenqu']
---

# SHOW DYNAMIC PARTITION TABLES

## 功能

该语句用于展示当前数据库 db 下所有设置过动态分区属性的分区表状态。

## 语法

```sql
SHOW DYNAMIC PARTITION TABLES [FROM <db_name>]
```

## 示例

1. 展示数据库 database 的所有设置过动态分区属性的分区表状态。

```sql
SHOW DYNAMIC PARTITION TABLES FROM database;
```
