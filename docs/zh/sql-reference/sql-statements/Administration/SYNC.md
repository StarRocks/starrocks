---
displayed_sidebar: "Chinese"
---

# SYNC

## 功能

当前StarRocks并不是强一致性的系统，只能保证session一致性，即：在同一个mysql session中发起的数据写入操作，下一时刻能读取到最新的数据。如果需要其他的session也能读取到最新的数据，在查询数据之前，先发送一个sync命令。

:::tip

该操作不需要权限。

:::

## 语法

```SQL
sync
```

## 示例

```Plain
mysql> sync;
Query OK, 0 rows affected (0.01 sec)
```
