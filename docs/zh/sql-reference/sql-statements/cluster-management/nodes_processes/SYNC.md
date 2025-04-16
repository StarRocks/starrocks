---
displayed_sidebar: docs
---

# SYNC

同步不同 Session 之间的数据一致性。

目前，StarRocks 只能保证同一 Session 内的一致性，也就是说，只有发起数据写入操作的 Session 才能在操作成功后立即读取最新数据。如果想立即从其他 Session 读取最新数据，必须使用 SYNC 语句同步数据一致性。如果不执行该语句，Session 之间通常会有毫秒级的延迟。

:::tip

该操作不需要权限。

:::

## 语法

```SQL
SYNC
```

## 示例

```Plain
mysql> SYNC;
Query OK, 0 rows affected (0.01 sec)
```
