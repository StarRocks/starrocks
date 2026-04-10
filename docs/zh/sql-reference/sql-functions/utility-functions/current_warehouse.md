---
title: CURRENT_WAREHOUSE
---

返回当前会话正在使用的计算仓库名称。该仓库可能通过 `SET WAREHOUSE` 显式设置，也可能是系统默认仓库。

## 语法

```SQL
current_warehouse()
```

## 返回值

`VARCHAR`。当前会话生效的仓库名称。

## 示例

```SQL
mysql> SET WAREHOUSE = 'compute_pool_x';
mysql> SELECT current_warehouse();
+---------------------+
| CURRENT_WAREHOUSE() |
+---------------------+
| compute_pool_x      |
+---------------------+
1 row in set (0.00 sec)
```

