---
title: CURRENT_WAREHOUSE
---

現在のセッションで有効なウェアハウス名を返します。`SET WAREHOUSE` で明示的に設定したウェアハウス、または設定されていない場合はデフォルトのウェアハウスが返されます。

## 構文

```SQL
current_warehouse()
```

## 戻り値

`VARCHAR`。セッションで使用中のウェアハウス名。

## 例

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

