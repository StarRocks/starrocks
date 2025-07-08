---
displayed_sidebar: docs
---

# inspect_table_partition_info

`inspect_table_partition_info(table_name)`

この関数は、テーブルのパーティション情報を返します。

## 引数

`table_name`: テーブルの名前 (VARCHAR)。

## 戻り値

テーブルのパーティション情報を含む JSON 形式の VARCHAR 文字列を返します。

## 例

例1: テーブルのパーティション情報を検査する:
```
mysql> select inspect_table_partition_info('ss');
+-----------------------------------------------------------------------------------------------------------+
| inspect_table_partition_info('ss')                                                                        |
+-----------------------------------------------------------------------------------------------------------+
| {"ss":{"id":28672,"version":4,"lastRefreshTime":1751439875145,"lastFileModifiedTime":-1,"fileNumber":-1}} |
+-----------------------------------------------------------------------------------------------------------+
1 row in set (0.00 sec)
