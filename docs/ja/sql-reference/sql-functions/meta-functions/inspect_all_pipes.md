---
displayed_sidebar: docs
---

# inspect_all_pipes

`inspect_all_pipes()`

この関数は、現在のデータベース内のすべてのパイプのメタデータを返します。

## 引数

なし。

## 戻り値

すべてのパイプのメタデータを含む JSON 形式の VARCHAR 文字列を返します。

## 例

例 1: 現在のすべてのパイプを取得する

```
mysql> select inspect_all_pipes();
+---------------------+
| inspect_all_pipes() |
+---------------------+
| []                  |
+---------------------+
1 row in set (0.01 sec)
```

