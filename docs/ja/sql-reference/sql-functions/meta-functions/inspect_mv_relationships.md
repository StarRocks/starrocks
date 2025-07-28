---
displayed_sidebar: docs
---

# inspect_mv_relationships

`inspect_mv_relationships()`

この関数は、`ConnectorTblMetaInfoMgr` 内の内容を返します。これには、ベース外部テーブルからマテリアライズドビューへのマッピング情報が含まれます。

## 引数

なし。

## 戻り値

マッピング情報を含む JSON 形式の VARCHAR 文字列を返します。

## 例

例1: ConnectorTblMetaInfoMgr 内の現在の mv 関係を検査する:
```
mysql> select inspect_mv_relationships();
+----------------------------+
| inspect_mv_relationships() |
+----------------------------+
| {}                         |
+----------------------------+
1 row in set (0.01 sec)

```

