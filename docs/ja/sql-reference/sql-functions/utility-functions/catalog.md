---
displayed_sidebar: docs
---

# catalog

## 説明

現在の catalog の名前を返します。catalog は StarRocks の内部 catalog または外部データソースにマッピングされた external catalog である可能性があります。catalog についての詳細は、 [Catalog overview](../../../data_source/catalog/catalog_overview.md) を参照してください。

catalog が選択されていない場合、StarRocks の内部 catalog `default_catalog` が返されます。

## 構文

```Haskell
catalog()
```

## パラメータ

この関数はパラメータを必要としません。

## 戻り値

現在の catalog の名前を文字列として返します。

## 例

例 1: 現在の catalog は StarRocks の内部 catalog `default_catalog` です。

```plaintext
select catalog();
+-----------------+
| CATALOG()       |
+-----------------+
| default_catalog |
+-----------------+
1 row in set (0.01 sec)
```

例 2: 現在の catalog は external catalog `hudi_catalog` です。

```sql
-- 外部 catalog に切り替えます。
set catalog hudi_catalog;

-- 現在の catalog の名前を返します。
select catalog();
+--------------+
| CATALOG()    |
+--------------+
| hudi_catalog |
+--------------+
```

## 関連項目

[SET CATALOG](../../sql-statements/Catalog/SET_CATALOG.md): 目的の catalog に切り替えます。