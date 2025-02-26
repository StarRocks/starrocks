---
displayed_sidebar: docs
---

# catalog

現在の catalog の名前を返します。catalog は StarRocks の内部 catalog か、外部データソースにマッピングされた external catalog である可能性があります。catalog についての詳細は、[Catalog overview](../../../data_source/catalog/catalog_overview.md) を参照してください。

catalog が選択されていない場合、StarRocks の内部 catalog `default_catalog` が返されます。

## Syntax

```Haskell
catalog()
```

## Parameters

この関数はパラメータを必要としません。

## Return value

現在の catalog の名前を文字列として返します。

## Examples

Example 1: 現在の catalog は StarRocks の内部 catalog `default_catalog` です。

```plaintext
select catalog();
+-----------------+
| CATALOG()       |
+-----------------+
| default_catalog |
+-----------------+
1 row in set (0.01 sec)
```

Example 2: 現在の catalog は external catalog `hudi_catalog` です。

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

## See also

[SET CATALOG](../../sql-statements/Catalog/SET_CATALOG.md): 目的の catalog に切り替えます。