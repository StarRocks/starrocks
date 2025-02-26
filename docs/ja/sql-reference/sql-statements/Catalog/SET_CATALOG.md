---
displayed_sidebar: docs
---

# SET CATALOG

現在のセッションで指定された catalog に切り替えます。

このコマンドは v3.0 以降でサポートされています。

> **注意**
>
> 新しくデプロイされた StarRocks v3.1 クラスターでは、目的の external catalog に切り替えるために SET CATALOG を実行する場合、その catalog に対する USAGE 権限が必要です。[GRANT](../account-management/GRANT.md) を使用して必要な権限を付与できます。以前のバージョンからアップグレードされた v3.1 クラスターでは、継承された権限で SET CATALOG を実行できます。

## 構文

```SQL
SET CATALOG <catalog_name>
```

## パラメータ

`catalog_name`: 現在のセッションで使用する catalog の名前です。内部または external catalog に切り替えることができます。指定した catalog が存在しない場合、例外がスローされます。

## 例

現在のセッションで `hive_metastore` という名前の Hive catalog に切り替えるには、次のコマンドを実行します。

```SQL
SET CATALOG hive_metastore;
```

現在のセッションで内部 catalog `default_catalog` に切り替えるには、次のコマンドを実行します。

```SQL
SET CATALOG default_catalog;
```