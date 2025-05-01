---
displayed_sidebar: docs
---

# SET CATALOG

現在のセッションで指定された catalog に切り替えます。

このコマンドは v3.0 以降でサポートされています。

> **NOTE**
>
> 新しくデプロイされた StarRocks v3.1 クラスターでは、SET CATALOG を実行してその catalog に切り替える場合、目的の external catalog に対する USAGE 権限が必要です。必要な権限を付与するには [GRANT](../account-management/GRANT.md) を使用できます。以前のバージョンからアップグレードされた v3.1 クラスターでは、継承された権限で SET CATALOG を実行できます。

## Syntax

```SQL
SET CATALOG <catalog_name>
```

## Parameter

`catalog_name`: 現在のセッションで使用する catalog の名前です。内部または external catalog に切り替えることができます。指定した catalog が存在しない場合、例外がスローされます。

## Examples

現在のセッションで `hive_metastore` という名前の Hive catalog に切り替えるには、次のコマンドを実行します。

```SQL
SET CATALOG hive_metastore;
```

現在のセッションで内部 catalog `default_catalog` に切り替えるには、次のコマンドを実行します。

```SQL
SET CATALOG default_catalog;
```