---
displayed_sidebar: docs
---

# ALTER RESOURCE

## 説明

ALTER RESOURCE 文を使用して、リソースのプロパティを変更できます。

## 構文

```SQL
ALTER RESOURCE 'resource_name' SET PROPERTIES ("key"="value", ...)
```

## パラメータ

- `resource_name`: 変更するリソースの名前。

- `PROPERTIES ("key"="value", ...)`: リソースのプロパティ。リソースの種類に応じて異なるプロパティを変更できます。現在、StarRocks は以下のリソースの Hive メタストアの URI を変更することをサポートしています。
  - Apache Iceberg リソースは、以下のプロパティの変更をサポートしています:
    - `iceberg.catalog-impl`: [custom catalog](../../../data_source/External_table.md) の完全修飾クラス名。
    - `iceberg.catalog.hive.metastore.uris`: Hive メタストアの URI。
  - Apache Hive™ リソースおよび Apache Hudi リソースは、Hive メタストアの URI を示す `hive.metastore.uris` の変更をサポートしています。

## 使用上の注意

リソースを参照して外部テーブルを作成した後、このリソースの Hive メタストアの URI を変更すると、外部テーブルは使用できなくなります。外部テーブルを使用してデータをクエリしたい場合は、新しいメタストアに元のメタストアと同じ名前とスキーマのテーブルが含まれていることを確認してください。

## 例

Hive リソース `hive0` の Hive メタストアの URI を変更します。

```SQL
ALTER RESOURCE 'hive0' SET PROPERTIES ("hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083")
```