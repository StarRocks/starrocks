---
displayed_sidebar: docs
---

# ALTER RESOURCE

## 説明

ALTER RESOURCE ステートメントを使用して、リソースのプロパティを変更できます。

## 構文

```SQL
ALTER RESOURCE 'resource_name' SET PROPERTIES ("key"="value", ...)
```

## パラメーター

- `resource_name`: 変更するリソースの名前。

- `PROPERTIES ("key"="value", ...)`: リソースのプロパティ。リソースの種類に応じて異なるプロパティを変更できます。現在、StarRocks は以下のリソースの Hive metastore の URI の変更をサポートしています。
  - Apache Iceberg リソースは、以下のプロパティの変更をサポートしています:
    - `iceberg.catalog-impl`: [custom catalog](../../../data_source/External_table.md) の完全修飾クラス名。
    - `iceberg.catalog.hive.metastore.uris`: Hive metastore の URI。
  - Apache Hive™ リソースと Apache Hudi リソースは、Hive metastore の URI を示す `hive.metastore.uris` の変更をサポートしています。

## 使用上の注意

リソースを参照して外部テーブルを作成した後、このリソースの Hive metastore の URI を変更すると、外部テーブルは使用できなくなります。外部テーブルを使用してデータをクエリしたい場合は、新しい metastore に元の metastore と同じ名前とスキーマを持つテーブルが含まれていることを確認してください。

## 例

Hive リソース `hive0` の Hive metastore の URI を変更します。

```SQL
ALTER RESOURCE 'hive0' SET PROPERTIES ("hive.metastore.uris" = "thrift://xx.xx.xx.xx:9083")
```