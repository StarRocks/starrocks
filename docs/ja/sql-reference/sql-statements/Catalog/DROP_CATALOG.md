---
displayed_sidebar: docs
---

# DROP CATALOG

## 説明

external catalog を削除します。internal catalog は削除できません。StarRocks クラスターには `default_catalog` という名前の internal catalog が 1 つだけあります。

## 構文

```SQL
DROP CATALOG [IF EXISTS] <catalog_name>
```

## パラメーター

`catalog_name`: external catalog の名前。

## 例

`hive1` という名前の Hive catalog を作成します。

```SQL
CREATE EXTERNAL CATALOG hive1
PROPERTIES(
  "type"="hive", 
  "hive.metastore.uris"="thrift://xx.xx.xx.xx:9083"
);
```

Hive catalog を削除します。

```SQL
DROP CATALOG hive1;
```