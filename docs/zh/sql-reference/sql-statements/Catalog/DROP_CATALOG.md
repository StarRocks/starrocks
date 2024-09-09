---
displayed_sidebar: docs
---

# DROP CATALOG

## 功能

删除指定的 external catalog。注意目前不支持删除 internal catalog。一个 StarRocks 集群中只有一个默认的 internal catalog，名为 `default_catalog`。

## 语法

```SQL
DROP CATALOG [IF EXISTS] catalog_name
```

## 参数说明

`catalog_name`：external catalog 名称，必选参数。

## 示例

创建名为`hive1`的 Hive catalog。

```SQL
CREATE EXTERNAL CATALOG hive1
PROPERTIES(
  "type"="hive", 
  "hive.metastore.uris"="thrift://xx.xx.xx.xx:9083"
);
```

删除`hive1`。

```SQL
DROP CATALOG hive1;
```
