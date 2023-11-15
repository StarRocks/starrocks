# ALTER RESOURCE

## 功能

修改资源的属性。仅 StarRocks 2.3 及以上版本支持修改资源属性。

## 语法

```SQL
ALTER RESOURCE 'resource_name' SET PROPERTIES ("key"="value", ...)
```

## 参数说明

- `resource_name`：待修改的资源名称。
- `PROPERTIES ("key"="value", ...)`：资源属性。不同类型的资源支持修改不同的属性，当前支持修改以下资源的 Hive metastore 地址。
  - Apache Iceberg 资源支持修改以下属性：
    - `iceberg.catalog-impl`：[custom catalog](/using_starrocks/External_table#步骤一创建--iceberg-资源) 的全限定类名。
    - `iceberg.catalog.hive.metastore.uris`：Hive metastore 地址。
  - Apache Hive™ 和 Apache Hudi 资源支持修改 `hive.metastore.uris`，即 Hive metastore 地址。

## 注意事项

引用一个资源创建外部表后，如果修改了该资源的 Hive metastore 地址会导致该外部表不可用。若仍想使用该外部表查询数据，需保证新 Hive metastore 中存在与原 Hive metastore 中名称和表结构相同的数据表。

## 示例

修改 Apache Hive™ 资源 `hive0` 的 Hive metastore 地址。

```SQL
ALTER RESOURCE 'hive0' SET PROPERTIES ("hive.metastore.uris" = "thrift://10.10.44.91:9083")
```
