---
displayed_sidebar: docs
---

# SHOW CREATE CATALOG

## 功能

查看某个 External Catalog 的创建语句。

当前支持查看如下 External Catalog:

- [Hive Catalog](../../../data_source/catalog/hive_catalog.md)
- [Iceberg Catalog](../../../data_source/catalog/iceberg/iceberg_catalog.md)
- [Hudi Catalog](../../../data_source/catalog/hudi_catalog.md)
- [Delta Lake Catalog](../../../data_source/catalog/deltalake_catalog.md)
- [JDBC catalog](../../../data_source/catalog/jdbc_catalog.md) (自 3.0 版本起)
- [Unified Catalog](../../../data_source/catalog/unified_catalog.md) (自 3.2 版本起)
- [MaxCompute Catalog](../../../data_source/catalog/maxcompute_catalog.md) (自 3.3 版本起)

其中认证相关的密钥信息会进行脱敏展示，无法查看。

该命令自 3.0 版本起支持。

## 语法

```SQL
SHOW CREATE CATALOG <catalog_name>;
```

## 参数说明

| **参数**     | **是否必选** | **说明**                  |
| ------------ | ------------ | ------------------------- |
| catalog_name | 是           | 待查看的 Catalog 的名称。 |

## 返回结果说明

```Plain
+------------+-----------------+
| Catalog    | Create Catalog  |
+------------+-----------------+
```

| **字段**       | **说明**             |
| -------------- | -------------------- |
| Catalog        | Catalog 的名称。     |
| Create Catalog | Catalog 的创建语句。 |

## 示例

以一个名为 `hive_catalog_glue` 的 Hive Catalog 为例，查询该 Catalog 的创建语句：

```SQL
SHOW CREATE CATALOG hive_catalog_glue;
```

返回如下信息：

```SQL
CREATE EXTERNAL CATALOG `hive_catalog_hms`
PROPERTIES ("aws.s3.access_key"  =  "AK******M4",
"hive.metastore.type"  =  "glue",
"aws.s3.secret_key"  =  "iV******iD",
"aws.glue.secret_key"  =  "iV******iD",
"aws.s3.use_instance_profile"  =  "false",
"aws.s3.region"  =  "us-west-1",
"aws.glue.region"  =  "us-west-1",
"type"  =  "hive",
"aws.glue.access_key"  =  "AK******M4"
)
```
