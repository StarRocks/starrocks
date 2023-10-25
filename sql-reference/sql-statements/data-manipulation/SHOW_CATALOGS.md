# SHOW CATALOGS

## 功能

查看当前集群中的所有 catalog，包括 internal catalog 和 external catalog。

## 语法

```SQL
SHOW CATALOGS
```

## 返回结果说明

```SQL
+----------+--------+----------+
| Catalog  | Type   | Comment  |
+----------+--------+----------+
```

返回结果中的参数说明如下：

| **参数** | **说明**                                                     |
| -------- | ------------------------------------------------------------ |
| Catalog  | Catalog 名称。                                               |
| Type     | Catalog 类型。                                               |
| Comment  | Catalog 的备注。<ul><li>在创建 external catalog 时不支持为 external catalog 添加备注，所以如果是 external catalog，则`Comment`取值为`NULL`。</li><li>如果是`default_catalog`，则`Comment`取值为`Internal Catalog`。`default_catalog`是一个 StarRocks 集群中唯一的 internal catalog。</li></ul> |

## 示例

查看当前集群中的所有 catalog。

```SQL
SHOW CATALOGS;

+---------------------------------------------------+----------+------------------+
| Catalog                                           | Type     | Comment          |
+---------------------------------------------------+----------+------------------+
| default_catalog                                   | Internal | Internal Catalog |
| hive_catalog_1acaac1c_19e0_11ed_9ca4_00163e0e550b | hive     | NULL             |
| hudi_catalog_dfd748ce_18a2_11ed_9f50_00163e0e550b | hudi     | NULL             |
| iceberg_catalog                                   | iceberg  | NULL             |
+---------------------------------------------------+----------+------------------+
```
