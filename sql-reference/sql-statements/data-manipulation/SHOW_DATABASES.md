# SHOW DATABASES

## 功能

查看当前 StarRocks 集群或外部数据源中的数据库。

## 语法

```SQL
SHOW DATABASES [FROM <catalog_name>]
```

## 参数说明

| **参数**          | **必选** | **说明**                                                     |
| ----------------- | -------- | ------------------------------------------------------------ |
| catalog_name | 否       | Internal catalog 或 external catalog 的名称。<ul><li>如不指定或指定为 internal catalog 名称，即 `default_catalog`，则查看当前 StarRocks 集群中的数据库。</li><li>如指定 external catalog 名称，则查看外部数据源中的数据库。</li></ul> |

## 示例

示例一：查看当前 StarRocks 集群中的数据库。

```SQL
SHOW DATABASES;
```

或

```SQL
SHOW DATABASES FROM default_catalog;
```

返回信息如下：

```SQL
+----------+
| Database |
+----------+
| db1      |
| db2      |
| db3      |
+----------+
```

示例二：通过 external catalog `hive1` 查看 Apache Hive™ 中的数据库。

```SQL
SHOW DATABASES FROM hive1;

+-----------+
| Database  |
+-----------+
| hive_db1  |
| hive_db2  |
| hive_db3  |
+-----------+
```

## 参考文档

- [CREATE DATABASE](../data-definition/CREATE_DATABASE.md)
- [SHOW CREATE DATABASE](SHOW_CREATE_DATABASE.md)
- [USE](../data-definition/USE.md)
- [DESC](../Utility/DESCRIBE.md)
- [DROP DATABASE](../data-definition/DROP_DATABASE.md)