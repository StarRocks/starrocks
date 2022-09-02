# SHOW DATABASES

## 功能

查看当前 StarRocks 集群或外部数据源中的数据库。

## 语法

- 查看当前 StarRocks 集群中的数据库。

    ```SQL
    SHOW DATABASES;
    ```

- 查看指定 catalog 中的数据库。

    ```SQL
    SHOW DATABASES FROM catalog_name;
    ```

## 参数说明

`catalog_name`：internal catalog 或 external catalog 的名称。

- 如指定 internal catalog 名称，即 `default_catalog`，则查看当前 StarRocks 集群中的数据库。
- 如指定 external catalog 名称，则查看外部数据源中的数据库。

## 示例

示例一：查看当前 StarRocks 集群中的数据库。

```SQL
SHOW DATABASES;

+----------+
| Database |
+----------+
| db1      |
| db2      |
| db3      |
+----------+
```

或

```SQL
SHOW DATABASES FROM default_catalog;
```

示例二：通过 external catalog `hive1`查看 Apache Hive™ 中的数据库。

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
