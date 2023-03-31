# SET CATALOG

设置当前目录。设置当前目录后，StarRocks 会从当前目录解析 SQL 语句所引用的表、函数和视图等的部分或非限定标识符。

设置目录还会将当前表结构重置为 `default`。

## 语法

```SQL
SET CATALOG [ catalog_name | 'catalog_name' ]
```

## 参数

`catalog_name`：要使用的目录的名称。 如果该目录不存在，则会引发异常。

## 示例

通过如下命令之一，设置 Hive Catalog `hive_metastore` 为当前要使用的目录：

```SQL
SET CATALOG hive_metastore;
```

或者

```SQL
SET CATALOG 'hive_metastore';
```
