# SET CATALOG

切换当前会话里生效的 Catalog。

## 语法

```SQL
SET CATALOG <catalog_name>
```

## 参数

`catalog_name`：当前会话里生效的 Catalog，支持 Internal Catalog 和 External Catalog。 如果指定的目录不存在，则会引发异常。

## 示例

通过如下命令，切换当前会话里生效的 Catalog 为 Hive Catalog `hive_metastore`：

```SQL
SET CATALOG hive_metastore;
```

通过如下命令，切换当前会话里生效的 Catalog 为 Internal Catalog `default_catalog`：

```SQL
SET CATALOG default_catalog;
```
