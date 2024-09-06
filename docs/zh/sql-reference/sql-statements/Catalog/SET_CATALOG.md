---
displayed_sidebar: docs
---

# SET CATALOG

## 功能

切换到指定的 Catalog。该命令自 3.0 版本起支持。

> **注意**
>
> 如果是新部署的 3.1 集群，用户需要有目标 Catalog 的 USAGE 权限才可以执行该操作。如果是从低版本升级上来的集群，则不需要重新赋权。您可以使用 [GRANT](../account-management/GRANT.md) 命令进行授权操作。

## 语法

```SQL
SET CATALOG <catalog_name>
```

## 参数

`catalog_name`：当前会话里生效的 Catalog，支持 Internal Catalog 和 External Catalog。如果指定的 Catalog 不存在，则会引发异常。

## 示例

通过如下命令，切换当前会话里生效的 Catalog 为 Hive Catalog `hive_metastore`：

```SQL
SET CATALOG hive_metastore;
```

通过如下命令，切换当前会话里生效的 Catalog 为 Internal Catalog `default_catalog`：

```SQL
SET CATALOG default_catalog;
```
