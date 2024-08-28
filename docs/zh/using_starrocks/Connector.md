---
displayed_sidebar: docs
---

# Connector

## 概念

### Catalog

StarRocks中的Catalog包括两种类型，分别为Internal Catalog与External Catalog. Catalog包含用户所有的Database. 当前StarRocks中存在一个默认的Internal Catalog实例，其默认值为`default_catalog`. 对于存储在StarRocks中的Database/Table/View 等，均在Internal Catalog下，如OlapTable和External Hive Table。对于External Catalog，是用户通过执行Create Catalog DDL语句所得。每个External Catalog下存在一个Connector来获取外部的数据源信息。当前版本仅支持Hive Connector. 用户可指定fully-qualified进行查询指定catalog的数据表。如通过指定`hive_catalog.hive_db.hive_table`来查询用户自定义的hive_catalog中的表。也可以通过指定`default_catalog.my_db.my_olap_table`来查询Olap表，同时也可对不同catalog间的数据进行联邦查询。

### Connector

StarRocks中的Connector为用户自定Catalog下对应的数据源连接器。通过Connector StarRocks可以获取运行期间需要的表信息，待扫描文件等信息。当前每个External Catalog对应一个Connector实例。Connector是在创建External Catalog过程中完成。用户可以根据需要来实现自定义的Connector.

## 使用方式

### 创建Catalog

#### 语法

```sql
CREATE EXTERNAL CATALOG <catalog_name> PROPERTIES ("key"="valuse", ...);
```

#### 示例

创建hive catalog

```sql
CREATE EXTERNAL CATALOG hive_catalog0 PROPERTIES("type"="hive", "hive.metastore.uris"="thrift://xx.xx.xx.xx:9083");
```

#### 说明

* 当前只支持创建hive external catalog
* PROPERTIES中的`type`为必填项，hive catalog中的值为`hive`

### 删除Catalog

#### 语法

```sql
DROP EXTERNAL CATALOG <catalog_name>
```

#### 示例

删除 hive catalog

```sql
DROP EXTERNAL CATALOG hive_catalog;
```

### 使用Catalog

当前Catalog分为两种，分别为Internal Catalog与External Catalog。用户可通过`SHOW CATALOGS`查看当前存在哪些Catalog。Internal Catalog在StarRocks中只存在一个默认实例，默认值为`default_catalog`。用户在从mysql client登录StarRocks之后，当前连接的默认Catalog为`default_catalog`，如果用户只使用Internal Catalog中的OLAP表功能，其使用方式与原有保持不变。用户可通过`show databases`查看Internal Catalog中有哪些databases。
对于External Catalog，用户可使用`SHOW DATABASES FROM <external_catalog_name>`的方式查看存在哪些Database，然后可通过`USE external_catalog.db`的方式切换当前连接的current_catalog和current_db. 当前只支持Use到DB级别，暂不支持Use到Catalog级别，后面会陆续开放该功能。

#### 示例

```sql
-- 在default_catalog下，use olap_db作为current database;
USE olap_db;

-- 在default_catalog.olap_db下查询olap_table;
SELECT * FROM olap_table limit 1;

-- 在default_catalog.olap_db下查询external catalog中的表，需要写external table的全名。
SELECT * FROM hive_catalog.hive_db.hive_tbl;

-- 切换current_catalog与current_database分别为hive_catalog和hive_db
USE hive_catalog.hive_db

-- 在hive_catalog.hive_db下查询hive table
SELECT * FROM hive_table limit 1;

-- 在hive_catalog.hive_db下查询Internal Catalog中的OLAP表
SELECT * FROM default_catalog.olap_db.olap_table;

-- 在hive_catalog.hive_db下与Internal Catalog中的OLAP表做联邦查询
SELECT * FROM hive_table h JOIN default_catalog.olap_db.olap_table o WHERE h.id = o.id;

-- 在其他hive catalog下对hive_catalog与Internal catalog的OLAP表做联邦查询
SELECT * FROM hive_catalog.hive_db.hive_tbl h JOIN default_catalog.olap_db.olap_table o WHERE h.id = o.id;
```

### Hive Connector元数据同步

当前 Hive Connector 对于用户 Hive Metastore 中记录的表结构以及分区文件信息在 FE 中进行了缓存，当前实现方式与刷新方式与 Hive 外表保持相同，请参见[Hive 外表](../data_source/External_table.md#手动更新元数据缓存)。
