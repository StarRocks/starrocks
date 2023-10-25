# 使用 catalog 管理内部和外部数据

Catalog（数据目录）用于管理数据。StarRocks 2.3 及以上版本提供以下两种数据目录：

- **Internal catalog**：内部数据目录，用于存储 StarRocks 内部所有的数据库和数据表。您可以使用内部数据目录管理内部数据。例如，执行 CREATE DATABASE 和 CREATE TABLE 语句创建的数据库和数据表都会存储在内部数据目录中。 每个 StarRocks 集群都有一个默认的内部数据目录 `default_catalog`**。**StarRocks 暂不支持修改默认的内部数据目录的名称，也不支持创建新的内部数据目录。

- **External catalog**：外部数据目录，用于管理外部数据源中的数据。创建外部数据目录时需指定外部数据源访问信息。创建后，无需创建外部表即可查询外部数据。

## 节点配置

如果要访问的 Apache Hadoop® 集群开启了 Kerberos 认证，那么您需要在每一个 FE 的配置文件路径 **$FE_HOME/conf** 和每一个 BE 的配置文件路径 **$BE_HOME/conf** 下添加 Hadoop 集群的配置文件。具体操作步骤如下：

1. 在 JDK 环境中为 BE 所在的机器配置 `JAVA_HOME` 环境变量。注意不能在 JRE 环境中配置该变量。
2. 在所有 FE 和 BE 机器上执行 `kinit -kt keytab_path principal` 命令登录。注意使用该命令登录是有实效性的，所以需要将该命令放入 crontab 中定期执行。登录用户需要有访问 Hive 集群和 HDFS 集群的权限。
3. 把 Hadoop 集群中的 **hive-site.xml**、**core-site.xml** 和 **hdfs-site.xml** 文件放到每个 FE 的 **$FE_HOME/conf** 下，再把 **core-site.xml** 和 **hdfs-site.xml** 文件放到每个 BE 的 **$BE_HOME/conf** 下。
4. 在每个 **$BE_HOME/conf/be.conf** 和每个 **$FE_HOME/conf/fe.conf** 文件中设置`JAVA_OPTS="-Djava.security.krb5.conf=/etc/krb5.conf"`和`JAVA_OPTS_FOR_JDK_9="-Djava.security.krb5.conf=/etc/krb5.conf"`，其中 `/etc/krb5.conf` 是 **krb5.conf** 文件的路径。
5. 将 Hive 节点域名和 IP 的映射关系，以及 HDFS 节点域名和 IP 的映射关系配置到 **/etc/hosts** 路径中。

## 创建 external catalog

### 语法

```SQL
CREATE EXTERNAL CATALOG <catalog_name> PROPERTIES ("key"="value", ...);
```

查询的数据源不同，创建 external catalog 时在`PROPERTIES`中添加的配置项也不同。StarRocks 2.3 版本仅支持为 Apache Hive™ 创建 external catalog。目录创建后，无需创建外部表即可在 StarRocks 中查询 Hive 集群中所有的数据。此外，StarRocks 仅支持读取 external catalog 中的表，不支持向表中写入数据。

### 示例

创建名为 `hive_catalog0` 的 external catalog。

```SQL
CREATE EXTERNAL CATALOG hive_catalog0 
PROPERTIES(
  "type"="hive", 
  "hive.metastore.uris"="thrift://127.0.0.1:9083"
);
```

参数说明：

| **参数**            | **描述**                                                     |
| ------------------- | ------------------------------------------------------------ |
| type                | 数据源类型，取值为 `hive`。                                  |
| hive.metastore.uris | Hive metastore 的 URI。格式为 `thrift://<Hive元数据的IP地址>:<端口号>`，端口号默认为 9083。|

## 查询数据

### 查询内部数据

1. 从 MySQL 客户端登录到 StarRocks。登录后会默认连接到 `default_catalog`。

2. 执行 `show databases` 或 `show databases from default_catalog` 查看当前集群中的所有内部数据库。

3. 指定库名 (`database_name`) 和表名 (`table_name`) 查询 `default_catalog` 中的数据。

### 查询外部数据

1. 从 MySQL 客户端登录到 StarRocks。登录后会默认连接到 `default_catalog`。

2. 执行 `show catalogs` 查看所有 catalog 并找到指定的 external catalog，然后执行 `show databases from external_catalog` 查看指定 external catalog 中的数据库。例如，要查看 `hive_catalog` 中的数据库，执行 `show databases from hive_catalog`。

3. 执行 `use external_catalog.database` 将当前会话切换到指定的 external catalog 下的指定数据库。

4. 指定表名 (`table_name`) 查询当前 external catalog 中的数据。

### 跨 catalog 查询数据

如想在一个 catalog 中查询其他 catalog 中数据，可通过 `catalog_name.database_name` 或`catalog_name.database_name.table_name` 的格式来引用目标数据。

### 示例

#### 查询内部数据

例如，查询`olap_db.olap_table`中的数据，操作如下：

1. 从 MySQL 客户端登录到 StarRocks 后查看当前集群中的所有内部数据库。

    ```SQL
    show databases;
    ```

    或

    ```SQL
    SHOW DATABASES FROM default_catalog;
    ```

2. 使用 `olap_db` 作为当前数据库。

    ```SQL
    USE olap_db;
    ```

    或

    ```SQL
    use default_catalog.olap_db;
    ```

3. 查询`olap_table`表中的数据。

    ```SQL
    SELECT * FROM olap_table limit 1;
    ```

#### 查询外部数据

例如，查询`hive_catalog.hive_db.hive_table`中的数据，操作如下：

1. 从 MySQL 客户端登录到 StarRocks，并查看当前集群中的所有 catalog。

    ```SQL
    show catalogs;
    ```

2. 查看 `hive_catalog` 中的数据库。

    ```SQL
    show databases from hive_catalog;
    ```

3. 将当前会话切换到`hive_catalog.hive_db`。

    ```SQL
    USE hive_catalog.hive_db;
    ```

4. 查询`hive_tabl`表中的数据。

    ```SQL
    SELECT * FROM hive_table limit 1;
    ```

#### 跨 catalog 查询数据

- 在`default_catalog.olap_db`下查询 `hive_catalog` 中的`hive_table`。

    ```SQL
    SELECT * FROM hive_catalog.hive_db.hive_table;
    ```

- 在`hive_catalog.hive_db`下查询 `default_catalog` 中的`olap_table`。

    ```SQL
    SELECT * FROM default_catalog.olap_db.olap_table;
    ```

- 在`hive_catalog.hive_db`中，对`hive_table`和 `default_catalog` 中的`olap_table`进行联邦查询。

    ```SQL
    SELECT * FROM hive_table h JOIN default_catalog.olap_db.olap_table o WHERE h.id = o.id;
    ```

- 在其他目录下，对`hive_catalog`中的`hive_table`和`default_catalog`中的`olap_table`进行联邦查询。

    ```SQL
    SELECT * FROM hive_catalog.hive_db.hive_table h JOIN default_catalog.olap_db.olap_table o WHERE h.id = o.id;
    ```

## 删除 external catalog

### 语法

```SQL
DROP CATALOG <catalog_name>;
```

### 示例

删除名为 `hive_catalog` 的 external catalog。

```SQL
DROP CATALOG hive_catalog;
```

## 更新 external catalog 中表的元数据

External catalog 中表的元数据缓存在 FE 中，因此表结构和分区文件等元数据的刷新机制和[缓存更新](../data_source/External_table.md#更新缓存的-hive-表元数据)相同。您可执行 `refresh external table catalog.db.table` 来进行刷新。
