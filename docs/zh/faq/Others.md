---
displayed_sidebar: "Chinese"
---

# 其他

本文汇总了使用 StarRocks 时的其他常见问题。

## VARCHAR(32) 和 STRING 占用的存储空间相同吗？

VARCHAR(32) 和 STRING 都是变长数据类型。当储存相同长度的数据时，VARCHAR(32) 和 STRING 占用的储存空间相同。

## 查询时，VARCHAR(32) 和 STRING 的性能相同吗？

相同。

## Oracle 导出的 TXT 文件，在将其字符集修改成 UTF-8 后仍然乱码，如何处理？

将文件字符集视为 GBK 进行字符集转换，步骤如下：

1. 例如，一个名为 **origin** 的文件乱码，用以下命令查看到其字符集为 ISO-8859-1。

    ```Plain_Text
    file --mime-encoding origin.txt
    origin.txt：iso-8859-1
    ```

2. 使用`iconv`命令将文件的字符集转换为 UTF-8。

    ```Plain_Text
    iconv -f iso-8859-1 -t utf-8 origin.txt > origin_utf-8.txt
    ```

3. 若转换后得到的文件还存在乱码，那么将文件的字符集视为 GBK，再转换成 UTF-8。

    ```Shell
    iconv -f gbk -t utf-8 origin.txt > origin_utf-8.txt
    ```

## MySQL 中定义的字符串长度跟 StarRocks 定义的是一致的吗？

在 StarRocks 中，VARCHAR(n) 中的n代表字节数，而在 MySQL 中，VARCHAR(n) 中的 n 代表字符数。根据 UTF-8，1 个汉字等于 3 个字节。当 StarRocks 和 MySQL 将 n 定义成相同数字时，MySQL 保存的字符数是 StarRocks 的 3 倍。

## 表的分区字段可以是 FLOAT、DOUBLE、或 DECIMAL 数据类型吗？

不可以，仅支持 DATE， DATETIME 和 INT 数据类型。

## 如何查看表中的数据占了多大的存储？

执行 SHOW DATA 语句查看数据所占存储空间以及数据量、副本数量和行数。

> 说明：数据导入非实时更新，在导入后 1 分钟左右可以查看到最新的数据。

## 如何调整 StarRocks 数据库配额 (quota)?

运行如下代码调整数据库配额：

```SQL
ALTER DATABASE example_db SET DATA QUOTA 10T;
```

## StarRocks 支持通过 UPSERT 语法实现部分字段更新吗？

StarRocks 2.2 及以上版本可以通过主键 (Primary Key) 模型实现部分字段更新。StarRocks 1.9 及以上版本可以通过主键 (Primary Key) 模型实现全部字段更新。更多信息，参见 StarRocks 2.2 版本的[主键模型](../table_design/table_types/primary_key_table.md)。

## 如何使用原子替换表和原子替换分区功能？

执行 SWAP WITH 语句实现原子替换表和原子替换分区功能。SWAP WITH 语句要比 INSERT OVERWRITE 语句更安全。原子替换前可以先检查数据，以便核对替换后的数据和替换前的数据是否相同。

- 原子替换表：例如，有一张表名为 `table1`。如果要用另一张表原子替换 `table1`，操作如下：

    1. 创建一张新表名为 `table2`。

        ```SQL
        create table2 like table1;
        ```

    2. 使用 Stream Load、Broker Load、或 Insert Into 等方式将 `table1` 的数据导入到新表`table2` 中。
    3. 原子替换 `table1` 与 `table2`。

        ```SQL
        ALTER TABLE table1 SWAP WITH table2;
        ```

    这样做，数据就会精准的导入到 `table1` 中。

- 原子替换分区：例如，有一个表名为 `table1`。如果想要原子替换 `table1` 中的分区数据，操作如下：

    1. 创建临时分区。

        ```SQL
        ALTER TABLE table1

        ADD TEMPORARY PARTITION tp1

        VALUES LESS THAN("2020-02-01");
        ```

    2. 将 `table1` 中的分区数据导入到临时分区。
    3. 原子替换分区。

        ```SQL
        ALTER TABLE table1

        REPLACE PARTITION (p1) WITH TEMPORARY PARTITION (tp1);
        ```

## 重启FE时报错 "error to open replicated environment, will exit"

该错误是 BDBJE 的漏洞导致的，将 BDBJE 升级到 1.17 或更高版本可修复此问题。

## 查询新创建的 Apache Hive™ 表时报错 "Broker list path exception"

### **问题描述**

```Plain_Text
msg:Broker list path exception

path=hdfs://172.31.3.136:9000/user/hive/warehouse/zltest.db/student_info/*, broker=TNetworkAddress(hostname:172.31.4.233, port:8000)
```

### **解决方案**

和 StarRocks 的技术支持确认 namenode 的地址和端口是否正确以及您是否有权限访问 namenode 的地址和端口。

## 查询新创建的 Apache Hive™ 表时报错 "get hive partition meta data failed"

### **问题描述**

```Plain_Text
msg:get hive partition meta data failed: java.net.UnknownHostException: emr-header-1.cluster-242
```

### **解决方案**

确认有网络连接并给每个 BE 机器传一份集群里的 **host** 文件。

## 访问 Apache Hive™ 的 ORC 外表报错 "do_open failed. reason = Invalid ORC postscript length"

### **问题描述**

Apache Hive™ 的元数据会缓存在 StarRocks 的 FE 中，但是 StarRocks 更新元数据有两个小时的时间差。在 StarRocks 完成更新之前，如果在 Apache Hive™ 表中插入新数据或更新数据，那么 BE 扫描的 HDFS 中的数据和 FE 获得的数据不一致就会发生这个错误。

```Plain_Text
MySQL [bdp_dim]> select * from dim_page_func_s limit 1;

ERROR 1064 (HY000): HdfsOrcScanner::do_open failed. reason = Invalid ORC postscript length
```

### **解决方案**

解决方案有以下两种：

- 将 StarRocks 升级到 2.2 或更高版本。
- 手动刷新 Apache Hive™ 表。更多信息，参见[缓存更新](../data_source/External_table.md#手动更新元数据缓存)。

## 连接 MySQL 外表报错 "caching_sha2_password cannot be loaded"

### **问题描述**

MySQL 5.7 版本默认的认证方式为 mysql_native_password，如使用 MySQL 8.0 版本默认的认证方式  caching_sha2_password 来进行认证会导致连接报错。

### **解决方案**

解决方案有以下两种：

- 设置 root 用户

    ```Plain_Text
    ALTER USER 'root'@'localhost' IDENTIFIED WITH mysql_native_password BY 'yourpassword';
    ```

- 修改 **my.cnf** 文件

    ```Plain_Text
    vim my.cnf

    [mysqld]

    default_authentication_plugin=mysql_native_password
    ```

## 为什么删除一张表后磁盘空间没有立即释放？

执行 DROP TABLE 语句删表后需等待磁盘空间释放。如果想要快速释放磁盘空间可以使用 DROP TABLE FORCE 语句。执行 DROP TABLE FORCE 语句删除表时不会检查该表是否存在未完成的事务，而是直接将表删除。建议谨慎使用 DROP TABLE FORCE 语句，因为使用该语句删除的表不能恢复。

## 如何查看 StarRocks 的版本？

执行 `select current_version();` 命令或者CLI `sh bin/show_fe_version.sh` 命令查看版本。

## 如何设置 FE 的内存大小？

元数据信息都保存在 FE 的内存中。可以按下表所示参考 Tablet 的数量来设置 FE 的内存大小。例如 Tablet 数量为 100 万以下，则最少要分配 16 GB 的内存给 FE。您需要在 **fe.conf** 文件的 `JAVA_OPTS` 中通过配置 `-Xms` 和 `-Xmx` 参数来设置 FE 内存大小，并且两者取值保持一致即可。注意，集群中所有 FE 需要统一配置，因为每个 FE 都可能成为 Leader。

| Tablet 数量    | FE 内存大小 |
| -------------- | ----------- |
| 100 万以下     | 16 GB        |
| 100 万 ～ 200 万 | 32 GB        |
| 200 万 ～ 500 万 | 64 GB        |
| 500 万 ～ 1 千万   | 128 GB       |

## StarRocks 如何计算查询时间?

StarRocks 是多线程计算，查询时间即为查询最慢的线程所用的时间。

## StarRocks 支持导出数据到本地时设置路径吗？

不支持。

## StarRocks 的并发量级是多少？

建议根据业务场景或模拟业务场景测试 StarRocks 的并发量级。在部分客户的并发量级最高达到 20,000 QPS 或 30,000 QPS。

## 为什么 StarRocks 的 SSB 测试首次执行速度较慢，后续执行较快？

第一次查询读盘跟磁盘性能相关。第一次查询后系统的页面缓存生效，后续查询会先扫描页面缓存，所以速度有所提升。

## 一个集群最少可以配置多少个 BE？

StarRocks 支持单节点部署，所以 BE 最小配置个数是 1 个。BE 需要支持 AVX2 指令集，所以推荐部署 BE 的机器配置在 8 核 16 GB及以上。建议正常应用环境配置 3 个 BE。

## 使用 Apache Superset 框架呈现 StarRocks 中的数据时，如何进行数据权限配置？

创建一个新用户，然后通过给该用户授予表查询权限进行数据权限控制。

## 为什么将 `enable_profile` 指定为 `true` 后 profile 无法显示？

因为报告信息只汇报给主 FE，只有主 FE 可以查看报告信息。同时，如果通过 StarRocks Manager 查看 profile， 必须确保 FE 配置项 `enable_collect_query_detail_info` 为 `true`。

## 如何查看 StarRocks 表里的字段注释？

可以通过 `show create table xxx` 命令查看。

## 建表时可以指定 now() 函数的默认值吗？

StarRocks 2.1 及更高版本支持为函数指定默认值。低于 StarRocks 2.1 的版本仅支持为函数指定常量。

## StarRocks 外部表同步出错，应该如何解决？

**提示问题**：

SQL 错误 [1064] [42000]: data cannot be inserted into table with empty partition.Use `SHOW PARTITIONS FROM external_t` to see the currently partitions of this table.

查看Partitions时提示另一错误：SHOW PARTITIONS FROM external_t
SQL 错误 [1064] [42000]: Table[external_t] is not a OLAP/ELASTICSEARCH/HIVE table

**解决方法**：

原来是建外部表时端口不对，正确的端口是"port"="9020"，不是9931.

## 磁盘存储空间不足时，如何释放可用空间？

您可以通过 `rm -rf` 命令直接删除 `trash` 目录下的内容。在完成恢复数据备份后，您也可以通过删除 `snapshot` 目录下的内容释放存储空间。

## 磁盘存储空间不足，如何扩展磁盘空间？

如果 BE 节点存储空间不足，您可以在 BE 配置项 `storage_root_path` 所对应目录下直接添加磁盘。
