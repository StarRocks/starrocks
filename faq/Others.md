# Other Frequently Asked Questions

## When creating a table, do varchar (32) and string occupy the same storage space? Is the performance the same when querying？

They are all variable length storage, and the query performance is the same.

## How to deal with the txt file exported from Oracle that is still garbled after modifying the character set utf-8 of the file？

You can try converting the file character set as gbk. Using the file "origin.txt" as an example, suppose you look at its character set with a command and find that its current character set is iso-8859-1:

```plain text
file --mime-encoding origin.txt
Return value[assumed]：iso-8859-1
```

Use the iconv command to convert the file character set to utf-8:

```shell
iconv -f iso-8859-1 -t utf-8 origin.txt > origin_utf-8.txt
```

If we find that the converted origin_utf-8.txt file still has garbled code, we can recast the origin.txt original character set as gbk:

```shell
iconv -f gbk -t utf-8 origin.txt > origin_utf-8.txt
```

## Does the string length defined in MySQL correspond to that defined by StarRocks?

Currently in StarRocks, varchar (n), n is limited by bytes and MySQL is limited by characters, so for tables on MySQL, n can be three or four times larger, and generally does not account for more storage.

## Can table partition fields be partitioned using float, double, decimal floating point types?

No, they can only be date, datetime, or int.

## How do I see how much data in tables is stored?

You can see from the`show data`. It can display the amount of data, the number of copies, and the number of rows counted. Also, pay attention to data statistics, there is a certain time delay.

## What happens if the data exceeds this quota? Can this value be changed?

```sql
ALTER DATABASE example_db SET DATA QUOTA 10T;
```

Change the quota of db to adjust the maximum storage capacity of this db.

## Does StarRocks have a syntax like upsert that update some fields in the table without specifying the updated fields, and the values do not change？

There is currently no upsert syntax to update individual fields in a table. For the time being, updates to the entire field can only be made through「Update Table Model」or「delete+insert」.

## Usage of Atomic Swap for Table/Partition in [Data Recovery]

Partition unloading, loading, cross-table partition movement functions like CK.

The following is an example of atomic swap of table 1 data, or partition data for table 1. It may be safer than insert overwrite as it can verify data beforehand.

### Atomic Swap「Table」

1. Create a new table table2;

    ```SQL
    create table2 like table1;
    ```

2. Import data into the new table table2 using stream load / broker load / insert into, etc.

3. Swap table1 with table2:

    ```SQL
    ATER TABLE table1 SWAP WITH table2;
    ```

This enables atomic swap between tables.

### Atomic Swap「Partition」

It can also be swapped  by "importing temporary partitions".

1. Create a temporary partition:

    ```SQL
    ALTER TABLE table1
    ADD TEMPORARY PARTITION tp1
    VALUES LESS THAN("2020-02-01");
    ```

2. Import data to the temporary partition;

3. Atomic swap「partition」:

    ```SQL
    ALTER TABLE table1
    REPLACE PARTITION (p1) WITH TEMPORARY PARTITION (tp1);
    ```

In this way, the data can be imported, verified and swapped later, and the atomic swap of temporary partitions can be carried out.

## fe restart error:error to open replicated environment，will exit

**Issue Description：**

This error is reported after restarting the cluster Fe, and the Fe cannot be started.

**Solution：**

It is a bug in bdbje. Before the community version and version 1.17 (excluding this version), restarting will trigger the bug with a small probability, you can upgrade to version 1.17 and later. This problem has been fixed.

## Create hive table and query error: Broker list path exception

**Issue Description：**

```plain text
msg:Broker list path exception
path=hdfs://172.31.3.136:9000/user/hive/warehouse/zltest.db/student_info/*, broker=TNetworkAddress(hostname:172.31.4.233, port:8000)
```

**Solution：**

Confirm with the operation and maintenance personnel whether the address and port of namenode are correct and whether the permission is enabled.

## Create hive table, query reports error: get hive partition meta data failed

**Issue Description：**

```plain text
msg:get hive partition meta data failed: java.net.UnknownHostException: emr-header-1.cluster-242
```

**Solution：**

You need to transfer a copy of the host file in the cluster to each be machine and confirm that the network is connected.

## Failure to access hive external table orc：do_open failed. reason = Invalid ORC postscript length

**Issue Description：**

When querying the same SQL, the previous queries were OK, but errors were reported later. After re creating the table, no errors were reported.

```plain text
MySQL [bdp_dim]> select * from dim_page_func_s limit 1;
ERROR 1064 (HY000): HdfsOrcScanner::do_open failed. reason = Invalid ORC postscript length
```

**Solution：**

The information synchronization between fe and hive in the current version has a time lag of 2h. During this period, the table data is updated or inserted, which will lead to the inconsistency between the judgment of scan data and fe, resulting in this error. The new version adds the manual reflush function to refresh the table structure information synchronization.

## Failure to connect mysql external table：caching_sha2_password cannot be loaded

**Issue Description：**

The default authentication method for MySQL 8.0 is caching_sha2_password.

The default authentication method for MySQL5.7 is mysql_native_password.

The authentication method is different, and there is an error in the external table link.

**Solution：**

Two options:

* Connect terminals

```sql
ALTER USER 'root'@'localhost' IDENTIFIED WITH mysql_native_password BY 'yourpassword';
```

* Modify my.cnf file

```plain text
vim my.cnf
[mysqld]
default_authentication_plugin=mysql_native_password
```

## Disk space is not released immediately after drop table

When you execute drop table, the disk space will be released later. If you want to quickly release the disk space, you can use drop table force, which will have a short waiting time. If you execute drop table force, the system will not check whether there are unfinished transactions in the table, and the table will be deleted directly and cannot be recovered. Generally, this operation is not recommended.

## How to view StarRocks version?

View version by`select current_version();`Or the CLI executes`sh bin/show_fe_version.sh`

## How to set the fe memory size?

You can refer to the number of tablets. The metadata information is stored in fe memory. The memory of 10 million tablets is about 20g. At present, the maximum supported meta is about this level.

## How is the StarRocks query time calculated?

StarRocks is multi-threaded computing, query time is the time spent by query threads, and ScanTime is the sum of the time used by all threads. The query time can be viewed through the Total under Query under the execution plan.

## Does export currently support setting paths when exporting data locally?

No.

## What is the concurrency level of StarRocks?

The concurrency level of StarRocks is recommended to be tested according to the business scenario or simulated business scenario. In some customer scenarios, the pressure is over 20000 or 30000 QPS.

## Why is the ssb test of StarRocks executed slowly for the first time and faster later?

The first query of disk reading is related to disk performance. After the first query, the pagecache of the operating system will take effect. The second query will scan the pagecache first and improve the speed.

## What is the minimum number of cluster BE configurations? Does it support single node deployment?

The minimum number of BE nodes is 1. It supports single node deployment. It is recommended that cluster deployment has better performance. The be node needs to support avx2. It is recommended to configure 8-core 16G and above machines.

## How to configure data permissions for superset + StarRocks?

After creating an individual user, you can create View to authorize the user to control data permissions.

## After running set is_report_success = true, it does not display profile

Only the leader node fe can view because the report information is only reported to the leader node.

## Comments are added to the fields. How to view them in the table? The comment column is not displayed. Does StarRocks support it?

You can view it through `show create table xxx`

## Can't the column specify the default value of function like now() when creating the table?

At present, the function default value is not supported, and it needs to be written as a constant.
