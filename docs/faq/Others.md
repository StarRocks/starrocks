# Other FAQs

This topic provides answers to some general questions.

## Do VARCHAR (32) and STRING occupy the same storage space?

Both are variable-length data types. When you store data of the same length, VARCHAR (32) and STRING occupy the same storage space.

## Do VARCHAR (32) and STRING perform the same for the data query?

Yes.

## Why do TXT files imported from Oracle still appear garbled after I set the character set to UTF-8?

To solve this problem, perform the following steps:

1. For example, there is a file named **original**, whose text is garbled. The character set of this file is ISO-8859-1. Run the following code to obtain the character set of the file.

    ```Plain_Text
    file --mime-encoding origin.txt
    origin.txt: iso-8859-1
    ```

2. Run the `iconv` command to convert the character set of this file into UTF-8.

    ```Plain_Text
    iconv -f iso-8859-1 -t utf-8 origin.txt > origin_utf-8.txt
    ```

3. After the conversion, the text of this file still appears garbled. You can then regrade the character set of this file as GBK and convert the character set into UTF-8 again.

    ```Plain_Text
    iconv -f gbk -t utf-8 origin.txt > origin_utf-8.txt
    ```

## Is the length of STRING defined by MySQL the same as that defined by StarRocks?

For VARCHAR(n), StarRocks defines "n" by bytes and MySQL defines "n" by characters. According to UTF-8, one Chinese character is equal to three bytes. When StarRocks and MySQL define "n" as the same number, MySQL saves three times as many characters as StarRocks.

## Can the data type of partitioned fields of a table be FLOAT, DOUBLE, or DECIMAL?

No, only DATE, DATETIME, and INT are supported.

## How to check the storage space that is occupied by the data in a table?

Execute the SHOW DATA statement to see the corresponding storage space. You can also see the data volume, the number of copies, and the number of rows.

**Note**: There is a time delay in data statistics.

## How to request a quota increase for the StarRocks database?

To request a quota increase, run the following code:

```Plain_Text
ALTER DATABASE example_db SET DATA QUOTA 10T;
```

## Does StarRocks support updating particular fields in a table by executing the UPSERT statement?

StarRocks 2.2 and later support updating specific fields in a table by using the Primary Key table. StarRocks 1.9 and later support updating all fields in a table by using the Primary Key table. For more information, see [Primary Key table](../table_design/table_types/primary_key_table.md) in StarRocks 2.2.

## How to swap the data between two tables or two partitions?

Execute the SWAP WITH statement to swap the data between two tables or two partitions. The SWAP WITH statement is more secure than the INSERT OVERWRITE statement. Before you swap the data, check the data first and then see whether the data after the swapping is consistent with the data before the swapping.

- Swap two tables: For example, there is a table named table 1. If you want to replace table 1 with another one, perform the following steps:

    1. Create a new table named table 2.

        ```SQL
        create table2 like table1;
        ```

    2. Use Stream Load, Broker Load, or Insert Into to load data from table 1 into table 2.

    3. Replace table 1 with table 2.

        ```SQL
        ALTER TABLE table1 SWAP WITH table2;
        ```

        By doing so, the data is loaded accurately into table 1.

- Swap two partitions: For example, there is a table named table 1. If you want to replace the partition data in table 1, perform the following steps:

    1. Create a temporary partition.

        ```SQL
        ALTER TABLE table1

        ADD TEMPORARY PARTITION tp1

        VALUES LESS THAN("2020-02-01");
        ```

    2. Load the partition data from table 1 into the temporary partition.

    3. Replace the partition of table 1 with the temporary partition.

        ```SQL
        ALTER TABLE table1

        REPLACE PARTITION (p1) WITH TEMPORARY PARTITION (tp1);
        ```

## This error "error to open replicated environment, will exit" occurs when I restart a frontend (FE)

This error occurs due to BDBJE's bug. To solve this problem, update the BDBJE version to 1.17 or later.

## This error "Broker list path exception" occurs when I query data from a new Apache Hive table

### Problem description

```Plain_Text
msg:Broker list path exception

path=hdfs://172.31.3.136:9000/user/hive/warehouse/zltest.db/student_info/*, broker=TNetworkAddress(hostname:172.31.4.233, port:8000)
```

### Solution

Contact the StarRocks technical support and check whether the address and port of the namenode are correct and whether you have permission to access the address and port of the namenode.

## This error "get hive partition metadata failed" occurs when I query data from a new Apache Hive table

### Problem description

```Plain_Text
msg:get hive partition meta data failed: java.net.UnknownHostException: emr-header-1.cluster-242
```

### Solution

Ensure that the network is connected and upload the **host** file to each backend (BE) in your StarRocks cluster.

## This error "do_open failed. reason = Invalid ORC postscript length" occurs when I access ORC external table in Apache Hive

### Problem description

The metadata of the Apache Hive is cached in the FEs. But there is a two-hours time lag for StarRocks to update the metadata. Before StarRocks finishes the update, If you insert new data or update data in the Apache Hive table, the data in HDFS scanned by the BEs and the data obtained by the FEs are different. Therefore, this error occurs.

```Plain_Text
MySQL [bdp_dim]> select * from dim_page_func_s limit 1;

ERROR 1064 (HY000): HdfsOrcScanner::do_open failed. reason = Invalid ORC postscript length
```

### Solution

To solve this problem, perform one of the following operations:

- Upgrade your current version to StarRocks 2.2 or later.
- Manually refresh your Apache Hive table. For more information, see [Metadata caching strategy](../data_source/External_table.md#metadata-caching-strategy).

## This error "caching_sha2_password cannot be loaded" occurs when I connect external tables of MySQL

### Problem description

The default authentication plugin of MySQL 8.0 is caching_sha2_password. The default authentication plugin of MySQL 5.7 is mysql_native_password. This error occurs because you use the wrong authentication plugin.

### Solution

To solve this problem, perform one of the following operations:

- Connect to the StarRocks.

```SQL
ALTER USER 'root'@'localhost' IDENTIFIED WITH mysql_native_password BY 'yourpassword';
```

- Modify the `my.cnf` file.

```Plain_Text
vim my.cnf

[mysqld]

default_authentication_plugin=mysql_native_password
```

## How to release disk space immediately after deleting a table?

If you execute the DROP TABLE statement to delete a table, StarRocks takes a while to release the allocated disk space. To release the allocated disk space immediately, execute the DROP TABLE FORCE statement to delete a table. When you execute the DROP TABLE FORCE statement, the StarRocks deletes the table directly without checking whether there are unfinished events in it. We recommend that you execute the DROP TABLE FORCE statement with caution. Because once the table is deleted, you cannot restore it.

## How to view the current version of StarRocks?

Run the `select current_version();` command or the CLI command `./bin/show_fe_version.sh` to view the current version.

## How to set the memory size of an FE?

FEs are used to store metadata. You can set the memory size of an FE based on the number of tablets. In StarRocks, you can set the memory size of an FE to 20 GB at most. 10 million tablets occupy about 20 GB of an FE memory.

## How does StarRocks calculate its query time?

StarRocks supports querying data by using multiple threads. Query time refers to the time used by multiple threads to query data.

## Does StarRocks support setting the path when I export data locally?

No.

## What are the concurrency upper limits of StarRocks?

You can test the concurrency limitations based on the actual business scenarios or simulated business scenarios. According to the feedback of some users, maximum of 20,000 QPS or 30,000 QPS can be achieved.

## Why is the first-time SSB test performance of StarRocks slower than that done the second time?

The speed to read disks for the first query relates to the performance of disks. After the first query, the page cache is generated for the subsequent queries, so the query is faster than before.

## How many BEs need to be configured at least for a cluster?

StarRocks supports single node deployment, so you need to configure at least one BE. BEs need to be run with AVX2, so we recommend that you deploy BEs on machines with 8-core and 16GB or higher configurations.

## How to set data permissions when I use Apache Superset to visualize the data in StarRocks?

You can create a new user account and then set the data permission by granting permissions on the table query to the user.

## Why does the profile fail to display after I set `is_report_success` to `true`?

The report is only submitted to the leader FE for access.

## How to check field annotations in the tables of StarRocks?

Run the `show create table xxx` command.

## When I create a table, how to specify the default value for the NOW() function?

Only StarRocks 2.1 or later version supports specifying the default value for a function. For versions earlier than StarRocks 2.1, you can only specify a constant for a function.

## How can I release the storage space of BE nodes?

You can remove the directory `trash` using `rm -rf` command. If you have already restored your data from snapshot, you can remove the directory `snapshot`.

## Can add extra disks to BE nodes?

Yes. You can add the disks to the directory specified by the BE configuration item `storage_root_path`.
