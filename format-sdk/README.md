# StarRocks Format Library

StarRocks Format Library is an SDK for reading and writing StarRocks data files, which provides C++ and Java version of tablet data file reader and writer. With the Format SDK, you can bypass the StarRocks BE node and directly read and write tablet data files that stored in the remote file system (e.g. S3, HDFS) in the shared-data mode. So that a piece of data can be accessed (read and write) by multiple computing engines (e.g. Spark, StarRocks) at the same time.

## Why we do it?

StarRocks is a next-gen, high-performance analytical data warehouse. However, the limitations of the MPP architecture still cause it to face some problems in large-data ETL scenarios. For example:

- __Low resource utilization__: StarRocks clusters adopt the resource reservation mode mostly, but in order to support large-data ETL scenarios, which require a large amount of resource overhead in a relatively short period of time, plan redundant resources in advance may reduce the overall resource utilization of the cluster.
- __Poor resource isolation__: In terms of resource isolation, StarRocks adopts Group rather than Query level isolation. In large-data ETL scenarios, there is a risk that a query with large resource overhead will run out of resources, thereby starving some small queries to death.
- __Lack of failure tolerance__: Due to the lack of task-level failure tolerance mechanism, when an ETL job fails, it will usually still fail even if rerun it manually.

In order to solve the above problems, we have proposed the idea of __bypassing StarRocks BE node to directly read and write tablet data files in the shared-data mode__. Taking the Apache Spark as an example（_In fact, we are not bound to it, and we can support more computing engines by design._）, the overall architecture design is as follows:

![starrocks-bypass-load](https://github.com/user-attachments/assets/00d37763-5b6c-42ac-a980-40a604746c59)

For complex query scenarios, users can submit jobs through the `spark-sql` client. Spark interacts with StarRocks to obtain tables's metadata information, manages data write transaction, and do read/write operation on shared storage through the Format SDK directly. Finally, for one piece of StarRocks's data files, multiple computing engines can read and write it.

## How to build？

The building process is divided into two steps. __Firstly__, execute the following command to compile `be` and `format-lib` module to generate the `libstarrocks_format.so` file:

```bash
# build be
./build.sh --clean --be --enable-shared-data

# build format-lib
./build.sh --format-lib --enable-shared-data
```

__Then__ execute the following command to compile the `format-sdk` module:

```bash
cd ./format-sdk

mvn clean package -DskipTests
```

After that, you will get two files: `libstarrocks_format_wrapper.so` and `format-sdk.jar`.

## How to use？

Assume there is a product sales information table, the table schema is as follows:

```sql
CREATE TABLE `tb_sales`
(
    `product_id` BIGINT NOT NULL COMMENT "Product ID",
    `date`       DATE   NOT NULL COMMENT "Sale Date",
    `quantity`   INT    NOT NULL COMMENT "Sale Quantity",
    `amount`     BIGINT NOT NULL COMMENT "Sale Amount"
) ENGINE = OLAP PRIMARY KEY(`product_id`, `date`)
DISTRIBUTED BY HASH(`product_id`)
PROPERTIES (
    "enable_persistent_index" = "true"
);
```

Next, we will take a Java program as an example to show how to use the Format SDK to read and write data files of this table.

> Note: you need to introduce the `format-sdk.jar` dependency into your project.

- __Write by StarRocksWriter__

You can write a program to use `StarRocksWriter` to perform write operations according to the following steps:

1. Request the metadata and partition information of the table by `RestClient`;
2. Begin a transaction;
3. Create and open a `StarRocksWriter` which binding to the target Tablet;
4. Create an Arrow `VectorSchemaRoot` object, and fill it with custom data according to the table schema definition;
5. Call the `StarRocksWriter#write` method to write the filled `VectorSchemaRoot` object to StarRocks;
6. Batch as needed, and call the `StarRocksWriter#flush` method to flush the data to the file system;
7. Repeat steps 4 to 6 above until all data is written;
8. Close and release `StarRocksWriter`, it's automatically closed and released using the `try-with-resources` statement in this example;
9. Commit or rollback the transaction.

Example code as follows：

```java
final String DEFAULT_CATALOG = "default_catalog";
final String DB_NAME = "bypass";
final String TB_NAME = "tb_sales";

Config config = Config.newBuilder()
        .feHttpUrl("http://127.0.0.1:8030")
        .feJdbcUrl("jdbc:mysql://127.0.0.1:9030")
        .database(DB_NAME)
        .username("root")
        .password("******")
        .s3Endpoint("https://tos-s3-cn-beijing.volces.com")
        .s3EndpointRegion("cn-beijing")
        .s3ConnectionSslEnabled(false)
        .s3PathStyleAccess(false)
        .s3AccessKey("******")
        .s3SecretKey("******")
        .build();

String label = String.format(
        "bypass_%s_%s_%s", DB_NAME, TB_NAME, RandomStringUtils.randomAlphabetic(8)
);
try (RestClient restClient = RestClient.newBuilder()
        .feHttpEndpoints("http://127.0.0.1:8030")
        .username("root")
        .password("******")
        .build()) {

    // Request the metadata and partition information of the table by RestClient
    TableSchema tableSchema = restClient.getTableSchema(DEFAULT_CATALOG, DB_NAME, TB_NAME);
    List<TablePartition> tablePartitions = restClient.listTablePartitions(DEFAULT_CATALOG, DB_NAME, TB_NAME, false);
    Schema arrowSchema = ArrowUtils.toArrowSchema(tableSchema, ZoneId.systemDefault());

    // Begin a transaction
    TransactionResult beginTxnResult = restClient.beginTransaction(DEFAULT_CATALOG, DB_NAME, TB_NAME, label);
    assertTrue(beginTxnResult.isOk());

    List<TabletCommitInfo> commitTablets = new ArrayList<>();
    TablePartition tablePartition = tablePartitions.get(0);
    for (int i = 0; i < tablePartition.getTablets().size(); i++) {
        TablePartition.Tablet tablet = tablePartition.getTablets().get(i);
        Long tabletId = tablet.getId();

        // Create and open a StarRocksWriter which binding to the target Tablet
        try (StarRocksWriter tabletWriter = new StarRocksWriter(
                tabletId, tablePartition.getStoragePath(), beginTxnResult.getTxnId(), arrowSchema, config)) {
            tabletWriter.open();

            try (VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, tabletWriter.getAllocator())) {
                // Fill the VectorSchemaRoot object with custom data according to the table schema definition
                fillSampleData(i, root);

                // Write the filled VectorSchemaRoot object to StarRocks
                tabletWriter.write(root);

                // Flush the data to the file system
                tabletWriter.flush();
            }

            tabletWriter.finish();
        }

        commitTablets.add(new TabletCommitInfo(tabletId, tablet.getPrimaryComputeNodeId()));
    }

    // Commit or rollback the transaction
    TransactionResult prepareTxnResult = restClient.prepareTransaction(
            DEFAULT_CATALOG, DB_NAME, beginTxnResult.getLabel(), commitTablets, null);
    if (prepareTxnResult.isOk()) {
        TransactionResult commitTxnResult = restClient.commitTransaction(
                DEFAULT_CATALOG, DB_NAME, prepareTxnResult.getLabel());
        assertTrue(commitTxnResult.isOk());
    } else {
        TransactionResult rollbackTxnResult = restClient.rollbackTransaction(
                DEFAULT_CATALOG, DB_NAME, prepareTxnResult.getLabel(), null);
        assertTrue(rollbackTxnResult.isOk());
    }
}
```

- __Read by StarRocksReader__

You can write a program to use `StarRocksWriter` to perform read operations according to the following steps:

1. Request the metadata and partition information of the table by `RestClient`;
2. Create and open a `StarRocksReader` which binding to the target Tablet;
3. `StarRocksReader` supports iterator access, so you can iterate and read the data;
4. Close and release `StarRocksReader`, it's automatically closed and released using the `try-with-resources` statement in this example.

Example code as follows：

```java
final String DEFAULT_CATALOG = "default_catalog";
final String DB_NAME = "bypass";
final String TB_NAME = "tb_sales";

Config config = Config.newBuilder()
        .feHttpUrl("http://127.0.0.1:8030")
        .feJdbcUrl("jdbc:mysql://127.0.0.1:9030")
        .database(DB_NAME)
        .username("root")
        .password("******")
        .s3Endpoint("https://tos-s3-cn-beijing.volces.com")
        .s3EndpointRegion("cn-beijing")
        .s3ConnectionSslEnabled(false)
        .s3PathStyleAccess(false)
        .s3AccessKey("******")
        .s3SecretKey("******")
        .build();

try (RestClient restClient = RestClient.newBuilder()
        .feHttpEndpoints("http://127.0.0.1:8030")
        .username("root")
        .password("******")
        .build()) {

    // Request the metadata and partition information of the table by RestClient
    TableSchema tableSchema = restClient.getTableSchema(DEFAULT_CATALOG, DB_NAME, TB_NAME);
    List<TablePartition> tablePartitions = restClient.listTablePartitions(DEFAULT_CATALOG, DB_NAME, TB_NAME, false);
    Schema arrowSchema = ArrowUtils.toArrowSchema(tableSchema, ZoneId.systemDefault());

    TablePartition partition = tablePartitions.get(0);
    for (TablePartition.Tablet tablet : partition.getTablets()) {
        Long tabletId = tablet.getId();
        // Create and open a StarRocksReader which binding to the target Tablet
        try (StarRocksReader reader = new StarRocksReader(
                tabletId,
                partition.getStoragePath(),
                partition.getVisibleVersion(),
                arrowSchema,
                arrowSchema,
                config)) {
            reader.open();

            // StarRocksReader supports iterator access, so you can iterate and read the data
            while (reader.hasNext()) {
                try (VectorSchemaRoot root = reader.next()) {
                    System.out.println(root.contentToTSVString());
                }
            }
        }
    }
}
```