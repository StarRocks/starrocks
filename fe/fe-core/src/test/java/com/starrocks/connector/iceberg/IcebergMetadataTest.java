// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.connector.iceberg;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.AlreadyExistsException;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.ExceptionChecker;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.PartitionInfo;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.iceberg.hive.IcebergHiveCatalog;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.thrift.TIcebergColumnStats;
import com.starrocks.thrift.TIcebergDataFile;
import com.starrocks.thrift.TSinkCommitInfo;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.hive.HiveTableOperations;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static com.starrocks.catalog.Table.TableType.ICEBERG;
import static com.starrocks.connector.iceberg.IcebergConnector.HIVE_METASTORE_URIS;
import static com.starrocks.connector.iceberg.IcebergConnector.ICEBERG_CATALOG_TYPE;

public class IcebergMetadataTest extends TableTestBase {
    private static final String CATALOG_NAME = "IcebergCatalog";
    private static final HdfsEnvironment HDFS_ENVIRONMENT = new HdfsEnvironment();

    @Test
    public void testListDatabaseNames(@Mocked IcebergCatalog icebergCatalog) {
        new Expectations() {
            {
                icebergCatalog.listAllDatabases();
                result = Lists.newArrayList("db1", "db2");
                minTimes = 0;
            }
        };

        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergCatalog);
        List<String> expectResult = Lists.newArrayList("db1", "db2");
        Assert.assertEquals(expectResult, metadata.listDbNames());
    }

    @Test
    public void testGetDB(@Mocked IcebergHiveCatalog icebergHiveCatalog) {
        String db = "db";

        new Expectations() {
            {
                icebergHiveCatalog.getDB(db);
                result = new Database(0, db);
                minTimes = 0;
            }
        };

        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog);
        Database expectResult = new Database(0, db);
        Assert.assertEquals(expectResult, metadata.getDb(db));
    }

    @Test
    public void testGetNotExistDB(@Mocked IcebergHiveCatalog icebergHiveCatalog) {
        String db = "db";

        new Expectations() {
            {
                icebergHiveCatalog.getDB(db);
                result = new NoSuchNamespaceException("database not found");
                minTimes = 0;
            }
        };

        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog);
        Assert.assertNull(metadata.getDb(db));
    }

    @Test
    public void testListTableNames(@Mocked IcebergHiveCatalog icebergHiveCatalog) {
        String db1 = "db1";
        String tbl1 = "tbl1";
        String tbl2 = "tbl2";

        new Expectations() {
            {
                icebergHiveCatalog.listTables(db1);
                result = Lists.newArrayList(tbl1, tbl2);
                minTimes = 0;
            }
        };

        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog);
        List<String> expectResult = Lists.newArrayList("tbl1", "tbl2");
        Assert.assertEquals(expectResult, metadata.listTableNames(db1));
    }

    @Test
    public void testGetTable(@Mocked IcebergHiveCatalog icebergHiveCatalog,
                             @Mocked HiveTableOperations hiveTableOperations) {

        new Expectations() {
            {
                icebergHiveCatalog.getTable("db", "tbl");
                result = new BaseTable(hiveTableOperations, "tbl");
                minTimes = 0;
            }
        };

        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog);
        Table actual = metadata.getTable("db", "tbl");
        Assert.assertEquals("tbl", actual.getName());
        Assert.assertEquals(ICEBERG, actual.getType());
    }

    @Test
    public void testNotExistTable(@Mocked IcebergHiveCatalog icebergHiveCatalog,
                                  @Mocked HiveTableOperations hiveTableOperations) {
        new Expectations() {
            {
                icebergHiveCatalog.getTable("db", "tbl");
                result = new BaseTable(hiveTableOperations, "tbl");
                minTimes = 0;

                icebergHiveCatalog.getTable("db", "tbl2");
                result = new StarRocksConnectorException("not found");
            }
        };

        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog);
        Assert.assertNull(metadata.getTable("db", "tbl2"));
    }

    @Test(expected = AlreadyExistsException.class)
    public void testCreateDuplicatedDb(@Mocked IcebergHiveCatalog icebergHiveCatalog) throws AlreadyExistsException {
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog);
        new Expectations() {
            {
                icebergHiveCatalog.listAllDatabases();
                result = Lists.newArrayList("iceberg_db");
                minTimes = 0;
            }
        };

        metadata.createDb("iceberg_db", new HashMap<>());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateDbWithErrorConfig() throws AlreadyExistsException {
        IcebergHiveCatalog hiveCatalog = new IcebergHiveCatalog("iceberg_catalog", new Configuration(), new HashMap<>());
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, hiveCatalog);

        new Expectations(hiveCatalog) {
            {
                hiveCatalog.listAllDatabases();
                result = Lists.newArrayList();
                minTimes = 0;
            }
        };

        metadata.createDb("iceberg_db", ImmutableMap.of("error_key", "error_value"));
    }

    @Test
    public void testCreateDbInvalidateLocation() {
        Map<String, String> config = new HashMap<>();
        config.put(HIVE_METASTORE_URIS, "thrift://188.122.12.1:8732");
        config.put(ICEBERG_CATALOG_TYPE, "hive");
        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog("iceberg_catalog", new Configuration(), config);
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog);

        new Expectations(icebergHiveCatalog) {
            {
                icebergHiveCatalog.listAllDatabases();
                result = Lists.newArrayList();
                minTimes = 0;
            }
        };

        try {
            metadata.createDb("iceberg_db", ImmutableMap.of("location", "hdfs:xx/aaaxx"));
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof StarRocksConnectorException);
            Assert.assertTrue(e.getMessage().contains("Invalid location URI"));
        }
    }

    @Test
    public void testNormalCreateDb() throws AlreadyExistsException, DdlException {
        Map<String, String> config = new HashMap<>();
        config.put(HIVE_METASTORE_URIS, "thrift://188.122.12.1:8732");
        config.put(ICEBERG_CATALOG_TYPE, "hive");
        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog("iceberg_catalog", new Configuration(), config);
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog);

        new Expectations(icebergHiveCatalog) {
            {
                icebergHiveCatalog.listAllDatabases();
                result = Lists.newArrayList();
                minTimes = 0;
            }
        };

        new MockUp<HiveCatalog>() {
            @Mock
            public void createNamespace(Namespace namespace, Map<String, String> meta) {

            }
        };

        metadata.createDb("iceberg_db");
    }

    @Test
    public void testDropNotEmptyTable() {
        Map<String, String> config = new HashMap<>();
        config.put(HIVE_METASTORE_URIS, "thrift://188.122.12.1:8732");
        config.put(ICEBERG_CATALOG_TYPE, "hive");
        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog("iceberg_catalog", new Configuration(), config);
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog);
        List<TableIdentifier> mockTables = new ArrayList<>();
        mockTables.add(TableIdentifier.of("table1"));
        mockTables.add(TableIdentifier.of("table2"));

        new Expectations(icebergHiveCatalog) {
            {
                icebergHiveCatalog.listTables("iceberg_db");
                result = mockTables;
                minTimes = 0;
            }
        };

        try {
            metadata.dropDb("iceberg_db", true);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof StarRocksConnectorException);
            Assert.assertTrue(e.getMessage().contains("Database iceberg_db not empty"));
        }
    }

    @Test
    public void testDropDbFailed() {
        Map<String, String> config = new HashMap<>();
        config.put(HIVE_METASTORE_URIS, "thrift://188.122.12.1:8732");
        config.put(ICEBERG_CATALOG_TYPE, "hive");
        Config.hive_meta_store_timeout_s = 1;
        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog("iceberg_catalog", new Configuration(), config);
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog);

        new Expectations(icebergHiveCatalog) {
            {
                icebergHiveCatalog.listTables("iceberg_db");
                result = Lists.newArrayList();
                minTimes = 0;
            }
        };

        try {
            metadata.dropDb("iceberg_db", true);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof MetaNotFoundException);
            Assert.assertTrue(e.getMessage().contains("Failed to access database"));
        }

        new Expectations(icebergHiveCatalog) {
            {
                icebergHiveCatalog.getDB("iceberg_db");
                result = null;
                minTimes = 0;
            }
        };

        try {
            metadata.dropDb("iceberg_db", true);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof MetaNotFoundException);
            Assert.assertTrue(e.getMessage().contains("Not found database"));
        }

        new Expectations(icebergHiveCatalog) {
            {
                icebergHiveCatalog.getDB("iceberg_db");
                result = new Database();
                minTimes = 0;
            }
        };

        try {
            metadata.dropDb("iceberg_db", true);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof MetaNotFoundException);
            Assert.assertTrue(e.getMessage().contains("Database location is empty"));
        }
    }

    @Test
    public void testNormalDropDb() throws MetaNotFoundException {
        Map<String, String> config = new HashMap<>();
        config.put(HIVE_METASTORE_URIS, "thrift://188.122.12.1:8732");
        config.put(ICEBERG_CATALOG_TYPE, "hive");
        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog("iceberg_catalog", new Configuration(), config);
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog);

        new Expectations(icebergHiveCatalog) {
            {
                icebergHiveCatalog.listTables("iceberg_db");
                result = Lists.newArrayList();
                minTimes = 0;

                icebergHiveCatalog.getDB("iceberg_db");
                result = new Database(1, "db", "hdfs:namenode:9000/user/hive/iceberg_location");
                minTimes = 0;
            }
        };

        new MockUp<HiveCatalog>() {
            @Mock
            public boolean dropNamespace(Namespace namespace) {
                return true;
            }
        };

        metadata.dropDb("iceberg_db", true);
    }

    @Test
    public void testFinishSink() {
        Map<String, String> config = new HashMap<>();
        config.put(HIVE_METASTORE_URIS, "thrift://188.122.12.1:8732");
        config.put(ICEBERG_CATALOG_TYPE, "hive");
        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog("iceberg_catalog", new Configuration(), config);

        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog);
        IcebergTable icebergTable = new IcebergTable(1, "srTableName", "iceberg_catalog", "resource_name", "iceberg_db",
                "iceberg_table", Lists.newArrayList(), table, Maps.newHashMap());

        new Expectations(metadata) {
            {
                metadata.getTable(anyString, anyString);
                result = icebergTable;
                minTimes = 0;
            }
        };

        TSinkCommitInfo tSinkCommitInfo = new TSinkCommitInfo();
        TIcebergDataFile tIcebergDataFile = new TIcebergDataFile();
        String path = table.location() + "/data/data_bucket=0/c.parquet";
        String format = "parquet";
        long recordCount = 10;
        long fileSize = 2000;
        String partitionPath = table.location() + "/data/data_bucket=0/";
        List<Long> splitOffsets = Lists.newArrayList(4L);
        tIcebergDataFile.setPath(path);
        tIcebergDataFile.setFormat(format);
        tIcebergDataFile.setRecord_count(recordCount);
        tIcebergDataFile.setSplit_offsets(splitOffsets);
        tIcebergDataFile.setPartition_path(partitionPath);
        tIcebergDataFile.setFile_size_in_bytes(fileSize);

        tSinkCommitInfo.setIs_overwrite(false);
        tSinkCommitInfo.setIceberg_data_file(tIcebergDataFile);

        metadata.finishSink("iceberg_db", "iceberg_table", Lists.newArrayList(tSinkCommitInfo));

        List<FileScanTask> fileScanTasks = Lists.newArrayList(table.newScan().planFiles());
        Assert.assertEquals(1, fileScanTasks.size());
        FileScanTask task = fileScanTasks.get(0);
        Assert.assertEquals(0, task.deletes().size());
        DataFile dataFile = task.file();
        Assert.assertEquals(path, dataFile.path());
        Assert.assertEquals(format, dataFile.format().name().toLowerCase(Locale.ROOT));
        Assert.assertEquals(1, dataFile.partition().size());
        Assert.assertEquals(recordCount, dataFile.recordCount());
        Assert.assertEquals(fileSize, dataFile.fileSizeInBytes());
        Assert.assertEquals(4, dataFile.splitOffsets().get(0).longValue());

        tSinkCommitInfo.setIs_overwrite(true);
        recordCount = 22;
        fileSize = 3333;
        tIcebergDataFile.setRecord_count(recordCount);
        tIcebergDataFile.setFile_size_in_bytes(fileSize);
        Map<Integer, Long> valueCounts = new HashMap<>();
        valueCounts.put(1, 111L);
        TIcebergColumnStats columnStats = new TIcebergColumnStats();
        columnStats.setColumn_sizes(new HashMap<>());
        columnStats.setValue_counts(valueCounts);
        columnStats.setNull_value_counts(new HashMap<>());
        columnStats.setLower_bounds(new HashMap<>());
        columnStats.setUpper_bounds(new HashMap<>());
        tIcebergDataFile.setColumn_stats(columnStats);

        tSinkCommitInfo.setIceberg_data_file(tIcebergDataFile);

        metadata.finishSink("iceberg_db", "iceberg_table", Lists.newArrayList(tSinkCommitInfo));
        table.refresh();
        TableScan scan = table.newScan().includeColumnStats();
        fileScanTasks = Lists.newArrayList(scan.planFiles());

        Assert.assertEquals(1, fileScanTasks.size());
        task = fileScanTasks.get(0);
        Assert.assertEquals(0, task.deletes().size());
        dataFile = task.file();
        Assert.assertEquals(path, dataFile.path());
        Assert.assertEquals(format, dataFile.format().name().toLowerCase(Locale.ROOT));
        Assert.assertEquals(1, dataFile.partition().size());
        Assert.assertEquals(recordCount, dataFile.recordCount());
        Assert.assertEquals(fileSize, dataFile.fileSizeInBytes());
        Assert.assertEquals(4, dataFile.splitOffsets().get(0).longValue());
        Assert.assertEquals(111L, dataFile.valueCounts().get(1).longValue());
    }

    @Test
    public void testFinishSinkWithCommitFailed(@Mocked IcebergMetadata.Append append) throws IOException {
        Map<String, String> config = new HashMap<>();
        config.put(HIVE_METASTORE_URIS, "thrift://188.122.12.1:8732");
        config.put(ICEBERG_CATALOG_TYPE, "hive");
        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog("iceberg_catalog", new Configuration(), config);

        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog);
        IcebergTable icebergTable = new IcebergTable(1, "srTableName", "iceberg_catalog", "resource_name", "iceberg_db",
                "iceberg_table", Lists.newArrayList(), table, Maps.newHashMap());

        new Expectations(append) {
            {
                append.commit();
                result = new Exception("commit failed");
                minTimes = 0;

                append.addFile((DataFile) any);
                result = null;
                minTimes = 0;
            }
        };

        new Expectations(metadata) {
            {
                metadata.getTable(anyString, anyString);
                result = icebergTable;
                minTimes = 0;

                metadata.getBatchWrite((Transaction) any, anyBoolean);
                result = append;
                minTimes = 0;
            }
        };

        File fakeFile = temp.newFile();
        fakeFile.createNewFile();
        Assert.assertTrue(fakeFile.exists());
        String path = fakeFile.getPath();
        TSinkCommitInfo tSinkCommitInfo = new TSinkCommitInfo();
        TIcebergDataFile tIcebergDataFile = new TIcebergDataFile();
        String format = "parquet";
        long recordCount = 10;
        long fileSize = 2000;
        String partitionPath = table.location() + "/data/data_bucket=0/";
        List<Long> splitOffsets = Lists.newArrayList(4L);
        tIcebergDataFile.setPath(path);
        tIcebergDataFile.setFormat(format);
        tIcebergDataFile.setRecord_count(recordCount);
        tIcebergDataFile.setSplit_offsets(splitOffsets);
        tIcebergDataFile.setPartition_path(partitionPath);
        tIcebergDataFile.setFile_size_in_bytes(fileSize);

        tSinkCommitInfo.setIs_overwrite(false);
        tSinkCommitInfo.setIceberg_data_file(tIcebergDataFile);

        ExceptionChecker.expectThrowsWithMsg(StarRocksConnectorException.class,
                "commit failed",
                () -> metadata.finishSink("iceberg_db", "iceberg_table", Lists.newArrayList(tSinkCommitInfo)));
        Assert.assertFalse(fakeFile.exists());
    }

    @Test
    public void testIcebergFilter() {
        List<ScalarOperator> arguments = new ArrayList<>(2);
        arguments.add(ConstantOperator.createVarchar("day"));
        arguments.add(new ColumnRefOperator(2, Type.INT, "date_col", true));
        ScalarOperator callOperator = new CallOperator("date_trunc", Type.DATE, arguments);

        List<ScalarOperator> newArguments = new ArrayList<>(2);
        newArguments.add(ConstantOperator.createVarchar("day"));
        newArguments.add(new ColumnRefOperator(22, Type.INT, "date_col", true));
        ScalarOperator newCallOperator = new CallOperator("date_trunc", Type.DATE, newArguments);

        IcebergFilter filter = IcebergFilter.of("db", "table", 1L, callOperator);
        IcebergFilter newFilter = IcebergFilter.of("db", "table", 1L, newCallOperator);
        Assert.assertEquals(filter, newFilter);

        Assert.assertEquals(newFilter, IcebergFilter.of("db", "table", 1L, newCallOperator));
        Assert.assertNotEquals(newFilter, IcebergFilter.of("db", "table", 1L, null));
    }

    @Test
    public void testGetPartitions1() {
        mockedNativeTableB.newAppend().appendFile(FILE_B_1).appendFile(FILE_B_2).commit();

        Map<String, String> config = new HashMap<>();
        config.put(HIVE_METASTORE_URIS, "thrift://188.122.12.1:8732");
        config.put(ICEBERG_CATALOG_TYPE, "hive");
        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog("iceberg_catalog", new Configuration(), config);
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog);

        IcebergTable icebergTable = new IcebergTable(1, "srTableName", "iceberg_catalog",
                "resource_name", "db",
                "table", Lists.newArrayList(), mockedNativeTableB, Maps.newHashMap());

        List<PartitionInfo> partitions = metadata.getPartitions(icebergTable, ImmutableList.of("k2=2", "k2=3"));
        Assert.assertEquals(2, partitions.size());
    }

    @Test
    public void testGetPartitions2() {
        mockedNativeTableG.newAppend().appendFile(FILE_B_5).commit();

        Map<String, String> config = new HashMap<>();
        config.put(HIVE_METASTORE_URIS, "thrift://188.122.12.1:8732");
        config.put(ICEBERG_CATALOG_TYPE, "hive");
        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog("iceberg_catalog", new Configuration(), config);
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog);

        IcebergTable icebergTable = new IcebergTable(1, "srTableName", "iceberg_catalog",
                "resource_name", "db",
                "table", Lists.newArrayList(), mockedNativeTableG, Maps.newHashMap());

        List<PartitionInfo> partitions = metadata.getPartitions(icebergTable, Lists.newArrayList());
        Assert.assertEquals(1, partitions.size());
    }
<<<<<<< HEAD
=======

    @Test
    public void testGetPartitionsWithExpireSnapshot() {
        mockedNativeTableB.newAppend().appendFile(FILE_B_1).commit();
        mockedNativeTableB.refresh();
        mockedNativeTableB.newAppend().appendFile(FILE_B_2).commit();
        mockedNativeTableB.refresh();
        mockedNativeTableB.expireSnapshots().expireOlderThan(System.currentTimeMillis()).commit();
        mockedNativeTableB.refresh();

        Map<String, String> config = new HashMap<>();
        config.put(HIVE_METASTORE_URIS, "thrift://188.122.12.1:8732");
        config.put(ICEBERG_CATALOG_TYPE, "hive");
        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog("iceberg_catalog", new Configuration(), config);
        CachingIcebergCatalog cachingIcebergCatalog = new CachingIcebergCatalog(icebergHiveCatalog, 3,
                Executors.newSingleThreadExecutor());
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, cachingIcebergCatalog,
                Executors.newSingleThreadExecutor(), Executors.newSingleThreadExecutor());

        IcebergTable icebergTable = new IcebergTable(1, "srTableName", "iceberg_catalog",
                "resource_name", "db",
                "table", Lists.newArrayList(), mockedNativeTableB, Maps.newHashMap());

        List<PartitionInfo> partitions = metadata.getPartitions(icebergTable, ImmutableList.of("k2=2", "k2=3"));
        Assert.assertEquals(2, partitions.size());
        Assert.assertTrue(partitions.stream().anyMatch(x -> x.getModifiedTime() == -1));
    }

    @Test
    public void testRefreshTableException(@Mocked CachingIcebergCatalog icebergCatalog) {
        new Expectations() {
            {
                icebergCatalog.refreshTable(anyString, anyString, null);
                result = new StarRocksConnectorException("refresh failed");
            }
        };

        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergCatalog,
                Executors.newSingleThreadExecutor(), Executors.newSingleThreadExecutor());
        IcebergTable icebergTable = new IcebergTable(1, "srTableName", "iceberg_catalog", "resource_name", "db_name",
                "table_name", new ArrayList<>(), mockedNativeTableD, Maps.newHashMap());
        metadata.refreshTable("db", icebergTable, null, true);
    }

    @Test
    public void testAlterTable(@Mocked IcebergHiveCatalog icebergHiveCatalog) throws UserException {
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                Executors.newSingleThreadExecutor(), Executors.newSingleThreadExecutor());

        TableName tableName = new TableName("db", "tbl");
        ColumnDef c1 = new ColumnDef("col1", TypeDef.create(PrimitiveType.INT), true);
        AddColumnClause addColumnClause = new AddColumnClause(c1, null, null, new HashMap<>());

        ColumnDef c2 = new ColumnDef("col2", TypeDef.create(PrimitiveType.BIGINT), true);
        ColumnDef c3 = new ColumnDef("col3", TypeDef.create(PrimitiveType.VARCHAR), true);
        List<ColumnDef> cols = new ArrayList<>();
        cols.add(c2);
        cols.add(c3);
        AddColumnsClause addColumnsClause = new AddColumnsClause(cols, null, new HashMap<>());

        List<AlterClause> clauses = Lists.newArrayList();
        clauses.add(addColumnClause);
        clauses.add(addColumnsClause);
        AlterTableStmt stmt = new AlterTableStmt(tableName, clauses);
        metadata.alterTable(stmt);
        clauses.clear();

        // must be default null
        ColumnDef c4 = new ColumnDef("col4", TypeDef.create(PrimitiveType.INT), false);
        AddColumnClause addC4 = new AddColumnClause(c4, null, null, new HashMap<>());
        clauses.add(addC4);
        AlterTableStmt stmtC4 = new AlterTableStmt(tableName, clauses);
        Assert.assertThrows(DdlException.class, () -> metadata.alterTable(stmtC4));
        clauses.clear();

        // drop/rename/modify column
        DropColumnClause dropColumnClause = new DropColumnClause("col1", null, new HashMap<>());
        ColumnRenameClause columnRenameClause = new ColumnRenameClause("col2", "col22");
        ColumnDef newCol = new ColumnDef("col1", TypeDef.create(PrimitiveType.BIGINT), true);
        Map<String, String> properties = new HashMap<>();
        ModifyColumnClause modifyColumnClause =
                new ModifyColumnClause(newCol, ColumnPosition.FIRST, null, properties);
        clauses.add(dropColumnClause);
        clauses.add(columnRenameClause);
        clauses.add(modifyColumnClause);
        metadata.alterTable(new AlterTableStmt(tableName, clauses));

        // rename table
        clauses.clear();
        TableRenameClause tableRenameClause = new TableRenameClause("newTbl");
        clauses.add(tableRenameClause);
        metadata.alterTable(new AlterTableStmt(tableName, clauses));

        // modify table properties/comment
        clauses.clear();
        Map<String, String> newProperties = new HashMap<>();
        newProperties.put(FILE_FORMAT, "orc");
        newProperties.put(LOCATION_PROPERTY, "new location");
        newProperties.put(COMPRESSION_CODEC, "gzip");
        newProperties.put(TableProperties.ORC_BATCH_SIZE, "10240");
        ModifyTablePropertiesClause modifyTablePropertiesClause = new ModifyTablePropertiesClause(newProperties);
        AlterTableCommentClause alterTableCommentClause = new AlterTableCommentClause("new comment", NodePosition.ZERO);
        clauses.add(modifyTablePropertiesClause);
        clauses.add(alterTableCommentClause);
        metadata.alterTable(new AlterTableStmt(tableName, clauses));

        // modify empty properties
        clauses.clear();
        Map<String, String> emptyProperties = new HashMap<>();
        ModifyTablePropertiesClause emptyPropertiesClause = new ModifyTablePropertiesClause(emptyProperties);
        clauses.add(emptyPropertiesClause);
        Assert.assertThrows(DdlException.class, () -> metadata.alterTable(new AlterTableStmt(tableName, clauses)));

        // modify unsupported properties
        clauses.clear();
        Map<String, String> invalidProperties = new HashMap<>();
        invalidProperties.put(FILE_FORMAT, "parquet");
        invalidProperties.put(COMPRESSION_CODEC, "zzz");
        ModifyTablePropertiesClause invalidCompressionClause = new ModifyTablePropertiesClause(invalidProperties);
        clauses.add(invalidCompressionClause);
        Assert.assertThrows(DdlException.class, () -> metadata.alterTable(new AlterTableStmt(tableName, clauses)));
    }

    @Test
    public void testGetIcebergMetricsConfig() {
        List<Column> columns = Lists.newArrayList(new Column("k1", INT),
                new Column("k2", STRING),
                new Column("k3", STRING),
                new Column("k4", STRING),
                new Column("k5", STRING));
        IcebergTable icebergTable = new IcebergTable(1, "srTableName", "iceberg_catalog", "resource_name", "db_name",
                "table_name", columns, mockedNativeTableH, Maps.newHashMap());
        Assert.assertEquals(0, IcebergMetadata.getIcebergMetricsConfig(icebergTable).size());
        Map<String, String> icebergProperties = Maps.newHashMap();
        icebergProperties.put("write.metadata.metrics.column.k1", "none");
        icebergProperties.put("write.metadata.metrics.column.k2", "counts");
        icebergProperties.put("write.metadata.metrics.column.k3", "truncate(16)");
        icebergProperties.put("write.metadata.metrics.column.k4", "truncate(32)");
        icebergProperties.put("write.metadata.metrics.column.k5", "full");
        UpdateProperties updateProperties = mockedNativeTableH.updateProperties();
        icebergProperties.forEach(updateProperties::set);
        updateProperties.commit();
        Map<String, MetricsModes.MetricsMode> actual2 = IcebergMetadata.getIcebergMetricsConfig(icebergTable);
        Assert.assertEquals(4, actual2.size());
        Map<String, MetricsModes.MetricsMode> expected2 = Maps.newHashMap();
        expected2.put("k1", MetricsModes.None.get());
        expected2.put("k2", MetricsModes.Counts.get());
        expected2.put("k4", MetricsModes.Truncate.withLength(32));
        expected2.put("k5", MetricsModes.Full.get());
        Assert.assertEquals(expected2, actual2);
    }
>>>>>>> 52d655f6d3 ([BugFix] fix refresh iceberg mv with expired snapshot (#41515))
}