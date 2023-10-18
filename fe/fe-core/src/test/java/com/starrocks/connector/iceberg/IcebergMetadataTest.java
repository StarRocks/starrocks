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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.BinaryType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.IcebergPartitionKey;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.AlreadyExistsException;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.ExceptionChecker;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.iceberg.hive.IcebergHiveCatalog;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.optimizer.Memo;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.thrift.TIcebergColumnStats;
import com.starrocks.thrift.TIcebergDataFile;
import com.starrocks.thrift.TSinkCommitInfo;
import com.starrocks.utframe.StarRocksAssert;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Snapshot;
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
import static com.starrocks.catalog.Type.INT;
import static com.starrocks.catalog.Type.STRING;
import static com.starrocks.connector.iceberg.IcebergConnector.HIVE_METASTORE_URIS;
import static com.starrocks.connector.iceberg.IcebergConnector.ICEBERG_CATALOG_TYPE;

public class IcebergMetadataTest extends TableTestBase {
    private static final String CATALOG_NAME = "IcebergCatalog";
    public static final HdfsEnvironment HDFS_ENVIRONMENT = new HdfsEnvironment();

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
                "iceberg_table", Lists.newArrayList(), mockedNativeTableA, Maps.newHashMap());

        new Expectations(metadata) {
            {
                metadata.getTable(anyString, anyString);
                result = icebergTable;
                minTimes = 0;
            }
        };

        TSinkCommitInfo tSinkCommitInfo = new TSinkCommitInfo();
        TIcebergDataFile tIcebergDataFile = new TIcebergDataFile();
        String path = mockedNativeTableA.location() + "/data/data_bucket=0/c.parquet";
        String format = "parquet";
        long recordCount = 10;
        long fileSize = 2000;
        String partitionPath = mockedNativeTableA.location() + "/data/data_bucket=0/";
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

        List<FileScanTask> fileScanTasks = Lists.newArrayList(mockedNativeTableA.newScan().planFiles());
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
        mockedNativeTableA.refresh();
        TableScan scan = mockedNativeTableA.newScan().includeColumnStats();
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
                "iceberg_table", Lists.newArrayList(), mockedNativeTableA, Maps.newHashMap());

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
        String partitionPath = mockedNativeTableA.location() + "/data/data_bucket=0/";
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
    public void testGetRemoteFile() throws IOException {
        Map<String, String> config = new HashMap<>();
        config.put(HIVE_METASTORE_URIS, "thrift://188.122.12.1:8732");
        config.put(ICEBERG_CATALOG_TYPE, "hive");
        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog("iceberg_catalog", new Configuration(), config);
        List<Column> columns = Lists.newArrayList(new Column("k1", INT), new Column("k2", INT));
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog);
        IcebergTable icebergTable = new IcebergTable(1, "srTableName", "iceberg_catalog", "resource_name", "iceberg_db",
                "iceberg_table", columns, mockedNativeTableB, Maps.newHashMap());

        mockedNativeTableB.newAppend().appendFile(FILE_B_1).appendFile(FILE_B_2).commit();
        mockedNativeTableB.refresh();

        long snapshotId = mockedNativeTableB.currentSnapshot().snapshotId();
        ScalarOperator predicate = new BinaryPredicateOperator(BinaryType.GE,
                new ColumnRefOperator(1, INT, "k2", true), ConstantOperator.createInt(1));
        List<RemoteFileInfo> res = metadata.getRemoteFileInfos(
                icebergTable, null, snapshotId, predicate, Lists.newArrayList(), 10);
        Assert.assertEquals(7, res.get(0).getFiles().get(0).getIcebergScanTasks().stream()
                .map(x -> x.file().recordCount()).reduce(0L, Long::sum), 0.001);

        StarRocksAssert starRocksAssert = new StarRocksAssert();
        starRocksAssert.getCtx().getSessionVariable().setEnablePruneIcebergManifest(true);
        mockedNativeTableB.refresh();
        snapshotId = mockedNativeTableB.currentSnapshot().snapshotId();
        predicate = new BinaryPredicateOperator(BinaryType.EQ,
                new ColumnRefOperator(1, INT, "k2", true), ConstantOperator.createInt(2));
        res = metadata.getRemoteFileInfos(
                icebergTable, null, snapshotId, predicate, Lists.newArrayList(), 10);
        Assert.assertEquals(1, res.get(0).getFiles().get(0).getIcebergScanTasks().size());
        Assert.assertEquals(3, res.get(0).getFiles().get(0).getIcebergScanTasks().get(0).file().recordCount());

        IcebergFilter filter = IcebergFilter.of("db", "table", 1, null);
        Assert.assertEquals("IcebergFilter{databaseName='db', tableName='table', snapshotId=1, predicate=true}",
                filter.toString());
    }

    @Test
    public void testGetTableStatistics() {
        Map<String, String> config = new HashMap<>();
        config.put(HIVE_METASTORE_URIS, "thrift://188.122.12.1:8732");
        config.put(ICEBERG_CATALOG_TYPE, "hive");
        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog("iceberg_catalog", new Configuration(), config);

        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog);
        mockedNativeTableA.newFastAppend().appendFile(FILE_A).commit();
        IcebergTable icebergTable = new IcebergTable(1, "srTableName", "iceberg_catalog", "resource_name", "db_name",
                "table_name", Lists.newArrayList(), mockedNativeTableA, Maps.newHashMap());
        Map<ColumnRefOperator, Column> colRefToColumnMetaMap = new HashMap<ColumnRefOperator, Column>();
        ColumnRefOperator columnRefOperator1 = new ColumnRefOperator(3, Type.INT, "id", true);
        ColumnRefOperator columnRefOperator2 = new ColumnRefOperator(4, Type.STRING, "data", true);
        colRefToColumnMetaMap.put(columnRefOperator1, new Column("id", Type.INT));
        colRefToColumnMetaMap.put(columnRefOperator2, new Column("data", Type.STRING));
        new ConnectContext().setThreadLocalInfo();
        OptimizerContext context = new OptimizerContext(new Memo(), new ColumnRefFactory());
        context.getSessionVariable().setEnableIcebergColumnStatistics(true);
        Assert.assertTrue(context.getSessionVariable().enableIcebergColumnStatistics());
        context.getSessionVariable().setEnableReadIcebergPuffinNdv(true);
        Assert.assertTrue(context.getSessionVariable().enableReadIcebergPuffinNdv());
        Statistics statistics = metadata.getTableStatistics(context, icebergTable, colRefToColumnMetaMap, null, null, -1);
        Assert.assertEquals(2.0, statistics.getOutputRowCount(), 0.001);
        Assert.assertEquals(2, statistics.getColumnStatistics().size());
        Assert.assertTrue(statistics.getColumnStatistic(columnRefOperator1).isUnknown());
        Assert.assertFalse(statistics.getColumnStatistic(columnRefOperator2).isUnknown());
    }

    @Test
    public void testGetTableStatisticsWithColumnStats() {
        Map<String, String> config = new HashMap<>();
        config.put(HIVE_METASTORE_URIS, "thrift://188.122.12.1:8732");
        config.put(ICEBERG_CATALOG_TYPE, "hive");
        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog("iceberg_catalog", new Configuration(), config);
        List<Column> columns = Lists.newArrayList(new Column("k1", INT), new Column("k2", INT));
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog);
        mockedNativeTableB.newFastAppend().appendFile(FILE_B_3).commit();
        mockedNativeTableB.newFastAppend().appendFile(FILE_B_4).commit();
        mockedNativeTableB.refresh();
        IcebergTable icebergTable = new IcebergTable(1, "srTableName", "iceberg_catalog", "resource_name", "db_name",
                "table_name", columns, mockedNativeTableB, Maps.newHashMap());
        Map<ColumnRefOperator, Column> colRefToColumnMetaMap = new HashMap<ColumnRefOperator, Column>();
        ColumnRefOperator columnRefOperator1 = new ColumnRefOperator(3, Type.INT, "k1", true);
        ColumnRefOperator columnRefOperator2 = new ColumnRefOperator(4, INT, "k2", true);
        colRefToColumnMetaMap.put(columnRefOperator1, new Column("k1", Type.INT));
        colRefToColumnMetaMap.put(columnRefOperator2, new Column("k2", Type.INT));
        new ConnectContext().setThreadLocalInfo();
        Statistics statistics = metadata.getTableStatistics(
                new OptimizerContext(null, null), icebergTable, colRefToColumnMetaMap, null, null, -1);
        Assert.assertEquals(4.0, statistics.getOutputRowCount(), 0.001);
        Assert.assertEquals(2, statistics.getColumnStatistics().size());
        Assert.assertTrue(statistics.getColumnStatistic(columnRefOperator1).isUnknown());
        ColumnStatistic columnStatistic = statistics.getColumnStatistic(columnRefOperator1);
        Assert.assertEquals(1.0, columnStatistic.getMinValue(), 0.001);
        Assert.assertEquals(2.0, columnStatistic.getMaxValue(), 0.001);
        Assert.assertEquals(0, columnStatistic.getNullsFraction(), 0.001);

        Assert.assertFalse(statistics.getColumnStatistic(columnRefOperator2).isUnknown());
    }

    @Test
    public void testPartitionPrune() {
        Map<String, String> config = new HashMap<>();
        config.put(HIVE_METASTORE_URIS, "thrift://188.122.12.1:8732");
        config.put(ICEBERG_CATALOG_TYPE, "hive");
        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog("iceberg_catalog", new Configuration(), config);
        List<Column> columns = Lists.newArrayList(new Column("id", INT), new Column("data", STRING));
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog);
        mockedNativeTableA.newFastAppend().appendFile(FILE_A).commit();
        IcebergTable icebergTable = new IcebergTable(1, "srTableName", "iceberg_catalog", "resource_name", "db_name",
                "table_name", columns, mockedNativeTableA, Maps.newHashMap());
        Map<ColumnRefOperator, Column> colRefToColumnMetaMap = new HashMap<ColumnRefOperator, Column>();
        ColumnRefOperator columnRefOperator1 = new ColumnRefOperator(3, Type.INT, "id", true);
        ColumnRefOperator columnRefOperator2 = new ColumnRefOperator(4, Type.STRING, "data", true);
        colRefToColumnMetaMap.put(columnRefOperator1, new Column("id", Type.INT));
        colRefToColumnMetaMap.put(columnRefOperator2, new Column("data", Type.STRING));
        new ConnectContext().setThreadLocalInfo();

        List<PartitionKey> partitionKeys = metadata.getPrunedPartitions(icebergTable, null, 1);
        Assert.assertEquals(1, partitionKeys.size());
        Assert.assertTrue(partitionKeys.get(0) instanceof IcebergPartitionKey);
        IcebergPartitionKey partitionKey =  (IcebergPartitionKey) partitionKeys.get(0);
        Assert.assertEquals("types: [VARCHAR]; keys: [0]; ", partitionKey.toString());

        mockedNativeTableA.newFastAppend().appendFile(FILE_A_2).commit();
        mockedNativeTableA.refresh();
        icebergTable = new IcebergTable(1, "srTableName", "iceberg_catalog", "resource_name", "db_name",
                "table_name", columns, mockedNativeTableA, Maps.newHashMap());
        partitionKeys = metadata.getPrunedPartitions(icebergTable, null, 100);
        Assert.assertEquals(2, partitionKeys.size());
    }

    @Test
    public void testPartitionPruneWithDuplicated() {
        Map<String, String> config = new HashMap<>();
        config.put(HIVE_METASTORE_URIS, "thrift://188.122.12.1:8732");
        config.put(ICEBERG_CATALOG_TYPE, "hive");
        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog("iceberg_catalog", new Configuration(), config);
        List<Column> columns = Lists.newArrayList(new Column("id", INT), new Column("data", STRING));
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog);
        mockedNativeTableA.newFastAppend().appendFile(FILE_A).appendFile(FILE_A_1).commit();
        mockedNativeTableA.refresh();
        IcebergTable icebergTable = new IcebergTable(1, "srTableName", "iceberg_catalog", "resource_name", "db_name",
                "table_name", columns, mockedNativeTableA, Maps.newHashMap());
        List<PartitionKey> partitionKeys = metadata.getPrunedPartitions(icebergTable, null, 1);
        Assert.assertEquals(1, partitionKeys.size());
        Assert.assertTrue(partitionKeys.get(0) instanceof IcebergPartitionKey);
        PartitionKey partitionKey = partitionKeys.get(0);
        Assert.assertEquals("types: [VARCHAR]; keys: [0]; ", partitionKey.toString());
    }

    @Test
    public void testRefreshTable() {
        Map<String, String> config = new HashMap<>();
        config.put(HIVE_METASTORE_URIS, "thrift://188.122.12.1:8732");
        config.put(ICEBERG_CATALOG_TYPE, "hive");
        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog("iceberg_catalog", new Configuration(), config);

        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog);
        IcebergTable icebergTable = new IcebergTable(1, "srTableName", "iceberg_catalog", "resource_name", "db_name",
                "table_name", Lists.newArrayList(), mockedNativeTableA, Maps.newHashMap());
        Assert.assertFalse(icebergTable.getSnapshot().isPresent());
        mockedNativeTableA.newAppend().appendFile(FILE_A).commit();
        Assert.assertTrue(icebergTable.getSnapshot().isPresent());
        Snapshot snapshot = icebergTable.getSnapshot().get();
        mockedNativeTableA.newAppend().appendFile(FILE_A).commit();
        Assert.assertSame(snapshot, icebergTable.getSnapshot().get());
        metadata.refreshTable("db_name", icebergTable, null, false);
        Assert.assertNotSame(snapshot, icebergTable.getSnapshot().get());
    }

    @Test
    public void testGetRepeatedTableStats() {
        Map<String, String> config = new HashMap<>();
        config.put(HIVE_METASTORE_URIS, "thrift://188.122.12.1:8732");
        config.put(ICEBERG_CATALOG_TYPE, "hive");
        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog("iceberg_catalog", new Configuration(), config);
        IcebergTable icebergTable = new IcebergTable(1, "srTableName", "iceberg_catalog", "resource_name", "db_name",
                "table_name", Lists.newArrayList(), mockedNativeTableA, Maps.newHashMap());
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog);
        Map<ColumnRefOperator, Column> colRefToColumnMetaMap = new HashMap<ColumnRefOperator, Column>();
        ColumnRefOperator columnRefOperator1 = new ColumnRefOperator(3, Type.INT, "id", true);
        ColumnRefOperator columnRefOperator2 = new ColumnRefOperator(4, Type.STRING, "data", true);
        colRefToColumnMetaMap.put(columnRefOperator1, new Column("id", Type.INT));
        colRefToColumnMetaMap.put(columnRefOperator2, new Column("data", Type.STRING));
        mockedNativeTableA.newFastAppend().appendFile(FILE_A).commit();
        mockedNativeTableA.newFastAppend().appendFile(FILE_A).commit();
        mockedNativeTableA.refresh();
        Statistics statistics = metadata.getTableStatistics(
                new OptimizerContext(null, null), icebergTable, colRefToColumnMetaMap, null, null, -1);
        Assert.assertEquals(2.0, statistics.getOutputRowCount(), 0.001);
    }
}