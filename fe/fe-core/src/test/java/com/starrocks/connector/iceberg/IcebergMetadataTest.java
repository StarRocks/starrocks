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
import com.starrocks.analysis.BinaryType;
import com.starrocks.analysis.ColumnPosition;
import com.starrocks.analysis.TableName;
import com.starrocks.analysis.TypeDef;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.IcebergPartitionKey;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.AlreadyExistsException;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.ExceptionChecker;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.UserException;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.PartitionInfo;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.iceberg.hive.IcebergHiveCatalog;
import com.starrocks.persist.EditLog;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AddColumnClause;
import com.starrocks.sql.ast.AddColumnsClause;
import com.starrocks.sql.ast.AlterClause;
import com.starrocks.sql.ast.AlterTableCommentClause;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.ColumnDef;
import com.starrocks.sql.ast.ColumnRenameClause;
import com.starrocks.sql.ast.DropColumnClause;
import com.starrocks.sql.ast.DropTableStmt;
import com.starrocks.sql.ast.ModifyColumnClause;
import com.starrocks.sql.ast.ModifyTablePropertiesClause;
import com.starrocks.sql.ast.TableRenameClause;
import com.starrocks.sql.optimizer.Memo;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.statistic.AnalyzeJob;
import com.starrocks.statistic.ExternalAnalyzeJob;
import com.starrocks.statistic.StatsConstants;
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
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetricsModes;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdateProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.hive.HiveTableOperations;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;

import static com.starrocks.catalog.Table.TableType.ICEBERG;
import static com.starrocks.catalog.Type.DATE;
import static com.starrocks.catalog.Type.DATETIME;
import static com.starrocks.catalog.Type.INT;
import static com.starrocks.catalog.Type.STRING;
import static com.starrocks.connector.iceberg.IcebergConnector.HIVE_METASTORE_URIS;
import static com.starrocks.connector.iceberg.IcebergConnector.ICEBERG_CATALOG_TYPE;
import static com.starrocks.connector.iceberg.IcebergMetadata.COMMENT;
import static com.starrocks.connector.iceberg.IcebergMetadata.COMPRESSION_CODEC;
import static com.starrocks.connector.iceberg.IcebergMetadata.FILE_FORMAT;
import static com.starrocks.connector.iceberg.IcebergMetadata.LOCATION_PROPERTY;
import static com.starrocks.server.CatalogMgr.ResourceMappingCatalog.RESOURCE_MAPPING_CATALOG_PREFIX;

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

        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergCatalog,
                Executors.newSingleThreadExecutor(), Executors.newSingleThreadExecutor());
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

        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                Executors.newSingleThreadExecutor(), Executors.newSingleThreadExecutor());
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

        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                Executors.newSingleThreadExecutor(), Executors.newSingleThreadExecutor());
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

        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                Executors.newSingleThreadExecutor(), Executors.newSingleThreadExecutor());
        List<String> expectResult = Lists.newArrayList("tbl1", "tbl2");
        Assert.assertEquals(expectResult, metadata.listTableNames(db1));
    }

    @Test
    public void testGetTable(@Mocked IcebergHiveCatalog icebergHiveCatalog,
                             @Mocked HiveTableOperations hiveTableOperations) {

        new Expectations() {
            {
                hiveTableOperations.current().properties();
                Map<String, String> properties = new HashMap<>();
                properties.put(FILE_FORMAT, "orc");
                properties.put(COMMENT, "xxx");
                result = properties;
                minTimes = 0;
            }
        };
        new Expectations() {
            {
                icebergHiveCatalog.getTable("db", "tbl");
                result = new BaseTable(hiveTableOperations, "tbl");
                minTimes = 0;
            }
        };

        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                Executors.newSingleThreadExecutor(), Executors.newSingleThreadExecutor());
        Table actual = metadata.getTable("db", "tbl");
        Assert.assertEquals("tbl", actual.getName());
        Assert.assertEquals(ICEBERG, actual.getType());
        Assert.assertEquals("orc", actual.getProperties().get(FILE_FORMAT));
        Assert.assertEquals("xxx", actual.getComment());
    }

    @Test
    public void testIcebergHiveCatalogTableExists(@Mocked IcebergHiveCatalog icebergHiveCatalog) {
        new Expectations() {
            {
                icebergHiveCatalog.tableExists("db", "tbl");
                result = true;
                minTimes = 0;
            }
        };
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                Executors.newSingleThreadExecutor(), Executors.newSingleThreadExecutor());
        Assert.assertTrue(metadata.tableExists("db", "tbl"));
    }

    @Test
    public void testIcebergCatalogTableExists(@Mocked IcebergCatalog icebergCatalog) {
        new Expectations() {
            {
                icebergCatalog.getTable("db", "tbl");
                result = null;
                minTimes = 0;
            }
        };
        MockIcebergCatalog mockIcebergCatalog = new MockIcebergCatalog();
        Assert.assertTrue(mockIcebergCatalog.tableExists("db", "tbl"));
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

        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                Executors.newSingleThreadExecutor(), Executors.newSingleThreadExecutor());
        Assert.assertNull(metadata.getTable("db", "tbl2"));
    }

    @Test(expected = AlreadyExistsException.class)
    public void testCreateDuplicatedDb(@Mocked IcebergHiveCatalog icebergHiveCatalog) throws AlreadyExistsException {
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                Executors.newSingleThreadExecutor(), Executors.newSingleThreadExecutor());
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
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, hiveCatalog,
                Executors.newSingleThreadExecutor(), Executors.newSingleThreadExecutor());

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
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                Executors.newSingleThreadExecutor(), Executors.newSingleThreadExecutor());

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
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                Executors.newSingleThreadExecutor(), Executors.newSingleThreadExecutor());

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
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                Executors.newSingleThreadExecutor(), Executors.newSingleThreadExecutor());
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
    public void testDropTable() {
        Map<String, String> config = new HashMap<>();
        config.put(HIVE_METASTORE_URIS, "thrift://188.122.12.1:8732");
        config.put(ICEBERG_CATALOG_TYPE, "hive");
        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog("iceberg_catalog", new Configuration(), config);
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                Executors.newSingleThreadExecutor(), Executors.newSingleThreadExecutor());
        List<TableIdentifier> mockTables = new ArrayList<>();
        mockTables.add(TableIdentifier.of("table1"));
        mockTables.add(TableIdentifier.of("table2"));

        new MockUp<GlobalStateMgr>() {
            @Mock
            public long getNextId() {
                return 1;
            }

            @Mock
            public EditLog getEditLog() {
                return new EditLog(new ArrayBlockingQueue<>(100));
            }
        };

        new MockUp<EditLog>() {
            @Mock
            public void logAddAnalyzeJob(AnalyzeJob job) {
                return;
            }

            @Mock
            public void logRemoveAnalyzeJob(AnalyzeJob job) {
                return;
            }
        };

        GlobalStateMgr.getCurrentAnalyzeMgr().addAnalyzeJob(new ExternalAnalyzeJob("iceberg_catalog",
                "iceberg_db", "table1", Lists.newArrayList(), StatsConstants.AnalyzeType.FULL,
                StatsConstants.ScheduleType.ONCE, Maps.newHashMap(), StatsConstants.ScheduleStatus.PENDING,
                LocalDateTime.MIN));

        new MockUp<IcebergMetadata>() {
            @Mock
            Table getTable(String dbName, String tblName) {
                return new IcebergTable(1, "table1", "iceberg_catalog",
                        "iceberg_catalog", "iceberg_db",
                        "table1", Lists.newArrayList(), mockedNativeTableA, Maps.newHashMap());

            }
        };

        new Expectations(icebergHiveCatalog) {
            {
                icebergHiveCatalog.dropTable("iceberg_db", "table1", true);
                result = true;
                minTimes = 0;
            }
        };

        try {
            metadata.dropTable(new DropTableStmt(false, new TableName("iceberg_catalog",
                    "iceberg_db", "table1"), true));
        } catch (Exception e) {
            Assert.fail();
        }

        new MockUp<IcebergMetadata>() {
            // mock table not exist
            @Mock
            Table getTable(String dbName, String tblName) {
                return null;

            }
        };
        try {
            metadata.dropTable(new DropTableStmt(false, new TableName("iceberg_catalog",
                    "iceberg_db", "table1"), true));
        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void testDropDbFailed() {
        Map<String, String> config = new HashMap<>();
        config.put(HIVE_METASTORE_URIS, "thrift://188.122.12.1:8732");
        config.put(ICEBERG_CATALOG_TYPE, "hive");
        Config.hive_meta_store_timeout_s = 1;
        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog("iceberg_catalog", new Configuration(), config);
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                Executors.newSingleThreadExecutor(), Executors.newSingleThreadExecutor());

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
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                Executors.newSingleThreadExecutor(), Executors.newSingleThreadExecutor());

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

        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                Executors.newSingleThreadExecutor(), Executors.newSingleThreadExecutor());
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

        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                Executors.newSingleThreadExecutor(), Executors.newSingleThreadExecutor());
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
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                Executors.newSingleThreadExecutor(), Executors.newSingleThreadExecutor());
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

        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                Executors.newSingleThreadExecutor(), Executors.newSingleThreadExecutor());
        mockedNativeTableA.newFastAppend().appendFile(FILE_A).appendFile(FILE_A_1).commit();
        IcebergTable icebergTable = new IcebergTable(1, "srTableName", "iceberg_catalog", "resource_name", "db_name",
                "table_name", Lists.newArrayList(), mockedNativeTableA, Maps.newHashMap());
        Map<ColumnRefOperator, Column> colRefToColumnMetaMap = new HashMap<ColumnRefOperator, Column>();
        ColumnRefOperator columnRefOperator1 = new ColumnRefOperator(3, Type.INT, "id", true);
        ColumnRefOperator columnRefOperator2 = new ColumnRefOperator(4, Type.STRING, "data", true);
        colRefToColumnMetaMap.put(columnRefOperator1, new Column("id", Type.INT));
        colRefToColumnMetaMap.put(columnRefOperator2, new Column("data", Type.STRING));
        OptimizerContext context = new OptimizerContext(new Memo(), new ColumnRefFactory());
        Assert.assertFalse(context.getSessionVariable().enableIcebergColumnStatistics());
        Assert.assertTrue(context.getSessionVariable().enableReadIcebergPuffinNdv());
        Statistics statistics = metadata.getTableStatistics(context, icebergTable, colRefToColumnMetaMap, null, null, -1);
        Assert.assertEquals(4.0, statistics.getOutputRowCount(), 0.001);
        Assert.assertEquals(2, statistics.getColumnStatistics().size());
        Assert.assertTrue(statistics.getColumnStatistic(columnRefOperator1).isUnknown());
        Assert.assertTrue(statistics.getColumnStatistic(columnRefOperator2).isUnknown());
    }

    @Test
    public void testGetTableStatisticsWithColumnStats() {
        Map<String, String> config = new HashMap<>();
        config.put(HIVE_METASTORE_URIS, "thrift://188.122.12.1:8732");
        config.put(ICEBERG_CATALOG_TYPE, "hive");
        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog("iceberg_catalog", new Configuration(), config);
        List<Column> columns = Lists.newArrayList(new Column("k1", INT), new Column("k2", INT));
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                Executors.newSingleThreadExecutor(), Executors.newSingleThreadExecutor());
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
        ConnectContext.get().getSessionVariable().setEnableIcebergColumnStatistics(true);
        Statistics statistics = metadata.getTableStatistics(
                new OptimizerContext(null, null, ConnectContext.get()), icebergTable, colRefToColumnMetaMap, null, null, -1);
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
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                Executors.newSingleThreadExecutor(), Executors.newSingleThreadExecutor());
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
        Assert.assertEquals("types: [INT]; keys: [0]; ", partitionKey.toString());

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
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                Executors.newSingleThreadExecutor(), Executors.newSingleThreadExecutor());
        mockedNativeTableA.newFastAppend().appendFile(FILE_A).appendFile(FILE_A_1).commit();
        mockedNativeTableA.refresh();
        IcebergTable icebergTable = new IcebergTable(1, "srTableName", "iceberg_catalog", "resource_name", "db_name",
                "table_name", columns, mockedNativeTableA, Maps.newHashMap());
        List<PartitionKey> partitionKeys = metadata.getPrunedPartitions(icebergTable, null, 1);
        Assert.assertEquals(1, partitionKeys.size());
        Assert.assertTrue(partitionKeys.get(0) instanceof IcebergPartitionKey);
        PartitionKey partitionKey = partitionKeys.get(0);
        Assert.assertEquals("types: [INT]; keys: [0]; ", partitionKey.toString());
    }

    @Test
    public void testRefreshTableWithResource() {
        Map<String, String> config = new HashMap<>();
        config.put(HIVE_METASTORE_URIS, "thrift://188.122.12.1:8732");
        config.put(ICEBERG_CATALOG_TYPE, "hive");
        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog("iceberg_catalog", new Configuration(), config);

        IcebergMetadata metadata = new IcebergMetadata(
                RESOURCE_MAPPING_CATALOG_PREFIX + CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                Executors.newSingleThreadExecutor(), Executors.newSingleThreadExecutor());
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
    public void testRefreshTableWithCatalog() {
        Map<String, String> config = new HashMap<>();
        config.put(HIVE_METASTORE_URIS, "thrift://188.122.12.1:8732");
        config.put(ICEBERG_CATALOG_TYPE, "hive");
        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog("iceberg_catalog", new Configuration(), config);
        CachingIcebergCatalog cachingIcebergCatalog = new CachingIcebergCatalog(icebergHiveCatalog, 10,
                Executors.newSingleThreadExecutor());
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, cachingIcebergCatalog,
                Executors.newSingleThreadExecutor(), Executors.newSingleThreadExecutor());
        mockedNativeTableA.newAppend().appendFile(FILE_A).commit();
        IcebergTable icebergTable = new IcebergTable(1, "srTableName", "iceberg_catalog", "resource_name", "db",
                "table", Lists.newArrayList(), mockedNativeTableA, Maps.newHashMap());
        metadata.refreshTable("sr_db", icebergTable, new ArrayList<>(), false);

        new MockUp<IcebergHiveCatalog>() {
            @Mock
            org.apache.iceberg.Table getTable(String dbName, String tableName) throws StarRocksConnectorException {
                return mockedNativeTableA;
            }
        };

        IcebergTable table = (IcebergTable) metadata.getTable("db", "table");
        Snapshot snapshotBeforeRefresh = table.getSnapshot().get();

        mockedNativeTableA.newAppend().appendFile(FILE_A).commit();
        Assert.assertEquals(snapshotBeforeRefresh.snapshotId() + 1,
                ((IcebergTable) metadata.getTable("db", "table")).getSnapshot().get().snapshotId());

        metadata.refreshTable("db", icebergTable, new ArrayList<>(), true);

    }

    @Test
    public void testGetRepeatedTableStats() {
        Map<String, String> config = new HashMap<>();
        config.put(HIVE_METASTORE_URIS, "thrift://188.122.12.1:8732");
        config.put(ICEBERG_CATALOG_TYPE, "hive");
        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog("iceberg_catalog", new Configuration(), config);
        IcebergTable icebergTable = new IcebergTable(1, "srTableName", "iceberg_catalog", "resource_name", "db_name",
                "table_name", Lists.newArrayList(), mockedNativeTableA, Maps.newHashMap());
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                Executors.newSingleThreadExecutor(), Executors.newSingleThreadExecutor());
        Map<ColumnRefOperator, Column> colRefToColumnMetaMap = new HashMap<ColumnRefOperator, Column>();
        ColumnRefOperator columnRefOperator1 = new ColumnRefOperator(3, Type.INT, "id", true);
        ColumnRefOperator columnRefOperator2 = new ColumnRefOperator(4, Type.STRING, "data", true);
        colRefToColumnMetaMap.put(columnRefOperator1, new Column("id", Type.INT));
        colRefToColumnMetaMap.put(columnRefOperator2, new Column("data", Type.STRING));
        mockedNativeTableA.newFastAppend().appendFile(FILE_A).commit();
        mockedNativeTableA.newFastAppend().appendFile(FILE_A).commit();
        mockedNativeTableA.refresh();

        new ConnectContext().setThreadLocalInfo();
        OptimizerContext context = new OptimizerContext(new Memo(), new ColumnRefFactory(), ConnectContext.get());
        context.getSessionVariable().setEnableIcebergColumnStatistics(true);

        Statistics statistics = metadata.getTableStatistics(context, icebergTable, colRefToColumnMetaMap, null, null, -1);
        Assert.assertEquals(2.0, statistics.getOutputRowCount(), 0.001);
    }

    @Test
    public void testTimeStampIdentityPartitionPrune() {
        Map<String, String> config = new HashMap<>();
        config.put(HIVE_METASTORE_URIS, "thrift://188.122.12.1:8732");
        config.put(ICEBERG_CATALOG_TYPE, "hive");
        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog("iceberg_catalog", new Configuration(), config);
        List<Column> columns = Lists.newArrayList(new Column("k1", INT), new Column("ts", DATETIME));
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                Executors.newSingleThreadExecutor(), Executors.newSingleThreadExecutor());
        IcebergTable icebergTable = new IcebergTable(1, "srTableName", "iceberg_catalog", "resource_name", "db_name",
                "table_name", columns, mockedNativeTableE, Maps.newHashMap());

        org.apache.iceberg.PartitionKey partitionKey = new org.apache.iceberg.PartitionKey(SPEC_D_1, SCHEMA_D);
        partitionKey.set(0, 1698608756000000L);
        DataFile tsDataFiles =
                DataFiles.builder(SPEC_D_1)
                        .withPath("/path/to/data-b4.parquet")
                        .withFileSizeInBytes(20)
                        .withPartition(partitionKey)
                        .withRecordCount(2)
                        .build();
        mockedNativeTableE.newAppend().appendFile(tsDataFiles).commit();
        mockedNativeTableE.refresh();
        List<PartitionKey> partitionKeys = metadata.getPrunedPartitions(icebergTable, null, 1);
        Assert.assertEquals("2023-10-30 03:45:56", partitionKeys.get(0).getKeys().get(0).getStringValue());
    }

    @Test
    public void testTransformedPartitionPrune() {
        Map<String, String> config = new HashMap<>();
        config.put(HIVE_METASTORE_URIS, "thrift://188.122.12.1:8732");
        config.put(ICEBERG_CATALOG_TYPE, "hive");
        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog("iceberg_catalog", new Configuration(), config);
        List<Column> columns = Lists.newArrayList(new Column("k1", INT), new Column("ts", DATETIME));
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                Executors.newSingleThreadExecutor(), Executors.newSingleThreadExecutor());
        IcebergTable icebergTable = new IcebergTable(1, "srTableName", "iceberg_catalog", "resource_name", "db_name",
                "table_name", columns, mockedNativeTableD, Maps.newHashMap());

        org.apache.iceberg.PartitionKey partitionKey = new org.apache.iceberg.PartitionKey(SPEC_D_5, SCHEMA_D);
        partitionKey.set(0, 438292);
        DataFile tsDataFiles =
                DataFiles.builder(SPEC_D_5)
                        .withPath("/path/to/data-d.parquet")
                        .withFileSizeInBytes(20)
                        .withPartition(partitionKey)
                        .withRecordCount(2)
                        .build();
        mockedNativeTableD.newAppend().appendFile(tsDataFiles).commit();
        mockedNativeTableD.refresh();
        List<PartitionKey> partitionKeys = metadata.getPrunedPartitions(icebergTable, null, -1);
        Assert.assertEquals("438292", partitionKeys.get(0).getKeys().get(0).getStringValue());
    }

    @Test
    public void testDateDayPartitionPrune() {
        Map<String, String> config = new HashMap<>();
        config.put(HIVE_METASTORE_URIS, "thrift://188.122.12.1:8732");
        config.put(ICEBERG_CATALOG_TYPE, "hive");
        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog("iceberg_catalog", new Configuration(), config);
        List<Column> columns = Lists.newArrayList(new Column("k1", INT), new Column("dt", DATE));
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                Executors.newSingleThreadExecutor(), Executors.newSingleThreadExecutor());
        IcebergTable icebergTable = new IcebergTable(1, "srTableName", "iceberg_catalog", "resource_name", "db_name",
                "table_name", columns, mockedNativeTableF, Maps.newHashMap());

        org.apache.iceberg.PartitionKey partitionKey = new org.apache.iceberg.PartitionKey(SPEC_F, SCHEMA_F);
        partitionKey.set(0, 19660);
        DataFile tsDataFiles = DataFiles.builder(SPEC_F)
                .withPath("/path/to/data-f.parquet")
                .withFileSizeInBytes(20)
                .withPartition(partitionKey)
                .withRecordCount(2)
                .build();
        mockedNativeTableF.newAppend().appendFile(tsDataFiles).commit();
        mockedNativeTableF.refresh();
        List<PartitionKey> partitionKeys = metadata.getPrunedPartitions(icebergTable, null, -1);
        Assert.assertEquals("19660", partitionKeys.get(0).getKeys().get(0).getStringValue());
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
    public void testListPartitionNames() {
        mockedNativeTableB.newAppend().appendFile(FILE_B_1).appendFile(FILE_B_2).commit();
        new MockUp<IcebergHiveCatalog>() {
            @Mock
            org.apache.iceberg.Table getTable(String dbName, String tableName) throws StarRocksConnectorException {
                return mockedNativeTableB;
            }
        };
        Map<String, String> config = new HashMap<>();
        config.put(HIVE_METASTORE_URIS, "thrift://188.122.12.1:8732");
        config.put(ICEBERG_CATALOG_TYPE, "hive");
        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog("iceberg_catalog", new Configuration(), config);
        CachingIcebergCatalog cachingIcebergCatalog = new CachingIcebergCatalog(icebergHiveCatalog, 3,
                Executors.newSingleThreadExecutor());
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, cachingIcebergCatalog,
                Executors.newSingleThreadExecutor(), Executors.newSingleThreadExecutor());
        List<String> partitionNames = metadata.listPartitionNames("db", "table");
        Assert.assertEquals(partitionNames, Lists.newArrayList("k2=2", "k2=3"));
    }

    @Test
    public void testGetPartitions1() {
        mockedNativeTableB.newAppend().appendFile(FILE_B_1).appendFile(FILE_B_2).commit();

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
    }

    @Test
    public void testGetPartitions2() {
        mockedNativeTableG.newAppend().appendFile(FILE_B_5).commit();

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
                "table", Lists.newArrayList(), mockedNativeTableG, Maps.newHashMap());

        List<PartitionInfo> partitions = metadata.getPartitions(icebergTable, Lists.newArrayList());
        Assert.assertEquals(1, partitions.size());
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
}