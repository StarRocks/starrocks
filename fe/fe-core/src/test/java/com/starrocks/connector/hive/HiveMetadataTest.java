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

package com.starrocks.connector.hive;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.AlreadyExistsException;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.ExceptionChecker;
import com.starrocks.common.FeConstants;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.connector.CachingRemoteFileIO;
import com.starrocks.connector.ConnectorMetadatRequestContext;
import com.starrocks.connector.ConnectorProperties;
import com.starrocks.connector.ConnectorType;
import com.starrocks.connector.GetRemoteFilesParams;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.MetastoreType;
import com.starrocks.connector.PartitionInfo;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.connector.RemoteFileBlockDesc;
import com.starrocks.connector.RemoteFileDesc;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.connector.RemoteFileOperations;
import com.starrocks.connector.RemotePathKey;
import com.starrocks.connector.TableVersionRange;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.hive.HivePartitionStats;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.sql.analyzer.AstToStringBuilder;
import com.starrocks.sql.ast.AddPartitionClause;
import com.starrocks.sql.ast.AlterClause;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.DropTableStmt;
import com.starrocks.sql.ast.SingleItemListPartitionDesc;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.thrift.THiveFileInfo;
import com.starrocks.thrift.TSinkCommitInfo;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static com.starrocks.connector.hive.HiveMetadata.STARROCKS_QUERY_ID;
import static com.starrocks.connector.hive.MockedRemoteFileSystem.HDFS_HIVE_TABLE;
import static com.starrocks.sql.analyzer.AnalyzeTestUtil.getStarRocksAssert;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class HiveMetadataTest {
    private HiveMetaClient client;
    private HiveMetastore metastore;
    private CachingHiveMetastore cachingHiveMetastore;
    private HiveMetastoreOperations hmsOps;
    private HiveRemoteFileIO hiveRemoteFileIO;
    private CachingRemoteFileIO cachingRemoteFileIO;
    private RemoteFileOperations fileOps;
    private ExecutorService executorForHmsRefresh;
    private ExecutorService executorForRemoteFileRefresh;
    private ExecutorService executorForPullFiles;
    private HiveStatisticsProvider statisticsProvider;
    private HiveMetadata hiveMetadata;

    private static ConnectContext connectContext;
    private static OptimizerContext optimizerContext;
    private static ColumnRefFactory columnRefFactory;

    @BeforeEach
    public void setUp() throws Exception {
        FeConstants.runningUnitTest = true;
        executorForHmsRefresh = Executors.newFixedThreadPool(5);
        executorForRemoteFileRefresh = Executors.newFixedThreadPool(5);
        executorForPullFiles = Executors.newFixedThreadPool(5);

        client = new HiveMetastoreTest.MockedHiveMetaClient();
        metastore = new HiveMetastore(client, "hive_catalog", MetastoreType.HMS);
        cachingHiveMetastore = CachingHiveMetastore.createCatalogLevelInstance(
                metastore, executorForHmsRefresh, executorForHmsRefresh,
                100, 10, 1000, false);
        hmsOps = new HiveMetastoreOperations(cachingHiveMetastore, true, new Configuration(), MetastoreType.HMS, "hive_catalog");

        hiveRemoteFileIO = new HiveRemoteFileIO(new Configuration());
        FileSystem fs = new MockedRemoteFileSystem(HDFS_HIVE_TABLE);
        hiveRemoteFileIO.setFileSystem(fs);
        cachingRemoteFileIO = CachingRemoteFileIO.createCatalogLevelInstance(
                hiveRemoteFileIO, executorForRemoteFileRefresh, 100, 10, 0.1);
        fileOps = new RemoteFileOperations(cachingRemoteFileIO, executorForPullFiles, executorForPullFiles,
                false, true, new Configuration());
        statisticsProvider = new HiveStatisticsProvider(hmsOps, fileOps);

        UtFrameUtils.createMinStarRocksCluster();
        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        columnRefFactory = new ColumnRefFactory();
        optimizerContext = OptimizerFactory.mockContext(connectContext, columnRefFactory);
        hiveMetadata = new HiveMetadata("hive_catalog", new HdfsEnvironment(), hmsOps, fileOps, statisticsProvider,
                Optional.empty(), executorForHmsRefresh, executorForHmsRefresh,
                new ConnectorProperties(ConnectorType.HIVE));
    }

    @AfterEach
    public void tearDown() {
        executorForHmsRefresh.shutdown();
        executorForRemoteFileRefresh.shutdown();
        executorForPullFiles.shutdown();
    }

    @Test
    public void testListDbNames() {
        List<String> databaseNames = hiveMetadata.listDbNames(new ConnectContext());
        Assertions.assertEquals(Lists.newArrayList("db1", "db2"), databaseNames);
        CachingHiveMetastore queryLevelCache = CachingHiveMetastore.createQueryLevelInstance(cachingHiveMetastore, 100);
        Assertions.assertEquals(Lists.newArrayList("db1", "db2"), queryLevelCache.getAllDatabaseNames());
    }

    @Test
    public void testListTableNames() {
        List<String> databaseNames = hiveMetadata.listTableNames(new ConnectContext(), "db1");
        Assertions.assertEquals(Lists.newArrayList("table1", "table2"), databaseNames);
    }

    @Test
    public void testGetPartitionKeys() {
        Assertions.assertEquals(
                Lists.newArrayList("col1"),
                hiveMetadata.listPartitionNames("db1", "tbl1", ConnectorMetadatRequestContext.DEFAULT));
    }

    @Test
    public void testGetDb() {
        Database database = hiveMetadata.getDb(new ConnectContext(), "db1");
        Assertions.assertEquals("db1", database.getFullName());

    }

    @Test
    public void testGetTable() {
        com.starrocks.catalog.Table table = hiveMetadata.getTable(new ConnectContext(), "db1", "tbl1");
        HiveTable hiveTable = (HiveTable) table;
        Assertions.assertEquals("db1", hiveTable.getCatalogDBName());
        Assertions.assertEquals("tbl1", hiveTable.getCatalogTableName());
        Assertions.assertEquals(Lists.newArrayList("col1"), hiveTable.getPartitionColumnNames());
        Assertions.assertEquals(Lists.newArrayList("col2"), hiveTable.getDataColumnNames());
        Assertions.assertEquals("hdfs://127.0.0.1:10000/hive", hiveTable.getTableLocation());
        Assertions.assertEquals(ScalarType.INT, hiveTable.getPartitionColumns().get(0).getType());
        Assertions.assertEquals(ScalarType.INT, hiveTable.getBaseSchema().get(0).getType());
        Assertions.assertEquals("hive_catalog", hiveTable.getCatalogName());
    }

    @Test
    public void testGetTableThrowConnectorException() {
        new Expectations(hmsOps) {
            {
                hmsOps.getTable("acid_db", "acid_table");
                result = new StarRocksConnectorException("hive acid table is not supported");
                minTimes = 1;
            }
        };

        Assertions.assertThrows(StarRocksConnectorException.class,
                () -> hiveMetadata.getTable(new ConnectContext(), "acid_db", "acid_table"));
    }

    @Test
    public void testTableExists() {
        boolean exists = hiveMetadata.tableExists(new ConnectContext(), "db1", "tbl1");
        Assertions.assertTrue(exists);
    }

    @Test
    public void testGetHiveRemoteFiles() throws AnalysisException {
        FeConstants.runningUnitTest = true;
        String tableLocation = "hdfs://127.0.0.1:10000/hive.db/hive_tbl";
        HiveMetaClient client = new HiveMetastoreTest.MockedHiveMetaClient();
        HiveMetastore metastore = new HiveMetastore(client, "hive_catalog", null);
        List<String> partitionNames = Lists.newArrayList("col1=1", "col1=2");
        Map<String, Partition> partitions = metastore.getPartitionsByNames("db1", "table1", partitionNames);
        HiveTable hiveTable = (HiveTable) hiveMetadata.getTable(new ConnectContext(), "db1", "table1");

        PartitionKey hivePartitionKey1 = PartitionUtil.createPartitionKey(
                Lists.newArrayList("1"), hiveTable.getPartitionColumns());
        PartitionKey hivePartitionKey2 = PartitionUtil.createPartitionKey(
                Lists.newArrayList("2"), hiveTable.getPartitionColumns());

        GetRemoteFilesParams params =
                GetRemoteFilesParams.newBuilder().setPartitionKeys(Lists.newArrayList(hivePartitionKey1, hivePartitionKey2))
                        .build();
        List<RemoteFileInfo> remoteFileInfos = hiveMetadata.getRemoteFiles(hiveTable, params);
        Assertions.assertEquals(2, remoteFileInfos.size());

        RemoteFileInfo fileInfo = remoteFileInfos.get(0);
        Assertions.assertEquals(RemoteFileInputFormat.ORC, fileInfo.getFormat());
        Assertions.assertEquals("hdfs://127.0.0.1:10000/hive.db/hive_tbl/col1=1", fileInfo.getFullPath());

        List<RemoteFileDesc> fileDescs = remoteFileInfos.get(0).getFiles();
        Assertions.assertNotNull(fileDescs);
        Assertions.assertEquals(1, fileDescs.size());

        RemoteFileDesc fileDesc = fileDescs.get(0);
        Assertions.assertNotNull(fileDesc);
        Assertions.assertNotNull(fileDesc.getTextFileFormatDesc());
        Assertions.assertEquals("", fileDesc.getCompression());
        Assertions.assertEquals(20, fileDesc.getLength());
        Assertions.assertTrue(fileDesc.isSplittable());

        List<RemoteFileBlockDesc> blockDescs = fileDesc.getBlockDescs();
        Assertions.assertEquals(1, blockDescs.size());
        RemoteFileBlockDesc blockDesc = blockDescs.get(0);
        Assertions.assertEquals(0, blockDesc.getOffset());
        Assertions.assertEquals(20, blockDesc.getLength());
        Assertions.assertEquals(2, blockDesc.getReplicaHostIds().length);
    }

    @Test
    public void testGetFileWithSubdir() throws StarRocksConnectorException {
        RemotePathKey pathKey = new RemotePathKey("hdfs://127.0.0.1:10000/hive.db", true);
        Map<RemotePathKey, List<RemoteFileDesc>> files = hiveRemoteFileIO.getRemoteFiles(pathKey);
        List<RemoteFileDesc> remoteFileDescs = files.get(pathKey);
        Assertions.assertEquals(1, remoteFileDescs.size());
        Assertions.assertEquals("hive_tbl/000000_0", remoteFileDescs.get(0).getFileName());
    }

    @Test
    public void testGetTableStatisticsWithUnknown() throws AnalysisException {
        optimizerContext.getSessionVariable().setEnableHiveColumnStats(false);
        HiveTable hiveTable = (HiveTable) hmsOps.getTable("db1", "table1");
        ColumnRefOperator partColumnRefOperator = new ColumnRefOperator(0, Type.INT, "col1", true);
        ColumnRefOperator dataColumnRefOperator = new ColumnRefOperator(1, Type.INT, "col2", true);
        PartitionKey hivePartitionKey1 = PartitionUtil.createPartitionKey(
                Lists.newArrayList("1"), hiveTable.getPartitionColumns());
        PartitionKey hivePartitionKey2 = PartitionUtil.createPartitionKey(
                Lists.newArrayList("2"), hiveTable.getPartitionColumns());

        Map<ColumnRefOperator, Column> columns = new HashMap<>();
        columns.put(partColumnRefOperator, null);
        columns.put(dataColumnRefOperator, null);
        Statistics statistics = hiveMetadata.getTableStatistics(optimizerContext, hiveTable, columns,
                Lists.newArrayList(hivePartitionKey1, hivePartitionKey2), null, -1, TableVersionRange.empty());
        Assertions.assertEquals(Config.default_statistics_output_row_count, statistics.getOutputRowCount(), 0.001);
        Assertions.assertEquals(2, statistics.getColumnStatistics().size());
        Assertions.assertTrue(statistics.getColumnStatistics().get(partColumnRefOperator).isUnknown());
        Assertions.assertTrue(statistics.getColumnStatistics().get(dataColumnRefOperator).isUnknown());
    }

    @Test
    public void testShowCreateHiveTbl() {
        HiveTable hiveTable = (HiveTable) hiveMetadata.getTable(new ConnectContext(), "db1", "table1");
        Assertions.assertEquals("CREATE TABLE `table1` (\n" +
                        "  `col2` int(11) DEFAULT NULL,\n" +
                        "  `col1` int(11) DEFAULT NULL\n" +
                        ")\n" +
                        "PARTITION BY (col1)\n" +
                        "PROPERTIES (\"hive.table.serde.lib\" = \"org.apache.hadoop.hive.ql.io.orc.OrcSerde\", \"totalSize\" = " +
                        "\"100\", \"hive.table.column.names\" = \"col2\", \"numRows\" = \"50\", \"hive.table.column.types\" = " +
                        "\"INT\", \"hive.table.input.format\" = \"org.apache.hadoop.hive.ql.io.orc.OrcInputFormat\", " +
                        "\"location\" = \"hdfs://127.0.0.1:10000/hive\");",
                AstToStringBuilder.getExternalCatalogTableDdlStmt(hiveTable));
    }

    @Test
    public void testGetTableStatisticsNormal() throws AnalysisException {
        HiveTable hiveTable = (HiveTable) hiveMetadata.getTable(new ConnectContext(), "db1", "table1");
        ColumnRefOperator partColumnRefOperator = new ColumnRefOperator(0, Type.INT, "col1", true);
        ColumnRefOperator dataColumnRefOperator = new ColumnRefOperator(1, Type.INT, "col2", true);
        PartitionKey hivePartitionKey1 = PartitionUtil.createPartitionKey(
                Lists.newArrayList("1"), hiveTable.getPartitionColumns());
        PartitionKey hivePartitionKey2 = PartitionUtil.createPartitionKey(
                Lists.newArrayList("2"), hiveTable.getPartitionColumns());
        Map<ColumnRefOperator, Column> columns = new HashMap<>();
        columns.put(partColumnRefOperator, null);
        columns.put(dataColumnRefOperator, null);

        Statistics statistics = hiveMetadata.getTableStatistics(
                optimizerContext, hiveTable, columns, Lists.newArrayList(hivePartitionKey1, hivePartitionKey2),
                null, -1, TableVersionRange.empty());
        Assertions.assertEquals(1, statistics.getOutputRowCount(), 0.001);
        Assertions.assertEquals(2, statistics.getColumnStatistics().size());

        cachingHiveMetastore.getPartitionStatistics(hiveTable, Lists.newArrayList("col1=1", "col1=2"));
        statistics = hiveMetadata.getTableStatistics(optimizerContext, hiveTable, columns,
                Lists.newArrayList(hivePartitionKey1, hivePartitionKey2), null, -1, TableVersionRange.empty());

        Assertions.assertEquals(100, statistics.getOutputRowCount(), 0.001);
        Map<ColumnRefOperator, ColumnStatistic> columnStatistics = statistics.getColumnStatistics();
        ColumnStatistic partitionColumnStats = columnStatistics.get(partColumnRefOperator);
        Assertions.assertEquals(1, partitionColumnStats.getMinValue(), 0.001);
        Assertions.assertEquals(2, partitionColumnStats.getMaxValue(), 0.001);
        Assertions.assertEquals(0, partitionColumnStats.getNullsFraction(), 0.001);
        Assertions.assertEquals(4, partitionColumnStats.getAverageRowSize(), 0.001);
        Assertions.assertEquals(2, partitionColumnStats.getDistinctValuesCount(), 0.001);

        ColumnStatistic dataColumnStats = columnStatistics.get(dataColumnRefOperator);
        Assertions.assertEquals(0, dataColumnStats.getMinValue(), 0.001);
        Assertions.assertEquals(0.03, dataColumnStats.getNullsFraction(), 0.001);
        Assertions.assertEquals(4, dataColumnStats.getAverageRowSize(), 0.001);
        Assertions.assertEquals(5, dataColumnStats.getDistinctValuesCount(), 0.001);
    }

    @Test
    public void createDbTest() throws AlreadyExistsException {
        ExceptionChecker.expectThrowsWithMsg(AlreadyExistsException.class,
                "Database Already Exists",
                () -> hiveMetadata.createDb(connectContext, "db1", new HashMap<>()));

        Map<String, String> conf = new HashMap<>();
        conf.put("location", "abs://xxx/zzz");
        ExceptionChecker.expectThrowsWithMsg(StarRocksConnectorException.class,
                "Invalid location URI: abs://xxx/zzz",
                () -> hiveMetadata.createDb(connectContext, "db3", conf));

        conf.clear();
        conf.put("not_support_prop", "xxx");
        ExceptionChecker.expectThrowsWithMsg(IllegalArgumentException.class,
                "Unrecognized property: not_support_prop",
                () -> hiveMetadata.createDb(connectContext, "db3", conf));

        conf.clear();
        hiveMetadata.createDb(connectContext, "db4", conf);
    }

    @Test
    public void dropDbTest() {
        ExceptionChecker.expectThrowsWithMsg(StarRocksConnectorException.class,
                "Database d1 not empty",
                () -> hiveMetadata.dropDb(new ConnectContext(), "d1", true));

        ExceptionChecker.expectThrowsWithMsg(MetaNotFoundException.class,
                "Failed to access database empty_db",
                () -> hiveMetadata.dropDb(new ConnectContext(), "empty_db", true));
    }

    @Test
    public void testMetastoreType() {
        Assertions.assertEquals(MetastoreType.HMS, MetastoreType.get("hive"));
        Assertions.assertEquals(MetastoreType.GLUE, MetastoreType.get("glue"));
        Assertions.assertEquals(MetastoreType.DLF, MetastoreType.get("dlf"));
    }

    @Test
    public void testDropTable() throws DdlException {
        TableName tableName = new TableName("hive_catalog", "hive_db", "hive_table");
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "Table location will be cleared. 'Force' must be set when dropping a hive table." +
                        " Please execute 'drop table hive_catalog.hive_db.hive_table force",
                () -> hiveMetadata.dropTable(connectContext, new DropTableStmt(false, tableName, false)));

        hiveMetadata.dropTable(connectContext, new DropTableStmt(false, tableName, true));

        new MockUp<HiveMetadata>() {
            @Mock
            public Table getTable(String dbName, String tblName) {
                return null;
            }
        };

        hiveMetadata.dropTable(connectContext, new DropTableStmt(true, tableName, true));
    }

    @Test
    public void testFinishSink() {
        assertThrows(StarRocksConnectorException.class, () -> {
            String stagingDir = "hdfs://127.0.0.1:10000/tmp/starrocks/queryid";
            THiveFileInfo fileInfo = new THiveFileInfo();
            fileInfo.setFile_name("myfile.parquet");
            fileInfo.setPartition_path("hdfs://127.0.0.1:10000/tmp/starrocks/queryid/col1=2");
            fileInfo.setRecord_count(10);
            fileInfo.setFile_size_in_bytes(100);
            TSinkCommitInfo tSinkCommitInfo = new TSinkCommitInfo();
            tSinkCommitInfo.setStaging_dir(stagingDir);
            tSinkCommitInfo.setIs_overwrite(false);
            tSinkCommitInfo.setHive_file_info(fileInfo);
            hiveMetadata.finishSink("hive_db", "hive_table", Lists.newArrayList(), null);
            hiveMetadata.finishSink("hive_db", "hive_table", Lists.newArrayList(tSinkCommitInfo), null);
        });
    }

    @Test
    public void testAbortSink() {
        TSinkCommitInfo tSinkCommitInfo = new TSinkCommitInfo();
        hiveMetadata.abortSink("hive_db", "hive_table", Lists.newArrayList());
        hiveMetadata.abortSink("hive_db", "hive_table", Lists.newArrayList(tSinkCommitInfo));

        THiveFileInfo fileInfo = new THiveFileInfo();
        fileInfo.setFile_name("myfile.parquet");
        fileInfo.setPartition_path("hdfs://127.0.0.1:10000/tmp/starrocks/queryid/col1=2");
        fileInfo.setRecord_count(10);
        fileInfo.setFile_size_in_bytes(100);
        tSinkCommitInfo.setHive_file_info(fileInfo);
        hiveMetadata.abortSink("hive_db", "hive_table", Lists.newArrayList(tSinkCommitInfo));
    }

    @Test
    public void testAddPartition() {
        assertThrows(StarRocksConnectorException.class, () -> {
            String stagingDir = "hdfs://127.0.0.1:10000/tmp/starrocks/queryid";
            THiveFileInfo fileInfo = new THiveFileInfo();
            fileInfo.setFile_name("myfile.parquet");
            fileInfo.setPartition_path("hdfs://127.0.0.1:10000/tmp/starrocks/queryid/col1=2");
            fileInfo.setRecord_count(10);
            fileInfo.setFile_size_in_bytes(100);
            TSinkCommitInfo tSinkCommitInfo = new TSinkCommitInfo();
            tSinkCommitInfo.setStaging_dir(stagingDir);
            tSinkCommitInfo.setIs_overwrite(false);
            tSinkCommitInfo.setHive_file_info(fileInfo);

            new MockUp<HiveMetastoreOperations>() {
                @Mock
                public boolean partitionExists(String dbName, String tableName, List<String> partitionValues) {
                    return false;
                }
            };

            new MockUp<RemoteFileOperations>() {
                @Mock
                public void renameDirectory(Path source, Path target, Runnable runWhenPathNotExist) {
                }
            };

            AnalyzeTestUtil.init();
            hiveMetadata.finishSink("hive_db", "hive_table", Lists.newArrayList(tSinkCommitInfo), null);

            new MockUp<HiveMetastoreOperations>() {
                @Mock
                public void addPartitions(String dbName, String tableName, List<HivePartitionWithStats> partitions) {
                    throw new StarRocksConnectorException("add partition failed");
                }
            };
            hiveMetadata.finishSink("hive_db", "hive_table", Lists.newArrayList(tSinkCommitInfo), null);
        });
    }

    @Test
    public void testAppendPartition() {
        assertThrows(StarRocksConnectorException.class, () -> {
            String stagingDir = "hdfs://127.0.0.1:10000/tmp/starrocks/queryid";
            THiveFileInfo fileInfo = new THiveFileInfo();
            fileInfo.setFile_name("myfile.parquet");
            fileInfo.setPartition_path("hdfs://127.0.0.1:10000/tmp/starrocks/queryid/col1=2");
            fileInfo.setRecord_count(10);
            fileInfo.setFile_size_in_bytes(100);
            TSinkCommitInfo tSinkCommitInfo = new TSinkCommitInfo();
            tSinkCommitInfo.setStaging_dir(stagingDir);
            tSinkCommitInfo.setIs_overwrite(false);
            tSinkCommitInfo.setHive_file_info(fileInfo);

            new MockUp<RemoteFileOperations>() {
                @Mock
                public void asyncRenameFiles(
                        List<CompletableFuture<?>> renameFileFutures,
                        AtomicBoolean cancelled,
                        Path writePath,
                        Path targetPath,
                        List<String> fileNames) {

                }
            };

            hiveMetadata.finishSink("hive_db", "hive_table", Lists.newArrayList(tSinkCommitInfo), null);

            new MockUp<HiveMetastoreOperations>() {
                @Mock
                public void updatePartitionStatistics(String dbName, String tableName, String partitionName,
                                                      Function<HivePartitionStats, HivePartitionStats> update) {
                    throw new StarRocksConnectorException("ERROR");
                }
            };
            hiveMetadata.finishSink("hive_db", "hive_table", Lists.newArrayList(tSinkCommitInfo), null);
        });
    }

    @Test
    public void testOverwritePartition() throws Exception {
        String stagingDir = "hdfs://127.0.0.1:10000/tmp/starrocks/queryid";
        THiveFileInfo fileInfo = new THiveFileInfo();
        fileInfo.setFile_name("myfile.parquet");
        fileInfo.setPartition_path("hdfs://127.0.0.1:10000/tmp/starrocks/queryid/col1=2");
        fileInfo.setRecord_count(10);
        fileInfo.setFile_size_in_bytes(100);
        TSinkCommitInfo tSinkCommitInfo = new TSinkCommitInfo();
        tSinkCommitInfo.setStaging_dir(stagingDir);
        tSinkCommitInfo.setIs_overwrite(true);
        tSinkCommitInfo.setHive_file_info(fileInfo);

        new MockUp<RemoteFileOperations>() {
            @Mock
            public void renameDirectory(Path source, Path target, Runnable runWhenPathNotExist) {
            }
        };

        AnalyzeTestUtil.init();
        hiveMetadata.finishSink("hive_db", "hive_table", Lists.newArrayList(tSinkCommitInfo), null);
    }

    @Test
    public void testAppendTable() throws Exception {
        String stagingDir = "hdfs://127.0.0.1:10000/tmp/starrocks/queryid";
        THiveFileInfo fileInfo = new THiveFileInfo();
        fileInfo.setFile_name("myfile.parquet");
        fileInfo.setPartition_path("hdfs://127.0.0.1:10000/tmp/starrocks/queryid/");
        fileInfo.setRecord_count(10);
        fileInfo.setFile_size_in_bytes(100);
        TSinkCommitInfo tSinkCommitInfo = new TSinkCommitInfo();
        tSinkCommitInfo.setStaging_dir(stagingDir);
        tSinkCommitInfo.setIs_overwrite(false);
        tSinkCommitInfo.setHive_file_info(fileInfo);

        new MockUp<RemoteFileOperations>() {
            @Mock
            public void asyncRenameFiles(
                    List<CompletableFuture<?>> renameFileFutures,
                    AtomicBoolean cancelled,
                    Path writePath,
                    Path targetPath,
                    List<String> fileNames) {

            }
        };

        AnalyzeTestUtil.init();
        hiveMetadata.finishSink("hive_db", "unpartitioned_table", Lists.newArrayList(tSinkCommitInfo), null);
    }

    @Test
    public void testOverwriteTable() throws Exception {
        String stagingDir = "hdfs://127.0.0.1:10000/tmp/starrocks/queryid";
        THiveFileInfo fileInfo = new THiveFileInfo();
        fileInfo.setFile_name("myfile.parquet");
        fileInfo.setPartition_path("hdfs://127.0.0.1:10000/tmp/starrocks/queryid/");
        fileInfo.setRecord_count(10);
        fileInfo.setFile_size_in_bytes(100);
        TSinkCommitInfo tSinkCommitInfo = new TSinkCommitInfo();
        tSinkCommitInfo.setStaging_dir(stagingDir);
        tSinkCommitInfo.setIs_overwrite(true);
        tSinkCommitInfo.setHive_file_info(fileInfo);

        new MockUp<RemoteFileOperations>() {
            @Mock
            public void renameDirectory(Path source, Path target, Runnable runWhenPathNotExist) {
            }
        };

        AnalyzeTestUtil.init();
        hiveMetadata.finishSink("hive_db", "unpartitioned_table", Lists.newArrayList(tSinkCommitInfo), null);
    }

    @Test
    public void tesRecursiveDeleteFiles(@Mocked HiveMetastoreOperations hmsOps,
                                        @Mocked RemoteFileOperations fileOps,
                                        @Mocked HiveTable hiveTable) throws Exception {
        HiveCommitter hiveCommitter = new HiveCommitter(hmsOps, fileOps, Executors.newSingleThreadExecutor(),
                Executors.newSingleThreadExecutor(), hiveTable, new Path("hdfs://hadoop01:9000/hive"));
        HiveCommitter.DeleteRecursivelyResult result = hiveCommitter.recursiveDeleteFiles(new Path("hdfs://aaa"), false);
        Assertions.assertTrue(result.dirNotExists());
        Assertions.assertTrue(result.getNotDeletedEligibleItems().isEmpty());

        new Expectations(fileOps) {
            {
                fileOps.pathExists((Path) any);
                result = new StarRocksConnectorException("ERROR");
                minTimes = 1;
            }
        };

        result = hiveCommitter.recursiveDeleteFiles(new Path("hdfs://aaa"), false);
        Assertions.assertFalse(result.dirNotExists());
        Assertions.assertEquals(Lists.newArrayList("hdfs://aaa/*"), result.getNotDeletedEligibleItems());

        AnalyzeTestUtil.init();

        new Expectations(fileOps) {
            {
                fileOps.pathExists((Path) any);
                result = true;
                minTimes = 1;

                fileOps.listStatus((Path) any);
                result = new StarRocksConnectorException("ERROR");
                minTimes = 1;
            }
        };
        result = hiveCommitter.recursiveDeleteFiles(new Path("hdfs://aaa"), false);
        Assertions.assertFalse(result.dirNotExists());
        Assertions.assertEquals(Lists.newArrayList("hdfs://aaa/*"), result.getNotDeletedEligibleItems());

        Path path = new Path("hdfs://hadoop01:9000/user/hive/warehouse/t1/my.parquet");
        FileStatus fileStatus = new FileStatus(10, false, 0, 0, 0, 0, null, null, null, path);
        FileStatus[] mockedStatus = new FileStatus[1];
        mockedStatus[0] = fileStatus;

        new Expectations(fileOps) {
            {
                fileOps.listStatus((Path) any);
                result = mockedStatus;
                minTimes = 1;

                fileOps.deleteIfExists((Path) any, false);
                result = true;
                minTimes = 1;
            }
        };

        new MockUp<HiveWriteUtils>() {
            @Mock
            public boolean fileCreatedByQuery(String fileName, String queryId) {
                return true;
            }
        };

        result = hiveCommitter.recursiveDeleteFiles(new Path("hdfs://aaa"), false);
        Assertions.assertFalse(result.dirNotExists());
        Assertions.assertTrue(result.getNotDeletedEligibleItems().isEmpty());

        result = hiveCommitter.recursiveDeleteFiles(new Path("hdfs://aaa"), true);
        Assertions.assertTrue(result.dirNotExists());
        Assertions.assertTrue(result.getNotDeletedEligibleItems().isEmpty());

        new Expectations(fileOps) {
            {
                fileOps.deleteIfExists((Path) any, false);
                result = false;
                minTimes = 1;
            }
        };
        result = hiveCommitter.recursiveDeleteFiles(new Path("hdfs://aaa"), true);
        Assertions.assertFalse(result.dirNotExists());
        Assertions.assertEquals(fileStatus.getPath().toString(), result.getNotDeletedEligibleItems().get(0));

        new MockUp<HiveWriteUtils>() {
            @Mock
            public boolean fileCreatedByQuery(String fileName, String queryId) {
                return false;
            }
        };
        result = hiveCommitter.recursiveDeleteFiles(new Path("hdfs://aaa"), false);
        Assertions.assertFalse(result.dirNotExists());
        Assertions.assertTrue(result.getNotDeletedEligibleItems().isEmpty());

        FileStatus fileStatus1 = new FileStatus(10, true, 0, 0, 0, 0, null, null, null, path);
        FileStatus[] mockedStatus1 = new FileStatus[1];
        mockedStatus1[0] = fileStatus1;

        new Expectations(fileOps) {
            {
                fileOps.listStatus(new Path("hdfs://aaa"));
                result = mockedStatus1;
                minTimes = 1;

                fileOps.listStatus((Path) any);
                result = mockedStatus;
                minTimes = 1;
            }
        };
        result = hiveCommitter.recursiveDeleteFiles(new Path("hdfs://aaa"), false);
        Assertions.assertFalse(result.dirNotExists());
        Assertions.assertTrue(result.getNotDeletedEligibleItems().isEmpty());
    }

    @Test
    public void testHiveCommitterPrepare(@Mocked HiveMetastoreOperations hmsOps,
                                         @Mocked RemoteFileOperations fileOps,
                                         @Mocked HiveTable hiveTable) {
        assertThrows(StarRocksConnectorException.class, () -> {
            HiveCommitter hiveCommitter = new HiveCommitter(hmsOps, fileOps, Executors.newSingleThreadExecutor(),
                    Executors.newSingleThreadExecutor(), hiveTable, new Path("hdfs://hadoop01:9000/hive"));
            new Expectations() {
                {
                    hiveTable.isUnPartitioned();
                    result = true;
                    minTimes = 1;
                }
            };

            PartitionUpdate pu1 = new PartitionUpdate("", null, null, null, 1, 1);
            PartitionUpdate pu2 = new PartitionUpdate("", null, null, null, 1, 1);
            hiveCommitter.prepare(Lists.newArrayList(pu1, pu2));
        });
    }

    @Test
    public void testGetRelativePathIfDescendant(@Mocked HiveMetastoreOperations hmsOps,
                                                @Mocked RemoteFileOperations fileOps,
                                                @Mocked HiveTable hiveTable) throws Exception {
        HiveCommitter hiveCommitter = new HiveCommitter(hmsOps, fileOps, Executors.newSingleThreadExecutor(),
                Executors.newSingleThreadExecutor(), hiveTable, new Path("/tmp/staging"));

        java.lang.reflect.Method method =
                HiveCommitter.class.getDeclaredMethod("getRelativePathIfDescendant", Path.class, Path.class);
        method.setAccessible(true);

        Optional<String> relative = (Optional<String>) method.invoke(hiveCommitter,
                new Path("hdfs://127.0.0.1/base"), new Path("hdfs://127.0.0.1/base/a/b"));
        Assertions.assertTrue(relative.isPresent());
        Assertions.assertEquals("a/b", relative.get());

        Optional<String> notDescendant = (Optional<String>) method.invoke(hiveCommitter,
                new Path("hdfs://127.0.0.1/base"), new Path("hdfs://127.0.0.1/other"));
        Assertions.assertFalse(notDescendant.isPresent());

        Optional<String> samePath = (Optional<String>) method.invoke(hiveCommitter,
                new Path("hdfs://127.0.0.1/base"), new Path("hdfs://127.0.0.1/base"));
        Assertions.assertFalse(samePath.isPresent());
    }

    @Test
    public void testPrepareOverwriteTableWithRelativeStaging(@Mocked HiveMetastoreOperations hmsOps,
                                                             @Mocked RemoteFileOperations fileOps,
                                                             @Mocked HiveTable hiveTable) throws Exception {
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        ctx.setQueryId(UUIDUtil.genUUID());
        ctx.setThreadLocalInfo();

        try {
            Path targetPath = new Path("hdfs://127.0.0.1:10000/warehouse/table");
            Path writePath = new Path("hdfs://127.0.0.1:10000/warehouse/table/_staging/abc");
            PartitionUpdate partitionUpdate = new PartitionUpdate("", writePath, targetPath,
                    Lists.newArrayList("file"), 1, 1);
            HiveCommitter hiveCommitter = new HiveCommitter(hmsOps, fileOps, Executors.newSingleThreadExecutor(),
                    Executors.newSingleThreadExecutor(), hiveTable, new Path("hdfs://127.0.0.1:10000/warehouse/table/_staging"));

            String queryId = ctx.getQueryId().toString();
            Path oldTableStagingPath = new Path(targetPath.getParent(), "_temp_" + targetPath.getName() + "_" + queryId);
            Path expectedSource = new Path(oldTableStagingPath, "_staging/abc");

            new Expectations() {
                {
                    fileOps.renameDirectory(targetPath, oldTableStagingPath, (Runnable) any);
                    times = 1;
                    fileOps.renameDirectory(expectedSource, targetPath, (Runnable) any);
                    times = 1;
                    hiveTable.getCatalogDBName();
                    result = "db";
                    minTimes = 0;
                    hiveTable.getCatalogTableName();
                    result = "table";
                    minTimes = 0;
                }
            };

            java.lang.reflect.Method method = HiveCommitter.class.getDeclaredMethod(
                    "prepareOverwriteTable", PartitionUpdate.class, HivePartitionStats.class);
            method.setAccessible(true);
            method.invoke(hiveCommitter, partitionUpdate, HivePartitionStats.fromCommonStats(1, 1, 1));
        } finally {
            ConnectContext.remove();
        }
    }

    @Test
    public void testIsSamePartition() {
        Assertions.assertFalse(HiveCommitter.checkIsSamePartition(null, null));
        Assertions.assertFalse(HiveCommitter.checkIsSamePartition(new Partition(new HashMap<>(), null, null, null, false), null));

        Map<String, String> map = new HashMap<>();
        map.put(STARROCKS_QUERY_ID, "abcd");
        Partition remotePartition = new Partition(map, null, null, null, false);
        HivePartition hivePartition = new HivePartition(null, null, null, null, null, null, map, new HashMap<>());
        Assertions.assertTrue(HiveCommitter.checkIsSamePartition(remotePartition, hivePartition));
    }

    @Test
    public void testCreateTableTimeout() throws Exception {
        AnalyzeTestUtil.init();
        String stmt = "create table hive_catalog.hive_db.hive_table (k1 int, k2 int)";
        String sql =
                "CREATE EXTERNAL CATALOG hive_catalog PROPERTIES(\"type\"=\"hive\", \"hive.metastore.uris\"=\"thrift://127.0.0" +
                        ".1:9083\")";
        StarRocksAssert starRocksAssert = getStarRocksAssert();
        starRocksAssert.withCatalog(sql);
        new MockUp<HiveMetadata>() {
            @Mock
            public Database getDb(ConnectContext context, String dbName) {
                return new Database();
            }
        };

        new MockUp<HiveMetastoreOperations>() {
            @Mock
            public Path getDefaultLocation(String dbName, String tableName) {
                return new Path("xxxxx");
            }

            @Mock
            public boolean tableExists(String dbName, String tableName) {
                return false;
            }
        };

        new MockUp<HiveWriteUtils>() {
            @Mock
            public void createDirectory(Path path, Configuration conf) {

            }
        };

        new MockUp<CachingHiveMetastore>() {
            @Mock
            public void createTable(String dbName, Table table) {
                throw new StarRocksConnectorException("timeout");
            }
        };

        CreateTableStmt createTableStmt =
                (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(stmt, AnalyzeTestUtil.getConnectContext());

        new MockUp<HiveMetastoreOperations>() {
            @Mock
            public boolean tableExists(String dbName, String tableName) {
                return true;
            }
        };
        Assertions.assertTrue(hiveMetadata.createTable(connectContext, createTableStmt));
    }

    @Test
    public void testGetRemotePartitions(
            @Mocked HiveTable table,
            @Mocked HiveMetastoreOperations hmsOps) {
        List<String> partitionNames = Lists.newArrayList("dt=20200101", "dt=20200102", "dt=20200103");
        Map<String, Partition> partitionMap = Maps.newHashMap();
        for (String name : partitionNames) {
            Map<String, String> parameters = Maps.newHashMap();
            TextFileFormatDesc formatDesc = new TextFileFormatDesc("a", "b", "c", "d");
            String fullPath = "hdfs://path_to_table/" + name;
            Partition partition = new Partition(parameters, RemoteFileInputFormat.PARQUET, formatDesc, fullPath, true);
            partitionMap.put(name, partition);
        }
        new Expectations() {
            {
                hmsOps.getPartitionByNames((Table) any, (List<String>) any);
                result = partitionMap;
                minTimes = 1;
            }
        };

        List<PartitionInfo> partitionInfoList = hiveMetadata.getRemotePartitions(table, partitionNames);
        Assertions.assertEquals(3, partitionInfoList.size());
    }

    @Test
    public void testGetRemoteFiles(
            @Mocked HiveTable table,
            @Mocked HiveMetastoreOperations hmsOps) {
        List<String> partitionNames = Lists.newArrayList("dt=20200101", "dt=20200102", "dt=20200103");
        Map<String, Partition> partitionMap = Maps.newHashMap();
        for (String name : partitionNames) {
            Map<String, String> parameters = Maps.newHashMap();
            TextFileFormatDesc formatDesc = new TextFileFormatDesc("a", "b", "c", "d");
            String fullPath = HDFS_HIVE_TABLE;
            Partition partition = new Partition(parameters, RemoteFileInputFormat.PARQUET, formatDesc, fullPath, true);
            partitionMap.put(name, partition);
        }

        new Expectations() {
            {
                hmsOps.getPartitionByNames((Table) any, (List<String>) any);
                result = partitionMap;
                minTimes = 1;
            }
        };

        GetRemoteFilesParams params = GetRemoteFilesParams.newBuilder().setPartitionNames(partitionNames).build();
        List<RemoteFileInfo> remoteFileInfos = hiveMetadata.getRemoteFiles(table, params);
        Assertions.assertEquals(3, remoteFileInfos.size());
    }

    @Test
    public void testBuildHivePartition() throws Exception {
        // Setup ConnectContext with queryId
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        ctx.setQueryId(UUIDUtil.genUUID());
        // ctx.setExecutionId(new com.starrocks.thrift.TUniqueId(100, 200));
        ctx.setThreadLocalInfo();

        // Get a HiveTable from the mocked metadata
        HiveTable hiveTable = (HiveTable) hiveMetadata.getTable(new ConnectContext(), "db1", "table1");

        // Create PartitionUpdate
        String partitionName = "col1=1";
        Path writePath = new Path("hdfs://127.0.0.1:10000/tmp/staging/col1=1");
        Path targetPath = new Path("hdfs://127.0.0.1:10000/hive.db/hive_tbl/col1=1");
        List<String> fileNames = Lists.newArrayList("file1.parquet", "file2.parquet");
        PartitionUpdate partitionUpdate =
                new PartitionUpdate(partitionName, writePath, targetPath, fileNames, 100L, 2000L);

        // Create HiveCommitter
        HiveCommitter hiveCommitter = new HiveCommitter(hmsOps, fileOps, Executors.newSingleThreadExecutor(),
                Executors.newSingleThreadExecutor(), hiveTable, new Path("hdfs://127.0.0.1:10000/tmp/staging"));

        // Use reflection to call private buildHivePartition method
        java.lang.reflect.Method method =
                HiveCommitter.class.getDeclaredMethod("buildHivePartition", PartitionUpdate.class);
        method.setAccessible(true);
        HivePartition hivePartition = (HivePartition) method.invoke(hiveCommitter, partitionUpdate);

        // Verify the result
        Assertions.assertNotNull(hivePartition);
        Assertions.assertEquals("db1", hivePartition.getDatabaseName());
        Assertions.assertEquals("table1", hivePartition.getTableName());
        Assertions.assertEquals(Lists.newArrayList("1"), hivePartition.getValues());
        Assertions.assertEquals(targetPath.toString(), hivePartition.getLocation());
        Assertions.assertNotNull(hivePartition.getParameters());
        Assertions.assertTrue(hivePartition.getParameters().containsKey("starrocks_version"));
        Assertions.assertTrue(hivePartition.getParameters().containsKey(STARROCKS_QUERY_ID));
        Assertions.assertEquals(ctx.getQueryId().toString(), hivePartition.getParameters().get(STARROCKS_QUERY_ID));
        Assertions.assertNotNull(hivePartition.getSerDeParameters());
        Assertions.assertEquals(hiveTable.getSerdeProperties(), hivePartition.getSerDeParameters());
        Assertions.assertEquals(hiveTable.getStorageFormat(), hivePartition.getStorage());
        Assertions.assertEquals(hiveTable.getDataColumnNames().size(), hivePartition.getColumns().size());

        // Clean up
        ConnectContext.remove();
    }

    @Test
    public void testAddPartitionInHiveMetadata() throws Exception {
        // Setup ConnectContext with queryId
        ConnectContext ctx = UtFrameUtils.createDefaultCtx();
        ctx.setQueryId(UUIDUtil.genUUID());
        ctx.setThreadLocalInfo();

        // Get a HiveTable from the mocked metadata
        HiveTable hiveTable = (HiveTable) hiveMetadata.getTable(new ConnectContext(), "db1", "table1");

        // Create SingleItemListPartitionDesc
        List<String> partitionValues = Lists.newArrayList("1");
        SingleItemListPartitionDesc partitionDesc =
                new SingleItemListPartitionDesc(false, "p1", partitionValues, new HashMap<>());

        // Create AddPartitionClause
        AddPartitionClause addPartitionClause = new AddPartitionClause(partitionDesc, null, new HashMap<>(), false);

        // Create AlterTableStmt
        TableName tableName = new TableName("hive_catalog", "db1", "table1");
        List<AlterClause> alterClauses = Lists.newArrayList(addPartitionClause);
        AlterTableStmt alterTableStmt = new AlterTableStmt(tableName, alterClauses);

        // Mock hmsOps.addPartitions to verify it's called
        final AtomicBoolean addPartitionsCalled = new AtomicBoolean(false);
        final List<HivePartitionWithStats> capturedPartitions = Lists.newArrayList();
        new Expectations(hmsOps) {
            {
                hmsOps.addPartitions("db1", "table1", (List<HivePartitionWithStats>) any);
                result = new mockit.Delegate() {
                    @SuppressWarnings("unused")
                    void addPartitions(String dbName, String tableName, List<HivePartitionWithStats> partitions) {
                        addPartitionsCalled.set(true);
                        capturedPartitions.addAll(partitions);
                    }
                };
                minTimes = 1;
            }
        };

        // Use reflection to call private addPartition method
        java.lang.reflect.Method method = HiveMetadata.class.getDeclaredMethod(
                "addPartition", ConnectContext.class, AlterTableStmt.class, AlterClause.class);
        method.setAccessible(true);
        method.invoke(hiveMetadata, ctx, alterTableStmt, addPartitionClause);

        // Verify addPartitions was called
        Assertions.assertTrue(addPartitionsCalled.get());
        Assertions.assertEquals(1, capturedPartitions.size());

        // Verify the partition details
        HivePartitionWithStats partitionWithStats = capturedPartitions.get(0);
        Assertions.assertEquals("col1=1", partitionWithStats.getPartitionName());
        HivePartition hivePartition = partitionWithStats.getHivePartition();
        Assertions.assertNotNull(hivePartition);
        Assertions.assertEquals("db1", hivePartition.getDatabaseName());
        Assertions.assertEquals("table1", hivePartition.getTableName());
        Assertions.assertEquals(partitionValues, hivePartition.getValues());
        Assertions.assertEquals(hiveTable.getTableLocation() + "/col1=1", hivePartition.getLocation());
        Assertions.assertNotNull(hivePartition.getParameters());
        Assertions.assertTrue(hivePartition.getParameters().containsKey("starrocks_version"));
        Assertions.assertTrue(hivePartition.getParameters().containsKey(STARROCKS_QUERY_ID));
        Assertions.assertEquals(ctx.getQueryId().toString(), hivePartition.getParameters().get(STARROCKS_QUERY_ID));
        Assertions.assertNotNull(hivePartition.getSerDeParameters());
        Assertions.assertEquals(hiveTable.getSerdeProperties(), hivePartition.getSerDeParameters());
        Assertions.assertEquals(hiveTable.getStorageFormat(), hivePartition.getStorage());
        Assertions.assertEquals(hiveTable.getDataColumnNames().size(), hivePartition.getColumns().size());

        // Clean up
        ConnectContext.remove();
    }
}
