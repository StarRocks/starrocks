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
import com.starrocks.common.DdlException;
import com.starrocks.common.ExceptionChecker;
import com.starrocks.common.FeConstants;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.connector.CachingRemoteFileIO;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.MetastoreType;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.connector.RemoteFileBlockDesc;
import com.starrocks.connector.RemoteFileDesc;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.connector.RemoteFileOperations;
import com.starrocks.connector.RemotePathKey;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.sql.ast.DropTableStmt;
import com.starrocks.sql.optimizer.Memo;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.thrift.THiveFileInfo;
import com.starrocks.thrift.TSinkCommitInfo;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

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

    @Before
    public void setUp() throws Exception {
        FeConstants.runningUnitTest = true;
        executorForHmsRefresh = Executors.newFixedThreadPool(5);
        executorForRemoteFileRefresh = Executors.newFixedThreadPool(5);
        executorForPullFiles = Executors.newFixedThreadPool(5);

        client = new HiveMetastoreTest.MockedHiveMetaClient();
        metastore = new HiveMetastore(client, "hive_catalog", MetastoreType.HMS);
        cachingHiveMetastore = CachingHiveMetastore.createCatalogLevelInstance(
                metastore, executorForHmsRefresh, 100, 10, 1000, false);
        hmsOps = new HiveMetastoreOperations(cachingHiveMetastore, true, new Configuration(), MetastoreType.HMS, "hive_catalog");

        hiveRemoteFileIO = new HiveRemoteFileIO(new Configuration());
        FileSystem fs = new MockedRemoteFileSystem(HDFS_HIVE_TABLE);
        hiveRemoteFileIO.setFileSystem(fs);
        cachingRemoteFileIO = CachingRemoteFileIO.createCatalogLevelInstance(
                hiveRemoteFileIO, executorForRemoteFileRefresh, 100, 10, 10);
        fileOps = new RemoteFileOperations(cachingRemoteFileIO, executorForPullFiles, executorForPullFiles,
                false, true, new Configuration());
        statisticsProvider = new HiveStatisticsProvider(hmsOps, fileOps);

        UtFrameUtils.createMinStarRocksCluster();
        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        columnRefFactory = new ColumnRefFactory();
        optimizerContext = new OptimizerContext(new Memo(), columnRefFactory, connectContext);
        hiveMetadata = new HiveMetadata("hive_catalog", new HdfsEnvironment(), hmsOps, fileOps, statisticsProvider,
                Optional.empty(), executorForHmsRefresh, executorForHmsRefresh);
    }

    @After
    public void tearDown() {
        executorForHmsRefresh.shutdown();
        executorForRemoteFileRefresh.shutdown();
        executorForPullFiles.shutdown();
    }

    @Test
    public void testListDbNames() {
        List<String> databaseNames = hiveMetadata.listDbNames();
        Assert.assertEquals(Lists.newArrayList("db1", "db2"), databaseNames);
        CachingHiveMetastore queryLevelCache = CachingHiveMetastore.createQueryLevelInstance(cachingHiveMetastore, 100);
        Assert.assertEquals(Lists.newArrayList("db1", "db2"), queryLevelCache.getAllDatabaseNames());
    }

    @Test
    public void testListTableNames() {
        List<String> databaseNames = hiveMetadata.listTableNames("db1");
        Assert.assertEquals(Lists.newArrayList("table1", "table2"), databaseNames);
    }

    @Test
    public void testGetPartitionKeys() {
        Assert.assertEquals(Lists.newArrayList("col1"), hiveMetadata.listPartitionNames("db1", "tbl1"));
    }

    @Test
    public void testGetDb() {
        Database database = hiveMetadata.getDb("db1");
        Assert.assertEquals("db1", database.getFullName());

    }

    @Test
    public void testGetTable() {
        com.starrocks.catalog.Table table = hiveMetadata.getTable("db1", "tbl1");
        HiveTable hiveTable = (HiveTable) table;
        Assert.assertEquals("db1", hiveTable.getDbName());
        Assert.assertEquals("tbl1", hiveTable.getTableName());
        Assert.assertEquals(Lists.newArrayList("col1"), hiveTable.getPartitionColumnNames());
        Assert.assertEquals(Lists.newArrayList("col2"), hiveTable.getDataColumnNames());
        Assert.assertEquals("hdfs://127.0.0.1:10000/hive", hiveTable.getTableLocation());
        Assert.assertEquals(ScalarType.INT, hiveTable.getPartitionColumns().get(0).getType());
        Assert.assertEquals(ScalarType.INT, hiveTable.getBaseSchema().get(0).getType());
        Assert.assertEquals("hive_catalog", hiveTable.getCatalogName());
    }

    @Test
    public void testTableExists() {
        boolean exists = hiveMetadata.tableExists("db1", "tbl1");
        Assert.assertTrue(exists);
    }

    @Test
    public void testGetHiveRemoteFiles() throws AnalysisException {
        FeConstants.runningUnitTest = true;
        String tableLocation = "hdfs://127.0.0.1:10000/hive.db/hive_tbl";
        HiveMetaClient client = new HiveMetastoreTest.MockedHiveMetaClient();
        HiveMetastore metastore = new HiveMetastore(client, "hive_catalog", null);
        List<String> partitionNames = Lists.newArrayList("col1=1", "col1=2");
        Map<String, Partition> partitions = metastore.getPartitionsByNames("db1", "table1", partitionNames);
        HiveTable hiveTable = (HiveTable) hiveMetadata.getTable("db1", "table1");

        PartitionKey hivePartitionKey1 = PartitionUtil.createPartitionKey(
                Lists.newArrayList("1"), hiveTable.getPartitionColumns());
        PartitionKey hivePartitionKey2 = PartitionUtil.createPartitionKey(
                Lists.newArrayList("2"), hiveTable.getPartitionColumns());

        List<RemoteFileInfo> remoteFileInfos = hiveMetadata.getRemoteFileInfos(
                hiveTable, Lists.newArrayList(hivePartitionKey1, hivePartitionKey2), -1, null, null, -1);
        Assert.assertEquals(2, remoteFileInfos.size());

        RemoteFileInfo fileInfo = remoteFileInfos.get(0);
        Assert.assertEquals(RemoteFileInputFormat.ORC, fileInfo.getFormat());
        Assert.assertEquals("hdfs://127.0.0.1:10000/hive.db/hive_tbl/col1=1", fileInfo.getFullPath());

        List<RemoteFileDesc> fileDescs = remoteFileInfos.get(0).getFiles();
        Assert.assertNotNull(fileDescs);
        Assert.assertEquals(1, fileDescs.size());

        RemoteFileDesc fileDesc = fileDescs.get(0);
        Assert.assertNotNull(fileDesc);
        Assert.assertNotNull(fileDesc.getTextFileFormatDesc());
        Assert.assertEquals("", fileDesc.getCompression());
        Assert.assertEquals(20, fileDesc.getLength());
        Assert.assertTrue(fileDesc.isSplittable());

        List<RemoteFileBlockDesc> blockDescs = fileDesc.getBlockDescs();
        Assert.assertEquals(1, blockDescs.size());
        RemoteFileBlockDesc blockDesc = blockDescs.get(0);
        Assert.assertEquals(0, blockDesc.getOffset());
        Assert.assertEquals(20, blockDesc.getLength());
        Assert.assertEquals(2, blockDesc.getReplicaHostIds().length);
    }

    @Test
    public void testGetFileWithSubdir() throws StarRocksConnectorException {
        RemotePathKey pathKey = new RemotePathKey("hdfs://127.0.0.1:10000/hive.db", true, Optional.empty());
        Map<RemotePathKey, List<RemoteFileDesc>> files = hiveRemoteFileIO.getRemoteFiles(pathKey);
        List<RemoteFileDesc> remoteFileDescs = files.get(pathKey);
        Assert.assertEquals(1, remoteFileDescs.size());
        Assert.assertEquals("hive_tbl/000000_0", remoteFileDescs.get(0).getFileName());
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
                Lists.newArrayList(hivePartitionKey1, hivePartitionKey2), null, -1);
        Assert.assertEquals(1, statistics.getOutputRowCount(), 0.001);
        Assert.assertEquals(2, statistics.getColumnStatistics().size());
        Assert.assertTrue(statistics.getColumnStatistics().get(partColumnRefOperator).isUnknown());
        Assert.assertTrue(statistics.getColumnStatistics().get(dataColumnRefOperator).isUnknown());
    }

    @Test
    public void testGetTableStatisticsNormal() throws AnalysisException {
        HiveTable hiveTable = (HiveTable) hiveMetadata.getTable("db1", "table1");
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
                null, -1);
        Assert.assertEquals(1,  statistics.getOutputRowCount(), 0.001);
        Assert.assertEquals(2, statistics.getColumnStatistics().size());

        cachingHiveMetastore.getPartitionStatistics(hiveTable, Lists.newArrayList("col1=1", "col1=2"));
        statistics = hiveMetadata.getTableStatistics(optimizerContext, hiveTable, columns,
                Lists.newArrayList(hivePartitionKey1, hivePartitionKey2), null, -1);

        Assert.assertEquals(100, statistics.getOutputRowCount(), 0.001);
        Map<ColumnRefOperator, ColumnStatistic> columnStatistics = statistics.getColumnStatistics();
        ColumnStatistic partitionColumnStats = columnStatistics.get(partColumnRefOperator);
        Assert.assertEquals(1, partitionColumnStats.getMinValue(), 0.001);
        Assert.assertEquals(2, partitionColumnStats.getMaxValue(), 0.001);
        Assert.assertEquals(0, partitionColumnStats.getNullsFraction(), 0.001);
        Assert.assertEquals(4, partitionColumnStats.getAverageRowSize(), 0.001);
        Assert.assertEquals(2, partitionColumnStats.getDistinctValuesCount(), 0.001);

        ColumnStatistic dataColumnStats = columnStatistics.get(dataColumnRefOperator);
        Assert.assertEquals(0, dataColumnStats.getMinValue(), 0.001);
        Assert.assertEquals(0.03, dataColumnStats.getNullsFraction(), 0.001);
        Assert.assertEquals(4, dataColumnStats.getAverageRowSize(), 0.001);
        Assert.assertEquals(5, dataColumnStats.getDistinctValuesCount(), 0.001);
    }

    @Test
    public void createDbTest() throws AlreadyExistsException {
        ExceptionChecker.expectThrowsWithMsg(AlreadyExistsException.class,
                "Database Already Exists",
                () -> hiveMetadata.createDb("db1", new HashMap<>()));

        Map<String, String> conf = new HashMap<>();
        conf.put("location", "abs://xxx/zzz");
        ExceptionChecker.expectThrowsWithMsg(StarRocksConnectorException.class,
                "Invalid location URI: abs://xxx/zzz",
                () -> hiveMetadata.createDb("db3", conf));

        conf.clear();
        conf.put("not_support_prop", "xxx");
        ExceptionChecker.expectThrowsWithMsg(IllegalArgumentException.class,
                "Unrecognized property: not_support_prop",
                () -> hiveMetadata.createDb("db3", conf));

        conf.clear();
        hiveMetadata.createDb("db4", conf);
    }

    @Test
    public void dropDbTest() {
        ExceptionChecker.expectThrowsWithMsg(StarRocksConnectorException.class,
                "Database d1 not empty",
                () -> hiveMetadata.dropDb("d1", true));

        ExceptionChecker.expectThrowsWithMsg(MetaNotFoundException.class,
                "Failed to access database empty_db",
                () -> hiveMetadata.dropDb("empty_db", true));
    }

    @Test
    public void testMetastoreType() {
        Assert.assertEquals(MetastoreType.HMS, MetastoreType.get("hive"));
        Assert.assertEquals(MetastoreType.GLUE, MetastoreType.get("glue"));
        Assert.assertEquals(MetastoreType.DLF, MetastoreType.get("dlf"));
    }

    @Test
    public void testDropTable() throws DdlException {
        TableName tableName = new TableName("hive_catalog", "hive_db", "hive_table");
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                "Table location will be cleared. 'Force' must be set when dropping a hive table." +
                        " Please execute 'drop table hive_catalog.hive_db.hive_table force",
                () -> hiveMetadata.dropTable(new DropTableStmt(false, tableName, false)));

        hiveMetadata.dropTable(new DropTableStmt(false, tableName, true));

        new MockUp<HiveMetadata>() {
            @Mock
            public Table getTable(String dbName, String tblName) {
                return null;
            }
        };

        hiveMetadata.dropTable(new DropTableStmt(true, tableName, true));
    }

    @Test(expected = StarRocksConnectorException.class)
    public void testFinishSink() {
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
        hiveMetadata.finishSink("hive_db", "hive_table", Lists.newArrayList());
        hiveMetadata.finishSink("hive_db", "hive_table", Lists.newArrayList(tSinkCommitInfo));
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

    @Test(expected = StarRocksConnectorException.class)
    public void testAddPartition() throws Exception {
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
        hiveMetadata.finishSink("hive_db", "hive_table", Lists.newArrayList(tSinkCommitInfo));

        new MockUp<HiveMetastoreOperations>() {
            @Mock
            public void addPartitions(String dbName, String tableName, List<HivePartitionWithStats> partitions) {
                throw new StarRocksConnectorException("add partition failed");
            }
        };
        hiveMetadata.finishSink("hive_db", "hive_table", Lists.newArrayList(tSinkCommitInfo));
    }

    @Test(expected = StarRocksConnectorException.class)
    public void testAppendPartition() {
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

        hiveMetadata.finishSink("hive_db", "hive_table", Lists.newArrayList(tSinkCommitInfo));

        new MockUp<HiveMetastoreOperations>() {
            @Mock
            public void updatePartitionStatistics(String dbName, String tableName, String partitionName,
                                                  Function<HivePartitionStats, HivePartitionStats> update) {
                throw new StarRocksConnectorException("ERROR");
            }
        };
        hiveMetadata.finishSink("hive_db", "hive_table", Lists.newArrayList(tSinkCommitInfo));
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
        hiveMetadata.finishSink("hive_db", "hive_table", Lists.newArrayList(tSinkCommitInfo));
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
        hiveMetadata.finishSink("hive_db", "unpartitioned_table", Lists.newArrayList(tSinkCommitInfo));
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
        hiveMetadata.finishSink("hive_db", "unpartitioned_table", Lists.newArrayList(tSinkCommitInfo));
    }

    @Test
    public void tesRecursiveDeleteFiles(@Mocked HiveMetastoreOperations hmsOps,
                                        @Mocked RemoteFileOperations fileOps,
                                        @Mocked HiveTable hiveTable) throws Exception {
        HiveCommitter hiveCommitter = new HiveCommitter(hmsOps, fileOps, Executors.newSingleThreadExecutor(),
                Executors.newSingleThreadExecutor(), hiveTable, new Path("hdfs://hadoop01:9000/hive"));
        HiveCommitter.DeleteRecursivelyResult result = hiveCommitter.recursiveDeleteFiles(new Path("hdfs://aaa"), false);
        Assert.assertTrue(result.dirNotExists());
        Assert.assertTrue(result.getNotDeletedEligibleItems().isEmpty());

        new Expectations(fileOps) {
            {
                fileOps.pathExists((Path) any);
                result = new StarRocksConnectorException("ERROR");
                minTimes = 1;
            }
        };

        result = hiveCommitter.recursiveDeleteFiles(new Path("hdfs://aaa"), false);
        Assert.assertFalse(result.dirNotExists());
        Assert.assertEquals(Lists.newArrayList("hdfs://aaa/*"), result.getNotDeletedEligibleItems());

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
        Assert.assertFalse(result.dirNotExists());
        Assert.assertEquals(Lists.newArrayList("hdfs://aaa/*"), result.getNotDeletedEligibleItems());

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
        Assert.assertFalse(result.dirNotExists());
        Assert.assertTrue(result.getNotDeletedEligibleItems().isEmpty());

        result = hiveCommitter.recursiveDeleteFiles(new Path("hdfs://aaa"), true);
        Assert.assertTrue(result.dirNotExists());
        Assert.assertTrue(result.getNotDeletedEligibleItems().isEmpty());

        new Expectations(fileOps) {
            {
                fileOps.deleteIfExists((Path) any, false);
                result = false;
                minTimes = 1;
            }
        };
        result = hiveCommitter.recursiveDeleteFiles(new Path("hdfs://aaa"), true);
        Assert.assertFalse(result.dirNotExists());
        Assert.assertEquals(fileStatus.getPath().toString(), result.getNotDeletedEligibleItems().get(0));

        new MockUp<HiveWriteUtils>() {
            @Mock
            public boolean fileCreatedByQuery(String fileName, String queryId) {
                return false;
            }
        };
        result = hiveCommitter.recursiveDeleteFiles(new Path("hdfs://aaa"), false);
        Assert.assertFalse(result.dirNotExists());
        Assert.assertTrue(result.getNotDeletedEligibleItems().isEmpty());

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
        Assert.assertFalse(result.dirNotExists());
        Assert.assertTrue(result.getNotDeletedEligibleItems().isEmpty());
    }

    @Test(expected = StarRocksConnectorException.class)
    public void testHiveCommitterPrepare(@Mocked HiveMetastoreOperations hmsOps,
                                         @Mocked RemoteFileOperations fileOps,
                                         @Mocked HiveTable hiveTable) {
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
    }

    @Test
    public void testIsSamePartition() {
        Assert.assertFalse(HiveCommitter.checkIsSamePartition(null, null));
        Assert.assertFalse(HiveCommitter.checkIsSamePartition(new Partition(new HashMap<>(), null, null, null, false), null));

        Map<String, String> map = new HashMap<>();
        map.put(STARROCKS_QUERY_ID, "abcd");
        Partition remotePartition = new Partition(map, null, null, null, false);
        HivePartition hivePartition = new HivePartition(null, null, null, null, null, null, map);
        Assert.assertTrue(HiveCommitter.checkIsSamePartition(remotePartition, hivePartition));
    }
}
