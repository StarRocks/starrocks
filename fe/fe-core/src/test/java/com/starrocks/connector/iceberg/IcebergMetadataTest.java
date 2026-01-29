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
import com.starrocks.analysis.DescriptorTable;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.NullLiteral;
import com.starrocks.analysis.SlotRef;
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
import com.starrocks.common.StarRocksException;
import com.starrocks.connector.ConnectorMetadatRequestContext;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.ConnectorProperties;
import com.starrocks.connector.ConnectorTableVersion;
import com.starrocks.connector.ConnectorType;
import com.starrocks.connector.GetRemoteFilesParams;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.PartitionInfo;
import com.starrocks.connector.PlanMode;
import com.starrocks.connector.PointerType;
import com.starrocks.connector.PredicateSearchKey;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.connector.RemoteMetaSplit;
import com.starrocks.connector.SerializedMetaSpec;
import com.starrocks.connector.TableVersionRange;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.iceberg.hive.IcebergHiveCatalog;
import com.starrocks.connector.metadata.MetadataCollectJob;
import com.starrocks.connector.metadata.MetadataTableType;
import com.starrocks.connector.metadata.iceberg.IcebergMetadataCollectJob;
import com.starrocks.persist.EditLog;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.server.MetadataMgr;
import com.starrocks.server.TemporaryTableMgr;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.sql.analyzer.AstToStringBuilder;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AddColumnClause;
import com.starrocks.sql.ast.AddColumnsClause;
import com.starrocks.sql.ast.AddPartitionColumnClause;
import com.starrocks.sql.ast.AlterClause;
import com.starrocks.sql.ast.AlterTableCommentClause;
import com.starrocks.sql.ast.AlterTableOperationClause;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.ColumnDef;
import com.starrocks.sql.ast.ColumnRenameClause;
import com.starrocks.sql.ast.DropColumnClause;
import com.starrocks.sql.ast.DropPartitionColumnClause;
import com.starrocks.sql.ast.DropTableStmt;
import com.starrocks.sql.ast.ModifyColumnClause;
import com.starrocks.sql.ast.ModifyTablePropertiesClause;
import com.starrocks.sql.ast.TableRenameClause;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.OptimizerFactory;
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
import com.starrocks.thrift.TIcebergTable;
import com.starrocks.thrift.TResultSinkType;
import com.starrocks.thrift.TSinkCommitInfo;
import com.starrocks.thrift.TTableDescriptor;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
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
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdateProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.hive.HiveTableOperations;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;

import static com.starrocks.catalog.Table.TableType.ICEBERG;
import static com.starrocks.catalog.Type.DATE;
import static com.starrocks.catalog.Type.DATETIME;
import static com.starrocks.catalog.Type.INT;
import static com.starrocks.catalog.Type.STRING;
import static com.starrocks.catalog.Type.VARCHAR;
import static com.starrocks.connector.iceberg.IcebergCatalogProperties.ENABLE_DISTRIBUTED_PLAN_LOAD_DATA_FILE_COLUMN_STATISTICS_WITH_EQ_DELETE;
import static com.starrocks.connector.iceberg.IcebergCatalogProperties.HIVE_METASTORE_URIS;
import static com.starrocks.connector.iceberg.IcebergCatalogProperties.ICEBERG_CATALOG_TYPE;
import static com.starrocks.connector.iceberg.IcebergMetadata.COMPRESSION_CODEC;
import static com.starrocks.connector.iceberg.IcebergMetadata.FILE_FORMAT;
import static com.starrocks.connector.iceberg.IcebergMetadata.LOCATION_PROPERTY;
import static com.starrocks.connector.iceberg.IcebergTableOperation.REMOVE_ORPHAN_FILES;
import static com.starrocks.connector.iceberg.IcebergTableOperation.ROLLBACK_TO_SNAPSHOT;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class IcebergMetadataTest extends TableTestBase {
    private static final String CATALOG_NAME = "iceberg_catalog";
    public static final HdfsEnvironment HDFS_ENVIRONMENT = new HdfsEnvironment();

    public static final IcebergCatalogProperties DEFAULT_CATALOG_PROPERTIES;
    public static final Map<String, String> DEFAULT_CONFIG = new HashMap<>();
    public static ConnectContext connectContext;

    public static GetRemoteFilesParams emptyParams = GetRemoteFilesParams.newBuilder()
            .setSnapshotId(1)
            .setPredicate(ConstantOperator.TRUE)
            .build();

    static {
        DEFAULT_CONFIG.put(HIVE_METASTORE_URIS, "thrift://188.122.12.1:8732"); // non-exist ip, prevent to connect local service
        DEFAULT_CONFIG.put(ICEBERG_CATALOG_TYPE, "hive");
        DEFAULT_CATALOG_PROPERTIES = new IcebergCatalogProperties(DEFAULT_CONFIG);
    }

    @BeforeAll
    public static void beforeClass() throws Exception {
        connectContext = UtFrameUtils.createDefaultCtx();
    }

    @Test
    public void testListDatabaseNames(@Mocked IcebergCatalog icebergCatalog) {
        new Expectations() {
            {
                icebergCatalog.listAllDatabases(connectContext);
                result = Lists.newArrayList("db1", "db2");
                minTimes = 0;
            }
        };

        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergCatalog,
                Executors.newSingleThreadExecutor(), Executors.newSingleThreadExecutor(), null);
        List<String> expectResult = Lists.newArrayList("db1", "db2");
        Assertions.assertEquals(expectResult, metadata.listDbNames(connectContext));
    }

    @Test
    public void testGetDB(@Mocked IcebergHiveCatalog icebergHiveCatalog) {
        String db = "db";

        new Expectations() {
            {
                icebergHiveCatalog.getDB(connectContext, db);
                result = new Database(0, db);
                minTimes = 0;
            }
        };

        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                Executors.newSingleThreadExecutor(), Executors.newSingleThreadExecutor(), null);
        Database expectResult = new Database(0, db);
        Assertions.assertEquals(expectResult, metadata.getDb(connectContext, db));
    }

    @Test
    public void testGetNotExistDB(@Mocked IcebergHiveCatalog icebergHiveCatalog) {
        String db = "db";

        new Expectations() {
            {
                icebergHiveCatalog.getDB(connectContext, db);
                result = new NoSuchNamespaceException("database not found");
                minTimes = 0;
            }
        };

        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                Executors.newSingleThreadExecutor(), Executors.newSingleThreadExecutor(), null);
        Assertions.assertNull(metadata.getDb(connectContext, db));
    }

    @Test
    public void testListTableNames(@Mocked IcebergHiveCatalog icebergHiveCatalog) {
        String db1 = "db1";
        String tbl1 = "tbl1";
        String tbl2 = "tbl2";

        new Expectations() {
            {
                icebergHiveCatalog.listTables(connectContext, db1);
                result = Lists.newArrayList(tbl1, tbl2);
                minTimes = 0;
            }
        };

        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                Executors.newSingleThreadExecutor(), Executors.newSingleThreadExecutor(), null);
        List<String> expectResult = Lists.newArrayList("tbl1", "tbl2");
        Assertions.assertEquals(expectResult, metadata.listTableNames(connectContext, db1));
    }

    @Test
    public void testGetTable(@Mocked IcebergHiveCatalog icebergHiveCatalog,
                             @Mocked HiveTableOperations hiveTableOperations) {

        new Expectations() {
            {
                icebergHiveCatalog.getTable(connectContext, "db", "tbl");
                result = new BaseTable(hiveTableOperations, "tbl");
                minTimes = 0;
            }
        };

        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                Executors.newSingleThreadExecutor(), Executors.newSingleThreadExecutor(), null);
        Table actual = metadata.getTable(new ConnectContext(), "db", "tbl");
        Assertions.assertEquals("tbl", actual.getName());
        Assertions.assertEquals(ICEBERG, actual.getType());
    }

    @Test
    public void testGetTableWithUpperName(@Mocked IcebergHiveCatalog icebergHiveCatalog,
                                          @Mocked HiveTableOperations hiveTableOperations) {
        new Expectations() {
            {
                icebergHiveCatalog.getIcebergCatalogType();
                result = IcebergCatalogType.HIVE_CATALOG;
                minTimes = 0;

                icebergHiveCatalog.getTable(connectContext, "DB", "TBL");
                result = new BaseTable(hiveTableOperations, "tbl");
                minTimes = 0;
            }
        };

        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                Executors.newSingleThreadExecutor(), Executors.newSingleThreadExecutor(), null);
        Table actual = metadata.getTable(new ConnectContext(), "DB", "TBL");
        Assertions.assertTrue(actual instanceof IcebergTable);
        IcebergTable icebergTable = (IcebergTable) actual;
        Assertions.assertEquals("db", icebergTable.getCatalogDBName());
        Assertions.assertEquals("tbl", icebergTable.getCatalogTableName());
        Assertions.assertEquals(ICEBERG, icebergTable.getType());
    }

    @Test
    public void testShowCreateTableWithSortOrder(@Mocked IcebergHiveCatalog icebergHiveCatalog,
                                                 @Mocked HiveTableOperations hiveTableOperations) {
        new Expectations() {
            {
                icebergHiveCatalog.getIcebergCatalogType();
                result = IcebergCatalogType.HIVE_CATALOG;
                minTimes = 0;

                icebergHiveCatalog.getTable(connectContext, "DB", "TBL");
                result = new BaseTable(hiveTableOperations, "tbl");
                minTimes = 0;
            }
        };

        new MockUp<Table>() {
            @Mock
            public List<Column> getFullSchema() {
                return ImmutableList.of(new Column("c1", Type.INT), new Column("c2", STRING));
            }

            @Mock
            public Table.TableType getType() {
                return ICEBERG;
            }
        };

        new MockUp<IcebergTable>() {
            @Mock
            public boolean isUnPartitioned() {
                return false;
            }

            @Mock
            public List<Integer> getSortKeyIndexes() {
                return ImmutableList.of(0, 1);
            }

            @Mock
            public List<String> getPartitionColumnNamesWithTransform() {
                return ImmutableList.of("hour(`c1`)");
            }
        };

        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                Executors.newSingleThreadExecutor(), Executors.newSingleThreadExecutor(), null);
        Table actual = metadata.getTable(new ConnectContext(), "DB", "TBL");
        Assertions.assertTrue(actual instanceof IcebergTable);
        IcebergTable icebergTable = (IcebergTable) actual;

        Schema schema = IcebergApiConverter.toIcebergApiSchema(actual.getFullSchema());
        SortOrder.Builder builder = SortOrder.builderFor(schema);
        builder.asc("c1", NullOrder.NULLS_FIRST);
        builder.desc("c2", NullOrder.NULLS_LAST);
        SortOrder sortOrder = builder.build();

        org.apache.iceberg.Table nativeTable = icebergTable.getNativeTable();

        new Expectations() {
            {
                nativeTable.sortOrder();
                result = sortOrder;
                minTimes = 0;
            }
        };

        Assertions.assertEquals("db", icebergTable.getCatalogDBName());
        Assertions.assertEquals("tbl", icebergTable.getCatalogTableName());
        String createSql = AstToStringBuilder.getExternalCatalogTableDdlStmt(actual);
        Assertions.assertEquals("CREATE TABLE `tbl` (\n" +
                        "  `c1` int(11) DEFAULT NULL,\n" +
                        "  `c2` varchar(1048576) DEFAULT NULL\n" +
                        ")\n" +
                        "PARTITION BY hour(`c1`)\n" +
                        "ORDER BY (c1 ASC NULLS FIRST,c2 DESC NULLS LAST);",
                createSql);
    }

    @Test
    public void testIcebergHiveCatalogTableExists(@Mocked IcebergHiveCatalog icebergHiveCatalog) {
        new Expectations() {
            {
                icebergHiveCatalog.tableExists(connectContext, "db", "tbl");
                result = true;
                minTimes = 0;
            }
        };
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                Executors.newSingleThreadExecutor(), Executors.newSingleThreadExecutor(), null);
        Assertions.assertTrue(metadata.tableExists(connectContext, "db", "tbl"));
    }

    @Test
    public void testIcebergCatalogTableExists(@Mocked IcebergCatalog icebergCatalog) {
        new Expectations() {
            {
                icebergCatalog.getTable(connectContext, "db", "tbl");
                result = null;
                minTimes = 0;
            }
        };
        MockIcebergCatalog mockIcebergCatalog = new MockIcebergCatalog();
        Assertions.assertTrue(mockIcebergCatalog.tableExists(connectContext, "db", "tbl"));
    }

    @Test
    public void testNotExistTable(@Mocked IcebergHiveCatalog icebergHiveCatalog,
                                  @Mocked HiveTableOperations hiveTableOperations) {
        new Expectations() {
            {
                icebergHiveCatalog.getTable(connectContext, "db", "tbl");
                result = new BaseTable(hiveTableOperations, "tbl");
                minTimes = 0;

                icebergHiveCatalog.getTable(connectContext, "db", "tbl2");
                result = new StarRocksConnectorException("not found");
            }
        };

        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                Executors.newSingleThreadExecutor(), Executors.newSingleThreadExecutor(), null);
        Assertions.assertThrows(StarRocksConnectorException.class,
                () -> metadata.getTable(connectContext, "db", "tbl2"));
    }

    @Test
    public void testCreateDuplicatedDb(@Mocked IcebergHiveCatalog icebergHiveCatalog) {
        assertThrows(AlreadyExistsException.class, () -> {
            IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                    Executors.newSingleThreadExecutor(), Executors.newSingleThreadExecutor(), null);
            new Expectations() {
                {
                    icebergHiveCatalog.listAllDatabases(connectContext);
                    result = Lists.newArrayList("iceberg_db");
                    minTimes = 0;
                }
            };

            metadata.createDb(connectContext, "iceberg_db", new HashMap<>());
        });
    }

    @Test
    public void testCreateDbWithErrorConfig() {
        assertThrows(IllegalArgumentException.class, () -> {
            IcebergHiveCatalog hiveCatalog = new IcebergHiveCatalog(CATALOG_NAME, new Configuration(), new HashMap<>());
            IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, hiveCatalog,
                    Executors.newSingleThreadExecutor(), Executors.newSingleThreadExecutor(), null);

            new Expectations(hiveCatalog) {
                {
                    hiveCatalog.listAllDatabases(connectContext);
                    result = Lists.newArrayList();
                    minTimes = 0;
                }
            };

            metadata.createDb(connectContext, "iceberg_db", ImmutableMap.of("error_key", "error_value"));
        });
    }

    @Test
    public void testCreateDbInvalidateLocation() {
        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog(CATALOG_NAME, new Configuration(), DEFAULT_CONFIG);
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                Executors.newSingleThreadExecutor(), Executors.newSingleThreadExecutor(), null);

        new Expectations(icebergHiveCatalog) {
            {
                icebergHiveCatalog.listAllDatabases(connectContext);
                result = Lists.newArrayList();
                minTimes = 0;
            }
        };

        try {
            metadata.createDb(connectContext, "iceberg_db", ImmutableMap.of("location", "hdfs:xx/aaaxx"));
            Assertions.fail();
        } catch (Exception e) {
            Assertions.assertTrue(e instanceof StarRocksConnectorException);
            Assertions.assertTrue(e.getMessage().contains("Invalid location URI"));
        }
    }

    @Test
    public void testNormalCreateDb() throws AlreadyExistsException, DdlException {
        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog(CATALOG_NAME, new Configuration(), DEFAULT_CONFIG);
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                Executors.newSingleThreadExecutor(), Executors.newSingleThreadExecutor(), null);

        new Expectations(icebergHiveCatalog) {
            {
                icebergHiveCatalog.listAllDatabases(connectContext);
                result = Lists.newArrayList();
                minTimes = 0;
            }
        };

        new MockUp<HiveCatalog>() {
            @Mock
            public void createNamespace(Namespace namespace, Map<String, String> meta) {

            }
        };

        metadata.createDb(connectContext, "iceberg_db", new HashMap<>());
    }

    @Test
    public void testDropNotEmptyTable() {
        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog(CATALOG_NAME, new Configuration(), DEFAULT_CONFIG);
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                Executors.newSingleThreadExecutor(), Executors.newSingleThreadExecutor(), null);
        List<TableIdentifier> mockTables = new ArrayList<>();
        mockTables.add(TableIdentifier.of("table1"));
        mockTables.add(TableIdentifier.of("table2"));

        new Expectations(icebergHiveCatalog) {
            {
                icebergHiveCatalog.listTables(connectContext, "iceberg_db");
                result = mockTables;
                minTimes = 0;
            }
        };

        try {
            metadata.dropDb(connectContext, "iceberg_db", true);
            Assertions.fail();
        } catch (Exception e) {
            Assertions.assertTrue(e instanceof StarRocksConnectorException);
            Assertions.assertTrue(e.getMessage().contains("Database iceberg_db not empty"));
        }
    }

    @Test
    public void testDropTable() throws AlreadyExistsException {
        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog(CATALOG_NAME, new Configuration(), DEFAULT_CONFIG);
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                Executors.newSingleThreadExecutor(), Executors.newSingleThreadExecutor(), null);
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

        GlobalStateMgr.getCurrentState().getAnalyzeMgr().addAnalyzeJob(new ExternalAnalyzeJob(CATALOG_NAME,
                "iceberg_db", "table1", Lists.newArrayList(), Lists.newArrayList(),
                StatsConstants.AnalyzeType.FULL, StatsConstants.ScheduleType.ONCE, Maps.newHashMap(),
                StatsConstants.ScheduleStatus.PENDING, LocalDateTime.MIN));

        new MockUp<IcebergMetadata>() {
            @Mock
            Table getTable(ConnectContext context, String dbName, String tblName) {
                return new IcebergTable(1, "table1", CATALOG_NAME,
                        CATALOG_NAME, "iceberg_db",
                        "table1", "", Lists.newArrayList(), mockedNativeTableA, Maps.newHashMap());

            }
        };

        new Expectations(icebergHiveCatalog) {
            {
                icebergHiveCatalog.dropTable(connectContext, "iceberg_db", "table1", true);
                result = true;
                minTimes = 0;
            }
        };

        try {
            metadata.dropTable(connectContext, new DropTableStmt(false, new TableName(CATALOG_NAME,
                    "iceberg_db", "table1"), true));
        } catch (Exception e) {
            Assertions.fail();
        }

        new MockUp<IcebergMetadata>() {
            // mock table not exist
            @Mock
            Table getTable(String dbName, String tblName) {
                return null;

            }
        };
        try {
            metadata.dropTable(connectContext, new DropTableStmt(false, new TableName(CATALOG_NAME,
                    "iceberg_db", "table1"), true));
        } catch (Exception e) {
            Assertions.fail();
        }
    }

    @Test
    public void testDropDbFailed() {
        Config.hive_meta_store_timeout_s = 1;
        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog(CATALOG_NAME, new Configuration(), DEFAULT_CONFIG);
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                Executors.newSingleThreadExecutor(), Executors.newSingleThreadExecutor(), null);

        new Expectations(icebergHiveCatalog) {
            {
                icebergHiveCatalog.listTables(connectContext, "iceberg_db");
                result = Lists.newArrayList();
                minTimes = 0;
            }
        };

        try {
            metadata.dropDb(connectContext, "iceberg_db", true);
            Assertions.fail();
        } catch (Exception e) {
            Assertions.assertTrue(e instanceof MetaNotFoundException);
            Assertions.assertTrue(e.getMessage().contains("Failed to access database"));
        }

        new Expectations(icebergHiveCatalog) {
            {
                icebergHiveCatalog.getDB(connectContext, "iceberg_db");
                result = null;
                minTimes = 0;
            }
        };

        try {
            metadata.dropDb(connectContext, "iceberg_db", true);
            Assertions.fail();
        } catch (Exception e) {
            Assertions.assertTrue(e instanceof MetaNotFoundException);
            Assertions.assertTrue(e.getMessage().contains("Not found database"));
        }

        new Expectations(icebergHiveCatalog) {
            {
                icebergHiveCatalog.getDB(connectContext, "iceberg_db");
                result = new Database();
                minTimes = 0;
            }
        };

        try {
            metadata.dropDb(connectContext, "iceberg_db", true);
            Assertions.fail();
        } catch (Exception e) {
            Assertions.assertTrue(e instanceof MetaNotFoundException);
            Assertions.assertTrue(e.getMessage().contains("Database location is empty"));
        }
    }

    @Test
    public void testNormalDropDb() throws MetaNotFoundException {
        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog(CATALOG_NAME, new Configuration(), DEFAULT_CONFIG);
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                Executors.newSingleThreadExecutor(), Executors.newSingleThreadExecutor(), null);

        new Expectations(icebergHiveCatalog) {
            {
                icebergHiveCatalog.listTables(connectContext, "iceberg_db");
                result = Lists.newArrayList();
                minTimes = 0;

                icebergHiveCatalog.getDB(connectContext, "iceberg_db");
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

        metadata.dropDb(connectContext, "iceberg_db", true);
    }

    @Test
    public void testFinishSink() {
        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog(CATALOG_NAME, new Configuration(), DEFAULT_CONFIG);

        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                Executors.newSingleThreadExecutor(), Executors.newSingleThreadExecutor(), null);
        IcebergTable icebergTable = new IcebergTable(1, "srTableName", CATALOG_NAME, "resource_name", "iceberg_db",
                "iceberg_table", "", Lists.newArrayList(), mockedNativeTableA, Maps.newHashMap());

        new Expectations(metadata) {
            {
                metadata.getTable((ConnectContext) any, anyString, anyString);
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
        tIcebergDataFile.setPartition_null_fingerprint("0");

        tSinkCommitInfo.setIs_overwrite(false);
        tSinkCommitInfo.setIceberg_data_file(tIcebergDataFile);

        metadata.finishSink("iceberg_db", "iceberg_table", Lists.newArrayList(tSinkCommitInfo), null);

        List<FileScanTask> fileScanTasks = Lists.newArrayList(mockedNativeTableA.newScan().planFiles());
        Assertions.assertEquals(1, fileScanTasks.size());
        FileScanTask task = fileScanTasks.get(0);
        Assertions.assertEquals(0, task.deletes().size());
        DataFile dataFile = task.file();
        Assertions.assertEquals(path, dataFile.path());
        Assertions.assertEquals(format, dataFile.format().name().toLowerCase(Locale.ROOT));
        Assertions.assertEquals(1, dataFile.partition().size());
        Assertions.assertEquals(recordCount, dataFile.recordCount());
        Assertions.assertEquals(fileSize, dataFile.fileSizeInBytes());
        Assertions.assertEquals(4, dataFile.splitOffsets().get(0).longValue());

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

        metadata.finishSink("iceberg_db", "iceberg_table", Lists.newArrayList(tSinkCommitInfo), null);
        mockedNativeTableA.refresh();
        TableScan scan = mockedNativeTableA.newScan().includeColumnStats();
        fileScanTasks = Lists.newArrayList(scan.planFiles());

        Assertions.assertEquals(1, fileScanTasks.size());
        task = fileScanTasks.get(0);
        Assertions.assertEquals(0, task.deletes().size());
        dataFile = task.file();
        Assertions.assertEquals(path, dataFile.path());
        Assertions.assertEquals(format, dataFile.format().name().toLowerCase(Locale.ROOT));
        Assertions.assertEquals(1, dataFile.partition().size());
        Assertions.assertEquals(recordCount, dataFile.recordCount());
        Assertions.assertEquals(fileSize, dataFile.fileSizeInBytes());
        Assertions.assertEquals(4, dataFile.splitOffsets().get(0).longValue());
        Assertions.assertEquals(111L, dataFile.valueCounts().get(1).longValue());
    }

    @Test
    public void testFinishSink2() {
        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog(CATALOG_NAME, new Configuration(), DEFAULT_CONFIG);

        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                Executors.newSingleThreadExecutor(), Executors.newSingleThreadExecutor(), null);
        IcebergTable icebergTable = new IcebergTable(1, "srTableName", CATALOG_NAME, "resource_name", "iceberg_db",
                "iceberg_table", "", Lists.newArrayList(), mockedNativeTableJ, Maps.newHashMap());

        new Expectations(metadata) {
            {
                metadata.getTable((ConnectContext) any, anyString, anyString);
                result = icebergTable;
                minTimes = 0;
            }
        };

        TSinkCommitInfo tSinkCommitInfo = new TSinkCommitInfo();
        TIcebergDataFile tIcebergDataFile = new TIcebergDataFile();
        String path = mockedNativeTableJ.location() + "/data/ts_month=2022-01/c.parquet";
        String format = "parquet";
        long recordCount = 10;
        long fileSize = 2000;
        String partitionPath = mockedNativeTableJ.location() + "/data/ts_month=2022-01/";
        List<Long> splitOffsets = Lists.newArrayList(4L);
        tIcebergDataFile.setPath(path);
        tIcebergDataFile.setFormat(format);
        tIcebergDataFile.setRecord_count(recordCount);
        tIcebergDataFile.setSplit_offsets(splitOffsets);
        tIcebergDataFile.setPartition_path(partitionPath);
        tIcebergDataFile.setFile_size_in_bytes(fileSize);
        tIcebergDataFile.setPartition_null_fingerprint("0");

        tSinkCommitInfo.setIs_overwrite(false);
        tSinkCommitInfo.setIceberg_data_file(tIcebergDataFile);

        metadata.finishSink("iceberg_db", "iceberg_table", Lists.newArrayList(tSinkCommitInfo), null);

        List<FileScanTask> fileScanTasks = Lists.newArrayList(mockedNativeTableJ.newScan().planFiles());
        Assertions.assertEquals(1, fileScanTasks.size());
        FileScanTask task = fileScanTasks.get(0);
        Assertions.assertEquals(0, task.deletes().size());
        DataFile dataFile = task.file();
        Assertions.assertEquals(path, dataFile.path());
        Assertions.assertEquals(format, dataFile.format().name().toLowerCase(Locale.ROOT));
        Assertions.assertEquals(1, dataFile.partition().size());
        Assertions.assertEquals(recordCount, dataFile.recordCount());
        Assertions.assertEquals(fileSize, dataFile.fileSizeInBytes());
        Assertions.assertEquals(4, dataFile.splitOffsets().get(0).longValue());

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

        metadata.finishSink("iceberg_db", "iceberg_table", Lists.newArrayList(tSinkCommitInfo), null);
        mockedNativeTableJ.refresh();
        TableScan scan = mockedNativeTableJ.newScan().includeColumnStats();
        fileScanTasks = Lists.newArrayList(scan.planFiles());

        Assertions.assertEquals(1, fileScanTasks.size());
        task = fileScanTasks.get(0);
        Assertions.assertEquals(0, task.deletes().size());
        dataFile = task.file();
        Assertions.assertEquals(path, dataFile.path());
        Assertions.assertEquals(format, dataFile.format().name().toLowerCase(Locale.ROOT));
        Assertions.assertEquals(1, dataFile.partition().size());
        Assertions.assertEquals(recordCount, dataFile.recordCount());
        Assertions.assertEquals(fileSize, dataFile.fileSizeInBytes());
        Assertions.assertEquals(4, dataFile.splitOffsets().get(0).longValue());
        Assertions.assertEquals(111L, dataFile.valueCounts().get(1).longValue());
    }

    @Test
    public void testFinishSink3() {
        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog(CATALOG_NAME, new Configuration(), DEFAULT_CONFIG);

        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                Executors.newSingleThreadExecutor(), Executors.newSingleThreadExecutor(), null);
        IcebergTable icebergTable = new IcebergTable(1, "srTableName", CATALOG_NAME, "resource_name", "iceberg_db",
                "iceberg_table", "", Lists.newArrayList(), mockedNativeTableJ, Maps.newHashMap());

        new Expectations(metadata) {
            {
                metadata.getTable((ConnectContext) any, anyString, anyString);
                result = icebergTable;
                minTimes = 0;
            }
        };

        TSinkCommitInfo tSinkCommitInfo = new TSinkCommitInfo();
        TIcebergDataFile tIcebergDataFile = new TIcebergDataFile();
        String path = mockedNativeTableJ.location() + "/data/ts_month=2022-01/c.parquet";
        String format = "parquet";
        long recordCount = 10;
        long fileSize = 2000;
        String partitionPath = mockedNativeTableJ.location() + "/data/ts_month=2022-01/";
        List<Long> splitOffsets = Lists.newArrayList(4L);
        tIcebergDataFile.setPath(path);
        tIcebergDataFile.setFormat(format);
        tIcebergDataFile.setRecord_count(recordCount);
        tIcebergDataFile.setSplit_offsets(splitOffsets);
        tIcebergDataFile.setPartition_path(partitionPath);
        tIcebergDataFile.setFile_size_in_bytes(fileSize);
        tIcebergDataFile.setPartition_null_fingerprint("1234124");

        tSinkCommitInfo.setIs_overwrite(false);
        tSinkCommitInfo.setIceberg_data_file(tIcebergDataFile);

        ExceptionChecker.expectThrowsWithMsg(InternalError.class,
                "Invalid partition and fingerprint size",
                () -> metadata.finishSink("iceberg_db", "iceberg_table", Lists.newArrayList(tSinkCommitInfo), null));

    }

    @Test
    public void testFinishSink4() {
        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog(CATALOG_NAME, new Configuration(), DEFAULT_CONFIG);

        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                Executors.newSingleThreadExecutor(), Executors.newSingleThreadExecutor(), null);
        IcebergTable icebergTable = new IcebergTable(1, "srTableName", CATALOG_NAME, "resource_name", "iceberg_db",
                "iceberg_table", "", Lists.newArrayList(), mockedNativeTableF, Maps.newHashMap());

        new Expectations(metadata) {
            {
                metadata.getTable((ConnectContext) any, anyString, anyString);
                result = icebergTable;
                minTimes = 0;
            }
        };

        TSinkCommitInfo tSinkCommitInfo = new TSinkCommitInfo();
        TIcebergDataFile tIcebergDataFile = new TIcebergDataFile();
        String path = mockedNativeTableF.location() + "/data/dt_day=2022-01-02/c.parquet";
        String format = "parquet";
        long recordCount = 10;
        long fileSize = 2000;
        String partitionPath = mockedNativeTableF.location() + "/data/dt_day=2022-01-02/";
        List<Long> splitOffsets = Lists.newArrayList(4L);
        tIcebergDataFile.setPath(path);
        tIcebergDataFile.setFormat(format);
        tIcebergDataFile.setRecord_count(recordCount);
        tIcebergDataFile.setSplit_offsets(splitOffsets);
        tIcebergDataFile.setPartition_path(partitionPath);
        tIcebergDataFile.setFile_size_in_bytes(fileSize);
        tIcebergDataFile.setPartition_null_fingerprint("0");

        tSinkCommitInfo.setIs_overwrite(false);
        tSinkCommitInfo.setIceberg_data_file(tIcebergDataFile);
        metadata.finishSink("iceberg_db", "iceberg_table", Lists.newArrayList(tSinkCommitInfo), null);
    }

    @Test
    public void testFinishSink5() {
        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog(CATALOG_NAME, new Configuration(), DEFAULT_CONFIG);

        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                Executors.newSingleThreadExecutor(), Executors.newSingleThreadExecutor(), null);
        IcebergTable icebergTable = new IcebergTable(1, "srTableName", CATALOG_NAME, "resource_name", "iceberg_db",
                "iceberg_table", "", Lists.newArrayList(), mockedNativeTableD, Maps.newHashMap());

        new Expectations(metadata) {
            {
                metadata.getTable((ConnectContext) any, anyString, anyString);
                result = icebergTable;
                minTimes = 0;
            }
        };

        TSinkCommitInfo tSinkCommitInfo = new TSinkCommitInfo();
        TIcebergDataFile tIcebergDataFile = new TIcebergDataFile();
        String path = mockedNativeTableD.location() + "/data/ts_hour=2022-01-02-11/c.parquet";
        String format = "parquet";
        long recordCount = 10;
        long fileSize = 2000;
        String partitionPath = mockedNativeTableD.location() + "/data/ts_hour=2022-01-02-11/";
        List<Long> splitOffsets = Lists.newArrayList(4L);
        tIcebergDataFile.setPath(path);
        tIcebergDataFile.setFormat(format);
        tIcebergDataFile.setRecord_count(recordCount);
        tIcebergDataFile.setSplit_offsets(splitOffsets);
        tIcebergDataFile.setPartition_path(partitionPath);
        tIcebergDataFile.setFile_size_in_bytes(fileSize);
        tIcebergDataFile.setPartition_null_fingerprint("0");

        tSinkCommitInfo.setIs_overwrite(false);
        tSinkCommitInfo.setIceberg_data_file(tIcebergDataFile);
        metadata.finishSink("iceberg_db", "iceberg_table", Lists.newArrayList(tSinkCommitInfo), null);
        tIcebergDataFile.setPartition_null_fingerprint("1");
        metadata.finishSink("iceberg_db", "iceberg_table", Lists.newArrayList(tSinkCommitInfo), null);
    }

    @Test
    public void testFinishSink6() {
        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog(CATALOG_NAME, new Configuration(), DEFAULT_CONFIG);

        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                Executors.newSingleThreadExecutor(), Executors.newSingleThreadExecutor(), null);
        IcebergTable icebergTable = new IcebergTable(1, "srTableName", CATALOG_NAME, "resource_name", "iceberg_db",
                "iceberg_table", "", Lists.newArrayList(), mockedNativeTableK, Maps.newHashMap());

        new Expectations(metadata) {
            {
                metadata.getTable((ConnectContext) any, anyString, anyString);
                result = icebergTable;
                minTimes = 0;
            }
        };

        TSinkCommitInfo tSinkCommitInfo = new TSinkCommitInfo();
        TIcebergDataFile tIcebergDataFile = new TIcebergDataFile();
        String path = mockedNativeTableK.location() + "/data/ts_year=2022/c.parquet";
        String format = "parquet";
        long recordCount = 10;
        long fileSize = 2000;
        String partitionPath = mockedNativeTableK.location() + "/data/ts_year=2022/";
        List<Long> splitOffsets = Lists.newArrayList(4L);
        tIcebergDataFile.setPath(path);
        tIcebergDataFile.setFormat(format);
        tIcebergDataFile.setRecord_count(recordCount);
        tIcebergDataFile.setSplit_offsets(splitOffsets);
        tIcebergDataFile.setPartition_path(partitionPath);
        tIcebergDataFile.setFile_size_in_bytes(fileSize);
        tIcebergDataFile.setPartition_null_fingerprint("0");

        tSinkCommitInfo.setIs_overwrite(false);
        tSinkCommitInfo.setIceberg_data_file(tIcebergDataFile);
        metadata.finishSink("iceberg_db", "iceberg_table", Lists.newArrayList(tSinkCommitInfo), null);
        tIcebergDataFile.setPartition_null_fingerprint("1");
        metadata.finishSink("iceberg_db", "iceberg_table", Lists.newArrayList(tSinkCommitInfo), null);
    }

    @Test
    public void testFinishSinkWithCommitFailed(@Mocked IcebergMetadata.Append append) throws IOException {
        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog(CATALOG_NAME, new Configuration(), DEFAULT_CONFIG);

        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                Executors.newSingleThreadExecutor(), Executors.newSingleThreadExecutor(), null);
        IcebergTable icebergTable = new IcebergTable(1, "srTableName", CATALOG_NAME, "resource_name", "iceberg_db",
                "iceberg_table", "", Lists.newArrayList(), mockedNativeTableA, Maps.newHashMap());

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
                metadata.getTable((ConnectContext) any, anyString, anyString);
                result = icebergTable;
                minTimes = 0;

                metadata.getBatchWrite((Transaction) any, anyBoolean, anyBoolean);
                result = append;
                minTimes = 0;
            }
        };

        File fakeFile = File.createTempFile("junit", null, temp);
        fakeFile.createNewFile();
        Assertions.assertTrue(fakeFile.exists());
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
                () -> metadata.finishSink("iceberg_db", "iceberg_table", Lists.newArrayList(tSinkCommitInfo), null));
        Assertions.assertFalse(fakeFile.exists());
    }

    @Test
    public void testGetRemoteFile() throws IOException {
        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog(CATALOG_NAME, new Configuration(), DEFAULT_CONFIG);
        List<Column> columns = Lists.newArrayList(new Column("k1", INT), new Column("k2", INT));
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                Executors.newSingleThreadExecutor(), Executors.newSingleThreadExecutor(), DEFAULT_CATALOG_PROPERTIES);
        IcebergTable icebergTable = new IcebergTable(1, "srTableName", CATALOG_NAME, "resource_name", "iceberg_db",
                "iceberg_table", "", columns, mockedNativeTableB, Maps.newHashMap());

        mockedNativeTableB.newAppend().appendFile(FILE_B_1).appendFile(FILE_B_2).commit();
        mockedNativeTableB.refresh();

        long snapshotId = mockedNativeTableB.currentSnapshot().snapshotId();
        ScalarOperator predicate = new BinaryPredicateOperator(BinaryType.GE,
                new ColumnRefOperator(1, INT, "k2", true), ConstantOperator.createInt(1));
        List<RemoteFileInfo> res = metadata.getRemoteFiles(icebergTable,
                GetRemoteFilesParams.newBuilder().setTableVersionRange(TableVersionRange.withEnd(Optional.of(snapshotId)))
                        .setPredicate(predicate).setFieldNames(Lists.newArrayList()).setLimit(10).build());
        Assertions.assertEquals(7, res.stream()
                .map(f -> (IcebergRemoteFileInfo) f)
                .map(fileInfo -> fileInfo.getFileScanTask().file().recordCount()).reduce(0L, Long::sum), 0.001);

        StarRocksAssert starRocksAssert = new StarRocksAssert();
        starRocksAssert.getCtx().getSessionVariable().setEnablePruneIcebergManifest(true);
        mockedNativeTableB.refresh();
        snapshotId = mockedNativeTableB.currentSnapshot().snapshotId();
        predicate = new BinaryPredicateOperator(BinaryType.EQ,
                new ColumnRefOperator(1, INT, "k2", true), ConstantOperator.createInt(2));
        res = metadata.getRemoteFiles(icebergTable,
                GetRemoteFilesParams.newBuilder().setTableVersionRange(TableVersionRange.withEnd(Optional.of(snapshotId)))
                        .setPredicate(predicate).setFieldNames(Lists.newArrayList()).setLimit(10).build());
        Assertions.assertEquals(1, res.size());
        Assertions.assertEquals(3, ((IcebergRemoteFileInfo) res.get(0)).getFileScanTask().file().recordCount());

        PredicateSearchKey filter = PredicateSearchKey.of("db", "table", emptyParams);
        Assertions.assertEquals(
                "Filter{databaseName='db', tableName='table', snapshotId=1, predicate=true, enableColumnStats=false}",
                filter.toString());
    }

    @Test
    public void testGetTableStatistics() {
        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog(CATALOG_NAME, new Configuration(), DEFAULT_CONFIG);

        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                Executors.newSingleThreadExecutor(), Executors.newSingleThreadExecutor(), DEFAULT_CATALOG_PROPERTIES,
                new ConnectorProperties(ConnectorType.ICEBERG,
                        Map.of(ConnectorProperties.ENABLE_GET_STATS_FROM_EXTERNAL_METADATA, "true")));
        mockedNativeTableA.newFastAppend().appendFile(FILE_A).appendFile(FILE_A_1).commit();
        IcebergTable icebergTable = new IcebergTable(1, "srTableName", CATALOG_NAME, "resource_name", "db_name",
                "table_name", "", Lists.newArrayList(), mockedNativeTableA, Maps.newHashMap());
        Map<ColumnRefOperator, Column> colRefToColumnMetaMap = new HashMap<ColumnRefOperator, Column>();
        ColumnRefOperator columnRefOperator1 = new ColumnRefOperator(3, Type.INT, "id", true);
        ColumnRefOperator columnRefOperator2 = new ColumnRefOperator(4, Type.STRING, "data", true);
        colRefToColumnMetaMap.put(columnRefOperator1, new Column("id", Type.INT));
        colRefToColumnMetaMap.put(columnRefOperator2, new Column("data", Type.STRING));
        OptimizerContext context = OptimizerFactory.mockContext(new ColumnRefFactory());
        Assertions.assertFalse(context.getSessionVariable().enableIcebergColumnStatistics());
        Assertions.assertTrue(context.getSessionVariable().enableReadIcebergPuffinNdv());
        TableVersionRange versionRange = TableVersionRange.withEnd(Optional.of(
                mockedNativeTableA.currentSnapshot().snapshotId()));
        Statistics statistics = metadata.getTableStatistics(
                context, icebergTable, colRefToColumnMetaMap, null, null, -1, versionRange);
        Assertions.assertEquals(4.0, statistics.getOutputRowCount(), 0.001);
        Assertions.assertEquals(2, statistics.getColumnStatistics().size());
        Assertions.assertTrue(statistics.getColumnStatistic(columnRefOperator1).isUnknown());
        Assertions.assertTrue(statistics.getColumnStatistic(columnRefOperator2).isUnknown());
    }

    @Test
    public void testGetTableStatisticsWithColumnStats() {
        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog(CATALOG_NAME, new Configuration(), DEFAULT_CONFIG);
        List<Column> columns = Lists.newArrayList(new Column("k1", INT), new Column("k2", INT));
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                Executors.newSingleThreadExecutor(), Executors.newSingleThreadExecutor(), DEFAULT_CATALOG_PROPERTIES,
                new ConnectorProperties(ConnectorType.ICEBERG,
                        Map.of(ConnectorProperties.ENABLE_GET_STATS_FROM_EXTERNAL_METADATA, "true")));
        mockedNativeTableB.newFastAppend().appendFile(FILE_B_3).commit();
        mockedNativeTableB.newFastAppend().appendFile(FILE_B_4).commit();
        mockedNativeTableB.refresh();
        IcebergTable icebergTable = new IcebergTable(1, "srTableName", CATALOG_NAME, "resource_name", "db_name",
                "table_name", "", columns, mockedNativeTableB, Maps.newHashMap());
        Map<ColumnRefOperator, Column> colRefToColumnMetaMap = new HashMap<ColumnRefOperator, Column>();
        ColumnRefOperator columnRefOperator1 = new ColumnRefOperator(3, Type.INT, "k1", true);
        ColumnRefOperator columnRefOperator2 = new ColumnRefOperator(4, INT, "k2", true);
        colRefToColumnMetaMap.put(columnRefOperator1, new Column("k1", Type.INT));
        colRefToColumnMetaMap.put(columnRefOperator2, new Column("k2", Type.INT));
        new ConnectContext().setThreadLocalInfo();
        ConnectContext.get().getSessionVariable().setEnableIcebergColumnStatistics(true);
        TableVersionRange versionRange = TableVersionRange.withEnd(Optional.of(
                mockedNativeTableB.currentSnapshot().snapshotId()));
        Statistics statistics = metadata.getTableStatistics(
                OptimizerFactory.mockContext(ConnectContext.get(), null),
                icebergTable, colRefToColumnMetaMap, null, null, -1, versionRange);
        Assertions.assertEquals(4.0, statistics.getOutputRowCount(), 0.001);
        Assertions.assertEquals(2, statistics.getColumnStatistics().size());
        Assertions.assertTrue(statistics.getColumnStatistic(columnRefOperator1).isUnknown());
        ColumnStatistic columnStatistic = statistics.getColumnStatistic(columnRefOperator1);
        Assertions.assertEquals(1.0, columnStatistic.getMinValue(), 0.001);
        Assertions.assertEquals(2.0, columnStatistic.getMaxValue(), 0.001);
        Assertions.assertEquals(0, columnStatistic.getNullsFraction(), 0.001);

        Assertions.assertFalse(statistics.getColumnStatistic(columnRefOperator2).isUnknown());
    }

    @Test
    public void testPartitionPrune() {
        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog(CATALOG_NAME, new Configuration(), DEFAULT_CONFIG);
        List<Column> columns = Lists.newArrayList(new Column("id", INT), new Column("data", STRING));
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                Executors.newSingleThreadExecutor(), Executors.newSingleThreadExecutor(), DEFAULT_CATALOG_PROPERTIES);
        mockedNativeTableA.newFastAppend().appendFile(FILE_A).commit();
        IcebergTable icebergTable = new IcebergTable(1, "srTableName", CATALOG_NAME, "resource_name", "db_name",
                "table_name", "", columns, mockedNativeTableA, Maps.newHashMap());
        Map<ColumnRefOperator, Column> colRefToColumnMetaMap = new HashMap<ColumnRefOperator, Column>();
        ColumnRefOperator columnRefOperator1 = new ColumnRefOperator(3, Type.INT, "id", true);
        ColumnRefOperator columnRefOperator2 = new ColumnRefOperator(4, Type.STRING, "data", true);
        colRefToColumnMetaMap.put(columnRefOperator1, new Column("id", Type.INT));
        colRefToColumnMetaMap.put(columnRefOperator2, new Column("data", Type.STRING));
        new ConnectContext().setThreadLocalInfo();

        TableVersionRange version = TableVersionRange.withEnd(Optional.of(
                mockedNativeTableA.currentSnapshot().snapshotId()));
        List<PartitionKey> partitionKeys = metadata.getPrunedPartitions(icebergTable, null, 1, version);
        Assertions.assertEquals(1, partitionKeys.size());
        Assertions.assertTrue(partitionKeys.get(0) instanceof IcebergPartitionKey);
        IcebergPartitionKey partitionKey = (IcebergPartitionKey) partitionKeys.get(0);
        Assertions.assertEquals("types: [INT]; keys: [0]; ", partitionKey.toString());

        mockedNativeTableA.newFastAppend().appendFile(FILE_A_2).commit();
        mockedNativeTableA.refresh();
        icebergTable = new IcebergTable(1, "srTableName", CATALOG_NAME, "resource_name", "db_name",
                "table_name", "", columns, mockedNativeTableA, Maps.newHashMap());
        TableVersionRange versionRange = TableVersionRange.withEnd(Optional.of(
                mockedNativeTableA.currentSnapshot().snapshotId()));
        partitionKeys = metadata.getPrunedPartitions(icebergTable, null, 100, versionRange);
        Assertions.assertEquals(2, partitionKeys.size());
    }

    @Test
    public void testPruneNullPartition() {
        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog(CATALOG_NAME, new Configuration(), DEFAULT_CONFIG);
        List<Column> columns = Lists.newArrayList(new Column("k1", INT), new Column("dt", DATE));
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                Executors.newSingleThreadExecutor(), Executors.newSingleThreadExecutor(), DEFAULT_CATALOG_PROPERTIES);
        IcebergTable icebergTable = new IcebergTable(1, "srTableName", CATALOG_NAME, "resource_name", "db_name",
                "table_name", "", columns, mockedNativeTableI, Maps.newHashMap());

        org.apache.iceberg.PartitionKey partitionKey = new org.apache.iceberg.PartitionKey(SPEC_F_1, SCHEMA_F);
        partitionKey.set(0, null);
        DataFile tsDataFiles = DataFiles.builder(SPEC_F_1)
                .withPath("/path/to/data-f1.parquet")
                .withFileSizeInBytes(20)
                .withPartition(partitionKey)
                .withRecordCount(2)
                .build();
        mockedNativeTableI.newAppend().appendFile(tsDataFiles).commit();
        mockedNativeTableI.refresh();
        TableVersionRange versionRange = TableVersionRange.withEnd(Optional.of(
                mockedNativeTableI.currentSnapshot().snapshotId()));
        List<PartitionKey> partitionKeys = metadata.getPrunedPartitions(icebergTable, null, -1, versionRange);
        Assertions.assertTrue(partitionKeys.get(0).getKeys().get(0) instanceof NullLiteral);
    }

    @Test
    public void testPartitionPruneWithDuplicated() {
        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog(CATALOG_NAME, new Configuration(), DEFAULT_CONFIG);
        List<Column> columns = Lists.newArrayList(new Column("id", INT), new Column("data", STRING));
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                Executors.newSingleThreadExecutor(), Executors.newSingleThreadExecutor(), DEFAULT_CATALOG_PROPERTIES);
        mockedNativeTableA.newFastAppend().appendFile(FILE_A).appendFile(FILE_A_1).commit();
        mockedNativeTableA.refresh();
        IcebergTable icebergTable = new IcebergTable(1, "srTableName", CATALOG_NAME, "resource_name", "db_name",
                "table_name", "", columns, mockedNativeTableA, Maps.newHashMap());
        TableVersionRange versionRange = TableVersionRange.withEnd(Optional.of(
                mockedNativeTableA.currentSnapshot().snapshotId()));
        List<PartitionKey> partitionKeys = metadata.getPrunedPartitions(icebergTable, null, 1, versionRange);
        Assertions.assertEquals(1, partitionKeys.size());
        Assertions.assertTrue(partitionKeys.get(0) instanceof IcebergPartitionKey);
        PartitionKey partitionKey = partitionKeys.get(0);
        Assertions.assertEquals("types: [INT]; keys: [0]; ", partitionKey.toString());
    }

    @Test
    public void testGetRepeatedTableStats() {
        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog(CATALOG_NAME, new Configuration(), DEFAULT_CONFIG);
        IcebergTable icebergTable = new IcebergTable(1, "srTableName", CATALOG_NAME, "resource_name", "db_name",
                "table_name", "", Lists.newArrayList(), mockedNativeTableA, Maps.newHashMap());
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                Executors.newSingleThreadExecutor(), Executors.newSingleThreadExecutor(), DEFAULT_CATALOG_PROPERTIES,
                new ConnectorProperties(ConnectorType.ICEBERG,
                        Map.of(ConnectorProperties.ENABLE_GET_STATS_FROM_EXTERNAL_METADATA, "true")));
        Map<ColumnRefOperator, Column> colRefToColumnMetaMap = new HashMap<ColumnRefOperator, Column>();
        ColumnRefOperator columnRefOperator1 = new ColumnRefOperator(3, Type.INT, "id", true);
        ColumnRefOperator columnRefOperator2 = new ColumnRefOperator(4, Type.STRING, "data", true);
        colRefToColumnMetaMap.put(columnRefOperator1, new Column("id", Type.INT));
        colRefToColumnMetaMap.put(columnRefOperator2, new Column("data", Type.STRING));
        mockedNativeTableA.newFastAppend().appendFile(FILE_A).commit();
        mockedNativeTableA.newFastAppend().appendFile(FILE_A).commit();
        mockedNativeTableA.refresh();

        new ConnectContext().setThreadLocalInfo();
        OptimizerContext context = OptimizerFactory.mockContext(ConnectContext.get(), new ColumnRefFactory());
        context.getSessionVariable().setEnableIcebergColumnStatistics(true);

        TableVersionRange version = TableVersionRange.withEnd(Optional.of(
                mockedNativeTableA.currentSnapshot().snapshotId()));
        Statistics statistics = metadata.getTableStatistics(context, icebergTable,
                colRefToColumnMetaMap, null, null, -1, version);
        Assertions.assertEquals(2.0, statistics.getOutputRowCount(), 0.001);
    }

    @Test
    public void testTimeStampIdentityPartitionPrune() {
        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog(CATALOG_NAME, new Configuration(), DEFAULT_CONFIG);
        List<Column> columns = Lists.newArrayList(new Column("k1", INT), new Column("ts", DATETIME));
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                Executors.newSingleThreadExecutor(), Executors.newSingleThreadExecutor(), DEFAULT_CATALOG_PROPERTIES);
        IcebergTable icebergTable = new IcebergTable(1, "srTableName", CATALOG_NAME, "resource_name", "db_name",
                "table_name", "", columns, mockedNativeTableE, Maps.newHashMap());

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
        TableVersionRange version = TableVersionRange.withEnd(Optional.of(
                mockedNativeTableE.currentSnapshot().snapshotId()));
        List<PartitionKey> partitionKeys = metadata.getPrunedPartitions(icebergTable, null, 1, version);
        Assertions.assertEquals("2023-10-30 03:45:56", partitionKeys.get(0).getKeys().get(0).getStringValue());
    }

    @Test
    public void testTransformedPartitionPrune() {
        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog(CATALOG_NAME, new Configuration(), DEFAULT_CONFIG);
        List<Column> columns = Lists.newArrayList(new Column("k1", INT), new Column("ts", DATETIME));
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                Executors.newSingleThreadExecutor(), Executors.newSingleThreadExecutor(), DEFAULT_CATALOG_PROPERTIES);
        IcebergTable icebergTable = new IcebergTable(1, "srTableName", CATALOG_NAME, "resource_name", "db_name",
                "table_name", "", columns, mockedNativeTableD, Maps.newHashMap());

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
        TableVersionRange version = TableVersionRange.withEnd(Optional.of(
                mockedNativeTableD.currentSnapshot().snapshotId()));
        List<PartitionKey> partitionKeys = metadata.getPrunedPartitions(icebergTable, null, -1, version);
        Assertions.assertEquals("438292", partitionKeys.get(0).getKeys().get(0).getStringValue());
    }

    @Test
    public void testDateDayPartitionPrune() {
        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog(CATALOG_NAME, new Configuration(), DEFAULT_CONFIG);
        List<Column> columns = Lists.newArrayList(new Column("k1", INT), new Column("dt", DATE));
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                Executors.newSingleThreadExecutor(), Executors.newSingleThreadExecutor(), DEFAULT_CATALOG_PROPERTIES);
        IcebergTable icebergTable = new IcebergTable(1, "srTableName", CATALOG_NAME, "resource_name", "db_name",
                "table_name", "", columns, mockedNativeTableF, Maps.newHashMap());

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
        TableVersionRange version = TableVersionRange.withEnd(Optional.of(
                mockedNativeTableF.currentSnapshot().snapshotId()));
        List<PartitionKey> partitionKeys = metadata.getPrunedPartitions(icebergTable, null, -1, version);
        Assertions.assertEquals("19660", partitionKeys.get(0).getKeys().get(0).getStringValue());
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

        GetRemoteFilesParams callParams = GetRemoteFilesParams.newBuilder()
                .setSnapshotId(1)
                .setPredicate(callOperator)
                .setFieldNames(Lists.newArrayList())
                .setLimit(10)
                .build();

        GetRemoteFilesParams newCallParams = GetRemoteFilesParams.newBuilder()
                .setSnapshotId(1)
                .setPredicate(newCallOperator)
                .setFieldNames(Lists.newArrayList())
                .setLimit(10)
                .build();

        PredicateSearchKey filter = PredicateSearchKey.of("db", "table", callParams);
        PredicateSearchKey newFilter = PredicateSearchKey.of("db", "table", newCallParams);
        Assertions.assertEquals(filter, newFilter);

        Assertions.assertEquals(newFilter, PredicateSearchKey.of("db", "table", newCallParams));
        Assertions.assertNotEquals(newFilter, PredicateSearchKey.of("db", "table", emptyParams));
    }

    @Test
    public void testListPartitionNames() {
        mockedNativeTableB.newAppend().appendFile(FILE_B_1).appendFile(FILE_B_2).appendFile(FILE_B_3).commit();
        new MockUp<IcebergHiveCatalog>() {
            @Mock
            org.apache.iceberg.Table getTable(ConnectContext context, String dbName, String tableName)
                    throws StarRocksConnectorException {
                return mockedNativeTableB;
            }

            @Mock
            Database getDB(ConnectContext context, String dbName) {
                return new Database(0, dbName);
            }
        };
        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog(CATALOG_NAME, new Configuration(), DEFAULT_CONFIG);
        CachingIcebergCatalog cachingIcebergCatalog = new CachingIcebergCatalog(CATALOG_NAME, icebergHiveCatalog,
                DEFAULT_CATALOG_PROPERTIES, Executors.newSingleThreadExecutor());
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, cachingIcebergCatalog,
                Executors.newSingleThreadExecutor(), Executors.newSingleThreadExecutor(), null);
        TableVersionRange version = TableVersionRange.withEnd(Optional.of(
                mockedNativeTableB.currentSnapshot().snapshotId()));
        ConnectorMetadatRequestContext requestContext = new ConnectorMetadatRequestContext();
        requestContext.setTableVersionRange(version);
        List<String> partitionNames = metadata.listPartitionNames("db", "table", requestContext);
        Assertions.assertEquals(2, partitionNames.size());
        Assertions.assertTrue(partitionNames.contains("k2=2"));
        Assertions.assertTrue(partitionNames.contains("k2=3"));
    }

    @Test
    public void testGetPartitions1() {
        mockedNativeTableB.newAppend().appendFile(FILE_B_1).appendFile(FILE_B_2).commit();
        new MockUp<IcebergHiveCatalog>() {
            @Mock
            org.apache.iceberg.Table getTable(ConnectContext context, String dbName, String tableName)
                    throws StarRocksConnectorException {
                return mockedNativeTableB;
            }
        };

        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog(CATALOG_NAME, new Configuration(), DEFAULT_CONFIG);
        CachingIcebergCatalog cachingIcebergCatalog = new CachingIcebergCatalog(CATALOG_NAME, icebergHiveCatalog,
                DEFAULT_CATALOG_PROPERTIES, Executors.newSingleThreadExecutor());
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, cachingIcebergCatalog,
                Executors.newSingleThreadExecutor(), Executors.newSingleThreadExecutor(), null);

        IcebergTable icebergTable = new IcebergTable(1, "srTableName", CATALOG_NAME,
                "resource_name", "db",
                "table", "", Lists.newArrayList(), mockedNativeTableB, Maps.newHashMap());

        List<PartitionInfo> partitions = metadata.getPartitions(icebergTable, ImmutableList.of("k2=2", "k2=3"));
        Assertions.assertEquals(2, partitions.size());
    }

    @Test
    public void testGetPartitions2() {
        mockedNativeTableG.newAppend().appendFile(FILE_B_5).commit();

        new MockUp<IcebergHiveCatalog>() {
            @Mock
            org.apache.iceberg.Table getTable(ConnectContext context, String dbName, String tableName)
                    throws StarRocksConnectorException {
                return mockedNativeTableG;
            }
        };

        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog(CATALOG_NAME, new Configuration(), DEFAULT_CONFIG);
        CachingIcebergCatalog cachingIcebergCatalog = new CachingIcebergCatalog(CATALOG_NAME, icebergHiveCatalog,
                DEFAULT_CATALOG_PROPERTIES, Executors.newSingleThreadExecutor());
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, cachingIcebergCatalog,
                Executors.newSingleThreadExecutor(), Executors.newSingleThreadExecutor(), null);

        IcebergTable icebergTable = new IcebergTable(1, "srTableName", CATALOG_NAME,
                "resource_name", "db",
                "table", "", Lists.newArrayList(), mockedNativeTableG, Maps.newHashMap());

        List<PartitionInfo> partitions = metadata.getPartitions(icebergTable, Lists.newArrayList());
        Assertions.assertEquals(1, partitions.size());
    }

    @Test
    public void testGetPartitionsWithExpireSnapshot() {
        mockedNativeTableB.newAppend().appendFile(FILE_B_1).commit();
        mockedNativeTableB.refresh();
        mockedNativeTableB.newAppend().appendFile(FILE_B_2).commit();
        mockedNativeTableB.refresh();
        mockedNativeTableB.expireSnapshots().expireOlderThan(System.currentTimeMillis()).commit();
        mockedNativeTableB.refresh();

        new MockUp<IcebergHiveCatalog>() {
            @Mock
            org.apache.iceberg.Table getTable(ConnectContext context, String dbName, String tableName)
                    throws StarRocksConnectorException {
                return mockedNativeTableB;
            }
        };

        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog(CATALOG_NAME, new Configuration(), DEFAULT_CONFIG);
        CachingIcebergCatalog cachingIcebergCatalog = new CachingIcebergCatalog(
                CATALOG_NAME, icebergHiveCatalog, DEFAULT_CATALOG_PROPERTIES, Executors.newSingleThreadExecutor());
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, cachingIcebergCatalog,
                Executors.newSingleThreadExecutor(), Executors.newSingleThreadExecutor(), null);

        IcebergTable icebergTable = new IcebergTable(1, "srTableName", CATALOG_NAME,
                "resource_name", "db",
                "table", "", Lists.newArrayList(), mockedNativeTableB, Maps.newHashMap());

        List<PartitionInfo> partitions = metadata.getPartitions(icebergTable, ImmutableList.of("k2=2", "k2=3"));
        Assertions.assertEquals(2, partitions.size());
        // partition's modified time should not be -1 even if snapshot has been expired
        Assertions.assertTrue(partitions.stream().noneMatch(x -> x.getModifiedTime() == -1));
    }

    @Test
    public void testPartitionWithReservedName() {
        // Create a schema with a column named "partition" (which is a reserved word)
        Schema schemaWithPartitionColumn = new Schema(
                required(1, "id", Types.IntegerType.get()),
                required(2, "partition", Types.StringType.get())
        );

        PartitionSpec specWithPartitionColumn = PartitionSpec.builderFor(schemaWithPartitionColumn)
                .identity("partition")
                .bucket("partition", 32)
                .truncate("partition", 32)
                .build();

        TestTables.TestTable testTable = create(schemaWithPartitionColumn, specWithPartitionColumn, "test_partition_table", 1);

        List<Column> columns = Lists.newArrayList(
                new Column("id", INT),
                new Column("partition", STRING)
        );

        IcebergTable icebergTable = new IcebergTable(1, "srTableName", CATALOG_NAME, "resource_name", "db_name",
                "table_name", "", columns, testTable, Maps.newHashMap());

        List<String> partitionColumnNames = icebergTable.getPartitionColumnNames();

        Assertions.assertNotNull(partitionColumnNames);
        Assertions.assertEquals(3, partitionColumnNames.size());
        Assertions.assertEquals("partition", partitionColumnNames.get(0));
        Assertions.assertEquals("partition", partitionColumnNames.get(1));
        Assertions.assertEquals("partition", partitionColumnNames.get(2));

        List<String> partitionColumnNamesWithTransform = icebergTable.getPartitionColumnNamesWithTransform();
        Assertions.assertNotNull(partitionColumnNamesWithTransform);
        Assertions.assertEquals(3, partitionColumnNamesWithTransform.size());
        Assertions.assertEquals("`partition`", partitionColumnNamesWithTransform.get(0));
        Assertions.assertEquals("bucket(`partition`, 32)", partitionColumnNamesWithTransform.get(1));
        Assertions.assertEquals("truncate(`partition`, 32)", partitionColumnNamesWithTransform.get(2));

        // convert the icebergTable into a thrift value 
        List<DescriptorTable.ReferencedPartitionInfo> partitions = Lists.newArrayList();
        TTableDescriptor tableDescriptor = icebergTable.toThrift(partitions);
        Assertions.assertNotNull(tableDescriptor);
        TIcebergTable tIcebergTable = tableDescriptor.icebergTable;
        Assertions.assertEquals(3, tIcebergTable.getPartition_column_names().size());
        Assertions.assertEquals("partition", tIcebergTable.getPartition_column_names().get(0));
        Assertions.assertEquals("partition", tIcebergTable.getPartition_column_names().get(1));
        Assertions.assertEquals("partition", tIcebergTable.getPartition_column_names().get(2));
    }

    @Test
    public void testRefreshTableException(@Mocked CachingIcebergCatalog icebergCatalog) {
        ConnectContext ctx = new ConnectContext();
        new Expectations() {
            {
                icebergCatalog.refreshTable(anyString, anyString, (ConnectContext) any, null);
                result = new StarRocksConnectorException("refresh failed");
            }
        };

        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergCatalog,
                Executors.newSingleThreadExecutor(), Executors.newSingleThreadExecutor(), null);
        IcebergTable icebergTable = new IcebergTable(1, "srTableName", CATALOG_NAME, "resource_name", "db_name",
                "table_name", "", new ArrayList<>(), mockedNativeTableD, Maps.newHashMap());
        metadata.refreshTable("db", icebergTable, null, true);
    }

    @Test
    public void testAlterTable(@Mocked IcebergHiveCatalog icebergHiveCatalog) throws StarRocksException {
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                Executors.newSingleThreadExecutor(), Executors.newSingleThreadExecutor(), null);

        TableName tableName = new TableName("db", "tbl");
        ColumnDef c1 = new ColumnDef("col1", TypeDef.create(PrimitiveType.INT), true);
        AddColumnClause addColumnClause = new AddColumnClause(c1, null, null, new HashMap<>());

        ColumnDef c2 = new ColumnDef("col2", TypeDef.create(PrimitiveType.BIGINT), true);
        ColumnDef c3 = new ColumnDef("col3", TypeDef.create(PrimitiveType.VARCHAR), true);
        ColumnDef cdt = new ColumnDef("dt", TypeDef.create(PrimitiveType.DATE), true);
        List<ColumnDef> cols = new ArrayList<>();
        cols.add(c2);
        cols.add(c3);
        cols.add(cdt);
        AddColumnsClause addColumnsClause = new AddColumnsClause(cols, null, new HashMap<>());

        List<AlterClause> clauses = Lists.newArrayList();
        clauses.add(addColumnClause);
        clauses.add(addColumnsClause);
        AlterTableStmt stmt = new AlterTableStmt(tableName, clauses);
        metadata.alterTable(new ConnectContext(), stmt);
        clauses.clear();

        // must be default null
        ColumnDef c4 = new ColumnDef("col4", TypeDef.create(PrimitiveType.INT), false);
        AddColumnClause addC4 = new AddColumnClause(c4, null, null, new HashMap<>());
        clauses.add(addC4);
        AlterTableStmt stmtC4 = new AlterTableStmt(tableName, clauses);
        Assertions.assertThrows(DdlException.class, () -> metadata.alterTable(new ConnectContext(), stmtC4));
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
        metadata.alterTable(new ConnectContext(), new AlterTableStmt(tableName, clauses));

        // rename table
        clauses.clear();
        TableRenameClause tableRenameClause = new TableRenameClause("newTbl");
        clauses.add(tableRenameClause);
        metadata.alterTable(new ConnectContext(), new AlterTableStmt(tableName, clauses));

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
        metadata.alterTable(new ConnectContext(), new AlterTableStmt(tableName, clauses));

        // modify empty properties
        clauses.clear();
        Map<String, String> emptyProperties = new HashMap<>();
        ModifyTablePropertiesClause emptyPropertiesClause = new ModifyTablePropertiesClause(emptyProperties);
        clauses.add(emptyPropertiesClause);
        Assertions.assertThrows(DdlException.class,
                () -> metadata.alterTable(new ConnectContext(), new AlterTableStmt(tableName, clauses)));

        // modify unsupported properties
        clauses.clear();
        Map<String, String> invalidProperties = new HashMap<>();
        invalidProperties.put(FILE_FORMAT, "parquet");
        invalidProperties.put(COMPRESSION_CODEC, "zzz");
        ModifyTablePropertiesClause invalidCompressionClause = new ModifyTablePropertiesClause(invalidProperties);
        clauses.add(invalidCompressionClause);
        Assertions.assertThrows(DdlException.class,
                () -> metadata.alterTable(new ConnectContext(), new AlterTableStmt(tableName, clauses)));

        // add & drop partition columns
        {
            SlotRef partitionSlot = new SlotRef(tableName, "dt");
            clauses.clear();
            AddPartitionColumnClause addPartitionColumnClause =
                    new AddPartitionColumnClause(List.of(partitionSlot), NodePosition.ZERO);
            clauses.add(addPartitionColumnClause);
            metadata.alterTable(new ConnectContext(), new AlterTableStmt(tableName, clauses));

            clauses.clear();
            DropPartitionColumnClause dropPartitionColumnClause = new DropPartitionColumnClause(List.of(partitionSlot),
                    NodePosition.ZERO);
            clauses.add(dropPartitionColumnClause);
            metadata.alterTable(new ConnectContext(), new AlterTableStmt(tableName, clauses));
        }
        // add & drop transformed partition columns
        {
            List<String> functions =
                    Lists.newArrayList("year", "month", "day", "truncate", "bucket", "identity", "void", "unknown", "trunc");
            Set<String> badFunctions = new HashSet<>(Lists.newArrayList("void", "unknown", "trunc"));
            SlotRef partitionSlot = new SlotRef(tableName, "dt");
            for (String fn : functions) {
                FunctionCallExpr functionCallExpr = null;
                if (fn.equals("truncate") || fn.equals("bucket")) {
                    functionCallExpr = new FunctionCallExpr(fn, Lists.newArrayList(partitionSlot,
                            new IntLiteral(16)));
                } else {
                    functionCallExpr = new FunctionCallExpr(fn, Lists.newArrayList(partitionSlot));
                }

                clauses.clear();
                AddPartitionColumnClause addPartitionColumnClause =
                        new AddPartitionColumnClause(List.of(functionCallExpr), NodePosition.ZERO);
                clauses.add(addPartitionColumnClause);

                if (badFunctions.contains(fn)) {
                    Assertions.assertThrows(SemanticException.class,
                            () -> metadata.alterTable(new ConnectContext(),
                                    new AlterTableStmt(tableName, clauses)));
                    continue;
                } else {
                    metadata.alterTable(new ConnectContext(), new AlterTableStmt(tableName, clauses));
                }

                clauses.clear();
                DropPartitionColumnClause dropPartitionColumnClause =
                        new DropPartitionColumnClause(List.of(functionCallExpr), NodePosition.ZERO);
                clauses.add(dropPartitionColumnClause);
                if (badFunctions.contains(fn)) {
                    Assertions.assertThrows(SemanticException.class,
                            () -> metadata.alterTable(new ConnectContext(),
                                    new AlterTableStmt(tableName, clauses)));
                } else {
                    metadata.alterTable(new ConnectContext(), new AlterTableStmt(tableName, clauses));
                }
            }
        }
    }

    @Test
    public void testGetIcebergMetricsConfig() {
        List<Column> columns = Lists.newArrayList(new Column("k1", INT),
                new Column("k2", STRING),
                new Column("k3", STRING),
                new Column("k4", STRING),
                new Column("k5", STRING));
        IcebergTable icebergTable = new IcebergTable(1, "srTableName", CATALOG_NAME, "resource_name", "db_name",
                "table_name", "", columns, mockedNativeTableH, Maps.newHashMap());
        Assertions.assertEquals(0, IcebergMetadata.traceIcebergMetricsConfig(mockedNativeTableH).size());
        Map<String, String> icebergProperties = Maps.newHashMap();
        icebergProperties.put("write.metadata.metrics.column.k1", "none");
        icebergProperties.put("write.metadata.metrics.column.k2", "counts");
        icebergProperties.put("write.metadata.metrics.column.k3", "truncate(16)");
        icebergProperties.put("write.metadata.metrics.column.k4", "truncate(32)");
        icebergProperties.put("write.metadata.metrics.column.k5", "full");
        UpdateProperties updateProperties = mockedNativeTableH.updateProperties();
        icebergProperties.forEach(updateProperties::set);
        updateProperties.commit();
        Map<String, MetricsModes.MetricsMode> actual2 = IcebergMetadata.traceIcebergMetricsConfig(mockedNativeTableH);
        Assertions.assertEquals(4, actual2.size());
        Map<String, MetricsModes.MetricsMode> expected2 = Maps.newHashMap();
        expected2.put("k1", MetricsModes.None.get());
        expected2.put("k2", MetricsModes.Counts.get());
        expected2.put("k4", MetricsModes.Truncate.withLength(32));
        expected2.put("k5", MetricsModes.Full.get());
        Assertions.assertEquals(expected2, actual2);
    }

    @Test
    public void testPlanMode() {
        assertThrows(IllegalArgumentException.class, () -> {
            Assertions.assertEquals(PlanMode.AUTO.modeName(), PlanMode.fromName("AUTO").modeName());
            Assertions.assertEquals(PlanMode.AUTO.modeName(), PlanMode.fromName("auto").modeName());
            Assertions.assertEquals(PlanMode.LOCAL.modeName(), PlanMode.fromName("local").modeName());
            Assertions.assertEquals(PlanMode.LOCAL.modeName(), PlanMode.fromName("LOCAL").modeName());
            Assertions.assertEquals(PlanMode.DISTRIBUTED.modeName(), PlanMode.fromName("distributed").modeName());
            Assertions.assertEquals(PlanMode.DISTRIBUTED.modeName(), PlanMode.fromName("DISTRIBUTED").modeName());

            PlanMode.fromName("unknown");
        });
    }

    @Test
    public void testGetMetaSpec(@Mocked LocalMetastore localMetastore, @Mocked TemporaryTableMgr temporaryTableMgr) {
        mockedNativeTableG.newAppend().appendFile(FILE_B_5).commit();
        new MockUp<IcebergHiveCatalog>() {
            @Mock
            org.apache.iceberg.Table getTable(ConnectContext context, String dbName, String tableName)
                    throws StarRocksConnectorException {
                return mockedNativeTableG;
            }

            @Mock
            Database getDB(ConnectContext context, String dbName) {
                return new Database(0, dbName);
            }
        };

        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog(CATALOG_NAME, new Configuration(), DEFAULT_CONFIG);
        CachingIcebergCatalog cachingIcebergCatalog = new CachingIcebergCatalog(
                CATALOG_NAME, icebergHiveCatalog, DEFAULT_CATALOG_PROPERTIES, Executors.newSingleThreadExecutor());
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, cachingIcebergCatalog,
                Executors.newSingleThreadExecutor(), Executors.newSingleThreadExecutor(),
                new IcebergCatalogProperties(DEFAULT_CONFIG));
        ConnectContext.get().getSessionVariable().setEnableIcebergColumnStatistics(false);

        MetadataMgr metadataMgr = new MetadataMgr(localMetastore, temporaryTableMgr, null, null);
        new MockUp<MetadataMgr>() {
            @Mock
            public Optional<ConnectorMetadata> getOptionalMetadata(String catalogName) {
                return Optional.of(metadata);
            }
        };

        SerializedMetaSpec metaSpec = metadataMgr.getSerializedMetaSpec(
                "catalog", "db", "tg", -1, null, MetadataTableType.LOGICAL_ICEBERG_METADATA);
        Assertions.assertTrue(metaSpec instanceof IcebergMetaSpec);
        IcebergMetaSpec icebergMetaSpec = metaSpec.cast();
        List<RemoteMetaSplit> splits = icebergMetaSpec.getSplits();
        Assertions.assertFalse(icebergMetaSpec.loadColumnStats());
        Assertions.assertEquals(1, splits.size());
    }

    @Test
    public void testGetMetaSpecWithDeleteFile(@Mocked LocalMetastore localMetastore,
                                              @Mocked TemporaryTableMgr temporaryTableMgr) {
        mockedNativeTableA.newAppend().appendFile(FILE_A).commit();
        // FILE_A_DELETES = positionalDelete / FILE_A2_DELETES = equalityDelete
        mockedNativeTableA.newRowDelta().addDeletes(FILE_A_DELETES).addDeletes(FILE_A2_DELETES).commit();

        new MockUp<IcebergHiveCatalog>() {
            @Mock
            org.apache.iceberg.Table getTable(ConnectContext context, String dbName, String tableName)
                    throws StarRocksConnectorException {
                return mockedNativeTableA;
            }

            @Mock
            Database getDB(ConnectContext context, String dbName) {
                return new Database(0, dbName);
            }
        };

        Map<String, String> copiedMap = new HashMap<>(DEFAULT_CONFIG);
        copiedMap.put(ENABLE_DISTRIBUTED_PLAN_LOAD_DATA_FILE_COLUMN_STATISTICS_WITH_EQ_DELETE, "false");
        IcebergCatalogProperties catalogProperties = new IcebergCatalogProperties(copiedMap);
        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog(CATALOG_NAME, new Configuration(), DEFAULT_CONFIG);
        CachingIcebergCatalog cachingIcebergCatalog = new CachingIcebergCatalog(
                CATALOG_NAME, icebergHiveCatalog, DEFAULT_CATALOG_PROPERTIES, Executors.newSingleThreadExecutor());
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, cachingIcebergCatalog,
                Executors.newSingleThreadExecutor(), Executors.newSingleThreadExecutor(), catalogProperties);

        MetadataMgr metadataMgr = new MetadataMgr(localMetastore, temporaryTableMgr, null, null);
        new MockUp<MetadataMgr>() {
            @Mock
            public Optional<ConnectorMetadata> getOptionalMetadata(String catalogName) {
                return Optional.of(metadata);
            }
        };

        SerializedMetaSpec metaSpec = metadataMgr.getSerializedMetaSpec(
                "catalog", "db", "tg", -1, null, MetadataTableType.LOGICAL_ICEBERG_METADATA);
        Assertions.assertTrue(metaSpec instanceof IcebergMetaSpec);
        IcebergMetaSpec icebergMetaSpec = metaSpec.cast();
        Assertions.assertFalse(icebergMetaSpec.loadColumnStats());
    }

    @Test
    public void testIcebergMetadataCollectJob() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
        String createCatalog = "CREATE EXTERNAL CATALOG iceberg_catalog PROPERTIES(\"type\"=\"iceberg\", " +
                "\"iceberg.catalog.hive.metastore.uris\"=\"thrift://127.0.0.1:9083\", \"iceberg.catalog.type\"=\"hive\")";
        StarRocksAssert starRocksAssert = new StarRocksAssert();
        starRocksAssert.withCatalog(createCatalog);
        mockedNativeTableC.newAppend().appendFile(FILE_B_1).commit();
        mockedNativeTableC.refresh();

        new MockUp<IcebergMetadata>() {
            @Mock
            public Database getDb(ConnectContext context, String dbName) {
                return new Database(1, "db");
            }
        };

        new MockUp<IcebergHiveCatalog>() {
            @Mock
            org.apache.iceberg.Table getTable(ConnectContext context, String dbName, String tableName)
                    throws StarRocksConnectorException {
                return mockedNativeTableC;
            }

            @Mock
            boolean tableExists(ConnectContext context, String dbName, String tableName) {
                return true;
            }
        };

        long snapshotId = mockedNativeTableC.currentSnapshot().snapshotId();
        MetadataCollectJob collectJob = new IcebergMetadataCollectJob("iceberg_catalog", "db", "table",
                TResultSinkType.METADATA_ICEBERG, snapshotId, "");
        collectJob.init(starRocksAssert.getCtx().getSessionVariable());
        String expectedSql = "SELECT content, file_path, file_format, spec_id, partition_data, record_count," +
                " file_size_in_bytes, split_offsets, sort_id, equality_ids, file_sequence_number," +
                " data_sequence_number , column_stats , key_metadata FROM" +
                " `iceberg_catalog`.`db`.`table$logical_iceberg_metadata` FOR VERSION AS OF 1 WHERE 1=1'";
        Assertions.assertEquals(expectedSql, collectJob.getSql());
        Assertions.assertNotNull(collectJob.getContext());
        Assertions.assertTrue(collectJob.getContext().isMetadataContext());
        collectJob.asyncCollectMetadata();
        Assertions.assertNotNull(collectJob.getMetadataJobCoord());
        Assertions.assertTrue(collectJob.getResultQueue().isEmpty());
    }

    @Test
    public void testFileWrapper() {
        DataFileWrapper wrapper = DataFileWrapper.wrap(FILE_B_1);
        Assertions.assertEquals(wrapper.pos(), FILE_B_1.pos());
        Assertions.assertEquals(wrapper.specId(), FILE_B_1.specId());
        Assertions.assertEquals(wrapper.pos(), FILE_B_1.pos());
        Assertions.assertEquals(wrapper.path(), FILE_B_1.path());
        Assertions.assertEquals(wrapper.format(), FILE_B_1.format());
        Assertions.assertEquals(wrapper.partition(), FILE_B_1.partition());
        Assertions.assertEquals(wrapper.recordCount(), FILE_B_1.recordCount());
        Assertions.assertEquals(wrapper.fileSizeInBytes(), FILE_B_1.fileSizeInBytes());
        Assertions.assertEquals(wrapper.columnSizes(), FILE_B_1.columnSizes());
        Assertions.assertEquals(wrapper.valueCounts(), FILE_B_1.valueCounts());
        Assertions.assertEquals(wrapper.nullValueCounts(), FILE_B_1.nullValueCounts());
        Assertions.assertEquals(wrapper.nanValueCounts(), FILE_B_1.nanValueCounts());
        Assertions.assertEquals(wrapper.lowerBounds(), FILE_B_1.lowerBounds());
        Assertions.assertEquals(wrapper.upperBounds(), FILE_B_1.upperBounds());
        Assertions.assertEquals(wrapper.splitOffsets(), FILE_B_1.splitOffsets());
        Assertions.assertEquals(wrapper.keyMetadata(), FILE_B_1.keyMetadata());

        DeleteFileWrapper deleteFileWrapper = DeleteFileWrapper.wrap(FILE_C_1);
        Assertions.assertEquals(deleteFileWrapper.pos(), FILE_C_1.pos());
        Assertions.assertEquals(deleteFileWrapper.specId(), FILE_C_1.specId());
        Assertions.assertEquals(deleteFileWrapper.pos(), FILE_C_1.pos());
        Assertions.assertEquals(deleteFileWrapper.path(), FILE_C_1.path());
        Assertions.assertEquals(deleteFileWrapper.format(), FILE_C_1.format());
        Assertions.assertEquals(deleteFileWrapper.partition(), FILE_C_1.partition());
        Assertions.assertEquals(deleteFileWrapper.recordCount(), FILE_C_1.recordCount());
        Assertions.assertEquals(deleteFileWrapper.fileSizeInBytes(), FILE_C_1.fileSizeInBytes());
        Assertions.assertEquals(deleteFileWrapper.columnSizes(), FILE_C_1.columnSizes());
        Assertions.assertEquals(deleteFileWrapper.valueCounts(), FILE_C_1.valueCounts());
        Assertions.assertEquals(deleteFileWrapper.nullValueCounts(), FILE_C_1.nullValueCounts());
        Assertions.assertEquals(deleteFileWrapper.nanValueCounts(), FILE_C_1.nanValueCounts());
        Assertions.assertEquals(deleteFileWrapper.lowerBounds(), FILE_C_1.lowerBounds());
        Assertions.assertEquals(deleteFileWrapper.upperBounds(), FILE_C_1.upperBounds());
        Assertions.assertEquals(deleteFileWrapper.splitOffsets(), FILE_C_1.splitOffsets());
        Assertions.assertEquals(deleteFileWrapper.keyMetadata(), FILE_C_1.keyMetadata());
        Assertions.assertEquals(deleteFileWrapper.content(), FILE_C_1.content());
        Assertions.assertEquals(deleteFileWrapper.dataSequenceNumber(), FILE_C_1.dataSequenceNumber());
        Assertions.assertEquals(deleteFileWrapper.fileSequenceNumber(), FILE_C_1.fileSequenceNumber());
    }

    @Test
    public void testVersionRange() {
        TableVersionRange versionRange = TableVersionRange.empty();
        Assertions.assertTrue(versionRange.isEmpty());
        Assertions.assertTrue(versionRange.start().isEmpty());
        versionRange = TableVersionRange.withEnd(Optional.of(1L));
        Assertions.assertFalse(versionRange.isEmpty());
        Assertions.assertNotNull(versionRange.toString());
    }

    @Test
    public void testGetSnapshotIdFromVersion() {
        ConstantOperator constantOperator = new ConstantOperator("2023-01-01", VARCHAR);
        ConnectorTableVersion tableVersion = new ConnectorTableVersion(PointerType.TEMPORAL, constantOperator);
        ConnectorTableVersion finalTableVersion = tableVersion;
        ExceptionChecker.expectThrowsWithMsg(StarRocksConnectorException.class,
                "Invalid temporal version",
                () -> IcebergMetadata.getSnapshotIdFromVersion(mockedNativeTableB, finalTableVersion));

        constantOperator = new ConstantOperator(LocalDateTime.now(), DATE);
        tableVersion = new ConnectorTableVersion(PointerType.TEMPORAL, constantOperator);
        ConnectorTableVersion finalTableVersion1 = tableVersion;
        ExceptionChecker.expectThrowsWithMsg(StarRocksConnectorException.class,
                "Invalid temporal version",
                () -> IcebergMetadata.getSnapshotIdFromVersion(mockedNativeTableB, finalTableVersion1));

        constantOperator = new ConstantOperator("2000-01-01 00:00:00", VARCHAR);
        tableVersion = new ConnectorTableVersion(PointerType.TEMPORAL, constantOperator);
        ConnectorTableVersion finalTableVersion2 = tableVersion;
        ExceptionChecker.expectThrowsWithMsg(StarRocksConnectorException.class,
                "Invalid temporal version",
                () -> IcebergMetadata.getSnapshotIdFromVersion(mockedNativeTableB, finalTableVersion2));

        constantOperator = new ConstantOperator("not_exist", VARCHAR);
        tableVersion = new ConnectorTableVersion(PointerType.VERSION, constantOperator);
        ConnectorTableVersion finalTableVersion3 = tableVersion;
        ExceptionChecker.expectThrowsWithMsg(StarRocksConnectorException.class,
                "Cannot find snapshot with reference name",
                () -> IcebergMetadata.getSnapshotIdFromVersion(mockedNativeTableB, finalTableVersion3));

        constantOperator = new ConstantOperator(123, INT);
        tableVersion = new ConnectorTableVersion(PointerType.VERSION, constantOperator);
        ConnectorTableVersion finalTableVersion4 = tableVersion;
        ExceptionChecker.expectThrowsWithMsg(StarRocksConnectorException.class,
                "Unsupported type for table version",
                () -> IcebergMetadata.getSnapshotIdFromVersion(mockedNativeTableB, finalTableVersion4));
    }

    public void testNullTableUUID() {
        IcebergTable icebergTable = new IcebergTable(1, "srTableName", CATALOG_NAME, "resource_name", "iceberg_db",
                "iceberg_table", "", Lists.newArrayList(), mockedNativeTableA, Maps.newHashMap());
        Assertions.assertEquals(2, icebergTable.getTableIdentifier().split(":").length);
        Assertions.assertEquals(4, icebergTable.getUUID().split("\\.").length);

        new MockUp<TableMetadata>() {
            @Mock
            public String uuid() {
                return null;
            }
        };
        Assertions.assertEquals(1, icebergTable.getTableIdentifier().split(":").length);
        Assertions.assertEquals(3, icebergTable.getUUID().split("\\.").length);
    }

    @Test
    public void testAlterTableExecuteRemoveOrphanFiles(@Mocked IcebergHiveCatalog icebergHiveCatalog,
                                                       @Mocked Snapshot snapshot) throws Exception {
        new MockUp<IcebergMetadata>() {
            @Mock
            public Database getDb(ConnectContext context, String dbName) {
                return new Database(1, "db");
            }
        };

        new MockUp<IcebergHiveCatalog>() {
            @Mock
            org.apache.iceberg.Table getTable(ConnectContext context, String dbName, String tableName)
                    throws StarRocksConnectorException {
                return mockedNativeTableA;
            }

            @Mock
            boolean tableExists(ConnectContext context, String dbName, String tableName) {
                return true;
            }
        };

        // Normalize Date
        TableName tableName = new TableName(CATALOG_NAME, "db", "table");
        AlterTableOperationClause clause = new AlterTableOperationClause(NodePosition.ZERO,
                REMOVE_ORPHAN_FILES.toString(), List.of(), null);
        clause.setArgs(List.of(ConstantOperator.createChar("2024-01-01 00:00:00")));

        IcebergAlterTableExecutor executor = new IcebergAlterTableExecutor(new AlterTableStmt(
                tableName,
                List.of(clause)),
                icebergHiveCatalog.getTable(connectContext, tableName.getDb(), tableName.getTbl()), icebergHiveCatalog,
                connectContext,
                HDFS_ENVIRONMENT);
        executor.execute();

        // Illegal date
        tableName = new TableName(CATALOG_NAME, "db", "table");
        clause = new AlterTableOperationClause(NodePosition.ZERO, REMOVE_ORPHAN_FILES.toString(), List.of(), null);
        clause.setArgs(List.of(ConstantOperator.createChar("illegal date")));

        executor = new IcebergAlterTableExecutor(new AlterTableStmt(
                tableName,
                List.of(clause)),
                icebergHiveCatalog.getTable(connectContext, tableName.getDb(), tableName.getTbl()), icebergHiveCatalog,
                connectContext,
                HDFS_ENVIRONMENT);
        IcebergAlterTableExecutor finalExecutor = executor;
        Assertions.assertThrows(DdlException.class, finalExecutor::execute);

        // Default retention interval
        tableName = new TableName(CATALOG_NAME, "db", "table");
        clause = new AlterTableOperationClause(NodePosition.ZERO, REMOVE_ORPHAN_FILES.toString(), List.of(), null);
        clause.setArgs(List.of(ConstantOperator.createChar("")));

        executor = new IcebergAlterTableExecutor(new AlterTableStmt(
                tableName,
                List.of(clause)),
                icebergHiveCatalog.getTable(connectContext, tableName.getDb(), tableName.getTbl()), icebergHiveCatalog,
                connectContext,
                HDFS_ENVIRONMENT);
        finalExecutor = executor;
        Assertions.assertThrows(DdlException.class, finalExecutor::execute);

        // Mock snapshot behavior
        new Expectations() {{
                snapshot.snapshotId();
                result = 1L;
                minTimes = 0;

                snapshot.timestampMillis();
                result = System.currentTimeMillis();
                minTimes = 0;

                snapshot.parentId();
                result = 0L;
                minTimes = 0;
            }};

        new MockUp<org.apache.iceberg.BaseTable>() {
            @Mock
            public org.apache.iceberg.Snapshot currentSnapshot() {
                return snapshot;
            }

            @Mock
            public Iterable<org.apache.iceberg.Snapshot> snapshots() {
                return List.of(snapshot, snapshot, snapshot);
            }
        };

        // inject snapshot
        tableName = new TableName(CATALOG_NAME, "db", "table");
        clause = new AlterTableOperationClause(NodePosition.ZERO, REMOVE_ORPHAN_FILES.toString(), List.of(), null);
        clause.setArgs(List.of(ConstantOperator.createChar("2024-01-01 00:00:00")));

        executor = new IcebergAlterTableExecutor(new AlterTableStmt(
                tableName,
                List.of(clause)),
                icebergHiveCatalog.getTable(connectContext, tableName.getDb(), tableName.getTbl()), icebergHiveCatalog,
                connectContext,
                HDFS_ENVIRONMENT);
        executor.execute();
    }

    @Test
    public void testRollbackToSnapshot(@Mocked IcebergHiveCatalog icebergHiveCatalog,
                                       @Mocked org.apache.iceberg.BaseTable table,
                                       @Mocked Snapshot snapshot) throws Exception {
        new MockUp<IcebergMetadata>() {
            @Mock
            public Database getDb(ConnectContext context, String dbName) {
                return new Database(1, "db");
            }
        };

        new MockUp<IcebergHiveCatalog>() {
            @Mock
            org.apache.iceberg.Table getTable(ConnectContext context, String dbName, String tableName)
                    throws StarRocksConnectorException {
                return table;
            }

            @Mock
            boolean tableExists(ConnectContext context, String dbName, String tableName) {
                return true;
            }
        };

        new Expectations() {{
                snapshot.snapshotId(); 
                result = 1L;
                minTimes = 0;
                snapshot.parentId(); 
                result = null;
                minTimes = 0;
                snapshot.timestampMillis(); 
                result = System.currentTimeMillis();
                minTimes = 0;

                table.currentSnapshot(); 
                result = snapshot;
                minTimes = 0;
                table.snapshot(1L); 
                result = snapshot;
                minTimes = 0;
                table.snapshots(); 
                result = List.of(snapshot);
                minTimes = 0;
            }};

        TableName tableName = new TableName(CATALOG_NAME, "db", "table");
        AlterTableOperationClause clause = new AlterTableOperationClause(
                NodePosition.ZERO, ROLLBACK_TO_SNAPSHOT.toString(),
                List.of(new IntLiteral(1, NodePosition.ZERO)), null);
        clause.setArgs(List.of(ConstantOperator.createBigint(1)));
        IcebergAlterTableExecutor executor = new IcebergAlterTableExecutor(
                new AlterTableStmt(tableName, List.of(clause)),
                icebergHiveCatalog.getTable(connectContext, tableName.getDb(), tableName.getTbl()),
                icebergHiveCatalog,
                connectContext,
                HDFS_ENVIRONMENT);
        executor.execute();
    }
}
