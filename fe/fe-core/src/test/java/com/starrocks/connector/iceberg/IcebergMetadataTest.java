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
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.IcebergPartitionKey;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.IcebergView;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableName;
import com.starrocks.common.AlreadyExistsException;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.ExceptionChecker;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.tvr.TvrTableDelta;
import com.starrocks.common.tvr.TvrTableDeltaTrait;
import com.starrocks.common.tvr.TvrTableSnapshot;
import com.starrocks.common.tvr.TvrVersion;
import com.starrocks.common.tvr.TvrVersionRange;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.ConnectorMetadataRequestContext;
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
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.iceberg.hive.IcebergHiveCatalog;
import com.starrocks.connector.iceberg.procedure.IcebergProcedureRegistry;
import com.starrocks.connector.iceberg.procedure.RemoveOrphanFilesProcedure;
import com.starrocks.connector.metadata.MetadataCollectJob;
import com.starrocks.connector.metadata.MetadataTableType;
import com.starrocks.connector.metadata.iceberg.IcebergMetadataCollectJob;
import com.starrocks.ha.FrontendNodeType;
import com.starrocks.metric.LongCounterMetric;
import com.starrocks.metric.Metric;
import com.starrocks.metric.MetricLabel;
import com.starrocks.metric.MetricRepo;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.WALApplier;
import com.starrocks.planner.DescriptorTable;
import com.starrocks.planner.IcebergMetadataDeleteNode;
import com.starrocks.planner.PlanNodeId;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.server.MetadataMgr;
import com.starrocks.server.NodeMgr;
import com.starrocks.server.TemporaryTableMgr;
import com.starrocks.sql.analyzer.AnalyzeTestUtil;
import com.starrocks.sql.analyzer.AstToStringBuilder;
import com.starrocks.sql.analyzer.QueryAnalyzer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AddColumnClause;
import com.starrocks.sql.ast.AddColumnsClause;
import com.starrocks.sql.ast.AddPartitionColumnClause;
import com.starrocks.sql.ast.AlterClause;
import com.starrocks.sql.ast.AlterTableCommentClause;
import com.starrocks.sql.ast.AlterTableOperationClause;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.ColumnDef;
import com.starrocks.sql.ast.ColumnPosition;
import com.starrocks.sql.ast.ColumnRenameClause;
import com.starrocks.sql.ast.DropColumnClause;
import com.starrocks.sql.ast.DropPartitionColumnClause;
import com.starrocks.sql.ast.DropTableStmt;
import com.starrocks.sql.ast.ModifyColumnClause;
import com.starrocks.sql.ast.ModifyTablePropertiesClause;
import com.starrocks.sql.ast.QualifiedName;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.ReplacePartitionColumnClause;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.TableRef;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.ast.TableRenameClause;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.ast.expression.NullLiteral;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.ast.expression.TypeDef;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.statistic.AnalyzeJob;
import com.starrocks.statistic.ExternalAnalyzeJob;
import com.starrocks.statistic.StatsConstants;
import com.starrocks.system.Frontend;
import com.starrocks.thrift.TIcebergColumnStats;
import com.starrocks.thrift.TIcebergDataFile;
import com.starrocks.thrift.TIcebergFileContent;
import com.starrocks.thrift.TIcebergTable;
import com.starrocks.thrift.TResultSinkType;
import com.starrocks.thrift.TSinkCommitInfo;
import com.starrocks.thrift.TTableDescriptor;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.StringType;
import com.starrocks.type.TypeFactory;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import mockit.Verifications;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ManifestFile;
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
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.TableScanUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.starrocks.catalog.Table.TableType.ICEBERG;
import static com.starrocks.connector.iceberg.IcebergCatalogProperties.ENABLE_DISTRIBUTED_PLAN_LOAD_DATA_FILE_COLUMN_STATISTICS_WITH_EQ_DELETE;
import static com.starrocks.connector.iceberg.IcebergCatalogProperties.HIVE_METASTORE_URIS;
import static com.starrocks.connector.iceberg.IcebergCatalogProperties.ICEBERG_CATALOG_TYPE;
import static com.starrocks.connector.iceberg.IcebergMetadata.COMPRESSION_CODEC;
import static com.starrocks.connector.iceberg.IcebergMetadata.FILE_FORMAT;
import static com.starrocks.connector.iceberg.IcebergMetadata.LOCATION_PROPERTY;
import static com.starrocks.connector.iceberg.IcebergTableOperation.REMOVE_ORPHAN_FILES;
import static com.starrocks.type.DateType.DATE;
import static com.starrocks.type.DateType.DATETIME;
import static com.starrocks.type.IntegerType.INT;
import static com.starrocks.type.StringType.STRING;
import static com.starrocks.type.VarcharType.VARCHAR;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class IcebergMetadataTest extends TableTestBase {
    private static final String CATALOG_NAME = "iceberg_catalog";
    public static final HdfsEnvironment HDFS_ENVIRONMENT = new HdfsEnvironment();

    public static final IcebergCatalogProperties DEFAULT_CATALOG_PROPERTIES;
    public static final Map<String, String> DEFAULT_CONFIG = new HashMap<>();
    public static ConnectContext connectContext;

    public static GetRemoteFilesParams emptyParams = GetRemoteFilesParams.newBuilder()
            .setTableVersionRange(TvrTableSnapshot.of((long) 1))
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
                Executors.newSingleThreadExecutor(), null);
        List<String> expectResult = Lists.newArrayList("db1", "db2");
        Assertions.assertEquals(expectResult, metadata.listDbNames(connectContext));
    }

    @Test
    public void testGetVersionCommitTimeMillis() {
        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog(CATALOG_NAME, new Configuration(), DEFAULT_CONFIG);
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                Executors.newSingleThreadExecutor(), null);
        mockedNativeTableA.newAppend().appendFile(FILE_A).commit();
        mockedNativeTableA.refresh();
        Snapshot snapshot = mockedNativeTableA.currentSnapshot();
        IcebergTable table = new IcebergTable(1, "tableA", CATALOG_NAME, CATALOG_NAME, "iceberg_db",
                "tableA", "", Lists.newArrayList(), mockedNativeTableA, Maps.newHashMap());

        Assertions.assertEquals(Optional.of(snapshot.timestampMillis()),
                metadata.getVersionCommitTimeMillis("iceberg_db", table, snapshot.snapshotId()));
        Assertions.assertEquals(Optional.empty(),
                metadata.getVersionCommitTimeMillis("iceberg_db", table, snapshot.snapshotId() + 1));
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
                Executors.newSingleThreadExecutor(), null);
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
                Executors.newSingleThreadExecutor(), null);
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
                Executors.newSingleThreadExecutor(), null);
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
                Executors.newSingleThreadExecutor(), null);
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
                Executors.newSingleThreadExecutor(), null);
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
                return ImmutableList.of(new Column("c1", IntegerType.INT, true), new Column("c2", STRING, true));
            }

            @Mock
            public List<Column> getFullVisibleSchema() {
                return ImmutableList.of(new Column("c1", IntegerType.INT, true), new Column("c2", STRING, true));
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

            @Mock
            public int getFormatVersion() {
                return 1;
            }
        };

        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                Executors.newSingleThreadExecutor(), null);
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
        Assertions.assertEquals("CREATE TABLE `tbl` (\n"
                        + "  `c1` int(11) DEFAULT NULL,\n"
                        + "  `c2` varchar(1048576) DEFAULT NULL\n"
                        + ")\n"
                        + "PARTITION BY hour(`c1`)\n"
                        + "ORDER BY (c1 ASC NULLS FIRST,c2 DESC NULLS LAST)\n"
                        + "PROPERTIES (\"format-version\" = \"1\");",
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
                Executors.newSingleThreadExecutor(), null);
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
                Executors.newSingleThreadExecutor(), null);
        Assertions.assertThrows(StarRocksConnectorException.class,
                () -> metadata.getTable(connectContext, "db", "tbl2"));
    }

    @Test
    public void testCreateDuplicatedDb(@Mocked IcebergHiveCatalog icebergHiveCatalog) {
        assertThrows(AlreadyExistsException.class, () -> {
            IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                    Executors.newSingleThreadExecutor(), null);
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
                    Executors.newSingleThreadExecutor(), null);

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
                Executors.newSingleThreadExecutor(), null);

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

    private TableRef createTableRef(TableName tableName) {
        List<String> parts = Lists.newArrayList();
        if (tableName.getCatalog() != null) {
            parts.add(tableName.getCatalog());
        }
        parts.add(tableName.getDb());
        parts.add(tableName.getTbl());
        return new TableRef(QualifiedName.of(parts), null, NodePosition.ZERO);
    }

    @Test
    public void testNormalCreateDb() throws AlreadyExistsException, DdlException {
        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog(CATALOG_NAME, new Configuration(), DEFAULT_CONFIG);
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                Executors.newSingleThreadExecutor(), null);

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
                Executors.newSingleThreadExecutor(), null);
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
                Executors.newSingleThreadExecutor(), null);
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
            public void logAddAnalyzeJob(AnalyzeJob job, WALApplier walApplier) {
                walApplier.apply(job);
            }

            @Mock
            public void logRemoveAnalyzeJob(AnalyzeJob job, WALApplier walApplier) {
                walApplier.apply(job);
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
            metadata.dropTable(connectContext, new DropTableStmt(false,
                    new TableRef(QualifiedName.of(Lists.newArrayList(CATALOG_NAME,
                            "iceberg_db", "table1")), null, NodePosition.ZERO), true));
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
            metadata.dropTable(connectContext, new DropTableStmt(false,
                    new TableRef(QualifiedName.of(Lists.newArrayList(CATALOG_NAME,
                            "iceberg_db", "table1")), null, NodePosition.ZERO), true));
        } catch (Exception e) {
            Assertions.fail();
        }
    }

    @Test
    public void testDropDbFailed() {
        Config.hive_meta_store_timeout_s = 1;
        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog(CATALOG_NAME, new Configuration(), DEFAULT_CONFIG);
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                Executors.newSingleThreadExecutor(), null);

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
                Executors.newSingleThreadExecutor(), null);

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
                Executors.newSingleThreadExecutor(), null);
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

        // Mock NodeMgr and Frontend for commit audit info
        new MockUp<NodeMgr>() {
            @Mock
            public Frontend getMySelf() {
                Frontend frontend = new Frontend(FrontendNodeType.LEADER, "test-fe", "127.0.0.1", 9010);
                return frontend;
            }
        };

        new MockUp<Frontend>() {
            @Mock
            public String getFeVersion() {
                return "test-version-3.5.0";
            }
        };

        // Set ConnectContext to ensure commit audit info can get the current user
        metadata.finishSink("iceberg_db", "iceberg_table", Lists.newArrayList(tSinkCommitInfo),
                null, null, connectContext);

        // Verify snapshot summary contains commit audit info
        mockedNativeTableA.refresh();
        Snapshot snapshot = mockedNativeTableA.currentSnapshot();
        Assertions.assertNotNull(snapshot, "Current snapshot should exist");
        Map<String, String> summary = snapshot.summary();
        Assertions.assertEquals("StarRocks", summary.get("engine-name"),
                "Snapshot summary should contain engine-name=StarRocks");
        Assertions.assertEquals("test-version-3.5.0", summary.get("engine-version"),
                "Snapshot summary should contain engine-version");
        Assertions.assertEquals("root", summary.get("starrocks_user"),
                "Snapshot summary should contain starrocks_user");

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

        metadata.finishSink("iceberg_db", "iceberg_table", Lists.newArrayList(tSinkCommitInfo),
                null, null, connectContext);
        mockedNativeTableA.refresh();

        // Verify overwrite operation also has commit audit info
        snapshot = mockedNativeTableA.currentSnapshot();
        Assertions.assertNotNull(snapshot, "Current snapshot should exist after overwrite");
        summary = snapshot.summary();
        Assertions.assertEquals("StarRocks", summary.get("engine-name"),
                "Snapshot summary should contain engine-name=StarRocks after overwrite");
        Assertions.assertEquals("test-version-3.5.0", summary.get("engine-version"),
                "Snapshot summary should contain engine-version after overwrite");

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
                Executors.newSingleThreadExecutor(), null);
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
    public void testFinishSinkV3RowLineageAutoMaintenance() {
        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog(CATALOG_NAME, new Configuration(), DEFAULT_CONFIG);

        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                Executors.newSingleThreadExecutor(), null);
        TestTables.TestTable mockedNativeTableV3 = create(SCHEMA_A, SPEC_A, "tv3_lineage", 3);
        IcebergTable icebergTable = new IcebergTable(1, "srTableName", CATALOG_NAME, "resource_name", "iceberg_db",
                "iceberg_table", "", Lists.newArrayList(), mockedNativeTableV3, Maps.newHashMap());

        new Expectations(metadata) {
            {
                metadata.getTable((ConnectContext) any, anyString, anyString);
                result = icebergTable;
                minTimes = 0;
            }
        };

        String partitionPath = mockedNativeTableV3.location() + "/data/data_bucket=0/";
        long initialNextRowId = ((BaseTable) mockedNativeTableV3).operations().current().nextRowId();
        String firstPath = partitionPath + "c1.parquet";
        long firstRecordCount = 10;
        TSinkCommitInfo firstCommitInfo = new TSinkCommitInfo();
        TIcebergDataFile firstDataFile = new TIcebergDataFile();
        firstDataFile.setPath(firstPath);
        firstDataFile.setFormat("parquet");
        firstDataFile.setRecord_count(firstRecordCount);
        firstDataFile.setFile_size_in_bytes(1024);
        firstDataFile.setPartition_path(partitionPath);
        firstDataFile.setSplit_offsets(Lists.newArrayList(4L));
        firstDataFile.setPartition_null_fingerprint("0");
        firstCommitInfo.setIs_overwrite(false);
        firstCommitInfo.setIceberg_data_file(firstDataFile);

        metadata.finishSink("iceberg_db", "iceberg_table", Lists.newArrayList(firstCommitInfo), null);
        mockedNativeTableV3.refresh();

        Snapshot firstSnapshot = mockedNativeTableV3.currentSnapshot();
        Assertions.assertNotNull(firstSnapshot);
        Assertions.assertNotNull(firstSnapshot.firstRowId());
        Assertions.assertEquals(initialNextRowId, firstSnapshot.firstRowId().longValue());
        Assertions.assertEquals(initialNextRowId + firstRecordCount,
                ((BaseTable) mockedNativeTableV3).operations().current().nextRowId());
        ManifestFile firstManifest = firstSnapshot.dataManifests(mockedNativeTableV3.io()).get(0);
        Assertions.assertNotNull(firstManifest.firstRowId());
        Assertions.assertEquals(initialNextRowId, firstManifest.firstRowId().longValue());

        List<FileScanTask> firstFileScanTasks = Lists.newArrayList(mockedNativeTableV3.newScan().planFiles());
        Assertions.assertEquals(1, firstFileScanTasks.size());
        DataFile firstCommittedFile = firstFileScanTasks.get(0).file();
        Assertions.assertNotNull(firstCommittedFile.firstRowId());
        Assertions.assertEquals(initialNextRowId, firstCommittedFile.firstRowId().longValue());
        Assertions.assertNotNull(firstCommittedFile.dataSequenceNumber());
        Assertions.assertEquals(firstSnapshot.sequenceNumber(), firstCommittedFile.dataSequenceNumber().longValue());

        long secondStartRowId = ((BaseTable) mockedNativeTableV3).operations().current().nextRowId();
        String secondPath = partitionPath + "c2.parquet";
        long secondRecordCount = 15;
        TSinkCommitInfo secondCommitInfo = new TSinkCommitInfo();
        TIcebergDataFile secondDataFile = new TIcebergDataFile();
        secondDataFile.setPath(secondPath);
        secondDataFile.setFormat("parquet");
        secondDataFile.setRecord_count(secondRecordCount);
        secondDataFile.setFile_size_in_bytes(2048);
        secondDataFile.setPartition_path(partitionPath);
        secondDataFile.setSplit_offsets(Lists.newArrayList(4L));
        secondDataFile.setPartition_null_fingerprint("0");
        secondCommitInfo.setIs_overwrite(false);
        secondCommitInfo.setIceberg_data_file(secondDataFile);

        metadata.finishSink("iceberg_db", "iceberg_table", Lists.newArrayList(secondCommitInfo), null);
        mockedNativeTableV3.refresh();

        Snapshot secondSnapshot = mockedNativeTableV3.currentSnapshot();
        Assertions.assertNotNull(secondSnapshot);
        Assertions.assertNotNull(secondSnapshot.firstRowId());
        Assertions.assertEquals(secondStartRowId, secondSnapshot.firstRowId().longValue());
        Assertions.assertEquals(secondStartRowId + secondRecordCount,
                ((BaseTable) mockedNativeTableV3).operations().current().nextRowId());

        List<FileScanTask> secondFileScanTasks = Lists.newArrayList(mockedNativeTableV3.newScan().planFiles());
        Assertions.assertEquals(2, secondFileScanTasks.size());
        Map<String, Long> firstRowIdByPath = new HashMap<>();
        for (FileScanTask task : secondFileScanTasks) {
            firstRowIdByPath.put(task.file().path().toString(), task.file().firstRowId());
        }
        Assertions.assertEquals(initialNextRowId, firstRowIdByPath.get(firstPath).longValue());
        Assertions.assertEquals(secondStartRowId, firstRowIdByPath.get(secondPath).longValue());
    }

    @Test
    public void testFinishSink3() {
        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog(CATALOG_NAME, new Configuration(), DEFAULT_CONFIG);

        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                Executors.newSingleThreadExecutor(), null);
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

        // Error types are preserved and rethrown directly (not wrapped) by commit queue
        Error error = Assertions.assertThrows(InternalError.class,
                () -> metadata.finishSink("iceberg_db", "iceberg_table", Lists.newArrayList(tSinkCommitInfo), null));
        Assertions.assertTrue(error.getMessage().contains("Invalid partition and fingerprint size"));

    }

    @Test
    public void testFinishSink4() {
        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog(CATALOG_NAME, new Configuration(), DEFAULT_CONFIG);

        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                Executors.newSingleThreadExecutor(), null);
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
                Executors.newSingleThreadExecutor(), null);
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
                Executors.newSingleThreadExecutor(), null);
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
    public void testFinishSinkSkipsEmptyCommit() {
        // Zero-row INSERT / UPDATE / DELETE should NOT produce a new snapshot.
        // Empty DML would otherwise pollute snapshot history and confuse
        // downstream CDC consumers.
        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog(CATALOG_NAME, new Configuration(), DEFAULT_CONFIG);
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                Executors.newSingleThreadExecutor(), null);
        IcebergTable icebergTable = new IcebergTable(1, "srTableName", CATALOG_NAME, "resource_name", "iceberg_db",
                "iceberg_table", "", Lists.newArrayList(), mockedNativeTableA, Maps.newHashMap());

        new Expectations(metadata) {
            {
                metadata.getTable((ConnectContext) any, anyString, anyString);
                result = icebergTable;
                minTimes = 0;
            }
        };

        // Case 1: empty commitInfos against a fresh table — no snapshot should appear.
        mockedNativeTableA.refresh();
        Assertions.assertNull(mockedNativeTableA.currentSnapshot(),
                "Precondition: fresh table has no snapshot");

        metadata.finishSink("iceberg_db", "iceberg_table", Lists.newArrayList(), null);

        mockedNativeTableA.refresh();
        Assertions.assertNull(mockedNativeTableA.currentSnapshot(),
                "Empty commitInfos must not create a snapshot");

        // Case 2: after a real commit, a subsequent empty commit must not advance the snapshot.
        TSinkCommitInfo tSinkCommitInfo = new TSinkCommitInfo();
        TIcebergDataFile tIcebergDataFile = new TIcebergDataFile();
        tIcebergDataFile.setPath(mockedNativeTableA.location() + "/data/data_bucket=0/c.parquet");
        tIcebergDataFile.setFormat("parquet");
        tIcebergDataFile.setRecord_count(10);
        tIcebergDataFile.setSplit_offsets(Lists.newArrayList(4L));
        tIcebergDataFile.setPartition_path(mockedNativeTableA.location() + "/data/data_bucket=0/");
        tIcebergDataFile.setFile_size_in_bytes(2000);
        tIcebergDataFile.setPartition_null_fingerprint("0");
        tSinkCommitInfo.setIs_overwrite(false);
        tSinkCommitInfo.setIceberg_data_file(tIcebergDataFile);

        metadata.finishSink("iceberg_db", "iceberg_table", Lists.newArrayList(tSinkCommitInfo), null);
        mockedNativeTableA.refresh();
        long snapshotIdAfterRealCommit = mockedNativeTableA.currentSnapshot().snapshotId();

        metadata.finishSink("iceberg_db", "iceberg_table", Lists.newArrayList(), null);
        mockedNativeTableA.refresh();
        Assertions.assertEquals(snapshotIdAfterRealCommit, mockedNativeTableA.currentSnapshot().snapshotId(),
                "Empty commitInfos after a real commit must not advance the snapshot");
    }

    @Test
    public void testFinishSinkWithCommitFailed(@Mocked IcebergMetadata.Append append) throws IOException {
        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog(CATALOG_NAME, new Configuration(), DEFAULT_CONFIG);

        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                Executors.newSingleThreadExecutor(), null);
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
                Executors.newSingleThreadExecutor(), DEFAULT_CATALOG_PROPERTIES);
        IcebergTable icebergTable = new IcebergTable(1, "srTableName", CATALOG_NAME, "resource_name", "iceberg_db",
                "iceberg_table", "", columns, mockedNativeTableB, Maps.newHashMap());

        mockedNativeTableB.newAppend().appendFile(FILE_B_1).appendFile(FILE_B_2).commit();
        mockedNativeTableB.refresh();

        long snapshotId = mockedNativeTableB.currentSnapshot().snapshotId();
        ScalarOperator predicate = new BinaryPredicateOperator(BinaryType.GE,
                new ColumnRefOperator(1, INT, "k2", true), ConstantOperator.createInt(1));
        List<RemoteFileInfo> res = metadata.getRemoteFiles(icebergTable,
                GetRemoteFilesParams.newBuilder().setTableVersionRange(TvrTableSnapshot.of(Optional.of(snapshotId)))
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
                GetRemoteFilesParams.newBuilder().setTableVersionRange(TvrTableSnapshot.of(Optional.of(snapshotId)))
                        .setPredicate(predicate).setFieldNames(Lists.newArrayList()).setLimit(10).build());
        Assertions.assertEquals(1, res.size());
        Assertions.assertEquals(3, ((IcebergRemoteFileInfo) res.get(0)).getFileScanTask().file().recordCount());

        PredicateSearchKey filter = PredicateSearchKey.of("db", "table", emptyParams);
        Assertions.assertEquals(
                "Filter{databaseName='db', tableName='table', version=Snapshot@(1), predicate=true, enableColumnStats=false}",
                filter.toString());
    }

    @Test
    public void testGetRemoteFileStringDatePartitionPrune() throws Exception {
        // A table partitioned by an identity STRING column holding date strings. A predicate
        // CAST(datestr AS DATETIME) = <ts> must prune to the matching partition StarRocks-side, since Iceberg's
        // native string comparison would render the constant with a time part and wrongly prune every file.
        PartitionSpec spec = PartitionSpec.builderFor(SCHEMA_J).identity("k2").build();
        TestTables.TestTable table = create(SCHEMA_J, spec, "tbStringDatePart", 1);

        DataFile file0614 = DataFiles.builder(spec)
                .withPath("/path/to/data-0614.parquet").withFileSizeInBytes(20)
                .withPartitionPath("k2=2020-06-14").withRecordCount(3).build();
        DataFile file0615 = DataFiles.builder(spec)
                .withPath("/path/to/data-0615.parquet").withFileSizeInBytes(20)
                .withPartitionPath("k2=2020-06-15").withRecordCount(4).build();
        table.newAppend().appendFile(file0614).appendFile(file0615).commit();
        table.refresh();
        long snapshotId = table.currentSnapshot().snapshotId();

        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog(CATALOG_NAME, new Configuration(), DEFAULT_CONFIG);
        List<Column> columns = Lists.newArrayList(new Column("id", INT), new Column("k1", INT), new Column("k2", VARCHAR));
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                Executors.newSingleThreadExecutor(), DEFAULT_CATALOG_PROPERTIES);
        IcebergTable icebergTable = new IcebergTable(1, "srTableName", CATALOG_NAME, "resource_name", "iceberg_db",
                "iceberg_table", "", columns, table, Maps.newHashMap());

        // CAST(k2 AS DATETIME) = '2020-06-14 00:00:00' -> only the k2=2020-06-14 file survives.
        ScalarOperator predicate = new BinaryPredicateOperator(BinaryType.EQ,
                new CastOperator(DATETIME, new ColumnRefOperator(3, VARCHAR, "k2", true)),
                ConstantOperator.createDatetime(LocalDateTime.of(2020, 6, 14, 0, 0, 0)));
        List<RemoteFileInfo> res = metadata.getRemoteFiles(icebergTable,
                GetRemoteFilesParams.newBuilder().setTableVersionRange(TvrTableSnapshot.of(Optional.of(snapshotId)))
                        .setPredicate(predicate).setFieldNames(Lists.newArrayList()).setLimit(10).build());
        Assertions.assertEquals(1, res.size());
        Assertions.assertEquals(3, ((IcebergRemoteFileInfo) res.get(0)).getFileScanTask().file().recordCount());

        // A non-matching constant prunes everything.
        ScalarOperator noMatch = new BinaryPredicateOperator(BinaryType.EQ,
                new CastOperator(DATETIME, new ColumnRefOperator(3, VARCHAR, "k2", true)),
                ConstantOperator.createDatetime(LocalDateTime.of(2020, 1, 1, 0, 0, 0)));
        List<RemoteFileInfo> empty = metadata.getRemoteFiles(icebergTable,
                GetRemoteFilesParams.newBuilder().setTableVersionRange(TvrTableSnapshot.of(Optional.of(snapshotId)))
                        .setPredicate(noMatch).setFieldNames(Lists.newArrayList()).setLimit(10).build());
        Assertions.assertEquals(0, empty.size());
    }

    @Test
    public void testGetRemoteFileFailsFastForEncryptedTable() {
        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog(CATALOG_NAME, new Configuration(), DEFAULT_CONFIG);
        List<Column> columns = Lists.newArrayList(new Column("k1", INT), new Column("k2", INT));
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                Executors.newSingleThreadExecutor(), DEFAULT_CATALOG_PROPERTIES);
        IcebergTable icebergTable = new IcebergTable(1, "srTableName", CATALOG_NAME, "resource_name", "iceberg_db",
                "iceberg_table", "", columns, mockedNativeTableB, Maps.newHashMap());

        mockedNativeTableB.newAppend().appendFile(FILE_B_1).commit();
        mockedNativeTableB.updateProperties().set(TableProperties.ENCRYPTION_TABLE_KEY, "test-key").commit();
        mockedNativeTableB.refresh();

        long snapshotId = mockedNativeTableB.currentSnapshot().snapshotId();
        ScalarOperator predicate = new BinaryPredicateOperator(BinaryType.GE,
                new ColumnRefOperator(1, INT, "k2", true), ConstantOperator.createInt(1));

        StarRocksConnectorException ex = assertThrows(StarRocksConnectorException.class,
                () -> metadata.getRemoteFiles(icebergTable,
                        GetRemoteFilesParams.newBuilder()
                                .setTableVersionRange(TvrTableSnapshot.of(Optional.of(snapshotId)))
                                .setPredicate(predicate)
                                .setFieldNames(Lists.newArrayList())
                                .setLimit(10)
                                .build()));
        assertTrue(ex.getMessage().contains("encryption is not supported"),
                "Expected encryption error, got: " + ex.getMessage());
    }

    @Test
    public void testGetTableStatistics() {
        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog(CATALOG_NAME, new Configuration(), DEFAULT_CONFIG);

        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                Executors.newSingleThreadExecutor(), DEFAULT_CATALOG_PROPERTIES,
                new ConnectorProperties(ConnectorType.ICEBERG,
                        Map.of(ConnectorProperties.ENABLE_GET_STATS_FROM_EXTERNAL_METADATA, "true")), null);
        mockedNativeTableA.newFastAppend().appendFile(FILE_A).appendFile(FILE_A_1).commit();
        IcebergTable icebergTable = new IcebergTable(1, "srTableName", CATALOG_NAME, "resource_name", "db_name",
                "table_name", "", Lists.newArrayList(), mockedNativeTableA, Maps.newHashMap());
        Map<ColumnRefOperator, Column> colRefToColumnMetaMap = new HashMap<ColumnRefOperator, Column>();
        ColumnRefOperator columnRefOperator1 = new ColumnRefOperator(3, IntegerType.INT, "id", true);
        ColumnRefOperator columnRefOperator2 = new ColumnRefOperator(4, StringType.STRING, "data", true);
        colRefToColumnMetaMap.put(columnRefOperator1, new Column("id", IntegerType.INT));
        colRefToColumnMetaMap.put(columnRefOperator2, new Column("data", StringType.STRING));
        OptimizerContext context = OptimizerFactory.mockContext(new ColumnRefFactory());
        Assertions.assertFalse(context.getSessionVariable().enableIcebergColumnStatistics());
        Assertions.assertTrue(context.getSessionVariable().enableReadIcebergPuffinNdv());
        TvrVersionRange versionRange = TvrTableSnapshot.of(Optional.of(
                mockedNativeTableA.currentSnapshot().snapshotId()));
        Statistics statistics = metadata.getTableStatistics(
                context, icebergTable, colRefToColumnMetaMap, null, null, -1, versionRange);
        Assertions.assertEquals(4.0, statistics.getOutputRowCount(), 0.001);
        Assertions.assertEquals(2, statistics.getColumnStatistics().size());
        Assertions.assertTrue(statistics.getColumnStatistic(columnRefOperator1).isUnknown());
        Assertions.assertTrue(statistics.getColumnStatistic(columnRefOperator2).isUnknown());
    }

    private IcebergMetadata newStatsMetadata() {
        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog(CATALOG_NAME, new Configuration(), DEFAULT_CONFIG);
        return new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                Executors.newSingleThreadExecutor(), DEFAULT_CATALOG_PROPERTIES,
                new ConnectorProperties(ConnectorType.ICEBERG,
                        Map.of(ConnectorProperties.ENABLE_GET_STATS_FROM_EXTERNAL_METADATA, "true")), null);
    }

    private double manifestPrunedRowCount(IcebergMetadata metadata, IcebergTable icebergTable,
                                          TestTables.TestTable table, ScalarOperator predicate) {
        Map<ColumnRefOperator, Column> colMap = new HashMap<>();
        colMap.put(new ColumnRefOperator(3, VARCHAR, "k2", true), new Column("k2", VARCHAR));
        OptimizerContext context = OptimizerFactory.mockContext(new ColumnRefFactory());
        // Force the manifest-statistics path (Path C) explicitly, so the test does not depend on the
        // default of enableIcebergColumnStatistics staying false.
        context.getSessionVariable().setEnableIcebergColumnStatistics(false);
        TvrVersionRange versionRange = TvrTableSnapshot.of(Optional.of(table.currentSnapshot().snapshotId()));
        return metadata.getTableStatistics(context, icebergTable, colMap, null, predicate, -1, versionRange)
                .getOutputRowCount();
    }

    private ScalarOperator k2EqDate(int year, int month, int day) {
        return new BinaryPredicateOperator(BinaryType.EQ,
                new CastOperator(DATETIME, new ColumnRefOperator(3, VARCHAR, "k2", true)),
                ConstantOperator.createDatetime(LocalDateTime.of(year, month, day, 0, 0, 0)));
    }

    @Test
    public void testGetDeleteFilesStringDatePartitionCast() throws Exception {
        // An equality-delete file lives in partition k2=2020-06-14. Previously getDeleteFiles pushed
        // CAST(k2 AS DATETIME) = <ts> to Iceberg's string-domain scan filter, which pruned this delete file
        // (its 'yyyy-MM-dd' partition value never equals the rendered '...00:00:00' string), so the equality
        // delete was not applied and deleted rows leaked. It must now be kept.
        PartitionSpec spec = PartitionSpec.builderFor(SCHEMA_J).identity("k2").build();
        TestTables.TestTable table = create(SCHEMA_J, spec, "tbGetDeleteFiles", 2);
        table.newFastAppend().appendFile(DataFiles.builder(spec)
                .withPath("/path/to/gdf-0614.parquet").withFileSizeInBytes(20)
                .withPartitionPath("k2=2020-06-14").withRecordCount(3).build()).commit();
        DeleteFile eqDelete = FileMetadata.deleteFileBuilder(spec)
                .ofEqualityDeletes(3)   // k2 field id
                .withPath("/path/to/gdf-0614-eqdel.orc").withFormat(FileFormat.ORC)
                .withFileSizeInBytes(10).withPartitionPath("k2=2020-06-14").withRecordCount(1).build();
        table.newRowDelta().addDeletes(eqDelete).commit();
        table.refresh();
        List<Column> columns = Lists.newArrayList(
                new Column("id", INT), new Column("k1", INT), new Column("k2", VARCHAR));
        IcebergTable icebergTable = new IcebergTable(1, "srTableName", CATALOG_NAME, "resource_name", "iceberg_db",
                "iceberg_table", "", columns, table, Maps.newHashMap());
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT,
                new IcebergHiveCatalog(CATALOG_NAME, new Configuration(), DEFAULT_CONFIG),
                Executors.newSingleThreadExecutor(), DEFAULT_CATALOG_PROPERTIES);
        long snapshotId = table.currentSnapshot().snapshotId();

        // cast predicate matching the partition -> delete file kept (previously wrongly pruned in the string domain).
        Set<DeleteFile> matched = metadata.getDeleteFiles(
                icebergTable, snapshotId, k2EqDate(2020, 6, 14), FileContent.EQUALITY_DELETES);
        Assertions.assertEquals(1, matched.size(),
                "equality-delete file must not be pruned by a cast-on-string-partition predicate");

        // cast predicate for a different date -> the 2020-06-14 delete file definitely cannot match -> pruned.
        Set<DeleteFile> none = metadata.getDeleteFiles(
                icebergTable, snapshotId, k2EqDate(2019, 1, 1), FileContent.EQUALITY_DELETES);
        Assertions.assertTrue(none.isEmpty(),
                "a definitely-non-matching partition's delete file should be pruned StarRocks-side");
    }

    @Test
    public void testGetManifestPrunedRowCountSinglePartitionManifests() throws Exception {
        // STRING partition column, CAST(k2 AS DATETIME) = <ts>. Two commits => two single-partition manifests.
        // The residual is evaluated against each manifest's partition range in the DATETIME domain: the
        // 2020-06-15 manifest is pruned, the 2020-06-14 one is kept -> rowCount=3 (NOT the string-pruned 1, and
        // tighter than the whole-table 7).
        IcebergMetadata metadata = newStatsMetadata();
        PartitionSpec spec = PartitionSpec.builderFor(SCHEMA_J).identity("k2").build();
        TestTables.TestTable table = create(SCHEMA_J, spec, "tbManifestSinglePart", 1);
        table.newFastAppend().appendFile(DataFiles.builder(spec)
                .withPath("/path/to/data-0614.parquet").withFileSizeInBytes(20)
                .withPartitionPath("k2=2020-06-14").withRecordCount(3).build()).commit();
        table.newFastAppend().appendFile(DataFiles.builder(spec)
                .withPath("/path/to/data-0615.parquet").withFileSizeInBytes(20)
                .withPartitionPath("k2=2020-06-15").withRecordCount(4).build()).commit();
        table.refresh();
        List<Column> columns = Lists.newArrayList(
                new Column("id", INT), new Column("k1", INT), new Column("k2", VARCHAR));
        IcebergTable icebergTable = new IcebergTable(1, "srTableName", CATALOG_NAME, "resource_name", "iceberg_db",
                "iceberg_table", "", columns, table, Maps.newHashMap());

        Assertions.assertEquals(3.0, manifestPrunedRowCount(metadata, icebergTable, table, k2EqDate(2020, 6, 14)), 0.001);
        // A date outside every partition -> both manifests pruned -> floor of 1.
        Assertions.assertEquals(1.0, manifestPrunedRowCount(metadata, icebergTable, table, k2EqDate(2019, 1, 1)), 0.001);
    }

    @Test
    public void testGetManifestPrunedRowCountMultiPartitionManifest() throws Exception {
        // A single commit => ONE manifest spanning two partitions [2020-06-14, 2020-06-15]. A kept manifest is
        // counted in full, so a match inside the range over-estimates (3+4=7); a const outside the whole range is
        // still pruned (floor of 1).
        IcebergMetadata metadata = newStatsMetadata();
        PartitionSpec spec = PartitionSpec.builderFor(SCHEMA_J).identity("k2").build();
        TestTables.TestTable table = create(SCHEMA_J, spec, "tbManifestMultiPart", 1);
        table.newFastAppend()
                .appendFile(DataFiles.builder(spec).withPath("/path/to/data-0614.parquet").withFileSizeInBytes(20)
                        .withPartitionPath("k2=2020-06-14").withRecordCount(3).build())
                .appendFile(DataFiles.builder(spec).withPath("/path/to/data-0615.parquet").withFileSizeInBytes(20)
                        .withPartitionPath("k2=2020-06-15").withRecordCount(4).build())
                .commit();
        table.refresh();
        List<Column> columns = Lists.newArrayList(
                new Column("id", INT), new Column("k1", INT), new Column("k2", VARCHAR));
        IcebergTable icebergTable = new IcebergTable(1, "srTableName", CATALOG_NAME, "resource_name", "iceberg_db",
                "iceberg_table", "", columns, table, Maps.newHashMap());

        // Const inside the manifest's [2020-06-14, 2020-06-15] range -> kept whole -> over-estimate 7.
        Assertions.assertEquals(7.0, manifestPrunedRowCount(metadata, icebergTable, table, k2EqDate(2020, 6, 14)), 0.001);
        // Const outside the whole range -> pruned -> floor of 1.
        Assertions.assertEquals(1.0, manifestPrunedRowCount(metadata, icebergTable, table, k2EqDate(2019, 1, 1)), 0.001);
    }

    @Test
    public void testGetTableStatisticsManifestPrunedKeepsIncrementalDelivery() throws Exception {
        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog(CATALOG_NAME, new Configuration(), DEFAULT_CONFIG);
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                Executors.newSingleThreadExecutor(), DEFAULT_CATALOG_PROPERTIES,
                new ConnectorProperties(ConnectorType.ICEBERG,
                        Map.of(ConnectorProperties.ENABLE_GET_STATS_FROM_EXTERNAL_METADATA, "true")), null);
        // Two separate commits => two data manifests. Manifest-pruned row count must sum across manifests
        // (FILE_A=2 + FILE_A_2=2 = 4).
        mockedNativeTableA.newFastAppend().appendFile(FILE_A).commit();
        mockedNativeTableA.newFastAppend().appendFile(FILE_A_2).commit();
        mockedNativeTableA.refresh();
        IcebergTable icebergTable = new IcebergTable(1, "srTableName", CATALOG_NAME, "resource_name", "db_name",
                "table_name", "", Lists.newArrayList(), mockedNativeTableA, Maps.newHashMap());
        Map<ColumnRefOperator, Column> colRefToColumnMetaMap = new HashMap<ColumnRefOperator, Column>();
        ColumnRefOperator columnRefOperator1 = new ColumnRefOperator(3, IntegerType.INT, "id", true);
        colRefToColumnMetaMap.put(columnRefOperator1, new Column("id", IntegerType.INT));
        OptimizerContext context = OptimizerFactory.mockContext(new ColumnRefFactory());
        Assertions.assertFalse(context.getSessionVariable().enableIcebergColumnStatistics());
        TvrVersionRange versionRange = TvrTableSnapshot.of(Optional.of(
                mockedNativeTableA.currentSnapshot().snapshotId()));
        Statistics statistics = metadata.getTableStatistics(
                context, icebergTable, colRefToColumnMetaMap, null, null, -1, versionRange);
        Assertions.assertEquals(4.0, statistics.getOutputRowCount(), 0.001);

        // The default (manifest-pruned) path must NOT enumerate DataFiles, i.e. it must not pre-populate
        // splitTasks. Otherwise getRemoteFilesAsync would replay an eager full list and incremental scan
        // range delivery would be defeated.
        java.lang.reflect.Field splitTasksField = IcebergMetadata.class.getDeclaredField("splitTasks");
        splitTasksField.setAccessible(true);
        Map<?, ?> splitTasks = (Map<?, ?>) splitTasksField.get(metadata);
        Assertions.assertTrue(splitTasks.isEmpty());
    }

    @Test
    public void testGetTableStatisticsIncrementalDeltaCostsDeltaOnly() {
        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog(CATALOG_NAME, new Configuration(), DEFAULT_CONFIG);
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                Executors.newSingleThreadExecutor(), DEFAULT_CATALOG_PROPERTIES,
                new ConnectorProperties(ConnectorType.ICEBERG,
                        Map.of(ConnectorProperties.ENABLE_GET_STATS_FROM_EXTERNAL_METADATA, "true")), null);
        // snap1: FILE_A (2 rows). snap2: FILE_A_1 (2 rows). Full table at snap2 = 4 rows.
        mockedNativeTableA.newFastAppend().appendFile(FILE_A).commit();
        Snapshot snap1 = mockedNativeTableA.currentSnapshot();
        mockedNativeTableA.newFastAppend().appendFile(FILE_A_1).commit();
        mockedNativeTableA.refresh();
        Snapshot snap2 = mockedNativeTableA.currentSnapshot();

        IcebergTable icebergTable = new IcebergTable(1, "srTableName", CATALOG_NAME, "resource_name", "db_name",
                "table_name", "", Lists.newArrayList(), mockedNativeTableA, Maps.newHashMap());
        Map<ColumnRefOperator, Column> colRefToColumnMetaMap = new HashMap<ColumnRefOperator, Column>();
        ColumnRefOperator columnRefOperator1 = new ColumnRefOperator(3, IntegerType.INT, "id", true);
        colRefToColumnMetaMap.put(columnRefOperator1, new Column("id", IntegerType.INT));
        OptimizerContext context = OptimizerFactory.mockContext(new ColumnRefFactory());
        Assertions.assertFalse(context.getSessionVariable().enableIcebergColumnStatistics());

        // Append-only delta snap1(exclusive) -> snap2(inclusive) must be costed on the delta only
        // (FILE_A_1 = 2 rows), NOT the full-table manifest-pruned count (4 rows).
        TvrTableDelta delta = TvrTableDelta.of(TvrVersion.of(snap1.snapshotId()), TvrVersion.of(snap2.snapshotId()));
        Statistics statistics = metadata.getTableStatistics(
                context, icebergTable, colRefToColumnMetaMap, null, null, -1, delta);
        Assertions.assertEquals(2.0, statistics.getOutputRowCount(), 0.001);
    }

    @Test
    public void testGetTableStatisticsWithColumnStats() {
        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog(CATALOG_NAME, new Configuration(), DEFAULT_CONFIG);
        List<Column> columns = Lists.newArrayList(new Column("k1", INT), new Column("k2", INT));
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                Executors.newSingleThreadExecutor(), DEFAULT_CATALOG_PROPERTIES,
                new ConnectorProperties(ConnectorType.ICEBERG,
                        Map.of(ConnectorProperties.ENABLE_GET_STATS_FROM_EXTERNAL_METADATA, "true")), null);
        mockedNativeTableB.newFastAppend().appendFile(FILE_B_3).commit();
        mockedNativeTableB.newFastAppend().appendFile(FILE_B_4).commit();
        mockedNativeTableB.refresh();
        IcebergTable icebergTable = new IcebergTable(1, "srTableName", CATALOG_NAME, "resource_name", "db_name",
                "table_name", "", columns, mockedNativeTableB, Maps.newHashMap());
        Map<ColumnRefOperator, Column> colRefToColumnMetaMap = new HashMap<ColumnRefOperator, Column>();
        ColumnRefOperator columnRefOperator1 = new ColumnRefOperator(3, IntegerType.INT, "k1", true);
        ColumnRefOperator columnRefOperator2 = new ColumnRefOperator(4, INT, "k2", true);
        colRefToColumnMetaMap.put(columnRefOperator1, new Column("k1", IntegerType.INT));
        colRefToColumnMetaMap.put(columnRefOperator2, new Column("k2", IntegerType.INT));
        new ConnectContext().setThreadLocalInfo();
        ConnectContext.get().getSessionVariable().setEnableIcebergColumnStatistics(true);
        TvrVersionRange versionRange = TvrTableSnapshot.of(Optional.of(
                mockedNativeTableB.currentSnapshot().snapshotId()));
        Statistics statistics = metadata.getTableStatistics(
                OptimizerFactory.mockContext(ConnectContext.get(), null),
                icebergTable, colRefToColumnMetaMap, null, null, -1, versionRange);
        Assertions.assertEquals(4.0, statistics.getOutputRowCount(), 0.001);
        Assertions.assertEquals(2, statistics.getColumnStatistics().size());
        Assertions.assertFalse(statistics.getColumnStatistic(columnRefOperator1).isUnknown());
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
                Executors.newSingleThreadExecutor(), DEFAULT_CATALOG_PROPERTIES);
        mockedNativeTableA.newFastAppend().appendFile(FILE_A).commit();
        IcebergTable icebergTable = new IcebergTable(1, "srTableName", CATALOG_NAME, "resource_name", "db_name",
                "table_name", "", columns, mockedNativeTableA, Maps.newHashMap());
        Map<ColumnRefOperator, Column> colRefToColumnMetaMap = new HashMap<ColumnRefOperator, Column>();
        ColumnRefOperator columnRefOperator1 = new ColumnRefOperator(3, IntegerType.INT, "id", true);
        ColumnRefOperator columnRefOperator2 = new ColumnRefOperator(4, StringType.STRING, "data", true);
        colRefToColumnMetaMap.put(columnRefOperator1, new Column("id", IntegerType.INT));
        colRefToColumnMetaMap.put(columnRefOperator2, new Column("data", StringType.STRING));
        new ConnectContext().setThreadLocalInfo();

        TvrVersionRange version = TvrTableSnapshot.of(Optional.of(
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
        TvrVersionRange versionRange = TvrTableSnapshot.of(Optional.of(
                mockedNativeTableA.currentSnapshot().snapshotId()));
        partitionKeys = metadata.getPrunedPartitions(icebergTable, null, 100, versionRange);
        Assertions.assertEquals(2, partitionKeys.size());
    }

    @Test
    public void testPruneNullPartition() {
        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog(CATALOG_NAME, new Configuration(), DEFAULT_CONFIG);
        List<Column> columns = Lists.newArrayList(new Column("k1", INT), new Column("dt", DATE));
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                Executors.newSingleThreadExecutor(), DEFAULT_CATALOG_PROPERTIES);
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
        TvrVersionRange versionRange = TvrTableSnapshot.of(Optional.of(
                mockedNativeTableI.currentSnapshot().snapshotId()));
        List<PartitionKey> partitionKeys = metadata.getPrunedPartitions(icebergTable, null, -1, versionRange);
        Assertions.assertTrue(partitionKeys.get(0).getKeys().get(0) instanceof NullLiteral);
    }

    @Test
    public void testPartitionPruneWithDuplicated() {
        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog(CATALOG_NAME, new Configuration(), DEFAULT_CONFIG);
        List<Column> columns = Lists.newArrayList(new Column("id", INT), new Column("data", STRING));
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                Executors.newSingleThreadExecutor(), DEFAULT_CATALOG_PROPERTIES);
        mockedNativeTableA.newFastAppend().appendFile(FILE_A).appendFile(FILE_A_1).commit();
        mockedNativeTableA.refresh();
        IcebergTable icebergTable = new IcebergTable(1, "srTableName", CATALOG_NAME, "resource_name", "db_name",
                "table_name", "", columns, mockedNativeTableA, Maps.newHashMap());
        TvrVersionRange versionRange = TvrTableSnapshot.of(Optional.of(
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
                Executors.newSingleThreadExecutor(), DEFAULT_CATALOG_PROPERTIES,
                new ConnectorProperties(ConnectorType.ICEBERG,
                        Map.of(ConnectorProperties.ENABLE_GET_STATS_FROM_EXTERNAL_METADATA, "true")), null);
        Map<ColumnRefOperator, Column> colRefToColumnMetaMap = new HashMap<ColumnRefOperator, Column>();
        ColumnRefOperator columnRefOperator1 = new ColumnRefOperator(3, IntegerType.INT, "id", true);
        ColumnRefOperator columnRefOperator2 = new ColumnRefOperator(4, StringType.STRING, "data", true);
        colRefToColumnMetaMap.put(columnRefOperator1, new Column("id", IntegerType.INT));
        colRefToColumnMetaMap.put(columnRefOperator2, new Column("data", StringType.STRING));
        mockedNativeTableA.newFastAppend().appendFile(FILE_A).commit();
        mockedNativeTableA.newFastAppend().appendFile(FILE_A).commit();
        mockedNativeTableA.refresh();

        new ConnectContext().setThreadLocalInfo();
        OptimizerContext context = OptimizerFactory.mockContext(ConnectContext.get(), new ColumnRefFactory());
        context.getSessionVariable().setEnableIcebergColumnStatistics(true);

        TvrVersionRange version = TvrTableSnapshot.of(Optional.of(
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
                Executors.newSingleThreadExecutor(), DEFAULT_CATALOG_PROPERTIES);
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
        TvrVersionRange version = TvrTableSnapshot.of(Optional.of(
                mockedNativeTableE.currentSnapshot().snapshotId()));
        List<PartitionKey> partitionKeys = metadata.getPrunedPartitions(icebergTable, null, 1, version);
        Assertions.assertEquals("2023-10-30 03:45:56", partitionKeys.get(0).getKeys().get(0).getStringValue());
    }

    @Test
    public void testTransformedPartitionPrune() {
        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog(CATALOG_NAME, new Configuration(), DEFAULT_CONFIG);
        List<Column> columns = Lists.newArrayList(new Column("k1", INT), new Column("ts", DATETIME));
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                Executors.newSingleThreadExecutor(), DEFAULT_CATALOG_PROPERTIES);
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
        TvrVersionRange version = TvrTableSnapshot.of(Optional.of(
                mockedNativeTableD.currentSnapshot().snapshotId()));
        List<PartitionKey> partitionKeys = metadata.getPrunedPartitions(icebergTable, null, -1, version);
        Assertions.assertEquals("438292", partitionKeys.get(0).getKeys().get(0).getStringValue());
    }

    @Test
    public void testDateDayPartitionPrune() {
        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog(CATALOG_NAME, new Configuration(), DEFAULT_CONFIG);
        List<Column> columns = Lists.newArrayList(new Column("k1", INT), new Column("dt", DATE));
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                Executors.newSingleThreadExecutor(), DEFAULT_CATALOG_PROPERTIES);
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
        TvrVersionRange version = TvrTableSnapshot.of(Optional.of(
                mockedNativeTableF.currentSnapshot().snapshotId()));
        List<PartitionKey> partitionKeys = metadata.getPrunedPartitions(icebergTable, null, -1, version);
        Assertions.assertEquals("19660", partitionKeys.get(0).getKeys().get(0).getStringValue());
    }

    @Test
    public void testIcebergFilter() {
        List<ScalarOperator> arguments = new ArrayList<>(2);
        arguments.add(ConstantOperator.createVarchar("day"));
        arguments.add(new ColumnRefOperator(2, IntegerType.INT, "date_col", true));
        ScalarOperator callOperator = new CallOperator("date_trunc", DateType.DATE, arguments);

        List<ScalarOperator> newArguments = new ArrayList<>(2);
        newArguments.add(ConstantOperator.createVarchar("day"));
        newArguments.add(new ColumnRefOperator(22, IntegerType.INT, "date_col", true));
        ScalarOperator newCallOperator = new CallOperator("date_trunc", DateType.DATE, newArguments);

        GetRemoteFilesParams callParams = GetRemoteFilesParams.newBuilder()
                .setTableVersionRange(TvrTableSnapshot.of(Optional.of(1L)))
                .setPredicate(callOperator)
                .setFieldNames(Lists.newArrayList())
                .setLimit(10)
                .build();

        GetRemoteFilesParams newCallParams = GetRemoteFilesParams.newBuilder()
                .setTableVersionRange(TvrTableSnapshot.of(Optional.of(1L)))
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
    public void testPredicateSearchKeyIgnoresFieldNames() {
        GetRemoteFilesParams leftParams = GetRemoteFilesParams.newBuilder()
                .setTableVersionRange(TvrTableSnapshot.of(Optional.of(1L)))
                .setFieldNames(Lists.newArrayList("c1"))
                .build();
        GetRemoteFilesParams rightParams = GetRemoteFilesParams.newBuilder()
                .setTableVersionRange(TvrTableSnapshot.of(Optional.of(1L)))
                .setFieldNames(Lists.newArrayList("c2"))
                .build();

        PredicateSearchKey leftKey = PredicateSearchKey.of("db", "table", leftParams);
        PredicateSearchKey rightKey = PredicateSearchKey.of("db", "table", rightParams);

        Assertions.assertEquals(leftKey, rightKey);
        Assertions.assertEquals(leftKey.hashCode(), rightKey.hashCode());
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
                Executors.newSingleThreadExecutor(), null);
        TvrVersionRange version = TvrTableSnapshot.of(Optional.of(
                mockedNativeTableB.currentSnapshot().snapshotId()));
        ConnectorMetadataRequestContext requestContext = new ConnectorMetadataRequestContext();
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
                Executors.newSingleThreadExecutor(), null);

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
                Executors.newSingleThreadExecutor(), null);

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
                Executors.newSingleThreadExecutor(), null);

        IcebergTable icebergTable = new IcebergTable(1, "srTableName", CATALOG_NAME,
                "resource_name", "db",
                "table", "", Lists.newArrayList(), mockedNativeTableB, Maps.newHashMap());

        List<PartitionInfo> partitions = metadata.getPartitions(icebergTable, ImmutableList.of("k2=2", "k2=3"));
        Assertions.assertEquals(2, partitions.size());
        // When snapshot has been expired, last_updated_at can be null and modifiedTime will be -1.
        // version (stats fingerprint) must be non-negative for DefaultTraits version comparison path.
        Assertions.assertTrue(partitions.stream().noneMatch(x -> x.getVersion() == -1));
    }

    @Test
    public void testExpiredSnapshotPartitionsHaveDistinctVersions() {
        // FILE_B_1: k2=2, recordCount=3, fileSize=20
        // FILE_B_2: k2=3, recordCount=4, fileSize=20
        // Append each to separate snapshots, then expire both.
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
        IcebergTable icebergTable = new IcebergTable(1, "srTableName", CATALOG_NAME,
                "resource_name", "db",
                "table", "", Lists.newArrayList(), mockedNativeTableB, Maps.newHashMap());

        Map<String, Partition> partitionMap = icebergHiveCatalog.getPartitions(icebergTable, -1, null);
        Assertions.assertEquals(2, partitionMap.size());
        Partition p1 = partitionMap.get("k2=2");
        Partition p2 = partitionMap.get("k2=3");
        Assertions.assertNotNull(p1);
        Assertions.assertNotNull(p2);
        // Both versions must be >= 0 (valid for DefaultTraits comparison)
        Assertions.assertTrue(p1.getVersion() >= 0,
                "Expired snapshot partition k2=2 must have non-negative version, got: " + p1.getVersion());
        Assertions.assertTrue(p2.getVersion() >= 0,
                "Expired snapshot partition k2=3 must have non-negative version, got: " + p2.getVersion());
        // Partitions with different stats (different recordCount) must have distinct versions
        Assertions.assertNotEquals(p1.getVersion(), p2.getVersion(),
                "Partitions with different record counts should have distinct version fingerprints");
    }

    @Test
    public void testExpiredSnapshotVersionStableWhenUnrelatedPartitionChanges() {
        // Append FILE_B_1 (k2=2, recordCount=3) and expire that snapshot.
        mockedNativeTableB.newAppend().appendFile(FILE_B_1).commit();
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
        IcebergTable icebergTable = new IcebergTable(1, "srTableName", CATALOG_NAME,
                "resource_name", "db",
                "table", "", Lists.newArrayList(), mockedNativeTableB, Maps.newHashMap());

        // Capture the version of k2=2 when its snapshot is expired.
        Map<String, Partition> partitionMapBefore = icebergHiveCatalog.getPartitions(icebergTable, -1, null);
        Partition p1Before = partitionMapBefore.get("k2=2");
        Assertions.assertNotNull(p1Before);
        long versionBefore = p1Before.getVersion();
        Assertions.assertTrue(versionBefore >= 0,
                "Expired snapshot partition must have non-negative version, got: " + versionBefore);

        // Now write to a different partition (k2=3) — this creates a new non-expired snapshot for the table.
        // FILE_B_2: k2=3, recordCount=4, fileSize=20
        mockedNativeTableB.newAppend().appendFile(FILE_B_2).commit();
        mockedNativeTableB.refresh();

        // Capture version of k2=2 again — its stats are unchanged, so the fingerprint must be stable.
        Map<String, Partition> partitionMapAfter = icebergHiveCatalog.getPartitions(icebergTable, -1, null);
        Partition p1After = partitionMapAfter.get("k2=2");
        Assertions.assertNotNull(p1After);
        long versionAfter = p1After.getVersion();

        // Core regression test: writing to k2=3 must NOT change k2=2's version.
        Assertions.assertEquals(versionBefore, versionAfter,
                "Writing to an unrelated partition (k2=3) must not change the version of k2=2's expired-snapshot fingerprint");
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
        new ConnectContext();
        new Expectations() {
            {
                icebergCatalog.refreshTable(anyString, anyString, (ConnectContext) any, null);
                result = new StarRocksConnectorException("refresh failed");
            }
        };

        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergCatalog,
                Executors.newSingleThreadExecutor(), null);
        IcebergTable icebergTable = new IcebergTable(1, "srTableName", CATALOG_NAME, "resource_name", "db_name",
                "table_name", "", new ArrayList<>(), mockedNativeTableD, Maps.newHashMap());
        metadata.refreshTable("db", icebergTable, null, true);
    }

    @Test
    public void testRefreshViewInvalidatesCache(@Mocked CachingIcebergCatalog icebergCatalog) {
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergCatalog,
                Executors.newSingleThreadExecutor(), null);
        // A view is a ConnectorView, not an IcebergTable, so refresh must invalidate by name, not cast.
        // The stored name is fully-qualified, so it strips down to the simple key ("v").
        IcebergView view = new IcebergView(1, CATALOG_NAME, "db", CATALOG_NAME + ".db.v", new ArrayList<>(),
                "select 1", CATALOG_NAME, "db", "s3://loc", Maps.newHashMap());
        metadata.refreshTable("db", view, null, true);

        new Verifications() {
            {
                icebergCatalog.invalidateCache("db", "v");
                times = 1;
            }
        };
    }

    @Test
    public void testAlterTable(@Mocked IcebergHiveCatalog icebergHiveCatalog) throws StarRocksException {
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                Executors.newSingleThreadExecutor(), null);

        TableName tableName = new TableName("db", "tbl");
        ColumnDef c1 = new ColumnDef("col1", new TypeDef(TypeFactory.createType(PrimitiveType.INT)), true);
        AddColumnClause addColumnClause = new AddColumnClause(c1, null, null, new HashMap<>());

        ColumnDef c2 = new ColumnDef("col2", new TypeDef(TypeFactory.createType(PrimitiveType.BIGINT)), true);
        ColumnDef c3 = new ColumnDef("col3", new TypeDef(TypeFactory.createType(PrimitiveType.VARCHAR)), true);
        ColumnDef cdt = new ColumnDef("dt", new TypeDef(TypeFactory.createType(PrimitiveType.DATE)), true);
        List<ColumnDef> cols = new ArrayList<>();
        cols.add(c2);
        cols.add(c3);
        cols.add(cdt);
        AddColumnsClause addColumnsClause = new AddColumnsClause(cols, null, new HashMap<>());

        List<AlterClause> clauses = Lists.newArrayList();
        clauses.add(addColumnClause);
        clauses.add(addColumnsClause);
        AlterTableStmt stmt = new AlterTableStmt(createTableRef(tableName), clauses);
        metadata.alterTable(new ConnectContext(), stmt);
        clauses.clear();

        // must be default null
        ColumnDef c4 = new ColumnDef("col4", new TypeDef(TypeFactory.createType(PrimitiveType.INT)), false);
        AddColumnClause addC4 = new AddColumnClause(c4, null, null, new HashMap<>());
        clauses.add(addC4);
        AlterTableStmt stmtC4 = new AlterTableStmt(createTableRef(tableName), clauses);
        Assertions.assertThrows(DdlException.class, () -> metadata.alterTable(new ConnectContext(), stmtC4));
        clauses.clear();

        // drop/rename/modify column
        DropColumnClause dropColumnClause = new DropColumnClause("col1", null, new HashMap<>());
        ColumnRenameClause columnRenameClause = new ColumnRenameClause("col2", "col22");
        ColumnDef newCol = new ColumnDef("col1", new TypeDef(TypeFactory.createType(PrimitiveType.BIGINT)), true);
        Map<String, String> properties = new HashMap<>();
        ModifyColumnClause modifyColumnClause =
                new ModifyColumnClause(newCol, ColumnPosition.FIRST, null, properties);
        clauses.add(dropColumnClause);
        clauses.add(columnRenameClause);
        clauses.add(modifyColumnClause);
        metadata.alterTable(new ConnectContext(), new AlterTableStmt(createTableRef(tableName), clauses));

        // rename table
        clauses.clear();
        TableRenameClause tableRenameClause = new TableRenameClause("newTbl");
        clauses.add(tableRenameClause);
        metadata.alterTable(new ConnectContext(), new AlterTableStmt(createTableRef(tableName), clauses));

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
        metadata.alterTable(new ConnectContext(), new AlterTableStmt(createTableRef(tableName), clauses));

        // modify empty properties
        clauses.clear();
        Map<String, String> emptyProperties = new HashMap<>();
        ModifyTablePropertiesClause emptyPropertiesClause = new ModifyTablePropertiesClause(emptyProperties);
        clauses.add(emptyPropertiesClause);
        Assertions.assertThrows(DdlException.class,
                () -> metadata.alterTable(new ConnectContext(), new AlterTableStmt(createTableRef(tableName), clauses)));

        // modify unsupported properties
        clauses.clear();
        Map<String, String> invalidProperties = new HashMap<>();
        invalidProperties.put(FILE_FORMAT, "parquet");
        invalidProperties.put(COMPRESSION_CODEC, "zzz");
        ModifyTablePropertiesClause invalidCompressionClause = new ModifyTablePropertiesClause(invalidProperties);
        clauses.add(invalidCompressionClause);
        Assertions.assertThrows(DdlException.class,
                () -> metadata.alterTable(new ConnectContext(), new AlterTableStmt(createTableRef(tableName), clauses)));

        // add & drop partition columns
        {
            SlotRef partitionSlot = new SlotRef(tableName, "dt");
            clauses.clear();
            AddPartitionColumnClause addPartitionColumnClause =
                    new AddPartitionColumnClause(List.of(partitionSlot), NodePosition.ZERO);
            clauses.add(addPartitionColumnClause);
            metadata.alterTable(new ConnectContext(), new AlterTableStmt(createTableRef(tableName), clauses));

            clauses.clear();
            DropPartitionColumnClause dropPartitionColumnClause = new DropPartitionColumnClause(List.of(partitionSlot),
                    NodePosition.ZERO);
            clauses.add(dropPartitionColumnClause);
            metadata.alterTable(new ConnectContext(), new AlterTableStmt(createTableRef(tableName), clauses));
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
                                    new AlterTableStmt(createTableRef(tableName), clauses)));
                    continue;
                } else {
                    metadata.alterTable(new ConnectContext(), new AlterTableStmt(createTableRef(tableName), clauses));
                }

                clauses.clear();
                DropPartitionColumnClause dropPartitionColumnClause =
                        new DropPartitionColumnClause(List.of(functionCallExpr), NodePosition.ZERO);
                clauses.add(dropPartitionColumnClause);
                if (badFunctions.contains(fn)) {
                    Assertions.assertThrows(SemanticException.class,
                            () -> metadata.alterTable(new ConnectContext(),
                                    new AlterTableStmt(createTableRef(tableName), clauses)));
                } else {
                    metadata.alterTable(new ConnectContext(), new AlterTableStmt(createTableRef(tableName), clauses));
                }
            }
        }
    }

    @Test
    public void testReplacePartitionColumnV1Rejected() throws StarRocksException {
        new MockUp<IcebergHiveCatalog>() {
            @Mock
            org.apache.iceberg.Table getTable(ConnectContext context, String dbName, String tblName) {
                return mockedNativeTableF;
            }

            @Mock
            Database getDB(ConnectContext context, String dbName) {
                return new Database(1, dbName);
            }

            @Mock
            boolean tableExists(ConnectContext context, String dbName, String tblName) {
                return true;
            }
        };

        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog(CATALOG_NAME, new Configuration(), DEFAULT_CONFIG);
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                Executors.newSingleThreadExecutor(), null);

        TableName tableName = new TableName("db", "tbl");
        SlotRef partitionSlot = new SlotRef(tableName, "dt");
        FunctionCallExpr dayExpr = new FunctionCallExpr("day", Lists.newArrayList(partitionSlot));
        FunctionCallExpr monthExpr = new FunctionCallExpr("month", Lists.newArrayList(partitionSlot));

        // REPLACE PARTITION COLUMN should be rejected for v1 tables
        Assertions.assertThrows(DdlException.class, () -> metadata.alterTable(new ConnectContext(),
                new AlterTableStmt(createTableRef(tableName),
                        List.of(new ReplacePartitionColumnClause(dayExpr, monthExpr, NodePosition.ZERO)))));
    }

    @Test
    public void testReplacePartitionColumn() throws StarRocksException {
        new MockUp<IcebergHiveCatalog>() {
            @Mock
            org.apache.iceberg.Table getTable(ConnectContext context, String dbName, String tblName) {
                return mockedNativeTableFV2;
            }

            @Mock
            Database getDB(ConnectContext context, String dbName) {
                return new Database(1, dbName);
            }

            @Mock
            boolean tableExists(ConnectContext context, String dbName, String tblName) {
                return true;
            }
        };

        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog(CATALOG_NAME, new Configuration(), DEFAULT_CONFIG);
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                Executors.newSingleThreadExecutor(), null);

        TableName tableName = new TableName("db", "tbl");
        SlotRef partitionSlot = new SlotRef(tableName, "dt");
        FunctionCallExpr dayExpr = new FunctionCallExpr("day", Lists.newArrayList(partitionSlot));
        FunctionCallExpr monthExpr = new FunctionCallExpr("month", Lists.newArrayList(partitionSlot));

        // replace day(dt) with month(dt) on v2 table should succeed
        metadata.alterTable(new ConnectContext(), new AlterTableStmt(createTableRef(tableName),
                List.of(new ReplacePartitionColumnClause(dayExpr, monthExpr, NodePosition.ZERO))));
        mockedNativeTableFV2.refresh();
        List<String> partitionFields = IcebergApiConverter.toPartitionFields(mockedNativeTableFV2.spec(), false);
        Assertions.assertTrue(partitionFields.contains("month(`dt`)"));
        Assertions.assertFalse(partitionFields.contains("day(`dt`)"));

        // replace with non-existent old partition column should fail
        Assertions.assertThrows(DdlException.class, () -> metadata.alterTable(new ConnectContext(),
                new AlterTableStmt(createTableRef(tableName),
                        List.of(new ReplacePartitionColumnClause(dayExpr, monthExpr, NodePosition.ZERO)))));

        // replace with same old and new partition column should fail
        Assertions.assertThrows(DdlException.class, () -> metadata.alterTable(new ConnectContext(),
                new AlterTableStmt(createTableRef(tableName),
                        List.of(new ReplacePartitionColumnClause(monthExpr, monthExpr, NodePosition.ZERO)))));
    }

    @Test
    public void testReplacePartitionColumnByFieldName() throws StarRocksException {
        new MockUp<IcebergHiveCatalog>() {
            @Mock
            org.apache.iceberg.Table getTable(ConnectContext context, String dbName, String tblName) {
                return mockedNativeTableFV2;
            }

            @Mock
            Database getDB(ConnectContext context, String dbName) {
                return new Database(1, dbName);
            }

            @Mock
            boolean tableExists(ConnectContext context, String dbName, String tblName) {
                return true;
            }
        };

        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog(CATALOG_NAME, new Configuration(), DEFAULT_CONFIG);
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                Executors.newSingleThreadExecutor(), null);

        TableName tableName = new TableName("db", "tbl");
        SlotRef partitionSlot = new SlotRef(tableName, "dt");
        FunctionCallExpr monthExpr = new FunctionCallExpr("month", Lists.newArrayList(partitionSlot));

        // replace by field name: "dt_day" is the default name for day(dt)
        SlotRef fieldNameRef = new SlotRef(tableName, "dt_day");
        metadata.alterTable(new ConnectContext(), new AlterTableStmt(createTableRef(tableName),
                List.of(new ReplacePartitionColumnClause(fieldNameRef, monthExpr, NodePosition.ZERO))));
        mockedNativeTableFV2.refresh();
        List<String> partitionFields = IcebergApiConverter.toPartitionFields(mockedNativeTableFV2.spec(), false);
        Assertions.assertTrue(partitionFields.contains("month(`dt`)"));
        Assertions.assertFalse(partitionFields.contains("day(`dt`)"));

        // non-existent field name should fail
        SlotRef badFieldName = new SlotRef(tableName, "no_such_field");
        Assertions.assertThrows(DdlException.class, () -> metadata.alterTable(new ConnectContext(),
                new AlterTableStmt(createTableRef(tableName),
                        List.of(new ReplacePartitionColumnClause(badFieldName, monthExpr, NodePosition.ZERO)))));
    }

    @Test
    public void testReplacePartitionColumnNewAlreadyExists() throws StarRocksException {
        new MockUp<IcebergHiveCatalog>() {
            @Mock
            org.apache.iceberg.Table getTable(ConnectContext context, String dbName, String tblName) {
                return mockedNativeTableFV2;
            }

            @Mock
            Database getDB(ConnectContext context, String dbName) {
                return new Database(1, dbName);
            }

            @Mock
            boolean tableExists(ConnectContext context, String dbName, String tblName) {
                return true;
            }
        };

        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog(CATALOG_NAME, new Configuration(), DEFAULT_CONFIG);
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                Executors.newSingleThreadExecutor(), null);

        TableName tableName = new TableName("db", "tbl");
        SlotRef partitionSlot = new SlotRef(tableName, "dt");
        FunctionCallExpr monthExpr = new FunctionCallExpr("month", Lists.newArrayList(partitionSlot));

        // First add month(dt) so table has both day(dt) and month(dt)
        metadata.alterTable(new ConnectContext(), new AlterTableStmt(createTableRef(tableName),
                List.of(new AddPartitionColumnClause(List.of(monthExpr), NodePosition.ZERO))));
        mockedNativeTableFV2.refresh();

        // Try to replace day(dt) with month(dt) - should fail because month(dt) already exists
        FunctionCallExpr dayExpr2 = new FunctionCallExpr("day", Lists.newArrayList(new SlotRef(tableName, "dt")));
        FunctionCallExpr monthExpr2 = new FunctionCallExpr("month", Lists.newArrayList(new SlotRef(tableName, "dt")));
        Assertions.assertThrows(DdlException.class, () -> metadata.alterTable(new ConnectContext(),
                new AlterTableStmt(createTableRef(tableName),
                        List.of(new ReplacePartitionColumnClause(dayExpr2, monthExpr2, NodePosition.ZERO)))));
    }

    @Test
    public void testReplacePartitionColumnSchemaColumnAsFieldName() throws StarRocksException {
        new MockUp<IcebergHiveCatalog>() {
            @Mock
            org.apache.iceberg.Table getTable(ConnectContext context, String dbName, String tblName) {
                return mockedNativeTableFV2;
            }

            @Mock
            Database getDB(ConnectContext context, String dbName) {
                return new Database(1, dbName);
            }

            @Mock
            boolean tableExists(ConnectContext context, String dbName, String tblName) {
                return true;
            }
        };

        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog(CATALOG_NAME, new Configuration(), DEFAULT_CONFIG);
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                Executors.newSingleThreadExecutor(), null);

        TableName tableName = new TableName("db", "tbl");
        SlotRef partitionSlot = new SlotRef(tableName, "dt");
        FunctionCallExpr monthExpr = new FunctionCallExpr("month", Lists.newArrayList(partitionSlot));

        // Using "dt" as old partition - "dt" is a schema column name, so resolvePartitionFieldName
        // returns null (treats it as identity transform, not a field name reference)
        // This should fail because identity(dt) is not an existing partition (day(dt) is)
        SlotRef schemaColRef = new SlotRef(tableName, "dt");
        Assertions.assertThrows(DdlException.class, () -> metadata.alterTable(new ConnectContext(),
                new AlterTableStmt(createTableRef(tableName),
                        List.of(new ReplacePartitionColumnClause(schemaColRef, monthExpr, NodePosition.ZERO)))));
    }

    @Test
    public void testGetIcebergMetricsConfig() {
        List<Column> columns = Lists.newArrayList(new Column("k1", INT),
                new Column("k2", STRING),
                new Column("k3", STRING),
                new Column("k4", STRING),
                new Column("k5", STRING));
        new IcebergTable(1, "srTableName", CATALOG_NAME, "resource_name", "db_name",
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
                Executors.newSingleThreadExecutor(),
                new IcebergCatalogProperties(DEFAULT_CONFIG));
        ConnectContext.set(connectContext);
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
                Executors.newSingleThreadExecutor(), catalogProperties);

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
    public void testIcebergMetadataTableQueryMetric(@Mocked LocalMetastore localMetastore,
                                                    @Mocked TemporaryTableMgr temporaryTableMgr) {
        mockedNativeTableG.newAppend().appendFile(FILE_B_5).commit();
        mockedNativeTableG.refresh();
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
                Executors.newSingleThreadExecutor(),
                new IcebergCatalogProperties(DEFAULT_CONFIG));
        ConnectContext.set(connectContext);
        ConnectContext.get().setMetadataContext(false);

        MetadataMgr metadataMgr = new MetadataMgr(localMetastore, temporaryTableMgr, null, null);
        new MockUp<MetadataMgr>() {
            @Mock
            public Optional<ConnectorMetadata> getOptionalMetadata(String catalogName) {
                return Optional.of(metadata);
            }
        };

        long before = getMetricValue("iceberg_metadata_table_query_total", "logical_iceberg_metadata");
        metadataMgr.getSerializedMetaSpec("catalog", "db", "tg", -1, null, MetadataTableType.LOGICAL_ICEBERG_METADATA);
        long after = getMetricValue("iceberg_metadata_table_query_total", "logical_iceberg_metadata");

        Assertions.assertEquals(before + 1, after);

        ConnectContext.get().setMetadataContext(true);
        metadataMgr.getSerializedMetaSpec("catalog", "db", "tg", -1, null, MetadataTableType.LOGICAL_ICEBERG_METADATA);
        long internalQueryAfter = getMetricValue("iceberg_metadata_table_query_total", "logical_iceberg_metadata");
        Assertions.assertEquals(after, internalQueryAfter);
        ConnectContext.get().setMetadataContext(false);
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

    private long getMetricValue(String name, String metadataTable) {
        for (Metric<?> metric : MetricRepo.getMetricsByName(name)) {
            boolean matched = false;
            for (MetricLabel label : metric.getLabels()) {
                if ("metadata_table".equals(label.getKey()) && metadataTable.equals(label.getValue())) {
                    matched = true;
                    break;
                }
            }
            if (matched) {
                return (Long) metric.getValue();
            }
        }
        return 0L;
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
        TvrVersionRange versionRange = TvrTableSnapshot.empty();
        Assertions.assertTrue(versionRange.isEmpty());
        versionRange = TvrTableSnapshot.of(Optional.of(1L));
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
    public void testGetCurrentTvrSnapshot(@Mocked IcebergHiveCatalog icebergHiveCatalog,
                                          @Mocked HiveTableOperations hiveTableOperations) {
        new Expectations() {
            {
                icebergHiveCatalog.getTable(connectContext, "db", "tbl");
                result = new BaseTable(hiveTableOperations, "tbl");
                minTimes = 0;
            }
        };

        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                Executors.newSingleThreadExecutor(), null);
        Table actual = metadata.getTable(new ConnectContext(), "db", "tbl");
        Assertions.assertEquals("tbl", actual.getName());
        Assertions.assertEquals(ICEBERG, actual.getType());
        TvrTableSnapshot tvrTableSnapshot = metadata.getCurrentTvrSnapshot("db", actual);
        Assertions.assertNotNull(tvrTableSnapshot);
        Assertions.assertFalse(tvrTableSnapshot.isEmpty());
        Assertions.assertEquals(0L, tvrTableSnapshot.to().getVersion());
    }

    @Test
    public void testListTableVersionRanges(@Mocked IcebergHiveCatalog icebergHiveCatalog,
                                           @Mocked HiveTableOperations hiveTableOperations) {
        new Expectations() {
            {
                icebergHiveCatalog.getTable(connectContext, "db", "tbl");
                result = new BaseTable(hiveTableOperations, "tbl");
                minTimes = 0;
            }
        };

        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                Executors.newSingleThreadExecutor(), null);
        Table actual = metadata.getTable(new ConnectContext(), "db", "tbl");
        Assertions.assertEquals("tbl", actual.getName());
        Assertions.assertEquals(ICEBERG, actual.getType());
        TvrTableSnapshot tvrTableSnapshot = metadata.getCurrentTvrSnapshot("db", actual);
        List<TvrTableDeltaTrait> deltas = metadata.listTableDeltaTraits("db", actual,
                TvrTableSnapshot.of(TvrVersion.of(0)), tvrTableSnapshot);
        Assertions.assertNotNull(deltas);
        Assertions.assertTrue(deltas.isEmpty());
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
        clause.setAnalyzedArgs(Map.of(RemoveOrphanFilesProcedure.OLDER_THAN,
                ConstantOperator.createChar("2024-01-01 00:00:00")));
        clause.setTableProcedure(RemoveOrphanFilesProcedure.getInstance());

        IcebergAlterTableExecutor executor = new IcebergAlterTableExecutor(new AlterTableStmt(
                createTableRef(tableName), List.of(clause)),
                icebergHiveCatalog.getTable(connectContext, tableName.getDb(), tableName.getTbl()), icebergHiveCatalog,
                connectContext,
                HDFS_ENVIRONMENT);
        executor.execute();

        // Illegal date
        tableName = new TableName(CATALOG_NAME, "db", "table");
        clause = new AlterTableOperationClause(NodePosition.ZERO, REMOVE_ORPHAN_FILES.toString(), List.of(), null);
        clause.setAnalyzedArgs(Map.of(RemoveOrphanFilesProcedure.OLDER_THAN,
                ConstantOperator.createChar("illegal date")));
        clause.setTableProcedure(RemoveOrphanFilesProcedure.getInstance());

        executor = new IcebergAlterTableExecutor(new AlterTableStmt(
                createTableRef(tableName), List.of(clause)),
                icebergHiveCatalog.getTable(connectContext, tableName.getDb(), tableName.getTbl()), icebergHiveCatalog,
                connectContext,
                HDFS_ENVIRONMENT);
        IcebergAlterTableExecutor finalExecutor = executor;
        Assertions.assertThrows(DdlException.class, finalExecutor::execute);

        // Default retention interval
        tableName = new TableName(CATALOG_NAME, "db", "table");
        clause = new AlterTableOperationClause(NodePosition.ZERO, REMOVE_ORPHAN_FILES.toString(), List.of(), null);
        clause.setAnalyzedArgs(Map.of(RemoveOrphanFilesProcedure.OLDER_THAN, ConstantOperator.createChar("")));
        clause.setTableProcedure(RemoveOrphanFilesProcedure.getInstance());

        executor = new IcebergAlterTableExecutor(new AlterTableStmt(
                createTableRef(tableName), List.of(clause)),
                icebergHiveCatalog.getTable(connectContext, tableName.getDb(), tableName.getTbl()), icebergHiveCatalog,
                connectContext,
                HDFS_ENVIRONMENT);
        finalExecutor = executor;
        Assertions.assertThrows(DdlException.class, finalExecutor::execute);

        // Mock snapshot behavior
        new Expectations() {
            {
                snapshot.snapshotId();
                result = 1L;
                minTimes = 0;

                snapshot.timestampMillis();
                result = System.currentTimeMillis();
                minTimes = 0;

                snapshot.parentId();
                result = 0L;
                minTimes = 0;
            }
        };

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
        clause.setAnalyzedArgs(Map.of(RemoveOrphanFilesProcedure.OLDER_THAN,
                ConstantOperator.createChar("2024-01-01 00:00:00")));
        clause.setTableProcedure(RemoveOrphanFilesProcedure.getInstance());

        executor = new IcebergAlterTableExecutor(new AlterTableStmt(
                createTableRef(tableName), List.of(clause)),
                icebergHiveCatalog.getTable(connectContext, tableName.getDb(), tableName.getTbl()), icebergHiveCatalog,
                connectContext,
                HDFS_ENVIRONMENT);
        executor.execute();
    }

    @Test
    public void testGetCatalogProperties(@Mocked IcebergCatalog icebergCatalog) {
        Map<String, String> expectedProps = ImmutableMap.of(
                "s3.access-key-id", "AKIA_TEST_KEY",
                "s3.secret-access-key", "test_secret_key",
                "client.region", "ap-northeast-2"
        );

        new Expectations() {
            {
                icebergCatalog.getCatalogProperties();
                result = expectedProps;
                times = 1;
            }
        };

        IcebergMetadata metadata = new IcebergMetadata(
                CATALOG_NAME,
                HDFS_ENVIRONMENT,
                icebergCatalog,
                Executors.newSingleThreadExecutor(),
                DEFAULT_CATALOG_PROPERTIES
        );

        Map<String, String> result = metadata.getCatalogProperties();

        Assertions.assertEquals(expectedProps, result);
        Assertions.assertEquals("AKIA_TEST_KEY", result.get("s3.access-key-id"));
        Assertions.assertEquals("test_secret_key", result.get("s3.secret-access-key"));
        Assertions.assertEquals("ap-northeast-2", result.get("client.region"));
    }

    @Test
    public void testGetCatalogPropertiesEmpty(@Mocked IcebergCatalog icebergCatalog) {
        new Expectations() {
            {
                icebergCatalog.getCatalogProperties();
                result = ImmutableMap.of();
                times = 1;
            }
        };

        IcebergMetadata metadata = new IcebergMetadata(
                CATALOG_NAME,
                HDFS_ENVIRONMENT,
                icebergCatalog,
                Executors.newSingleThreadExecutor(),
                DEFAULT_CATALOG_PROPERTIES
        );

        Map<String, String> result = metadata.getCatalogProperties();

        Assertions.assertTrue(result.isEmpty());
    }

    @Test
    public void testDeleteOperationCommit() throws Exception {
        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog(CATALOG_NAME, new Configuration(), DEFAULT_CONFIG);

        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                Executors.newSingleThreadExecutor(), DEFAULT_CATALOG_PROPERTIES);
        mockedNativeTableA.newFastAppend().appendFile(FILE_A).commit();
        IcebergTable icebergTable = new IcebergTable(1, "srTableName", CATALOG_NAME, "resource_name", "iceberg_db",
                "iceberg_table", "", Lists.newArrayList(), mockedNativeTableA, Maps.newHashMap());

        new Expectations(metadata) {
            {
                metadata.getTable((ConnectContext) any, anyString, anyString);
                result = icebergTable;
                minTimes = 0;
            }
        };

        // Create a delete file with POSITION_DELETES content
        TIcebergDataFile deleteFile = new TIcebergDataFile();
        String deleteFilePath = mockedNativeTableA.location() + "/data/data_bucket=1/delete_file.parquet";
        deleteFile.setPath(deleteFilePath);
        deleteFile.setFormat("parquet");
        deleteFile.setRecord_count(10);
        deleteFile.setFile_size_in_bytes(1024);
        deleteFile.setPartition_path(mockedNativeTableA.location() + "/data/data_bucket=1/");
        deleteFile.setPartition_null_fingerprint("0");
        deleteFile.setFile_content(TIcebergFileContent.POSITION_DELETES);
        // Set referenced data file
        deleteFile.setReferenced_data_file(FILE_A.path().toString());

        TIcebergColumnStats columnStats = new TIcebergColumnStats();
        columnStats.setColumn_sizes(Map.of(1, 1000L, 2, 2000L));
        columnStats.setValue_counts(Map.of(1, 10L, 2, 10L));
        columnStats.setNull_value_counts(Map.of(1, 0L, 2, 0L));
        columnStats.setLower_bounds(Map.of(1, ByteBuffer.wrap("min_val".getBytes()),
                2, ByteBuffer.wrap("min_val2".getBytes())));

        columnStats.setUpper_bounds(Map.of(1, ByteBuffer.wrap("max_val".getBytes()),
                2, ByteBuffer.wrap("max_val2".getBytes())));
        deleteFile.setColumn_stats(columnStats);

        TSinkCommitInfo commitInfo = new TSinkCommitInfo();
        commitInfo.setIceberg_data_file(deleteFile);

        // Mock NodeMgr and Frontend for commit audit info
        new MockUp<NodeMgr>() {
            @Mock
            public Frontend getMySelf() {
                Frontend frontend = new Frontend(FrontendNodeType.LEADER, "test-fe", "127.0.0.1", 9010);
                return frontend;
            }
        };

        new MockUp<Frontend>() {
            @Mock
            public String getFeVersion() {
                return "test-version-delete-3.5.0";
            }
        };

        // Test commit with delete operation
        metadata.finishSink("iceberg_db", "iceberg_table", Lists.newArrayList(commitInfo),
                null, null, connectContext);

        // Verify the delete file was committed
        mockedNativeTableA.refresh();

        // Verify snapshot summary contains commit audit info for delete operation
        Snapshot snapshot = mockedNativeTableA.currentSnapshot();
        Assertions.assertNotNull(snapshot, "Current snapshot should exist");
        Map<String, String> summary = snapshot.summary();
        Assertions.assertEquals("StarRocks", summary.get("engine-name"),
                "Snapshot summary should contain engine-name=StarRocks for delete operation");
        Assertions.assertEquals("test-version-delete-3.5.0", summary.get("engine-version"),
                "Snapshot summary should contain engine-version for delete operation");
        Assertions.assertEquals("root", summary.get("starrocks_user"),
                "Snapshot summary should contain starrocks_user for delete operation");
        List<FileScanTask> fileScanTasks = Lists.newArrayList(mockedNativeTableA.newScan().planFiles());
        // We should have delete files in the scan
        boolean foundDelete = false;
        for (FileScanTask task : fileScanTasks) {
            if (!task.deletes().isEmpty()) {
                foundDelete = true;
                DeleteFile delete = task.deletes().get(0);
                Assertions.assertEquals(deleteFilePath, delete.path().toString());
                Assertions.assertEquals(10, delete.recordCount());
            }
        }
        Assertions.assertTrue(foundDelete, "Delete file should be committed");
    }

    @Test
    public void testDeleteOperationWithConflictDetection() throws Exception {
        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog(CATALOG_NAME, new Configuration(), DEFAULT_CONFIG);

        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                Executors.newSingleThreadExecutor(), DEFAULT_CATALOG_PROPERTIES);
        mockedNativeTableA.newFastAppend().appendFile(FILE_A).commit();
        IcebergTable icebergTable = new IcebergTable(1, "srTableName", CATALOG_NAME, "resource_name", "iceberg_db",
                "iceberg_table", "", Lists.newArrayList(), mockedNativeTableA, Maps.newHashMap());

        new Expectations(metadata) {
            {
                metadata.getTable((ConnectContext) any, anyString, anyString);
                result = icebergTable;
                minTimes = 0;
            }
        };

        // Create a delete file
        TIcebergDataFile deleteFile = new TIcebergDataFile();
        deleteFile.setPath(mockedNativeTableA.location() + "/data/delete_file.parquet");
        deleteFile.setFormat("parquet");
        deleteFile.setRecord_count(5);
        deleteFile.setFile_size_in_bytes(512);
        deleteFile.setPartition_path(mockedNativeTableA.location() + "/data/data_bucket=1/");
        deleteFile.setPartition_null_fingerprint("0");
        deleteFile.setFile_content(TIcebergFileContent.POSITION_DELETES);
        deleteFile.setReferenced_data_file(FILE_A.path().toString());

        TIcebergColumnStats columnStats = new TIcebergColumnStats();
        columnStats.setColumn_sizes(Map.of(1, 1000L, 2, 2000L));
        columnStats.setValue_counts(Map.of(1, 10L, 2, 10L));
        columnStats.setNull_value_counts(Map.of(1, 0L, 2, 0L));
        columnStats.setLower_bounds(Map.of(1, ByteBuffer.wrap("min_val".getBytes()),
                2, ByteBuffer.wrap("min_val2".getBytes())));

        columnStats.setUpper_bounds(Map.of(1, ByteBuffer.wrap("max_val".getBytes()),
                2, ByteBuffer.wrap("max_val2".getBytes())));
        deleteFile.setColumn_stats(columnStats);

        TSinkCommitInfo commitInfo = new TSinkCommitInfo();
        commitInfo.setIceberg_data_file(deleteFile);

        // Create IcebergSinkExtra with conflict detection filter
        IcebergMetadata.IcebergSinkExtra extra = new IcebergMetadata.IcebergSinkExtra();
        // Set a simple conflict detection filter (id > 100)
        org.apache.iceberg.expressions.Expression filter = org.apache.iceberg.expressions.Expressions.greaterThan("id", 100);
        extra.setConflictDetectionFilter(filter);

        // Test commit with conflict detection
        metadata.finishSink("iceberg_db", "iceberg_table", Lists.newArrayList(commitInfo), null, extra);

        // Verify commit succeeded
        mockedNativeTableA.refresh();
        List<FileScanTask> fileScanTasks = Lists.newArrayList(mockedNativeTableA.newScan().planFiles());
        Assertions.assertFalse(fileScanTasks.isEmpty());
    }

    // Helpers for row-delta commit tests. A row-delta commit (UPDATE / MERGE) goes
    // through commitRowDeltaOperation when commitInfos contains BOTH a position-delete
    // file and a new data file, exercising RowDelta-specific validations.
    //
    // column_stats must be set on both files: buildPositionDeleteFile passes
    // null metrics to Iceberg's FileMetadata.Builder when column_stats is unset,
    // and Iceberg then NPEs on metrics.recordCount() during build.
    private TIcebergColumnStats emptyColumnStats() {
        TIcebergColumnStats stats = new TIcebergColumnStats();
        stats.setColumn_sizes(Map.of());
        stats.setValue_counts(Map.of());
        stats.setNull_value_counts(Map.of());
        stats.setLower_bounds(Map.of());
        stats.setUpper_bounds(Map.of());
        return stats;
    }

    private TIcebergDataFile buildRowDeltaPositionDeleteFile() {
        TIcebergDataFile deleteFile = new TIcebergDataFile();
        deleteFile.setPath(mockedNativeTableA.location() + "/data/delete_for_update.parquet");
        deleteFile.setFormat("parquet");
        deleteFile.setRecord_count(3);
        deleteFile.setFile_size_in_bytes(256);
        deleteFile.setPartition_path(mockedNativeTableA.location() + "/data/data_bucket=1/");
        deleteFile.setPartition_null_fingerprint("0");
        deleteFile.setFile_content(TIcebergFileContent.POSITION_DELETES);
        deleteFile.setReferenced_data_file(FILE_A.path().toString());
        deleteFile.setColumn_stats(emptyColumnStats());
        return deleteFile;
    }

    private TIcebergDataFile buildRowDeltaDataFile() {
        TIcebergDataFile dataFile = new TIcebergDataFile();
        dataFile.setPath(mockedNativeTableA.location() + "/data/data_bucket=0/new_after_update.parquet");
        dataFile.setFormat("parquet");
        dataFile.setRecord_count(3);
        dataFile.setSplit_offsets(Lists.newArrayList(4L));
        dataFile.setPartition_path(mockedNativeTableA.location() + "/data/data_bucket=0/");
        dataFile.setFile_size_in_bytes(512);
        dataFile.setPartition_null_fingerprint("0");
        dataFile.setColumn_stats(emptyColumnStats());
        return dataFile;
    }

    @Test
    public void testCommitRowDeltaOperationWithBaseSnapshotAndConflictFilter() throws Exception {
        // Establish a baseline snapshot so RowDelta has something to validate against.
        mockedNativeTableA.newFastAppend().appendFile(FILE_A).commit();
        long baseSnapshotId = mockedNativeTableA.currentSnapshot().snapshotId();

        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog(CATALOG_NAME, new Configuration(), DEFAULT_CONFIG);
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                Executors.newSingleThreadExecutor(), DEFAULT_CATALOG_PROPERTIES);
        IcebergTable icebergTable = new IcebergTable(1, "srTableName", CATALOG_NAME, "resource_name", "iceberg_db",
                "iceberg_table", "", Lists.newArrayList(), mockedNativeTableA, Maps.newHashMap());

        new Expectations(metadata) {
            {
                metadata.getTable((ConnectContext) any, anyString, anyString);
                result = icebergTable;
                minTimes = 0;
            }
        };

        TSinkCommitInfo deleteCommit = new TSinkCommitInfo();
        deleteCommit.setIceberg_data_file(buildRowDeltaPositionDeleteFile());
        TSinkCommitInfo dataCommit = new TSinkCommitInfo();
        dataCommit.setIceberg_data_file(buildRowDeltaDataFile());

        // Explicit baseSnapshotId frozen at plan time + conflict detection filter:
        // exercises the IcebergSinkExtra.baseSnapshotId branch and the
        // conflictDetectionFilter set-up in commitRowDeltaOperation.
        IcebergMetadata.IcebergSinkExtra extra = new IcebergMetadata.IcebergSinkExtra();
        extra.setBaseSnapshotId(baseSnapshotId);
        extra.setConflictDetectionFilter(org.apache.iceberg.expressions.Expressions.greaterThan("id", 100));

        metadata.finishSink("iceberg_db", "iceberg_table",
                Lists.newArrayList(deleteCommit, dataCommit), null, extra);

        mockedNativeTableA.refresh();
        Snapshot newSnapshot = mockedNativeTableA.currentSnapshot();
        Assertions.assertNotNull(newSnapshot, "row-delta commit must produce a snapshot");
        Assertions.assertNotEquals(baseSnapshotId, newSnapshot.snapshotId(),
                "row-delta commit must advance the snapshot id past the plan-time base");
    }

    private long mergeCounterValue(String name, String labelKey, String labelValue) {
        for (Metric<?> metric : MetricRepo.getMetricsByName(name)) {
            if (!(metric instanceof LongCounterMetric)) {
                continue;
            }
            boolean matched = labelKey == null;
            for (MetricLabel label : metric.getLabels()) {
                if (labelKey != null && labelKey.equals(label.getKey()) && labelValue.equals(label.getValue())) {
                    matched = true;
                    break;
                }
            }
            if (matched) {
                return ((LongCounterMetric) metric).getValue();
            }
        }
        return 0L;
    }

    @Test
    public void testCommitRowDeltaOperationMergeMetrics() throws Exception {
        // A MERGE row-delta commit takes the isMerge branch of finishSink, which reports the
        // iceberg_merge_* metrics from the resulting snapshot summary. Confirm the merge_rows
        // counter is split by file_type: added-position-deletes -> position_delete (the delete
        // file's 3 records) and added-records -> data (the data file's 3 records).
        mockedNativeTableA.newFastAppend().appendFile(FILE_A).commit();
        long baseSnapshotId = mockedNativeTableA.currentSnapshot().snapshotId();

        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog(CATALOG_NAME, new Configuration(), DEFAULT_CONFIG);
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                Executors.newSingleThreadExecutor(), DEFAULT_CATALOG_PROPERTIES);
        IcebergTable icebergTable = new IcebergTable(1, "srTableName", CATALOG_NAME, "resource_name", "iceberg_db",
                "iceberg_table", "", Lists.newArrayList(), mockedNativeTableA, Maps.newHashMap());

        new Expectations(metadata) {
            {
                metadata.getTable((ConnectContext) any, anyString, anyString);
                result = icebergTable;
                minTimes = 0;
            }
        };

        long posDeleteRowsBefore = mergeCounterValue("iceberg_merge_rows", "file_type", "position_delete");
        long dataRowsBefore = mergeCounterValue("iceberg_merge_rows", "file_type", "data");
        long totalBefore = mergeCounterValue("iceberg_merge_total", "status", "success");

        TSinkCommitInfo deleteCommit = new TSinkCommitInfo();
        deleteCommit.setIceberg_data_file(buildRowDeltaPositionDeleteFile());
        TSinkCommitInfo dataCommit = new TSinkCommitInfo();
        dataCommit.setIceberg_data_file(buildRowDeltaDataFile());

        IcebergMetadata.IcebergSinkExtra extra = new IcebergMetadata.IcebergSinkExtra();
        extra.setBaseSnapshotId(baseSnapshotId);
        extra.setOperationType(IcebergMetadata.IcebergSinkExtra.OperationType.MERGE);

        metadata.finishSink("iceberg_db", "iceberg_table",
                Lists.newArrayList(deleteCommit, dataCommit), null, extra);

        mockedNativeTableA.refresh();
        Snapshot newSnapshot = mockedNativeTableA.currentSnapshot();
        Assertions.assertNotNull(newSnapshot, "MERGE row-delta commit must produce a snapshot");
        Assertions.assertNotEquals(baseSnapshotId, newSnapshot.snapshotId(),
                "MERGE row-delta commit must advance the snapshot id");

        // position_delete rows come from added-position-deletes (3), data rows from added-records (3).
        Assertions.assertEquals(posDeleteRowsBefore + 3,
                mergeCounterValue("iceberg_merge_rows", "file_type", "position_delete"),
                "iceberg_merge_rows{file_type=position_delete} must count the added position deletes");
        Assertions.assertEquals(dataRowsBefore + 3,
                mergeCounterValue("iceberg_merge_rows", "file_type", "data"),
                "iceberg_merge_rows{file_type=data} must count the added data records");
        Assertions.assertEquals(totalBefore + 1,
                mergeCounterValue("iceberg_merge_total", "status", "success"),
                "iceberg_merge_total{status=success} must increment after a successful MERGE commit");
        Assertions.assertTrue(mergeCounterValue("iceberg_merge_files", "file_type", "position_delete") >= 1,
                "iceberg_merge_files{file_type=position_delete} must count the committed delete file");
        Assertions.assertTrue(mergeCounterValue("iceberg_merge_files", "file_type", "data") >= 1,
                "iceberg_merge_files{file_type=data} must count the committed data file");
    }

    @Test
    public void testCommitRowDeltaOperationSerializableIsolation() throws Exception {
        // SERIALIZABLE turns on validateNoConflictingDataFiles in addition to the
        // unconditional validateNoConflictingDeleteFiles. Cover both checks here.
        mockedNativeTableA.updateProperties()
                .set(org.apache.iceberg.TableProperties.UPDATE_ISOLATION_LEVEL, "serializable")
                .commit();
        mockedNativeTableA.newFastAppend().appendFile(FILE_A).commit();

        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog(CATALOG_NAME, new Configuration(), DEFAULT_CONFIG);
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                Executors.newSingleThreadExecutor(), DEFAULT_CATALOG_PROPERTIES);
        IcebergTable icebergTable = new IcebergTable(1, "srTableName", CATALOG_NAME, "resource_name", "iceberg_db",
                "iceberg_table", "", Lists.newArrayList(), mockedNativeTableA, Maps.newHashMap());

        new Expectations(metadata) {
            {
                metadata.getTable((ConnectContext) any, anyString, anyString);
                result = icebergTable;
                minTimes = 0;
            }
        };

        TSinkCommitInfo deleteCommit = new TSinkCommitInfo();
        deleteCommit.setIceberg_data_file(buildRowDeltaPositionDeleteFile());
        TSinkCommitInfo dataCommit = new TSinkCommitInfo();
        dataCommit.setIceberg_data_file(buildRowDeltaDataFile());

        IcebergMetadata.IcebergSinkExtra extra = new IcebergMetadata.IcebergSinkExtra();
        extra.setBaseSnapshotId(mockedNativeTableA.currentSnapshot().snapshotId());

        metadata.finishSink("iceberg_db", "iceberg_table",
                Lists.newArrayList(deleteCommit, dataCommit), null, extra);

        mockedNativeTableA.refresh();
        Assertions.assertNotNull(mockedNativeTableA.currentSnapshot(),
                "serializable-isolation row-delta commit must still produce a snapshot");
    }

    @Test
    public void testCommitRowDeltaOperationFallsBackToCurrentSnapshotWhenExtraMissing() throws Exception {
        // When IcebergSinkExtra (or its baseSnapshotId) is not provided, the commit
        // path falls back to nativeTbl.currentSnapshot() to scope validateFromSnapshot.
        // This branch covers the null-extra / null-baseSnapshotId fallback.
        mockedNativeTableA.newFastAppend().appendFile(FILE_A).commit();
        long baseSnapshotId = mockedNativeTableA.currentSnapshot().snapshotId();

        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog(CATALOG_NAME, new Configuration(), DEFAULT_CONFIG);
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                Executors.newSingleThreadExecutor(), DEFAULT_CATALOG_PROPERTIES);
        IcebergTable icebergTable = new IcebergTable(1, "srTableName", CATALOG_NAME, "resource_name", "iceberg_db",
                "iceberg_table", "", Lists.newArrayList(), mockedNativeTableA, Maps.newHashMap());

        new Expectations(metadata) {
            {
                metadata.getTable((ConnectContext) any, anyString, anyString);
                result = icebergTable;
                minTimes = 0;
            }
        };

        TSinkCommitInfo deleteCommit = new TSinkCommitInfo();
        deleteCommit.setIceberg_data_file(buildRowDeltaPositionDeleteFile());
        TSinkCommitInfo dataCommit = new TSinkCommitInfo();
        dataCommit.setIceberg_data_file(buildRowDeltaDataFile());

        // No extra → commitRowDeltaOperation must derive baseSnapshotId from currentSnapshot().
        metadata.finishSink("iceberg_db", "iceberg_table",
                Lists.newArrayList(deleteCommit, dataCommit), null, null);

        mockedNativeTableA.refresh();
        Snapshot newSnapshot = mockedNativeTableA.currentSnapshot();
        Assertions.assertNotNull(newSnapshot, "row-delta commit must produce a snapshot");
        Assertions.assertNotEquals(baseSnapshotId, newSnapshot.snapshotId(),
                "row-delta commit must advance past the implicit base snapshot");
    }

    @Test
    public void testConcurrentFinishSinkWithCommitQueue() throws Exception {
        // Test that concurrent commits to the same table are serialized by the commit queue
        // This test verifies that the commit queue integration in IcebergMetadata is working correctly

        // Create a commit queue manager for testing
        IcebergCommitQueueManager.Config config = new IcebergCommitQueueManager.Config(true, 30, 100);
        IcebergCommitQueueManager manager = new IcebergCommitQueueManager(config);

        try {
            IcebergHiveCatalog icebergHiveCatalog =
                    new IcebergHiveCatalog(CATALOG_NAME, new Configuration(), DEFAULT_CONFIG);
            // Use 9-argument constructor to pass the commit queue manager
            IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                    Executors.newSingleThreadExecutor(), null,
                    new ConnectorProperties(ConnectorType.ICEBERG), new IcebergProcedureRegistry(), manager);
            IcebergTable icebergTable = new IcebergTable(1, "srTableName", CATALOG_NAME, "resource_name", "iceberg_db",
                    "iceberg_table", "", Lists.newArrayList(), mockedNativeTableA, Maps.newHashMap());

            new Expectations(metadata) {
                {
                    metadata.getTable((ConnectContext) any, anyString, anyString);
                    result = icebergTable;
                    minTimes = 0;
                }
            };

            // Prepare test data - append file to table
            mockedNativeTableA.newFastAppend().appendFile(FILE_A).commit();
            mockedNativeTableA.refresh();

            int numThreads = 5;
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch endLatch = new CountDownLatch(numThreads);
            AtomicInteger successCount = new AtomicInteger(0);
            List<Exception> exceptions = new ArrayList<>();

            ExecutorService executor = Executors.newFixedThreadPool(numThreads);

            // Submit multiple concurrent commits to the same table
            for (int i = 0; i < numThreads; i++) {
                final int threadId = i;
                executor.submit(() -> {
                    try {
                        startLatch.await();

                        // Create commit info for this thread
                        TIcebergDataFile dataFile = new TIcebergDataFile();
                        dataFile.setPath(FILE_A.path().toString() + "_thread_" + threadId);
                        dataFile.setFormat("parquet");
                        dataFile.setRecord_count(10);
                        dataFile.setFile_size_in_bytes(1024);
                        dataFile.setPartition_path(mockedNativeTableA.location() + "/data/data_bucket=0/");

                        TSinkCommitInfo commitInfo = new TSinkCommitInfo();
                        commitInfo.setIceberg_data_file(dataFile);

                        // This should be serialized by the commit queue
                        metadata.finishSink("iceberg_db", "iceberg_table", Lists.newArrayList(commitInfo), null);

                        successCount.incrementAndGet();
                    } catch (Exception e) {
                        exceptions.add(e);
                    } finally {
                        endLatch.countDown();
                    }
                });
            }

            // Start all threads
            startLatch.countDown();

            // Wait for all commits to complete
            assertTrue(endLatch.await(60, TimeUnit.SECONDS),
                    "All commits should complete within timeout");
            executor.shutdown();

            // Verify all commits succeeded
            assertEquals(numThreads, successCount.get(),
                    "All commits should succeed when using commit queue");
            assertTrue(exceptions.isEmpty(),
                    "There should be no exceptions: " + exceptions);

            // Verify that the commit queue manager was used
            assertEquals(1, manager.getActiveTableCount(),
                    "Commit queue should have one active table");

        } finally {
            manager.shutdownAll();
        }
    }

    @Test
    public void testFinishSinkWithCommitQueueDisabled() throws Exception {
        // Test that commits work when commit queue is disabled

        // Save original config values
        boolean originalEnableQueue = Config.enable_iceberg_commit_queue;

        try {
            // Disable commit queue
            Config.enable_iceberg_commit_queue = false;

            IcebergHiveCatalog icebergHiveCatalog =
                    new IcebergHiveCatalog(CATALOG_NAME, new Configuration(), DEFAULT_CONFIG);
            IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                    Executors.newSingleThreadExecutor(), null);
            IcebergTable icebergTable = new IcebergTable(1, "srTableName", CATALOG_NAME, "resource_name", "iceberg_db",
                    "iceberg_table", "", Lists.newArrayList(), mockedNativeTableA, Maps.newHashMap());

            new Expectations(metadata) {
                {
                    metadata.getTable((ConnectContext) any, anyString, anyString);
                    result = icebergTable;
                    minTimes = 0;
                }
            };

            // Prepare test data
            mockedNativeTableA.newFastAppend().appendFile(FILE_A).commit();

            // Create commit info
            TIcebergDataFile dataFile = new TIcebergDataFile();
            dataFile.setPath(FILE_A.path().toString());
            dataFile.setFormat("parquet");
            dataFile.setRecord_count(10);
            dataFile.setFile_size_in_bytes(1024);
            dataFile.setPartition_path(mockedNativeTableA.location() + "/data/data_bucket=0/");

            TSinkCommitInfo commitInfo = new TSinkCommitInfo();
            commitInfo.setIceberg_data_file(dataFile);

            // Mock NodeMgr and Frontend for commit audit info
            new MockUp<NodeMgr>() {
                @Mock
                public Frontend getMySelf() {
                    Frontend frontend = new Frontend(FrontendNodeType.LEADER, "test-fe", "127.0.0.1", 9010);
                    return frontend;
                }
            };

            new MockUp<Frontend>() {
                @Mock
                public String getFeVersion() {
                    return "test-version";
                }
            };

            // Commit should succeed even when queue is disabled
            metadata.finishSink("iceberg_db", "iceberg_table", Lists.newArrayList(commitInfo), null);

            // Verify commit succeeded
            mockedNativeTableA.refresh();
            List<FileScanTask> fileScanTasks = Lists.newArrayList(mockedNativeTableA.newScan().planFiles());
            Assertions.assertFalse(fileScanTasks.isEmpty());

        } finally {
            // Restore original config values
            Config.enable_iceberg_commit_queue = originalEnableQueue;
        }
    }

    @Test
    public void testFinishSinkNormalizesCaseForHiveCatalogCommitQueue() throws Exception {
        IcebergCommitQueueManager.Config config = new IcebergCommitQueueManager.Config(true, 30, 100);
        IcebergCommitQueueManager manager = new IcebergCommitQueueManager(config);
        try {
            IcebergHiveCatalog icebergHiveCatalog =
                    new IcebergHiveCatalog(CATALOG_NAME, new Configuration(), DEFAULT_CONFIG);
            IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                    Executors.newSingleThreadExecutor(), null,
                    new ConnectorProperties(ConnectorType.ICEBERG), new IcebergProcedureRegistry(), manager);
            IcebergTable icebergTable = new IcebergTable(1, "srTableName", CATALOG_NAME, "resource_name", "iceberg_db",
                    "iceberg_table", "", Lists.newArrayList(), mockedNativeTableA, Maps.newHashMap());

            new Expectations(metadata) {
                {
                    metadata.getTable((ConnectContext) any, anyString, anyString);
                    result = icebergTable;
                    minTimes = 0;
                }
            };

            // Prepare test data
            mockedNativeTableA.newFastAppend().appendFile(FILE_A).commit();

            TIcebergDataFile dataFile = new TIcebergDataFile();
            dataFile.setPath(FILE_A.path().toString());
            dataFile.setFormat("parquet");
            dataFile.setRecord_count(10);
            dataFile.setFile_size_in_bytes(1024);
            dataFile.setPartition_path(mockedNativeTableA.location() + "/data/data_bucket=0/");

            TSinkCommitInfo commitInfo = new TSinkCommitInfo();
            commitInfo.setIceberg_data_file(dataFile);

            metadata.finishSink("Iceberg_DB", "Iceberg_Table", Lists.newArrayList(commitInfo), null);
            metadata.finishSink("ICEBERG_DB", "ICEBERG_TABLE", Lists.newArrayList(commitInfo), null);

            assertEquals(1, manager.getActiveTableCount(),
                    "Commit queue key should be normalized for Hive catalog");
        } finally {
            manager.shutdownAll();
        }
    }

    @Test
    public void testFinishSinkDirectExecutionWrapsCheckedException() throws Exception {
        IcebergCommitQueueManager disabledManager = new IcebergCommitQueueManager(
                new IcebergCommitQueueManager.Config(false, 120, 10));
        IcebergHiveCatalog icebergHiveCatalog =
                new IcebergHiveCatalog(CATALOG_NAME, new Configuration(), DEFAULT_CONFIG);
        IcebergMetadata metadata = new IcebergMetadata(
                CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                Executors.newSingleThreadExecutor(),
                DEFAULT_CATALOG_PROPERTIES, new ConnectorProperties(ConnectorType.ICEBERG),
                new IcebergProcedureRegistry(), disabledManager) {
            @Override
            public Table getTable(ConnectContext context, String dbName, String tblName) {
                throw new StarRocksConnectorException("checked failure");
            }
        };

        // Provide a non-empty commit list so finishSink does not short-circuit on the
        // empty-commit path (see testFinishSinkSkipsEmptyCommit) and actually reaches getTable().
        TSinkCommitInfo dummyCommit = new TSinkCommitInfo();
        dummyCommit.setIceberg_data_file(new TIcebergDataFile());
        RuntimeException exception = Assertions.assertThrows(RuntimeException.class,
                () -> metadata.finishSink("iceberg_db", "iceberg_table", Lists.newArrayList(dummyCommit), null));
        Assertions.assertTrue(exception.getMessage().contains("checked failure"));
    }

    @Test
    public void testCanDeleteUsingMetadataWithPartitionLevelDelete() {
        // Test partition-level delete detection for identity partition spec
        // When delete expression matches entire partition, canDeleteUsingMetadata should return true
        mockedNativeTableB.newFastAppend().appendFile(FILE_B_1).appendFile(FILE_B_2).commit();

        IcebergTable icebergTable = new IcebergTable(1, "srTableName", CATALOG_NAME, "resource_name", "iceberg_db",
                "iceberg_table", "", Lists.newArrayList(), mockedNativeTableB, Maps.newHashMap());

        // Create predicate: k2 = 2 (which matches entire partition k2=2)
        ScalarOperator predicate = new BinaryPredicateOperator(BinaryType.EQ,
                new ColumnRefOperator(1, INT, "k2", true), ConstantOperator.createInt(2));

        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT,
                new IcebergHiveCatalog(CATALOG_NAME, new Configuration(), DEFAULT_CONFIG),
                Executors.newSingleThreadExecutor(), DEFAULT_CATALOG_PROPERTIES);

        // For identity partition, this should return true since k2 is partition column
        boolean canUseMetadataDelete = metadata.canDeleteUsingMetadata(icebergTable, predicate);
        Assertions.assertTrue(canUseMetadataDelete,
                "Partition-level delete should be eligible for metadata delete");
    }

    @Test
    public void testCanDeleteUsingMetadataWithPartialMatch() {
        // Test when delete expression does not match entire partition or file
        mockedNativeTableB.newFastAppend().appendFile(FILE_B_3).commit();

        IcebergTable icebergTable = new IcebergTable(1, "srTableName", CATALOG_NAME, "resource_name", "iceberg_db",
                "iceberg_table", "", Lists.newArrayList(), mockedNativeTableB, Maps.newHashMap());

        // Create predicate: k1 = 1 (which only matches some rows, not entire file)
        ScalarOperator predicate = new BinaryPredicateOperator(BinaryType.EQ,
                new ColumnRefOperator(0, INT, "k1", true), ConstantOperator.createInt(1));

        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT,
                new IcebergHiveCatalog(CATALOG_NAME, new Configuration(), DEFAULT_CONFIG),
                Executors.newSingleThreadExecutor(), DEFAULT_CATALOG_PROPERTIES);

        // This should return false since expression doesn't match entire file
        boolean canUseMetadataDelete = metadata.canDeleteUsingMetadata(icebergTable, predicate);
        Assertions.assertFalse(canUseMetadataDelete,
                "Partial row match should not be eligible for metadata delete");
    }

    @Test
    public void testCanDeleteUsingMetadataWithAlwaysTrue() {
        // Test delete all rows (no predicate)
        mockedNativeTableB.newFastAppend().appendFile(FILE_B_1).appendFile(FILE_B_2).commit();

        IcebergTable icebergTable = new IcebergTable(1, "srTableName", CATALOG_NAME, "resource_name", "iceberg_db",
                "iceberg_table", "", Lists.newArrayList(), mockedNativeTableB, Maps.newHashMap());

        // Create predicate: null represents delete all (alwaysTrue)
        ScalarOperator predicate = null;

        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT,
                new IcebergHiveCatalog(CATALOG_NAME, new Configuration(), DEFAULT_CONFIG),
                Executors.newSingleThreadExecutor(), DEFAULT_CATALOG_PROPERTIES);

        // Delete all should be eligible for metadata delete
        boolean canUseMetadataDelete = metadata.canDeleteUsingMetadata(icebergTable, predicate);
        Assertions.assertTrue(canUseMetadataDelete,
                "Delete all should be eligible for metadata delete");
    }

    @Test
    public void testExecuteMetadataDelete() throws Exception {
        // Test metadata delete execution
        mockedNativeTableB.newFastAppend().appendFile(FILE_B_1).appendFile(FILE_B_2).commit();

        IcebergTable icebergTable = new IcebergTable(1, "srTableName", CATALOG_NAME, "resource_name", "iceberg_db",
                "iceberg_table", "", Lists.newArrayList(), mockedNativeTableB, Maps.newHashMap());

        // Create predicate: k2 = 2 (delete entire partition)
        ScalarOperator predicate = new BinaryPredicateOperator(BinaryType.EQ,
                new ColumnRefOperator(1, INT, "k2", true), ConstantOperator.createInt(2));

        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog(CATALOG_NAME, new Configuration(), DEFAULT_CONFIG);
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                Executors.newSingleThreadExecutor(), DEFAULT_CATALOG_PROPERTIES);

        new MockUp<NodeMgr>() {
            @Mock
            public Frontend getMySelf() {
                Frontend frontend = new Frontend(FrontendNodeType.LEADER, "test-fe", "127.0.0.1", 9010);
                return frontend;
            }
        };

        new MockUp<Frontend>() {
            @Mock
            public String getFeVersion() {
                return "test-version";
            }
        };

        // Execute metadata delete
        metadata.executeMetadataDelete(icebergTable, predicate, connectContext);

        // Verify the delete was committed
        mockedNativeTableB.refresh();
        List<FileScanTask> fileScanTasks = Lists.newArrayList(mockedNativeTableB.newScan().planFiles());

        // After deleting partition k2=2, only files with k2=3 should remain
        Assertions.assertEquals(1, fileScanTasks.size(), "Only one file should remain after partition delete");
        Assertions.assertEquals("PartitionData{k2=3}", fileScanTasks.get(0).file().partition().toString(),
                "Remaining file should be from partition k2=3");
    }

    @Test
    public void testExecuteMetadataDeleteDeletesAll() throws Exception {
        // Test metadata delete that removes all data
        mockedNativeTableB.newFastAppend().appendFile(FILE_B_1).appendFile(FILE_B_2).commit();

        IcebergTable icebergTable = new IcebergTable(1, "srTableName", CATALOG_NAME, "resource_name", "iceberg_db",
                "iceberg_table", "", Lists.newArrayList(), mockedNativeTableB, Maps.newHashMap());

        // Create predicate: null represents delete all (alwaysTrue)
        ScalarOperator predicate = null;

        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog(CATALOG_NAME, new Configuration(), DEFAULT_CONFIG);
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                Executors.newSingleThreadExecutor(), DEFAULT_CATALOG_PROPERTIES);

        // Mock NodeMgr.getMySelf() to return a valid Frontend
        new MockUp<NodeMgr>() {
            @Mock
            public Frontend getMySelf() {
                return new Frontend(FrontendNodeType.LEADER, "test-fe", "127.0.0.1", 9010);
            }
        };

        new MockUp<Frontend>() {
            @Mock
            public String getFeVersion() {
                return "test-version";
            }
        };

        // Execute metadata delete
        metadata.executeMetadataDelete(icebergTable, predicate, connectContext);

        // Verify all data was deleted
        mockedNativeTableB.refresh();
        List<FileScanTask> fileScanTasks = Lists.newArrayList(mockedNativeTableB.newScan().planFiles());

        Assertions.assertEquals(0, fileScanTasks.size(), "All files should be deleted");
    }

    @Test
    public void testCanDeleteUsingMetadataStringDatePartitionCast() throws Exception {
        // A STRING date partition column compared with a temporal value coerces to CAST(k2 AS DATETIME) = <ts>.
        // The whole predicate is on the identity partition column, so metadata (whole-file) delete is eligible;
        // the specific matching partitions are resolved StarRocks-side at execution time.
        PartitionSpec spec = PartitionSpec.builderFor(SCHEMA_J).identity("k2").build();
        TestTables.TestTable table = create(SCHEMA_J, spec, "tbDeleteStrPartCanDelete", 1);
        table.newFastAppend().appendFile(DataFiles.builder(spec)
                .withPath("/path/to/del-0614.parquet").withFileSizeInBytes(20)
                .withPartitionPath("k2=2020-06-14").withRecordCount(3).build()).commit();
        table.refresh();
        List<Column> columns = Lists.newArrayList(
                new Column("id", INT), new Column("k1", INT), new Column("k2", VARCHAR));
        IcebergTable icebergTable = new IcebergTable(1, "srTableName", CATALOG_NAME, "resource_name", "iceberg_db",
                "iceberg_table", "", columns, table, Maps.newHashMap());
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT,
                new IcebergHiveCatalog(CATALOG_NAME, new Configuration(), DEFAULT_CONFIG),
                Executors.newSingleThreadExecutor(), DEFAULT_CATALOG_PROPERTIES);

        Assertions.assertTrue(metadata.canDeleteUsingMetadata(icebergTable, k2EqDate(2020, 6, 14)),
                "cast-on-string-partition predicate is partition level -> metadata delete eligible");
    }

    @Test
    public void testCanDeleteUsingMetadataStringDatePartitionCastMixedIneligible() throws Exception {
        // A residual (cast on the partition column) combined with a non-partition pushable conjunct must NOT be
        // eligible for metadata delete: a file could contain both matching and non-matching rows.
        PartitionSpec spec = PartitionSpec.builderFor(SCHEMA_J).identity("k2").build();
        TestTables.TestTable table = create(SCHEMA_J, spec, "tbDeleteStrPartMixed", 1);
        table.newFastAppend().appendFile(DataFiles.builder(spec)
                .withPath("/path/to/del-mix-0614.parquet").withFileSizeInBytes(20)
                .withPartitionPath("k2=2020-06-14").withRecordCount(3).build()).commit();
        table.refresh();
        List<Column> columns = Lists.newArrayList(
                new Column("id", INT), new Column("k1", INT), new Column("k2", VARCHAR));
        IcebergTable icebergTable = new IcebergTable(1, "srTableName", CATALOG_NAME, "resource_name", "iceberg_db",
                "iceberg_table", "", columns, table, Maps.newHashMap());
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT,
                new IcebergHiveCatalog(CATALOG_NAME, new Configuration(), DEFAULT_CONFIG),
                Executors.newSingleThreadExecutor(), DEFAULT_CATALOG_PROPERTIES);

        // residual + a data-column (non-partition) conjunct: the pushable part is not partition level, so a file
        // could hold both matching and non-matching rows -> not metadata-delete eligible -> row-level fallback.
        ScalarOperator dataColConj = new BinaryPredicateOperator(BinaryType.EQ,
                new ColumnRefOperator(2, INT, "k1", true), ConstantOperator.createInt(1));
        Assertions.assertFalse(metadata.canDeleteUsingMetadata(icebergTable,
                new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.AND, k2EqDate(2020, 6, 14), dataColConj)),
                "residual + non-partition data-column conjunct must not be metadata-delete eligible");
    }

    @Test
    public void testExecuteMetadataDeleteStringDatePartitionCast() throws Exception {
        // Regression: previously CAST(k2 AS DATETIME) = <ts> was pushed to Iceberg as a string-domain filter
        // (k2 = '2020-06-14 00:00:00'), which never matched the 'yyyy-MM-dd' partition value, so the delete was a
        // silent no-op. Now the matching partition file is deleted and the non-matching one is kept.
        PartitionSpec spec = PartitionSpec.builderFor(SCHEMA_J).identity("k2").build();
        TestTables.TestTable table = create(SCHEMA_J, spec, "tbDeleteStrPartExec", 1);
        table.newFastAppend()
                .appendFile(DataFiles.builder(spec).withPath("/path/to/del-0614.parquet").withFileSizeInBytes(20)
                        .withPartitionPath("k2=2020-06-14").withRecordCount(3).build())
                .appendFile(DataFiles.builder(spec).withPath("/path/to/del-0615.parquet").withFileSizeInBytes(20)
                        .withPartitionPath("k2=2020-06-15").withRecordCount(4).build())
                .commit();
        table.refresh();
        List<Column> columns = Lists.newArrayList(
                new Column("id", INT), new Column("k1", INT), new Column("k2", VARCHAR));
        IcebergTable icebergTable = new IcebergTable(1, "srTableName", CATALOG_NAME, "resource_name", "iceberg_db",
                "iceberg_table", "", columns, table, Maps.newHashMap());
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT,
                new IcebergHiveCatalog(CATALOG_NAME, new Configuration(), DEFAULT_CONFIG),
                Executors.newSingleThreadExecutor(), DEFAULT_CATALOG_PROPERTIES);

        new MockUp<NodeMgr>() {
            @Mock
            public Frontend getMySelf() {
                return new Frontend(FrontendNodeType.LEADER, "test-fe", "127.0.0.1", 9010);
            }
        };
        new MockUp<Frontend>() {
            @Mock
            public String getFeVersion() {
                return "test-version";
            }
        };

        metadata.executeMetadataDelete(icebergTable, k2EqDate(2020, 6, 14), connectContext);

        table.refresh();
        List<FileScanTask> fileScanTasks = Lists.newArrayList(table.newScan().planFiles());
        Assertions.assertEquals(1, fileScanTasks.size(), "only the 2020-06-14 partition file should be deleted");
        Assertions.assertEquals("PartitionData{k2=2020-06-15}", fileScanTasks.get(0).file().partition().toString(),
                "the non-matching 2020-06-15 partition must remain");
    }

    @Test
    public void testMetadataDeleteNodeExplainString() {
        // Test that IcebergMetadataDeleteNode generates correct EXPLAIN output
        mockedNativeTableB.newFastAppend().appendFile(FILE_B_1).appendFile(FILE_B_2).commit();

        IcebergTable icebergTable = new IcebergTable(1, "srTableName", CATALOG_NAME,
                "resource_name", "iceberg_db", "iceberg_table", "",
                Lists.newArrayList(), mockedNativeTableB, Maps.newHashMap());

        // Create predicate: k2 = 2
        ScalarOperator predicate = new BinaryPredicateOperator(BinaryType.EQ,
                new ColumnRefOperator(1, INT, "k2", true), ConstantOperator.createInt(2));

        // Create the metadata delete node
        IcebergMetadataDeleteNode node = new IcebergMetadataDeleteNode(
                new PlanNodeId(0), icebergTable, predicate);

        // Verify EXPLAIN output contains expected information
        String explainString = node.getExplainString();
        Assertions.assertTrue(explainString.contains("ICEBERG METADATA DELETE"),
                "EXPLAIN should contain 'ICEBERG METADATA DELETE'");
        Assertions.assertTrue(explainString.contains("catalog: " + CATALOG_NAME),
                "EXPLAIN should contain catalog name");
        Assertions.assertTrue(explainString.contains("database: iceberg_db"),
                "EXPLAIN should contain database name");
        Assertions.assertTrue(explainString.contains("table: iceberg_table"),
                "EXPLAIN should contain table name");
        Assertions.assertTrue(explainString.contains("predicate:"),
                "EXPLAIN should contain predicate info");
    }

    @Test
    public void testMetadataDeleteNodeExplainStringWithNullPredicate() {
        // Test EXPLAIN output when predicate is null (delete all)
        mockedNativeTableB.newFastAppend().appendFile(FILE_B_1).commit();

        IcebergTable icebergTable = new IcebergTable(1, "srTableName", CATALOG_NAME,
                "resource_name", "iceberg_db",
                "iceberg_table", "", Lists.newArrayList(), mockedNativeTableB, Maps.newHashMap());

        // Create node with null predicate (delete all)
        IcebergMetadataDeleteNode node = new IcebergMetadataDeleteNode(
                new PlanNodeId(0), icebergTable, null);

        // Verify EXPLAIN output
        String explainString = node.getExplainString();
        Assertions.assertTrue(explainString.contains("ICEBERG METADATA DELETE"),
                "EXPLAIN should contain 'ICEBERG METADATA DELETE'");
        Assertions.assertTrue(explainString.contains("predicate: ALL (delete all rows)"),
                "EXPLAIN should indicate 'delete all rows' when predicate is null");
    }

    @Test
    public void testExecuteMetadataDeleteMetrics() throws Exception {
        // Test metadata delete execution with metrics recording
        mockedNativeTableB.newFastAppend().appendFile(FILE_B_1).appendFile(FILE_B_2).commit();

        IcebergTable icebergTable = new IcebergTable(1, "srTableName", CATALOG_NAME, "resource_name", "iceberg_db",
                "iceberg_table", "", Lists.newArrayList(), mockedNativeTableB, Maps.newHashMap());

        // Create predicate: k2 = 2 (delete entire partition)
        ScalarOperator predicate = new BinaryPredicateOperator(BinaryType.EQ,
                new ColumnRefOperator(1, INT, "k2", true), ConstantOperator.createInt(2));

        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog(CATALOG_NAME, new Configuration(), DEFAULT_CONFIG);
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                Executors.newSingleThreadExecutor(), DEFAULT_CATALOG_PROPERTIES);

        new MockUp<NodeMgr>() {
            @Mock
            public Frontend getMySelf() {
                Frontend frontend = new Frontend(FrontendNodeType.LEADER, "test-fe", "127.0.0.1", 9010);
                return frontend;
            }
        };

        new MockUp<Frontend>() {
            @Mock
            public String getFeVersion() {
                return "test-version";
            }
        };

        // Execute metadata delete
        metadata.executeMetadataDelete(icebergTable, predicate, connectContext);

        // Verify the delete was committed
        mockedNativeTableB.refresh();
        List<FileScanTask> fileScanTasks = Lists.newArrayList(mockedNativeTableB.newScan().planFiles());

        // After deleting partition k2=2, only files with k2=3 should remain
        Assertions.assertEquals(1, fileScanTasks.size(), "Only one file should remain after partition delete");

        // Verify a new snapshot was created after metadata delete
        org.apache.iceberg.Snapshot latestSnapshot = mockedNativeTableB.currentSnapshot();
        Assertions.assertNotNull(latestSnapshot, "Latest snapshot should exist after metadata delete");
    }

    @Test
    public void testExecuteMetadataDeleteWithNullPredicate() throws Exception {
        // Test metadata delete with null predicate (delete all)
        mockedNativeTableB.newFastAppend().appendFile(FILE_B_1).commit();

        IcebergTable icebergTable = new IcebergTable(1, "srTableName", CATALOG_NAME, "resource_name", "iceberg_db",
                "iceberg_table", "", Lists.newArrayList(), mockedNativeTableB, Maps.newHashMap());

        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog(CATALOG_NAME, new Configuration(), DEFAULT_CONFIG);
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                Executors.newSingleThreadExecutor(), DEFAULT_CATALOG_PROPERTIES);

        // Mock NodeMgr.getMySelf() to return a valid Frontend
        new MockUp<NodeMgr>() {
            @Mock
            public Frontend getMySelf() {
                return new Frontend(FrontendNodeType.LEADER, "test-fe", "127.0.0.1", 9010);
            }
        };

        new MockUp<Frontend>() {
            @Mock
            public String getFeVersion() {
                return "test-version";
            }
        };

        // Test that null predicate is handled correctly (returns alwaysTrue expression)
        // This should succeed, not throw exception
        Assertions.assertDoesNotThrow(() -> {
            metadata.executeMetadataDelete(icebergTable, null, connectContext);
        }, "Null predicate should be handled as delete all");

        // Verify all data was deleted
        mockedNativeTableB.refresh();
        List<FileScanTask> fileScanTasks = Lists.newArrayList(mockedNativeTableB.newScan().planFiles());
        Assertions.assertEquals(0, fileScanTasks.size(), "All files should be deleted with null predicate");

        // Verify a new snapshot was created after metadata delete
        org.apache.iceberg.Snapshot latestSnapshot = mockedNativeTableB.currentSnapshot();
        Assertions.assertNotNull(latestSnapshot, "Latest snapshot should exist after metadata delete");
    }

    @Test
    public void testSplitFileScanTaskCoalescesPositionDeleteTasks() throws Exception {
        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog(CATALOG_NAME, new Configuration(), DEFAULT_CONFIG);
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                Executors.newSingleThreadExecutor(), DEFAULT_CATALOG_PROPERTIES);

        FileScanTask task = Mockito.mock(FileScanTask.class);
        DeleteFile deleteFile = Mockito.mock(DeleteFile.class);
        Mockito.when(deleteFile.content()).thenReturn(FileContent.POSITION_DELETES);
        Mockito.when(task.deletes()).thenReturn(List.of(deleteFile));
        Mockito.when(task.start()).thenReturn(0L);
        Mockito.when(task.length()).thenReturn(180L);

        FileScanTask splitTask1 = Mockito.mock(FileScanTask.class);
        Mockito.when(splitTask1.start()).thenReturn(0L);
        Mockito.when(splitTask1.length()).thenReturn(64L);

        FileScanTask splitTask2 = Mockito.mock(FileScanTask.class);
        Mockito.when(splitTask2.start()).thenReturn(64L);
        Mockito.when(splitTask2.length()).thenReturn(64L);

        FileScanTask splitTask3 = Mockito.mock(FileScanTask.class);
        Mockito.when(splitTask3.start()).thenReturn(128L);
        Mockito.when(splitTask3.length()).thenReturn(52L);

        Mockito.when(task.split(128L)).thenReturn(List.of(splitTask1, splitTask2, splitTask3));

        Method splitMethod = IcebergMetadata.class.getDeclaredMethod("splitFileScanTask",
                FileScanTask.class, long.class);
        splitMethod.setAccessible(true);

        CloseableIterable<FileScanTask> iterable =
                (CloseableIterable<FileScanTask>) splitMethod.invoke(metadata, task, 128L);
        List<FileScanTask> actualTasks = Lists.newArrayList(iterable);

        Assertions.assertEquals(2, actualTasks.size());
        Assertions.assertEquals(0L, actualTasks.get(0).start());
        Assertions.assertEquals(128L, actualTasks.get(0).length());
        Assertions.assertEquals(128L, actualTasks.get(1).start());
        Assertions.assertEquals(52L, actualTasks.get(1).length());
        Assertions.assertSame(task.deletes(), actualTasks.get(0).deletes());
        Assertions.assertSame(task.deletes(), actualTasks.get(1).deletes());
    }

    @Test
    public void testSplitFileScanTaskUsesTableScanUtilForNonPositionDeleteTasks() throws Exception {
        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog(CATALOG_NAME, new Configuration(), DEFAULT_CONFIG);
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                Executors.newSingleThreadExecutor(), DEFAULT_CATALOG_PROPERTIES);

        FileScanTask task = Mockito.mock(FileScanTask.class);
        DeleteFile deleteFile = Mockito.mock(DeleteFile.class);
        Mockito.when(deleteFile.content()).thenReturn(FileContent.EQUALITY_DELETES);
        Mockito.when(task.deletes()).thenReturn(List.of(deleteFile));

        CloseableIterable<FileScanTask> expectedIterable = CloseableIterable.withNoopClose(List.of(task));
        Method splitMethod = IcebergMetadata.class.getDeclaredMethod("splitFileScanTask",
                FileScanTask.class, long.class);
        splitMethod.setAccessible(true);

        try (MockedStatic<TableScanUtil> tableScanUtilMock = Mockito.mockStatic(TableScanUtil.class)) {
            tableScanUtilMock.when(() -> TableScanUtil.splitFiles(Mockito.any(), Mockito.eq(128L)))
                    .thenReturn(expectedIterable);

            CloseableIterable<FileScanTask> actualIterable =
                    (CloseableIterable<FileScanTask>) splitMethod.invoke(metadata, task, 128L);

            Assertions.assertSame(expectedIterable, actualIterable);
            tableScanUtilMock.verify(() -> TableScanUtil.splitFiles(Mockito.any(), Mockito.eq(128L)),
                    Mockito.times(1));
        }
    }

    @Test
    public void testListTableDeltaTraitsSkipsReplaceSnapshots() {
        // Step 1: APPEND FILE_A
        mockedNativeTableA.newAppend().appendFile(FILE_A).commit();
        Snapshot snap1 = mockedNativeTableA.currentSnapshot();
        Assertions.assertEquals("append", snap1.operation());

        // Step 2: REPLACE (rewrite FILE_A -> FILE_A_1, simulating compaction)
        mockedNativeTableA.newRewrite().deleteFile(FILE_A).addFile(FILE_A_1).commit();
        Snapshot snap2 = mockedNativeTableA.currentSnapshot();
        Assertions.assertEquals("replace", snap2.operation());

        // Step 3: APPEND FILE_A_2
        mockedNativeTableA.newAppend().appendFile(FILE_A_2).commit();
        Snapshot snap3 = mockedNativeTableA.currentSnapshot();
        Assertions.assertEquals("append", snap3.operation());

        // Build IcebergTable wrapping the native table
        IcebergTable icebergTable = new IcebergTable(1, "testTbl", CATALOG_NAME, CATALOG_NAME,
                "db", "testTbl", "", Lists.newArrayList(), mockedNativeTableA, Maps.newHashMap());

        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT,
                new IcebergHiveCatalog(CATALOG_NAME, new Configuration(), DEFAULT_CONFIG),
                Executors.newSingleThreadExecutor(), null);

        // Query delta traits from snap1 (exclusive) to snap3 (inclusive)
        // This range includes snap2 (REPLACE) and snap3 (APPEND)
        TvrTableSnapshot from = TvrTableSnapshot.of(Optional.of(snap1.snapshotId()));
        TvrTableSnapshot to = TvrTableSnapshot.of(Optional.of(snap3.snapshotId()));

        List<TvrTableDeltaTrait> traits = metadata.listTableDeltaTraits("db", icebergTable, from, to);

        // REPLACE snapshot should be skipped, only APPEND (snap3) should remain
        Assertions.assertEquals(1, traits.size());
        Assertions.assertTrue(traits.get(0).isAppendOnly());

        // Verify the returned trait corresponds to snap3, not snap2
        TvrTableDelta delta = traits.get(0).getTvrDelta();
        Assertions.assertEquals(snap3.snapshotId(), delta.end().get());
    }

    @Test
    public void testListTableDeltaTraitsSkipsReplacePreservesContiguousBoundaries() {
        // snap1: APPEND
        mockedNativeTableA.newAppend().appendFile(FILE_A).commit();
        Snapshot snap1 = mockedNativeTableA.currentSnapshot();

        // snap2: APPEND
        mockedNativeTableA.newAppend().appendFile(FILE_A_1).commit();
        Snapshot snap2 = mockedNativeTableA.currentSnapshot();

        // snap3: REPLACE (compaction)
        mockedNativeTableA.newRewrite().deleteFile(FILE_A).addFile(FILE_A_2).commit();
        Snapshot snap3 = mockedNativeTableA.currentSnapshot();
        Assertions.assertEquals("replace", snap3.operation());

        // snap4: APPEND
        mockedNativeTableA.newAppend().appendFile(FILE_A).commit();
        Snapshot snap4 = mockedNativeTableA.currentSnapshot();

        IcebergTable icebergTable = new IcebergTable(1, "testTbl", CATALOG_NAME, CATALOG_NAME,
                "db", "testTbl", "", Lists.newArrayList(), mockedNativeTableA, Maps.newHashMap());

        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT,
                new IcebergHiveCatalog(CATALOG_NAME, new Configuration(), DEFAULT_CONFIG),
                Executors.newSingleThreadExecutor(), null);

        // Query from snap1 (exclusive) to snap4 (inclusive): snap2(APPEND), snap3(REPLACE), snap4(APPEND)
        TvrTableSnapshot from = TvrTableSnapshot.of(Optional.of(snap1.snapshotId()));
        TvrTableSnapshot to = TvrTableSnapshot.of(Optional.of(snap4.snapshotId()));

        List<TvrTableDeltaTrait> traits = metadata.listTableDeltaTraits("db", icebergTable, from, to);

        // Two APPENDs should remain, REPLACE skipped
        Assertions.assertEquals(2, traits.size());
        Assertions.assertTrue(traits.get(0).isAppendOnly());
        Assertions.assertTrue(traits.get(1).isAppendOnly());

        // Verify contiguous delta boundaries: snap2→snap3, snap4→snap4
        // (snap3 is the REPLACE snapshot ID — used as boundary but not emitted as a trait)
        TvrTableDelta delta0 = traits.get(0).getTvrDelta();
        Assertions.assertEquals(snap2.snapshotId(), delta0.start().get());
        Assertions.assertEquals(snap3.snapshotId(), delta0.end().get());

        TvrTableDelta delta1 = traits.get(1).getTvrDelta();
        Assertions.assertEquals(snap4.snapshotId(), delta1.start().get());
        Assertions.assertEquals(snap4.snapshotId(), delta1.end().get());
    }

    @Test
    public void testListTableDeltaTraitsAllReplaceReturnsEmpty() {
        // snap1: APPEND (used as exclusive start)
        mockedNativeTableA.newAppend().appendFile(FILE_A).commit();
        Snapshot snap1 = mockedNativeTableA.currentSnapshot();

        // snap2: REPLACE
        mockedNativeTableA.newRewrite().deleteFile(FILE_A).addFile(FILE_A_1).commit();
        Snapshot snap2 = mockedNativeTableA.currentSnapshot();
        Assertions.assertEquals("replace", snap2.operation());

        IcebergTable icebergTable = new IcebergTable(1, "testTbl", CATALOG_NAME, CATALOG_NAME,
                "db", "testTbl", "", Lists.newArrayList(), mockedNativeTableA, Maps.newHashMap());

        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT,
                new IcebergHiveCatalog(CATALOG_NAME, new Configuration(), DEFAULT_CONFIG),
                Executors.newSingleThreadExecutor(), null);

        TvrTableSnapshot from = TvrTableSnapshot.of(Optional.of(snap1.snapshotId()));
        TvrTableSnapshot to = TvrTableSnapshot.of(Optional.of(snap2.snapshotId()));

        List<TvrTableDeltaTrait> traits = metadata.listTableDeltaTraits("db", icebergTable, from, to);

        // Only REPLACE in range — should return empty
        Assertions.assertTrue(traits.isEmpty());
    }

    @Test
    public void testListTableDeltaTraitsAllowsExpiredExclusiveStartSnapshot() {
        mockedNativeTableA.newAppend().appendFile(FILE_A).commit();
        Snapshot snap1 = mockedNativeTableA.currentSnapshot();

        mockedNativeTableA.newAppend().appendFile(FILE_A_1).commit();
        Snapshot snap2 = mockedNativeTableA.currentSnapshot();

        mockedNativeTableA.expireSnapshots().expireSnapshotId(snap1.snapshotId()).commit();
        mockedNativeTableA.refresh();

        IcebergTable icebergTable = new IcebergTable(1, "testTbl", CATALOG_NAME, CATALOG_NAME,
                "db", "testTbl", "", Lists.newArrayList(), mockedNativeTableA, Maps.newHashMap());
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT,
                new IcebergHiveCatalog(CATALOG_NAME, new Configuration(), DEFAULT_CONFIG),
                Executors.newSingleThreadExecutor(), null);

        TvrTableSnapshot from = TvrTableSnapshot.of(Optional.of(snap1.snapshotId()));
        TvrTableSnapshot to = TvrTableSnapshot.of(Optional.of(snap2.snapshotId()));

        List<TvrTableDeltaTrait> traits = metadata.listTableDeltaTraits("db", icebergTable, from, to);
        Assertions.assertEquals(1, traits.size());
        Assertions.assertTrue(traits.get(0).isAppendOnly());
        Assertions.assertEquals(snap2.snapshotId(), traits.get(0).getTvrDelta().start().get());
        Assertions.assertEquals(snap2.snapshotId(), traits.get(0).getTvrDelta().end().get());
    }

    @Test
    public void testListTableDeltaTraitsFailsWhenSnapshotLineageIsBroken() {
        mockedNativeTableA.newAppend().appendFile(FILE_A).commit();
        Snapshot snap1 = mockedNativeTableA.currentSnapshot();

        mockedNativeTableA.newAppend().appendFile(FILE_A_1).commit();
        Snapshot snap2 = mockedNativeTableA.currentSnapshot();

        mockedNativeTableA.newAppend().appendFile(FILE_A_2).commit();
        Snapshot snap3 = mockedNativeTableA.currentSnapshot();

        mockedNativeTableA.expireSnapshots().expireSnapshotId(snap2.snapshotId()).commit();
        mockedNativeTableA.refresh();

        IcebergTable icebergTable = new IcebergTable(1, "testTbl", CATALOG_NAME, CATALOG_NAME,
                "db", "testTbl", "", Lists.newArrayList(), mockedNativeTableA, Maps.newHashMap());
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT,
                new IcebergHiveCatalog(CATALOG_NAME, new Configuration(), DEFAULT_CONFIG),
                Executors.newSingleThreadExecutor(), null);

        TvrTableSnapshot from = TvrTableSnapshot.of(Optional.of(snap1.snapshotId()));
        TvrTableSnapshot to = TvrTableSnapshot.of(Optional.of(snap3.snapshotId()));

        StarRocksConnectorException exception = assertThrows(StarRocksConnectorException.class,
                () -> metadata.listTableDeltaTraits("db", icebergTable, from, to));
        assertTrue(exception.getMessage().contains("Starting snapshot (exclusive)"));
        assertTrue(exception.getMessage().contains(String.valueOf(snap1.snapshotId())));
        assertTrue(exception.getMessage().contains(String.valueOf(snap3.snapshotId())));
    }

    @Test
    public void testTimeTravelHonorsSnapshotSchema() {
        // S1 committed under the original schema (k1, k2); then k2 is renamed; then S2 is
        // committed under the new schema (k1, k2_renamed). A time-travel read of S1 must honor
        // S1's schema (k2), not the latest table schema (k2_renamed). See POST-1557.
        mockedNativeTableB.newAppend().appendFile(FILE_B_1).commit();
        mockedNativeTableB.refresh();
        long s1 = mockedNativeTableB.currentSnapshot().snapshotId();
        int s1SchemaId = mockedNativeTableB.currentSnapshot().schemaId();

        mockedNativeTableB.updateSchema().renameColumn("k2", "k2_renamed").commit();
        mockedNativeTableB.newAppend().appendFile(FILE_B_2).commit();
        mockedNativeTableB.refresh();

        // The current (latest) schema carries the renamed column.
        Assertions.assertNotEquals(s1SchemaId, mockedNativeTableB.schema().schemaId());
        Assertions.assertNotNull(mockedNativeTableB.schema().findField("k2_renamed"));
        Assertions.assertNull(mockedNativeTableB.schema().findField("k2"));

        IcebergTable icebergTable = new IcebergTable(1, "srTableName", CATALOG_NAME, "resource_name", "iceberg_db",
                "iceberg_table", "", IcebergApiConverter.toFullSchemas(mockedNativeTableB.schema(), mockedNativeTableB),
                mockedNativeTableB, Maps.newHashMap());
        Assertions.assertNotNull(icebergTable.getColumn("k2_renamed"));
        Assertions.assertNull(icebergTable.getColumn("k2"));

        // getSnapshotSchema resolves the schema bound to the targeted snapshot.
        Schema snapshotSchema = IcebergMetadata.getSnapshotSchema(mockedNativeTableB, s1);
        Assertions.assertNotNull(snapshotSchema);
        Assertions.assertEquals(s1SchemaId, snapshotSchema.schemaId());
        Assertions.assertNotNull(snapshotSchema.findField("k2"));
        Assertions.assertNull(snapshotSchema.findField("k2_renamed"));

        // Rebinding the table to the snapshot schema must flip both the StarRocks-side full
        // schema (column resolution / result-set metadata) and the schema fed to the BE.
        icebergTable = icebergTable.withReadMetadata(snapshotSchema,
                IcebergMetadata.getSnapshotSpecs(mockedNativeTableB, s1));
        Assertions.assertNotNull(icebergTable.getColumn("k2"));
        Assertions.assertNull(icebergTable.getColumn("k2_renamed"));
        Assertions.assertNotNull(icebergTable.getReadSchema().findField("k2"));
        Assertions.assertNull(icebergTable.getReadSchema().findField("k2_renamed"));

        // The table is partitioned by identity(k2): partition columns must resolve through the
        // snapshot schema (old name) instead of going null on the renamed current name.
        List<Column> partitionColumns = icebergTable.getPartitionColumns();
        Assertions.assertEquals(1, partitionColumns.size());
        Assertions.assertNotNull(partitionColumns.get(0));
        Assertions.assertEquals("k2", partitionColumns.get(0).getName());

        // The BE descriptor must also build its partition info from the snapshot schema:
        // expressions and source column names reference the old name, matching fullSchema.
        TTableDescriptor tableDescriptor = icebergTable.toThrift(Lists.newArrayList());
        TIcebergTable tIcebergTable = tableDescriptor.getIcebergTable();
        Assertions.assertEquals("k2", tIcebergTable.getPartition_info().get(0).getSource_column_name());
        Assertions.assertEquals("k2", tIcebergTable.getIceberg_schema().getFields().get(1).getName());

        // Scan planning converts predicates against the snapshot schema, so filtering on the
        // old column name must plan files of the targeted snapshot instead of failing.
        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog(CATALOG_NAME, new Configuration(), DEFAULT_CONFIG);
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                Executors.newSingleThreadExecutor(), DEFAULT_CATALOG_PROPERTIES);
        ScalarOperator predicate = new BinaryPredicateOperator(BinaryType.GE,
                new ColumnRefOperator(1, INT, "k2", true), ConstantOperator.createInt(1));
        List<RemoteFileInfo> res = metadata.getRemoteFiles(icebergTable,
                GetRemoteFilesParams.newBuilder().setTableVersionRange(TvrTableSnapshot.of(Optional.of(s1)))
                        .setPredicate(predicate).setFieldNames(Lists.newArrayList()).setLimit(10).build());
        Assertions.assertEquals(3, res.stream()
                .map(f -> (IcebergRemoteFileInfo) f)
                .map(fileInfo -> fileInfo.getFileScanTask().file().recordCount()).reduce(0L, Long::sum), 0.001);
    }

    @Test
    public void testCurrentReadHonorsSchemaAfterMetadataOnlyAddColumn() {
        // ADD COLUMN is a metadata-only commit: it advances the schema without a new snapshot, so the
        // current snapshot still references the pre-evolution schema (no k3). An ordinary current read
        // must resolve the new column against the current table schema (backfilled NULL) instead of the
        // stale snapshot schema, so a filter on k3 binds instead of failing with "Cannot find field".
        mockedNativeTableC.newAppend().appendFile(FILE_B_1).commit();
        mockedNativeTableC.updateSchema().addColumn("k3", Types.IntegerType.get()).commit();
        mockedNativeTableC.refresh();
        long currentSnapshotId = mockedNativeTableC.currentSnapshot().snapshotId();

        IcebergTable icebergTable = new IcebergTable(1, "srTableName", CATALOG_NAME, "resource_name", "iceberg_db",
                "iceberg_table", "", IcebergApiConverter.toFullSchemas(mockedNativeTableC.schema(), mockedNativeTableC),
                mockedNativeTableC, Maps.newHashMap());
        // Ordinary read: no time-travel read view, so getReadSchema() is the current schema (has k3).
        Assertions.assertFalse(icebergTable.isTimeTravelRead());
        Assertions.assertNotNull(icebergTable.getReadSchema().findField("k3"));

        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog(CATALOG_NAME, new Configuration(), DEFAULT_CONFIG);
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                Executors.newSingleThreadExecutor(), DEFAULT_CATALOG_PROPERTIES);
        ScalarOperator predicate = new IsNullPredicateOperator(false,
                new ColumnRefOperator(3, INT, "k3", true));
        List<RemoteFileInfo> res = metadata.getRemoteFiles(icebergTable,
                GetRemoteFilesParams.newBuilder().setTableVersionRange(TvrTableSnapshot.of(Optional.of(currentSnapshotId)))
                        .setPredicate(predicate).setFieldNames(Lists.newArrayList("k1", "k3")).setLimit(10).build());
        Assertions.assertEquals(1, res.size());
        Assertions.assertEquals(3, ((IcebergRemoteFileInfo) res.get(0)).getFileScanTask().file().recordCount());
    }

    @Test
    public void testTimeTravelSnapshotBeforePartitionEvolution() {
        // S1 committed when the table was partitioned only by k2. Later a new column `k3` is
        // added and the partition spec evolves to also partition by k3. Time travel to S1 must
        // still be readable: the partition column added after S1 is dropped from the snapshot's
        // partitioning rather than rejecting the query or failing to resolve column `k3`.
        mockedNativeTableB.newAppend().appendFile(FILE_B_1).commit();
        mockedNativeTableB.refresh();
        long s1 = mockedNativeTableB.currentSnapshot().snapshotId();

        mockedNativeTableB.updateSchema().addColumn("k3", Types.IntegerType.get()).commit();
        mockedNativeTableB.updateSpec().addField("k3").commit();
        mockedNativeTableB.refresh();

        Schema snapshotSchema = IcebergMetadata.getSnapshotSchema(mockedNativeTableB, s1);
        Assertions.assertNotNull(snapshotSchema);
        Assertions.assertNull(snapshotSchema.findField("k3"));

        IcebergTable icebergTable = new IcebergTable(1, "srTableName", CATALOG_NAME, "resource_name", "iceberg_db",
                "iceberg_table", "", IcebergApiConverter.toFullSchemas(mockedNativeTableB.schema(), mockedNativeTableB),
                mockedNativeTableB, Maps.newHashMap());
        // Latest table is partitioned by both k2 and k3.
        Assertions.assertEquals(List.of("k2", "k3"), icebergTable.getPartitionColumnNames());

        // Binding the snapshot read metadata must not throw, and must drop the k3 partition column.
        IcebergTable snapshotTable = icebergTable.withReadMetadata(snapshotSchema,
                IcebergMetadata.getSnapshotSpecs(mockedNativeTableB, s1));
        Assertions.assertEquals(List.of("k2"), snapshotTable.getPartitionColumnNames());
        Assertions.assertTrue(snapshotTable.getPartitionColumns().stream().allMatch(java.util.Objects::nonNull));

        // The BE descriptor must build without referencing the evolved partition column.
        TTableDescriptor tableDescriptor = snapshotTable.toThrift(Lists.newArrayList());
        TIcebergTable tIcebergTable = tableDescriptor.getIcebergTable();
        Assertions.assertEquals(1, tIcebergTable.getPartition_info().size());
        Assertions.assertEquals("k2", tIcebergTable.getPartition_info().get(0).getSource_column_name());
        Assertions.assertNull(tIcebergTable.getIceberg_schema().getFields().stream()
                .filter(f -> f.getName().equals("k3")).findAny().orElse(null));

        // A scan of the pre-evolution snapshot plans its files (which use the old spec).
        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog(CATALOG_NAME, new Configuration(), DEFAULT_CONFIG);
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                Executors.newSingleThreadExecutor(), DEFAULT_CATALOG_PROPERTIES);
        List<RemoteFileInfo> res = metadata.getRemoteFiles(snapshotTable,
                GetRemoteFilesParams.newBuilder().setTableVersionRange(TvrTableSnapshot.of(Optional.of(s1)))
                        .setFieldNames(Lists.newArrayList()).setLimit(10).build());
        Assertions.assertEquals(3, res.stream()
                .map(f -> (IcebergRemoteFileInfo) f)
                .map(fileInfo -> fileInfo.getFileScanTask().file().recordCount()).reduce(0L, Long::sum), 0.001);
    }

    @Test
    public void testTimeTravelDropsPartitionFieldAddedOnPreexistingColumn() {
        // S1 committed when the table was partitioned only by k2. Later the spec evolves to also
        // partition by k1 -- a column that ALREADY existed at S1. Filtering the current spec by
        // schema membership would wrongly keep k1 (its source column is in the snapshot schema), so
        // the snapshot must be read with its own spec: at S1 only k2 is a partition column.
        mockedNativeTableB.newAppend().appendFile(FILE_B_1).commit();
        mockedNativeTableB.refresh();
        long s1 = mockedNativeTableB.currentSnapshot().snapshotId();

        mockedNativeTableB.updateSpec().addField("k1").commit();
        mockedNativeTableB.refresh();

        IcebergTable icebergTable = new IcebergTable(1, "srTableName", CATALOG_NAME, "resource_name", "iceberg_db",
                "iceberg_table", "", IcebergApiConverter.toFullSchemas(mockedNativeTableB.schema(), mockedNativeTableB),
                mockedNativeTableB, Maps.newHashMap());
        // Latest table is partitioned by both k2 and the newly added k1.
        Assertions.assertTrue(icebergTable.getPartitionColumnNames().contains("k1"));

        Schema snapshotSchema = IcebergMetadata.getSnapshotSchema(mockedNativeTableB, s1);
        IcebergTable snapshotTable = icebergTable.withReadMetadata(snapshotSchema,
                IcebergMetadata.getSnapshotSpecs(mockedNativeTableB, s1));
        // The k1 partition field was added after S1, so S1 is partitioned only by k2.
        Assertions.assertEquals(List.of("k2"), snapshotTable.getPartitionColumnNames());
        Assertions.assertFalse(snapshotTable.isUnPartitioned());
        TTableDescriptor tableDescriptor = snapshotTable.toThrift(Lists.newArrayList());
        Assertions.assertEquals(1, tableDescriptor.getIcebergTable().getPartition_info().size());
        Assertions.assertEquals("k2",
                tableDescriptor.getIcebergTable().getPartition_info().get(0).getSource_column_name());
    }

    @Test
    public void testTimeTravelForcesLocalPlanning() throws Exception {
        // Time travel must plan locally even when the session requests distributed planning: the remote
        // metadata scanner binds pushed-down predicates against the current specs, which fails for a
        // column renamed after the snapshot. Local planning rebinds to the snapshot spec, so a predicate
        // on the old column name plans the snapshot's files instead of routing to the remote scanner.
        StarRocksAssert starRocksAssert = new StarRocksAssert();
        // Restore the plan mode afterwards: this test shares the thread-local ConnectContext, so leaking
        // DISTRIBUTED would push later tests (e.g. testGetRemoteFileStringDatePartitionPrune) onto the remote
        // metadata-planning path and make them fail.
        String previousPlanMode = starRocksAssert.getCtx().getSessionVariable().getPlanMode();
        starRocksAssert.getCtx().getSessionVariable().setPlanMode(PlanMode.DISTRIBUTED.modeName());
        try {
            mockedNativeTableB.newAppend().appendFile(FILE_B_1).commit();
            mockedNativeTableB.refresh();
            long s1 = mockedNativeTableB.currentSnapshot().snapshotId();
            mockedNativeTableB.updateSchema().renameColumn("k2", "k2_renamed").commit();
            mockedNativeTableB.newAppend().appendFile(FILE_B_2).commit();
            mockedNativeTableB.refresh();

            IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog(CATALOG_NAME, new Configuration(), DEFAULT_CONFIG);
            IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                    Executors.newSingleThreadExecutor(), DEFAULT_CATALOG_PROPERTIES);
            IcebergTable icebergTable = new IcebergTable(1, "srTableName", CATALOG_NAME, "resource_name", "iceberg_db",
                    "iceberg_table", "", IcebergApiConverter.toFullSchemas(mockedNativeTableB.schema(), mockedNativeTableB),
                    mockedNativeTableB, Maps.newHashMap());
            icebergTable = icebergTable.withReadMetadata(IcebergMetadata.getSnapshotSchema(mockedNativeTableB, s1),
                    IcebergMetadata.getSnapshotSpecs(mockedNativeTableB, s1));

            ScalarOperator predicate = new BinaryPredicateOperator(BinaryType.GE,
                    new ColumnRefOperator(1, INT, "k2", true), ConstantOperator.createInt(1));
            List<RemoteFileInfo> res = metadata.getRemoteFiles(icebergTable,
                    GetRemoteFilesParams.newBuilder().setTableVersionRange(TvrTableSnapshot.of(Optional.of(s1)))
                            .setPredicate(predicate).setFieldNames(Lists.newArrayList()).setLimit(10).build());
            Assertions.assertEquals(3, res.stream()
                    .map(f -> (IcebergRemoteFileInfo) f)
                    .map(fileInfo -> fileInfo.getFileScanTask().file().recordCount()).reduce(0L, Long::sum), 0.001);
        } finally {
            starRocksAssert.getCtx().getSessionVariable().setPlanMode(previousPlanMode);
        }
    }

    @Test
    public void testGetDeleteFilesBindsReadSchemaForTimeTravel() {
        // The equality-delete rewrite path (getDeleteFiles) converts its predicate against getReadSchema(),
        // so it must also pin that read schema on the scan context -- mirroring buildFileScanTaskIterator --
        // otherwise useSnapshot falls back to the snapshot's per-snapshot schema and the pushed predicate /
        // file specs bind against the wrong schema. Capture the context handed to getTableScan and assert the
        // targeted snapshot schema is set on it.
        mockedNativeTableB.newAppend().appendFile(FILE_B_1).commit();
        mockedNativeTableB.refresh();
        long s1 = mockedNativeTableB.currentSnapshot().snapshotId();
        mockedNativeTableB.updateSchema().renameColumn("k2", "k2_renamed").commit();
        mockedNativeTableB.newAppend().appendFile(FILE_B_2).commit();
        mockedNativeTableB.refresh();

        Schema snapshotSchema = IcebergMetadata.getSnapshotSchema(mockedNativeTableB, s1);
        Assertions.assertNotNull(snapshotSchema);

        IcebergHiveCatalog icebergHiveCatalog = new IcebergHiveCatalog(CATALOG_NAME, new Configuration(), DEFAULT_CONFIG);
        IcebergMetadata metadata = new IcebergMetadata(CATALOG_NAME, HDFS_ENVIRONMENT, icebergHiveCatalog,
                Executors.newSingleThreadExecutor(), DEFAULT_CATALOG_PROPERTIES);
        IcebergTable icebergTable = new IcebergTable(1, "srTableName", CATALOG_NAME, "resource_name", "iceberg_db",
                "iceberg_table", "", IcebergApiConverter.toFullSchemas(mockedNativeTableB.schema(), mockedNativeTableB),
                mockedNativeTableB, Maps.newHashMap());
        IcebergTable snapshotTable = icebergTable.withReadMetadata(snapshotSchema,
                IcebergMetadata.getSnapshotSpecs(mockedNativeTableB, s1));
        Assertions.assertTrue(snapshotTable.isTimeTravelRead());

        // Capture the read schema bound onto the scan context. Intercept setReadSchema directly (a regular
        // instance method) rather than the inherited default getTableScan, which MockUp cannot fake:
        // getDeleteFiles must call setReadSchema with the targeted snapshot schema (it never did before the fix).
        Schema[] boundReadSchema = new Schema[1];
        new MockUp<StarRocksIcebergTableScanContext>() {
            @Mock
            public void setReadSchema(Schema schema) {
                boundReadSchema[0] = schema;
            }
        };

        ScalarOperator predicate = new BinaryPredicateOperator(BinaryType.GE,
                new ColumnRefOperator(1, INT, "k2", true), ConstantOperator.createInt(1));
        try {
            metadata.getDeleteFiles(snapshotTable, s1, predicate, FileContent.EQUALITY_DELETES);
        } catch (Exception ignore) {
            // The mock table carries no equality deletes; we assert only the read-schema binding, which happens
            // before the scan runs.
        }

        Assertions.assertSame(snapshotSchema, boundReadSchema[0],
                "getDeleteFiles must bind the targeted snapshot schema on the scan context for time travel");
    }

    @Test
    public void testTimeTravelSnapshotSchemaThroughAnalyzer() throws Exception {
        // End-to-end through QueryAnalyzer: SELECT * VERSION AS OF <old snapshot> must expose the
        // old column name, and the analyzer must pin the resolved version range on the relation
        // so the transformer does not resolve the query period a second time.
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
        String createCatalog = "CREATE EXTERNAL CATALOG iceberg_tt_catalog PROPERTIES(\"type\"=\"iceberg\", " +
                "\"iceberg.catalog.hive.metastore.uris\"=\"thrift://127.0.0.1:9083\", \"iceberg.catalog.type\"=\"hive\")";
        StarRocksAssert starRocksAssert = new StarRocksAssert();
        starRocksAssert.withCatalog(createCatalog);

        mockedNativeTableB.newAppend().appendFile(FILE_B_1).commit();
        mockedNativeTableB.refresh();
        long s1 = mockedNativeTableB.currentSnapshot().snapshotId();
        // TestTables snapshot ids are small integers whose SQL literals don't parse as BIGINT,
        // so target the snapshot through a tag (the VARCHAR version path).
        mockedNativeTableB.manageSnapshots().createTag("tag_s1", s1).commit();
        mockedNativeTableB.updateSchema().renameColumn("k2", "k2_renamed").commit();
        mockedNativeTableB.newAppend().appendFile(FILE_B_2).commit();
        mockedNativeTableB.refresh();

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
                return mockedNativeTableB;
            }

            @Mock
            boolean tableExists(ConnectContext context, String dbName, String tableName) {
                return true;
            }
        };

        String sql = "SELECT * FROM iceberg_tt_catalog.db.tb VERSION AS OF 'tag_s1'";
        QueryStatement stmt = (QueryStatement) AnalyzeTestUtil.analyzeSuccess(sql);
        Assertions.assertEquals(List.of("k1", "k2"), stmt.getQueryRelation().getColumnOutputNames());

        TableRelation tableRelation =
                (TableRelation) ((SelectRelation) stmt.getQueryRelation()).getRelation();
        Assertions.assertNotNull(tableRelation.getTvrVersionRange());
        Assertions.assertEquals(Optional.of(s1), tableRelation.getTvrVersionRange().end());

        // The latest schema keeps working without a query period.
        QueryStatement latestStmt =
                (QueryStatement) AnalyzeTestUtil.analyzeSuccess("SELECT * FROM iceberg_tt_catalog.db.tb");
        Assertions.assertEquals(List.of("k1", "k2_renamed"), latestStmt.getQueryRelation().getColumnOutputNames());
    }

    @Test
    public void testTimeTravelSnapshotSchemaThroughExternalTablePreparse() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        AnalyzeTestUtil.init();
        String createCatalog = "CREATE EXTERNAL CATALOG iceberg_tt_catalog_preparse PROPERTIES(\"type\"=\"iceberg\", " +
                "\"iceberg.catalog.hive.metastore.uris\"=\"thrift://127.0.0.1:9083\", \"iceberg.catalog.type\"=\"hive\")";
        new StarRocksAssert().withCatalog(createCatalog);

        mockedNativeTableB.newAppend().appendFile(FILE_B_1).commit();
        mockedNativeTableB.refresh();
        long s1 = mockedNativeTableB.currentSnapshot().snapshotId();
        mockedNativeTableB.manageSnapshots().createTag("tag_s1", s1).commit();
        mockedNativeTableB.updateSchema().renameColumn("k2", "k2_renamed").commit();
        mockedNativeTableB.newAppend().appendFile(FILE_B_2).commit();
        mockedNativeTableB.refresh();

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
                return mockedNativeTableB;
            }

            @Mock
            boolean tableExists(ConnectContext context, String dbName, String tableName) {
                return true;
            }
        };

        StatementBase statement =
                AnalyzeTestUtil.parseSql("SELECT * FROM iceberg_tt_catalog_preparse.db.tb VERSION AS OF 'tag_s1'");
        QueryAnalyzer queryAnalyzer = new QueryAnalyzer(AnalyzeTestUtil.getConnectContext());
        queryAnalyzer.analyzeExternalTablesOnly(statement);
        queryAnalyzer.analyze(statement);

        QueryStatement stmt = (QueryStatement) statement;
        Assertions.assertEquals(List.of("k1", "k2"), stmt.getQueryRelation().getColumnOutputNames());

        TableRelation tableRelation =
                (TableRelation) ((SelectRelation) stmt.getQueryRelation()).getRelation();
        Assertions.assertNotNull(tableRelation.getTvrVersionRange());
        Assertions.assertEquals(Optional.of(s1), tableRelation.getTvrVersionRange().end());
    }
}
