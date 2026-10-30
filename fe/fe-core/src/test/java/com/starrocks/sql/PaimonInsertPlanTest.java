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

package com.starrocks.sql;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.PaimonTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.connector.CatalogConnector;
import com.starrocks.connector.ColumnTypeConverter;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.MockedMetadataMgr;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.planner.PaimonTableSink;
import com.starrocks.planner.PlanFragment;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.DmlStmt;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TPartitionType;
import org.apache.paimon.fs.Path;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PaimonInsertPlanTest extends PlanTestBase {
    private static final String CATALOG = "paimon_insert_planning";
    private static final Map<String, PaimonTable> TARGETS = new LinkedHashMap<>();
    private static MetadataMgr originalMetadata;
    private SessionVariable originalVariables;

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        GlobalStateMgr state = GlobalStateMgr.getCurrentState();
        state.getCatalogMgr().createCatalog("paimon", CATALOG, "", Map.of("type", "paimon",
                "paimon.catalog.type", "filesystem", "paimon.catalog.warehouse", "file:///unused-paimon-planning"));
        register("target", Schema.newBuilder().column("p", DataTypes.DATE()).column("id", DataTypes.INT().notNull())
                .column("value", DataTypes.STRING()).partitionKeys("p").option("bucket", "-1").build());
        register("unpartitioned", Schema.newBuilder().column("id", DataTypes.INT().notNull())
                .column("value", DataTypes.STRING()).option("bucket", "-1").build());
        register("fixed", Schema.newBuilder().column("p", DataTypes.DATE().notNull()).column("id", DataTypes.INT().notNull())
                .column("value", DataTypes.STRING()).partitionKeys("p").primaryKey("p", "id")
                .option("bucket", "4").build());
        register("bad_format", Schema.newBuilder().column("id", DataTypes.INT()).option("bucket", "-1")
                .option("file.format", "orc").build());
        register("with_default", Schema.newBuilder().column("id", DataTypes.INT().notNull())
                .column("value", DataTypes.INT(), null, "7").option("bucket", "-1").build());
        ConnectorMetadata metadata = new ConnectorMetadata() {
            @Override
            public Database getDb(ConnectContext context, String dbName) {
                return "db".equals(dbName) ? new Database(500000, dbName) : null;
            }

            @Override
            public Table getTable(ConnectContext context, String dbName, String tableName) {
                return "db".equals(dbName) ? TARGETS.get(tableName) : null;
            }

            @Override
            public List<String> listTableNames(ConnectContext context, String dbName) {
                return List.copyOf(TARGETS.keySet());
            }

            @Override
            public CloudConfiguration getCloudConfiguration() {
                return new CloudConfiguration();
            }
        };
        // Both metadata and credentials are in-memory: planning must not open a native catalog.
        state.getConnectorMgr().removeConnector(CATALOG);
        CatalogConnector connector = mock(CatalogConnector.class);
        when(connector.getMetadata()).thenReturn(metadata);
        state.getConnectorMgr().addConnector(CATALOG, connector);
        originalMetadata = state.getMetadataMgr();
        MockedMetadataMgr metadataMgr = new MockedMetadataMgr(state.getLocalMetastore(), state.getConnectorMgr());
        metadataMgr.registerMockedMetadata(CATALOG, metadata);
        state.setMetadataMgr(metadataMgr);
    }

    @AfterAll
    public static void restoreMetadata() throws Exception {
        GlobalStateMgr.getCurrentState().setMetadataMgr(originalMetadata);
        starRocksAssert.dropCatalog(CATALOG);
    }

    @BeforeEach
    public void saveSessionVariables() {
        originalVariables = connectContext.getSessionVariable();
        SessionVariable variables = (SessionVariable) originalVariables.clone();
        variables.setEnablePipelineEngine(true);
        variables.setEnableAdaptiveSinkDop(false);
        variables.setParallelExecInstanceNum(4);
        variables.setEnableInsertSelectExternalAutoRefresh(false);
        variables.setDynamicOverwrite(false);
        connectContext.setSessionVariable(variables);
    }

    @AfterEach
    public void restoreSessionVariables() {
        connectContext.setSessionVariable(originalVariables);
        connectContext.setTxnId(0);
        connectContext.getPreResolvedState().clear();
    }

    @Test
    public void testValuesAndSelectDoNotBeginNativeTransaction() throws Exception {
        Planned values = plan("insert into " + target("target") + " values ('2026-10-29', 1, 'a')");
        Assertions.assertEquals(DmlStmt.INVALID_TXN_ID, values.statement().getTxnId());
        Assertions.assertEquals("APPEND", values.sink().getPaimonSinkExtra().getOverwriteMode());
        Assertions.assertEquals(3, values.plan().getOutputExprs().size());
        Assertions.assertEquals(connectContext.getQueryId().toString().replace('-', '_'),
                values.sink().getPaimonSinkExtra().getOperationId());
        Assertions.assertFalse(values.sink().getTupleDescriptor().getSlots().get(1).getIsNullable());
        Assertions.assertFalse(values.sink().getPaimonSinkExtra().getOptions().isEmpty());
        Planned select = plan("insert into " + target("target")
                + " select cast('2026-10-29' as date), v1, cast(v2 as varchar) from t0");
        Assertions.assertEquals(DmlStmt.INVALID_TXN_ID, select.statement().getTxnId());
        Assertions.assertEquals(4, select.plan().getTopFragment().getPipelineDop());
        Assertions.assertTrue(select.plan().getTopFragment().hasPaimonTableSink());
        Assertions.assertFalse(select.plan().getTopFragment().canUseRuntimeAdaptiveDop());
    }

    @Test
    public void testByNameAndNativeOrderForStaticPartitions() throws Exception {
        Planned byName = plan("insert into " + target("target")
                + " by name select 'a' as value, 1 as id, cast('2026-10-29' as date) as p");
        Assertions.assertEquals(List.of("p", "id", "value"), byName.sink().getPaimonSinkExtra().getColumnNames());
        Planned partition = plan("insert into " + target("target") + " partition(p='2026-10-29') values (7, 'a')");
        Assertions.assertEquals(List.of("id", "value"), partition.statement().getTargetColumnNames());
        Assertions.assertEquals(3, partition.plan().getOutputExprs().size());
        Assertions.assertTrue(partition.plan().getOutputExprs().get(0).getType().isDate());
        Assertions.assertTrue(partition.plan().getOutputExprs().get(1).getType().isInt());
        Assertions.assertTrue(partition.plan().getOutputExprs().get(2).getType().isStringType());
        Assertions.assertEquals(Map.of("p", "2026-10-29"), partition.sink().getPaimonSinkExtra().getStaticPartition());
        Planned permutation = plan("insert into " + target("target")
                + " partition(p='2026-10-29') (value, id) values ('a', 9)");
        Assertions.assertTrue(permutation.plan().getOutputExprs().get(0).getType().isDate());
        Assertions.assertTrue(permutation.plan().getOutputExprs().get(1).getType().isInt());
    }

    @Test
    public void testNullableStaticPartitionUsesNativeMarkerOnlyInCommitTarget() throws Exception {
        Planned partition = plan("insert into " + target("target") + " partition(p=NULL) values (1, 'a')");
        Assertions.assertEquals(Map.of("p", "__DEFAULT_PARTITION__"),
                partition.sink().getPaimonSinkExtra().getStaticPartition());
        Assertions.assertTrue(partition.plan().getOutputExprs().get(0).getType().isDate());
        Assertions.assertTrue(partition.plan().getOutputExprs().get(0).isNullable());
    }

    @Test
    public void testOverwriteModesAndEmptyStaticInput() throws Exception {
        Assertions.assertEquals("FULL", plan("insert overwrite " + target("unpartitioned")
                + " select 1, 'a' where false").sink().getPaimonSinkExtra().getOverwriteMode());
        Assertions.assertEquals("STATIC", plan("insert overwrite " + target("target")
                + " partition(p='2026-10-29') select 1, 'a' where false")
                .sink().getPaimonSinkExtra().getOverwriteMode());
        connectContext.getSessionVariable().setDynamicOverwrite(true);
        Assertions.assertEquals("DYNAMIC", plan("insert overwrite " + target("target")
                + " select cast('2026-10-29' as date), 1, 'a'").sink().getPaimonSinkExtra().getOverwriteMode());
        Assertions.assertEquals("FULL", plan("insert overwrite " + target("unpartitioned")
                + " values(1, 'a')").sink().getPaimonSinkExtra().getOverwriteMode());
    }

    @Test
    public void testFixedBucketsKeepParallelNativeOwnerRouting() throws Exception {
        Planned fixed = plan("insert into " + target("fixed")
                + " select cast('2026-10-29' as date), v1, cast(v2 as varchar) from t0 limit 5");
        PlanFragment fragment = fixed.plan().getTopFragment();
        Assertions.assertEquals(4, fragment.getPipelineDop());
        Assertions.assertEquals(TPartitionType.HASH_PARTITIONED, fragment.getDataPartition().getType());
        Assertions.assertEquals(2, fragment.getDataPartition().getPartitionExprs().size());
        Assertions.assertTrue(fixed.sink().getThriftTableSink().isBucket_routed());
        Assertions.assertTrue(fixed.sink().getThriftTableSink().getRouting_bucket_slot_id() >= 0);
        Assertions.assertTrue(fixed.plan().getExplainString(TExplainLevel.NORMAL).contains("BUCKET ROUTING: NATIVE"));
        Assertions.assertEquals(3, fixed.plan().getOutputExprs().size());
    }

    @Test
    public void testCapabilitiesAndSchemaFailuresAreSemanticErrors() {
        assertAnalysisError("insert into " + target("bad_format") + " values (1)", "Parquet");
        assertAnalysisError("insert into " + target("target") + " (value) values ('a')", "Paimon column 'id'");
        assertAnalysisError("insert into " + target("target") + " partition(unknown='x') values (1, 'a')",
                "Can't find partition column");
        assertAnalysisError("insert into " + target("fixed") + " partition(p=NULL) values (1, 'a')", "does not allow NULL");
        assertAnalysisError("insert into " + target("target") + " partition(p='2026-10-29') values (1)",
                "column count");
        assertAnalysisError("insert into " + target("target") + " (missing) values (1)", "Unknown column");
        assertAnalysisError("insert into " + target("target") + " by name select 1 as id, 2 as id", "Duplicate");
        connectContext.setTxnId(1234);
        assertAnalysisError("insert into " + target("target") + " values ('2026-10-29', 1, 'a')", "explicit transaction");
    }

    @Test
    public void testNativeDefaultIsNotSilentlyReplacedWithNull() throws Exception {
        assertAnalysisError("insert into " + target("with_default") + " (id) values (1)",
                "Paimon native default values are not supported");
        assertAnalysisError("insert into " + target("with_default") + " values (1, default)",
                "Paimon native default values are not supported");
        Assertions.assertEquals(2, plan("insert into " + target("with_default") + " values (1, 9)")
                .plan().getOutputExprs().size());
    }

    @Test
    public void testIdentityPrepassDoesNotDependOnGenericRefreshFlags() throws Exception {
        boolean preparse = Config.enable_experimental_external_table_preparse;
        PaimonTable table = TARGETS.get("target");
        FileStoreTable nativeTable = (FileStoreTable) table.getNativeTable();
        table.setPaimonNativeTable(nativeTable);
        clearInvocations(nativeTable);
        Config.enable_experimental_external_table_preparse = false;
        try {
            Planned planned = plan("insert into " + target("target") + " values ('2026-10-29', 1, 'a')");
            Assertions.assertEquals("native-target", planned.sink().getPaimonSinkExtra().getTableIdentity());
            verify(nativeTable).uuid();
        } finally {
            Config.enable_experimental_external_table_preparse = preparse;
            table.resolveNativeWriteIdentity();
        }
    }

    @Test
    public void testWriteIdentityIsNotReloadedInPlanner() throws Exception {
        FileStoreTable nativeTable = (FileStoreTable) TARGETS.get("target").getNativeTable();
        clearInvocations(nativeTable);
        plan("insert into " + target("target") + " values ('2026-10-29', 1, 'a')");
        verify(nativeTable, never()).uuid();
    }

    private static void register(String name, Schema schema) {
        TableSchema tableSchema = TableSchema.create(5, schema);
        FileStoreTable nativeTable = mock(FileStoreTable.class);
        when(nativeTable.schema()).thenReturn(tableSchema);
        when(nativeTable.rowType()).thenReturn(tableSchema.logicalRowType());
        when(nativeTable.options()).thenReturn(schema.options());
        when(nativeTable.partitionKeys()).thenReturn(tableSchema.partitionKeys());
        when(nativeTable.primaryKeys()).thenReturn(tableSchema.primaryKeys());
        when(nativeTable.uuid()).thenReturn("native-" + name);
        when(nativeTable.location()).thenReturn(new Path("file:///paimon-planning/" + name));
        PaimonTable table = new PaimonTable(CATALOG, "db", name,
                ColumnTypeConverter.fromPaimonSchemas(schema.fields()), nativeTable);
        table.resolveNativeWriteIdentity();
        TARGETS.put(name, table);
    }

    private static String target(String table) {
        return CATALOG + ".db." + table;
    }

    private static Planned plan(String sql) throws Exception {
        connectContext.setQueryId(com.starrocks.common.util.UUIDUtil.genUUID());
        connectContext.setExecutionId(com.starrocks.common.util.UUIDUtil.toTUniqueId(connectContext.getQueryId()));
        InsertStmt statement = (InsertStmt) SqlParser.parse(sql, connectContext.getSessionVariable()).get(0);
        ExecPlan plan = StatementPlanner.plan(statement, connectContext);
        return new Planned(statement, plan, (PaimonTableSink) plan.getTopFragment().getSink());
    }

    private static void assertAnalysisError(String sql, String expected) {
        InsertStmt statement = (InsertStmt) SqlParser.parse(sql, connectContext.getSessionVariable()).get(0);
        SemanticException error = Assertions.assertThrows(SemanticException.class,
                () -> Analyzer.analyze(statement, connectContext));
        Assertions.assertTrue(error.getMessage().contains(expected), error.getMessage());
    }

    private record Planned(InsertStmt statement, ExecPlan plan, PaimonTableSink sink) {
    }
}
