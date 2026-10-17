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

package com.starrocks.sql.plan;

import com.starrocks.catalog.PaimonTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.DdlException;
import com.starrocks.common.tvr.TvrVersionRange;
import com.starrocks.connector.CatalogConnectorMetadata;
import com.starrocks.connector.GetRemoteFilesParams;
import com.starrocks.connector.MockedMetadataMgr;
import com.starrocks.connector.index.ConnectorIndexDescriptor;
import com.starrocks.connector.index.ConnectorIndexDescriptor.Coverage;
import com.starrocks.connector.index.ConnectorIndexDescriptor.IndexType;
import com.starrocks.connector.index.ConnectorIndexDescriptor.Operation;
import com.starrocks.connector.index.ConnectorIndexMetadata;
import com.starrocks.connector.index.ConnectorIndexMetadata.TableType;
import com.starrocks.connector.index.ConnectorIndexShard;
import com.starrocks.connector.index.IndexCondition;
import com.starrocks.connector.index.IndexTable;
import com.starrocks.connector.informationschema.InformationSchemaMetadata;
import com.starrocks.connector.metadata.TableMetaMetadata;
import com.starrocks.connector.paimon.PaimonGlobalIndexRequest;
import com.starrocks.connector.paimon.PaimonGlobalIndexService;
import com.starrocks.connector.paimon.PaimonMetadata;
import com.starrocks.planner.PaimonIndexScanNode;
import com.starrocks.planner.SlotDescriptor;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.SimpleExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TExprNodeType;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.thrift.TResultBatch;
import com.starrocks.thrift.TResultSinkType;
import com.starrocks.type.VarcharType;
import mockit.Mock;
import mockit.MockUp;
import org.apache.paimon.globalindex.GlobalIndexResult;
import org.apache.paimon.globalindex.GlobalIndexResultSerializer;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.RoaringNavigableMap64;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

public class PaimonGlobalIndexPlanTest extends ConnectorPlanTestBase {
    private static final Map<String, String> APPROXIMATE_SCORE_ORDERS = Map.of(
            "approx_l2_distance", "ASC",
            "approx_cosine_similarity", "DESC",
            "approx_inner_product", "DESC");
    private static PaimonMetadata indexedMetadata;
    private static AtomicInteger indexMetadataLoads;

    @BeforeAll
    public static void installIndexMetadataWrapper() {
        MockedMetadataMgr metadataMgr = (MockedMetadataMgr) GlobalStateMgr.getCurrentState().getMetadataMgr();
        PaimonMetadata paimonMetadata = (PaimonMetadata) metadataMgr
                .getOptionalMetadata(MOCK_PAIMON_CATALOG_NAME).orElseThrow();
        indexedMetadata = spy(paimonMetadata);
        indexMetadataLoads = new AtomicInteger();
        doAnswer(invocation -> {
            indexMetadataLoads.incrementAndGet();
            Table table = invocation.getArgument(0);
            TvrVersionRange versionRange = invocation.getArgument(1);
            long snapshotId = versionRange == null ? ConnectorIndexMetadata.UNKNOWN_SNAPSHOT_ID
                    : versionRange.end().orElse(ConnectorIndexMetadata.UNKNOWN_SNAPSHOT_ID);
            if ("vector_table".equals(table.getName())) {
                ConnectorIndexDescriptor descriptor = new ConnectorIndexDescriptor(
                        IndexType.RANGE, "btree", 0, "pk", Map.of(),
                        Set.of(Operation.EQUAL), Coverage.UNKNOWN);
                return ConnectorIndexMetadata.of(snapshotId, TableType.DATA_EVOLUTION,
                        List.of(descriptor), Map.of("pk", 0, "embedding", 1), Set.of());
            }
            ConnectorIndexDescriptor descriptor = new ConnectorIndexDescriptor(
                    IndexType.BITMAP, "bitmap", 0, "pk", Map.of(),
                    Set.of(Operation.EQUAL), Coverage.UNKNOWN);
            return ConnectorIndexMetadata.of(snapshotId, TableType.DATA_EVOLUTION,
                    List.of(descriptor));
        }).when(indexedMetadata).getIndexMetadata(any(Table.class), any(TvrVersionRange.class));

        // Match the production connector path: MetadataMgr returns CatalogConnectorMetadata,
        // which must delegate index metadata to the Paimon provider selected for the table.
        metadataMgr.registerMockedMetadata(MOCK_PAIMON_CATALOG_NAME, new CatalogConnectorMetadata(
                indexedMetadata,
                new InformationSchemaMetadata(MOCK_PAIMON_CATALOG_NAME),
                new TableMetaMetadata(MOCK_PAIMON_CATALOG_NAME, "paimon")));
        try {
            connectContext.changeCatalogDb(MOCK_PAIMON_CATALOG_NAME + ".pmn_db1");
        } catch (DdlException e) {
            throw new RuntimeException(e);
        }
    }

    @BeforeEach
    public void resetIndexMetadataTracking() {
        clearInvocations(indexedMetadata);
        indexMetadataLoads.set(0);
        connectContext.getSessionVariable().setPaimonGlobalIndexScanStage(1);
    }

    @Test
    public void testStageZeroSkipsIndexDiscovery() throws Exception {
        connectContext.getSessionVariable().setPaimonGlobalIndexScanStage(0);
        try {
            String plan = getLogicalFragmentPlan("SELECT pk FROM unpartitioned_table WHERE pk = '1'");
            Assertions.assertFalse(plan.contains("index["));
            Assertions.assertEquals(0, indexMetadataLoads.get());
        } finally {
            connectContext.getSessionVariable().setPaimonGlobalIndexScanStage(1);
        }
    }

    @Test
    public void testJoinStatisticsRespectGlobalIndexScanStage() throws Exception {
        AtomicBoolean computingStatistics = new AtomicBoolean();
        List<Boolean> statisticsIndexDisabled = new ArrayList<>();
        boolean columnStatistics = connectContext.getSessionVariable().enablePaimonColumnStatistics();
        doAnswer(invocation -> {
            computingStatistics.set(true);
            try {
                return invocation.callRealMethod();
            } finally {
                computingStatistics.set(false);
            }
        }).when(indexedMetadata).getTableStatistics(any(), any(), anyMap(), any(), any(), anyLong(), any());
        doAnswer(invocation -> {
            if (computingStatistics.get()) {
                GetRemoteFilesParams params = invocation.getArgument(1);
                statisticsIndexDisabled.add(params.isDisableGlobalIndex());
            }
            return invocation.callRealMethod();
        }).when(indexedMetadata).getRemoteFiles(any(), any());
        try {
            connectContext.getSessionVariable().setEnablePaimonColumnStatistics(false);
            for (int stage : List.of(0, 1, 2)) {
                statisticsIndexDisabled.clear();
                connectContext.getSessionVariable().setPaimonGlobalIndexScanStage(stage);
                getFragmentPlan("SELECT a.pk, b.d FROM unpartitioned_table a "
                        + "JOIN partitioned_table b ON a.pk = b.pk WHERE a.pk = '1'");
                Assertions.assertTrue(statisticsIndexDisabled.size() >= 2,
                        "both join inputs must go through MetadataMgr statistics planning");
                Assertions.assertTrue(statisticsIndexDisabled.stream().allMatch(disabled -> disabled == (stage != 1)));
            }
        } finally {
            doCallRealMethod().when(indexedMetadata)
                    .getTableStatistics(any(), any(), anyMap(), any(), any(), anyLong(), any());
            doCallRealMethod().when(indexedMetadata).getRemoteFiles(any(), any());
            connectContext.getSessionVariable().setEnablePaimonColumnStatistics(columnStatistics);
            connectContext.getSessionVariable().setPaimonGlobalIndexScanStage(1);
        }
    }

    @Test
    public void testScalarExecutionContractThroughMetadataMgr() throws Exception {
        connectContext.getSessionVariable().setPaimonGlobalIndexScanStage(2);
        AtomicInteger evaluations = new AtomicInteger();
        doAnswer(invocation -> {
            com.starrocks.connector.index.IndexCondition condition = invocation.getArgument(1);
            Assertions.assertFalse(condition.getFieldIds().isEmpty());
            Assertions.assertTrue(condition.getFieldIds().containsValue(0));
            evaluations.incrementAndGet();
            return java.util.Optional.empty();
        }).when(indexedMetadata).evaluateIndex(any(Table.class),
                any(com.starrocks.connector.index.IndexCondition.class), any(TvrVersionRange.class));
        try {
            String plan = getFragmentPlan("SELECT pk FROM unpartitioned_table WHERE pk = '1'");
            assertContains(plan, "PREDICATES:");
            Assertions.assertEquals(1, evaluations.get());
        } finally {
            connectContext.getSessionVariable().setPaimonGlobalIndexScanStage(1);
        }
    }

    @Test
    public void testPredicateIndexPlanningThroughMetadataMgr() throws Exception {
        String plan = getLogicalFragmentPlan(
                "SELECT pk FROM unpartitioned_table WHERE pk = '1'");

        assertContains(plan, "index[");
        verify(indexedMetadata, atLeastOnce()).getIndexMetadata(
                any(Table.class), any(TvrVersionRange.class));
        Assertions.assertTrue(indexMetadataLoads.get() > 0);
    }

    @Test
    public void testVirtualIndexSqlPlansDistributedShards() throws Exception {
        PaimonTable table = (PaimonTable) indexedMetadata.getTable(
                connectContext, "pmn_db1", "unpartitioned_table");
        PaimonGlobalIndexRequest request = createVirtualIndexRequest(table);
        doAnswer(invocation -> List.of(new ConnectorIndexShard(0, 4), new ConnectorIndexShard(5, 9)))
                .when(indexedMetadata).getIndexShards(any(Table.class), anyLong(), anyMap());
        try {
            // COUNT and args-only projections prune index_result, exercising the scan-predicate
            // getter/setter used by PruneHDFSScanColumnRule rather than only the two-column path.
            for (String projection : List.of("to_base64(index_result)", "count(*)", "args")) {
                String plan = getFragmentPlan("SELECT " + projection + " FROM "
                        + "`unpartitioned_table$global_index` WHERE args = '" + request.toTransportString() + "'");
                assertContains(plan, "TABLE: unpartitioned_table$global_index",
                        "SNAPSHOT: " + request.getSnapshotId(), "SHARDS: 2");
            }

            SessionVariable defaults = GlobalStateMgr.getCurrentState().getVariableMgr().getDefaultSessionVariable();
            long globalLimit = defaults.getSqlSelectLimit();
            long callerLimit = connectContext.getSessionVariable().getSqlSelectLimit();
            String emptyBitmap = Base64.getEncoder().encodeToString(new GlobalIndexResultSerializer()
                    .serialize(GlobalIndexResult.create(new RoaringNavigableMap64())));
            new MockUp<SimpleExecutor>() {
                @Mock
                public List<TResultBatch> executeDQL(String sql, ConnectContext context) {
                    Assertions.assertSame(connectContext, context.getParent());
                    ExecPlan plan = StatementPlanner.plan(SqlParser.parseOneWithStarRocksDialect(sql,
                            context.getSessionVariable()), context, TResultSinkType.HTTP_PROTOCAL);
                    String explain = plan.getExplainString(TExplainLevel.NORMAL);
                    assertContains(explain, "TABLE: unpartitioned_table$global_index", "SHARDS: 2");
                    Assertions.assertFalse(explain.contains("limit:"), explain);
                    TResultBatch batch = new TResultBatch();
                    batch.addToRows(ByteBuffer.wrap(("{\"data\":[\"" + emptyBitmap + "\"]}")
                            .getBytes(StandardCharsets.UTF_8)));
                    return List.of(batch);
                }
            };
            try {
                defaults.setSqlSelectLimit(1);
                for (long limit : List.of(0L, 1L)) {
                    connectContext.getSessionVariable().setSqlSelectLimit(limit);
                    // Exercise the production service's explicit complete-result policy, not a
                    // separately constructed executor that could hide a missing call-site override.
                    Assertions.assertEquals(0L, new PaimonGlobalIndexService(table, request)
                            .evaluate().getResult().results().getLongCardinality());
                    Assertions.assertEquals(limit, connectContext.getSessionVariable().getSqlSelectLimit());
                    Assertions.assertEquals(1, defaults.getSqlSelectLimit());
                }
            } finally {
                defaults.setSqlSelectLimit(globalLimit);
                connectContext.getSessionVariable().setSqlSelectLimit(callerLimit);
                connectContext.setThreadLocalInfo();
            }
        } finally {
            org.mockito.Mockito.doCallRealMethod().when(indexedMetadata)
                    .getIndexShards(any(Table.class), anyLong(), anyMap());
        }
    }

    @Test
    public void testVirtualIndexResultPredicatesRunAboveScan() throws Exception {
        PaimonTable table = (PaimonTable) indexedMetadata.getTable(
                connectContext, "pmn_db1", "unpartitioned_table");
        String transport = createVirtualIndexRequest(table).toTransportString();
        doAnswer(invocation -> List.of(new ConnectorIndexShard(0, 4), new ConnectorIndexShard(5, 9)))
                .when(indexedMetadata).getIndexShards(any(Table.class), anyLong(), anyMap());
        try {
            for (String projection : List.of("to_base64(index_result)", "count(*)", "args")) {
                for (String limit : List.of("", " LIMIT 1")) {
                    ExecPlan plan = getExecPlan("SELECT " + projection + " FROM `unpartitioned_table$global_index` "
                            + "WHERE args = '" + transport + "' AND index_result IS NULL" + limit);
                    String explain = plan.getExplainString(TExplainLevel.NORMAL);
                    assertContains(explain, "SELECT", "index_result IS NULL", "SHARDS: 2");
                    Assertions.assertEquals(1, plan.getScanNodes().size());
                    PaimonIndexScanNode scan = (PaimonIndexScanNode) plan.getScanNodes().get(0);
                    SlotDescriptor resultSlot = scan.getDesc().getSlots().stream()
                            .filter(slot -> IndexTable.INDEX_RESULT_COLUMN_NAME.equals(slot.getColumn().getName()))
                            .findFirst().orElseThrow();
                    Assertions.assertTrue(resultSlot.isMaterialized());
                    Assertions.assertTrue(resultSlot.toThrift().isIsMaterialized());

                    List<TPlanNode> nodes = scan.getFragment().getPlanRoot().treeToThrift().getNodes();
                    List<TPlanNode> selects = nodes.stream()
                            .filter(node -> node.getNode_type() == TPlanNodeType.SELECT_NODE).toList();
                    Assertions.assertEquals(1, selects.size(), explain);
                    TPlanNode select = selects.get(0);
                    TPlanNode scanThrift = nodes.get(nodes.indexOf(select) + 1);
                    Assertions.assertEquals(1, select.getNum_children());
                    Assertions.assertEquals(TPlanNodeType.HDFS_SCAN_NODE, scanThrift.getNode_type());
                    Assertions.assertEquals(scan.getId().asInt(), scanThrift.getNode_id());
                    Assertions.assertEquals(-1, scanThrift.getLimit(), "LIMIT must not precede the result filter");
                    Assertions.assertEquals(0, scanThrift.getConjunctsSize(), "args must not become a BE conjunct");
                    Assertions.assertEquals(1, select.getConjunctsSize());
                    Assertions.assertEquals("is_null_pred", select.getConjuncts().get(0).getNodes().get(0)
                            .getFn().getName().getFunction_name());
                    Assertions.assertEquals(List.of(resultSlot.getId().asInt()),
                            select.getConjuncts().get(0).getNodes().stream()
                                    .filter(node -> node.getNode_type() == TExprNodeType.SLOT_REF)
                                    .map(node -> node.getSlot_ref().getSlot_id()).toList());
                    if (!"count(*)".equals(projection)) {
                        Assertions.assertEquals(limit.isEmpty() ? -1 : 1, select.getLimit(), explain);
                    }
                }
            }

            ExecPlan argsOnlyPlan = getExecPlan("SELECT args FROM `unpartitioned_table$global_index` "
                    + "WHERE '" + transport + "' = args LIMIT 1");
            PaimonIndexScanNode scan = (PaimonIndexScanNode) argsOnlyPlan.getScanNodes().get(0);
            Assertions.assertEquals(1, scan.getLimit());
            Assertions.assertEquals(2, scan.getScanRangeLocations(0L).size());
            Assertions.assertTrue(scan.getFragment().getPlanRoot().treeToThrift().getNodes().stream()
                    .noneMatch(node -> node.getNode_type() == TPlanNodeType.SELECT_NODE));
        } finally {
            doCallRealMethod().when(indexedMetadata).getIndexShards(any(Table.class), anyLong(), anyMap());
        }
    }

    @Test
    public void testVirtualIndexRejectsUnsupportedArgsPredicates() {
        for (String predicate : List.of("args <> 'request'", "args = ''", "args IS NULL",
                "args = to_base64(index_result)", "args = 'request' OR index_result IS NULL")) {
            Exception exception = Assertions.assertThrows(Exception.class,
                    () -> getExecPlan("SELECT args FROM `unpartitioned_table$global_index` WHERE " + predicate));
            Assertions.assertTrue(exception.getMessage().contains("exactly one non-empty string"),
                    exception.toString());
        }
        verify(indexedMetadata, never()).getIndexShards(any(Table.class), anyLong(), anyMap());
    }

    private static PaimonGlobalIndexRequest createVirtualIndexRequest(PaimonTable table) {
        FileStoreTable nativeTable = (FileStoreTable) table.getNativeTable();
        long snapshotId = nativeTable.latestSnapshot().orElseThrow().id();
        ColumnRefOperator column = new ColumnRefOperator(1, VarcharType.VARCHAR, "pk", true);
        IndexCondition condition = new IndexCondition(new BinaryPredicateOperator(
                BinaryType.EQ, column, ConstantOperator.createVarchar("1")), Map.of(1, 0));
        return PaimonGlobalIndexRequest.create(nativeTable.location().toString(), "main", snapshotId,
                nativeTable.schema().id(), nativeTable.schema().timeMillis(), condition);
    }

    @Test
    public void testUnsupportedPredicateSkipsMetadataIo() throws Exception {
        String plan = getLogicalFragmentPlan(
                "SELECT pk FROM unpartitioned_table WHERE length(pk) = 1");

        Assertions.assertFalse(plan.contains("index["));
        verify(indexedMetadata, never()).getIndexMetadata(
                any(Table.class), any(TvrVersionRange.class));
        Assertions.assertEquals(0, indexMetadataLoads.get());
    }

    @Test
    public void testPartitionOnlyPredicateSkipsMetadataIo() throws Exception {
        String plan = getLogicalFragmentPlan(
                "SELECT pk FROM partitioned_table WHERE pt >= DATE '2000-01-01'");

        Assertions.assertFalse(plan.contains("index["));
        verify(indexedMetadata, never()).getIndexMetadata(
                any(Table.class), any(TvrVersionRange.class));
        Assertions.assertEquals(0, indexMetadataLoads.get());
    }

    @Test
    public void testPartitionAndScalarPredicateStillLoadsMetadata() throws Exception {
        String plan = getLogicalFragmentPlan(
                "SELECT pk FROM partitioned_table WHERE pt >= DATE '2000-01-01' AND pk = '1'");

        assertContains(plan, "index[");
        verify(indexedMetadata, atLeastOnce()).getIndexMetadata(
                any(Table.class), any(TvrVersionRange.class));
        Assertions.assertTrue(indexMetadataLoads.get() > 0);
    }

    @Test
    public void testApproximateTopNUsesOrdinaryScanWithoutIndexDiscovery() throws Exception {
        try {
            for (int stage : List.of(0, 1, 2)) {
                connectContext.getSessionVariable().setPaimonGlobalIndexScanStage(stage);
                for (Map.Entry<String, String> score : APPROXIMATE_SCORE_ORDERS.entrySet()) {
                    String sql = "SELECT pk, " + score.getKey() + "([1.0, 2.0], embedding) AS score "
                            + "FROM vector_table ORDER BY score " + score.getValue() + " LIMIT 10";
                    String plan = getLogicalFragmentPlan(sql);
                    assertContains(plan, "PAIMON SCAN", "TOP-N");
                    Assertions.assertFalse(plan.contains("index["), plan);
                    assertContains(getFragmentPlan(sql), score.getKey(), "TOP-N");
                }
            }
            verify(indexedMetadata, never()).getIndexMetadata(
                    any(Table.class), any(TvrVersionRange.class));
            Assertions.assertEquals(0, indexMetadataLoads.get());
        } finally {
            connectContext.getSessionVariable().setPaimonGlobalIndexScanStage(1);
        }
    }

    @Test
    public void testBoundVectorCastUsesOrdinaryScanWithoutIndexDiscovery() throws Exception {
        // This is the SQL expression shape after binding a prepared vector parameter.
        String sql = "SELECT pk, approx_l2_distance(CAST('[ 1e0, 2.0 ]' AS ARRAY<FLOAT>), embedding) AS score "
                + "FROM vector_table ORDER BY score ASC LIMIT 10";
        String plan = getLogicalFragmentPlan(sql);

        assertContains(plan, "PAIMON SCAN", "TOP-N");
        Assertions.assertFalse(plan.contains("index["), plan);
        assertContains(getFragmentPlan(sql), "approx_l2_distance", "TOP-N");
        verify(indexedMetadata, never()).getIndexMetadata(
                any(Table.class), any(TvrVersionRange.class));
    }

    @Test
    public void testApproximateTopNPreservesIndependentScalarIndexPlanning() throws Exception {
        try {
            for (int stage : List.of(1, 2)) {
                connectContext.getSessionVariable().setPaimonGlobalIndexScanStage(stage);
                for (Map.Entry<String, String> score : APPROXIMATE_SCORE_ORDERS.entrySet()) {
                    String sql = "SELECT pk, " + score.getKey() + "([1.0, 2.0], embedding) AS score "
                            + "FROM vector_table WHERE pk = 1 ORDER BY score " + score.getValue() + " LIMIT 10";
                    String plan = getLogicalFragmentPlan(sql);
                    assertContains(plan, "PAIMON SCAN", "TOP-N", "index[predicate=");
                    int start = plan.indexOf("index[");
                    String indexRequest = plan.substring(start, plan.indexOf(']', start) + 1);
                    assertContains(indexRequest, "pk = 1");
                    Assertions.assertFalse(indexRequest.contains("approx_"), indexRequest);
                    Assertions.assertFalse(plan.contains("_INDEX_SCORE"), plan);
                    assertContains(getFragmentPlan(sql), score.getKey(), "PREDICATES:", "TOP-N");
                }
            }
            verify(indexedMetadata, atLeastOnce()).getIndexMetadata(
                    any(Table.class), any(TvrVersionRange.class));
            Assertions.assertTrue(indexMetadataLoads.get() > 0);
        } finally {
            connectContext.getSessionVariable().setPaimonGlobalIndexScanStage(1);
        }
    }
}
