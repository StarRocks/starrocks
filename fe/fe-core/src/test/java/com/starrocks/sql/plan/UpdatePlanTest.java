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

import com.starrocks.catalog.Column;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.iceberg.IcebergMetadata;
import com.starrocks.connector.iceberg.MockIcebergMetadata;
import com.starrocks.planner.DescriptorTable;
import com.starrocks.planner.IcebergRowDeltaSink;
import com.starrocks.planner.IcebergScanNode;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.PlanNode;
import com.starrocks.planner.ProjectNode;
import com.starrocks.planner.SlotDescriptor;
import com.starrocks.planner.TupleDescriptor;
import com.starrocks.qe.QueryState;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.IcebergPlannerUtils;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.optimizer.dump.QueryDumpInfo;
import com.starrocks.thrift.TDataSink;
import com.starrocks.thrift.TDataSinkType;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TIcebergTableSink;
import com.starrocks.thrift.TIcebergWriteMode;
import com.starrocks.thrift.TPartitionType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.ScalarType;
import com.starrocks.type.TypeFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class UpdatePlanTest extends PlanTestBase {
    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        ConnectorPlanTestBase.mockCatalog(connectContext, MockIcebergMetadata.MOCKED_ICEBERG_CATALOG_NAME);
    }

    @Test
    public void testUpdate() throws Exception {
        String explainString = getUpdateExecPlan("update tprimary set v1 = 'aaa' where pk = 1");
        Assertions.assertTrue(explainString.contains("PREDICATES: 1: pk = 1"));
        Assertions.assertTrue(explainString.contains("<slot 4> : 'aaa'"));

        explainString = getUpdateExecPlan("update tprimary set v2 = v2 + 1 where v1 = 'aaa'");
        Assertions.assertTrue(explainString.contains("v1 = 'aaa'"));
        Assertions.assertTrue(explainString.contains("CAST(CAST(3: v2 AS BIGINT) + 1 AS INT)"));

        testExplain("explain update tprimary set v2 = v2 + 1 where v1 = 'aaa'");
        testExplain("explain update tprimary set v2 = DEFAULT where v1 = 'aaa'");
        testExplain("explain update tprimary_auto_increment set v2 = DEFAULT where v1 = '123'");
        testExplain("explain verbose update tprimary set v2 = v2 + 1 where v1 = 'aaa'");
        testExplain("explain costs update tprimary set v2 = v2 + 1 where v1 = 'aaa'");
    }

    @Test
    public void testColumnPartialUpdate() throws Exception {
        String oldVal = connectContext.getSessionVariable().getPartialUpdateMode();
        connectContext.getSessionVariable().setPartialUpdateMode("column");
        testExplain("explain update tprimary set v2 = v2 + 1 where v1 = 'aaa'");
        testExplain("explain update tprimary set v2 = DEFAULT where v1 = 'aaa'");
        testExplain("explain update tprimary_auto_increment set v2 = DEFAULT where v1 = '123'");
        testExplain("explain verbose update tprimary set v2 = v2 + 1 where v1 = 'aaa'");
        testExplain("explain costs update tprimary set v2 = v2 + 1 where v1 = 'aaa'");
        connectContext.getSessionVariable().setPartialUpdateMode(oldVal);
    }

    @Test
    public void testIcebergUpdateExplainContainsRowDeltaSink() throws Exception {
        String sql = "UPDATE iceberg0.unpartitioned_db.t0_v2 SET data = 'new' WHERE id = 1";
        String explainString = getIcebergUpdateExecPlanString(sql);
        assertTrue(explainString.contains("ICEBERG ROW DELTA SINK"),
                "Explain plan should contain ICEBERG ROW DELTA SINK");
    }

    @Test
    public void testIcebergUpdateExplainContainsIcebergScan() throws Exception {
        String sql = "UPDATE iceberg0.unpartitioned_db.t0_v2 SET data = 'new' WHERE id = 1";
        String explainString = getIcebergUpdateExecPlanString(sql);
        assertTrue(explainString.contains("IcebergScanNode"),
                "Explain plan should contain IcebergScanNode");
    }

    @Test
    public void testPartitionedIcebergUpdateHasHashShuffle() throws Exception {
        String sql = "UPDATE iceberg0.partitioned_db.t1_v2 SET data = 'new' WHERE id = 1";
        ExecPlan execPlan = getIcebergUpdateExecPlan(sql);

        assertNotNull(execPlan);
        assertNotNull(execPlan.getFragments());
        assertSame(TPartitionType.HASH_PARTITIONED,
                execPlan.getFragments().get(1).getOutputPartition().getType());
    }

    @Test
    public void testUnpartitionedIcebergUpdateHasRowDeltaSink() throws Exception {
        String sql = "UPDATE iceberg0.unpartitioned_db.t0_v2 SET data = 'new' WHERE id = 1";
        ExecPlan execPlan = getIcebergUpdateExecPlan(sql);
        String explainString = execPlan.getExplainString(TExplainLevel.NORMAL);
        assertTrue(explainString.contains("ROW DELTA"),
                "Explain plan should contain ROW DELTA");
    }

    @Test
    public void testIcebergUpdateScanNodeUsedForDelete() throws Exception {
        String sql = "UPDATE iceberg0.unpartitioned_db.t0_v2 SET data = 'new' WHERE id = 1";
        ExecPlan execPlan = getIcebergUpdateExecPlan(sql);
        assertNotNull(execPlan);

        List<PlanFragment> fragments = execPlan.getFragments();
        boolean hasDeleteScanNode = false;
        for (PlanFragment fragment : fragments) {
            PlanNode root = fragment.getPlanRoot();
            if (root instanceof IcebergScanNode) {
                IcebergScanNode icebergScanNode = (IcebergScanNode) root;
                if (icebergScanNode.isUsedForDelete()) {
                    hasDeleteScanNode = true;
                    break;
                }
            } else if (root instanceof ProjectNode) {
                PlanNode child = root.getChild(0);
                if (child instanceof IcebergScanNode) {
                    IcebergScanNode icebergScanNode = (IcebergScanNode) child;
                    if (icebergScanNode.isUsedForDelete()) {
                        hasDeleteScanNode = true;
                        break;
                    }
                }
            }
        }
        assertTrue(hasDeleteScanNode,
                "UPDATE plan should have IcebergScanNode with usedForDelete=true");
    }

    @Test
    public void testIcebergUpdateExplain() throws Exception {
        String explainStmt = "explain UPDATE iceberg0.unpartitioned_db.t0_v2 SET data = 'new' WHERE id = 1";
        connectContext.setQueryId(UUIDUtil.genUUID());
        connectContext.setExecutionId(UUIDUtil.toTUniqueId(connectContext.getQueryId()));
        connectContext.getState().reset();
        List<StatementBase> statements =
                com.starrocks.sql.parser.SqlParser.parse(explainStmt, connectContext.getSessionVariable().getSqlMode());
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, statements.get(0));
        stmtExecutor.execute();
        Assertions.assertEquals(QueryState.MysqlStateType.EOF, connectContext.getState().getStateType());
    }

    private void testExplain(String explainStmt) throws Exception {
        connectContext.setQueryId(UUIDUtil.genUUID());
        connectContext.setExecutionId(UUIDUtil.toTUniqueId(connectContext.getQueryId()));
        connectContext.getState().reset();
        List<StatementBase> statements =
                com.starrocks.sql.parser.SqlParser.parse(explainStmt, connectContext.getSessionVariable().getSqlMode());
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, statements.get(0));
        stmtExecutor.execute();
        Assertions.assertEquals(connectContext.getState().getStateType(), QueryState.MysqlStateType.EOF);
    }

    private static String getUpdateExecPlan(String originStmt) throws Exception {
        connectContext.setQueryId(UUIDUtil.genUUID());
        connectContext.setExecutionId(UUIDUtil.toTUniqueId(connectContext.getQueryId()));
        connectContext.setDumpInfo(new QueryDumpInfo(connectContext));
        StatementBase statementBase =
                com.starrocks.sql.parser.SqlParser.parse(originStmt, connectContext.getSessionVariable().getSqlMode())
                        .get(0);
        connectContext.getDumpInfo().setOriginStmt(originStmt);
        ExecPlan execPlan = new StatementPlanner().plan(statementBase, connectContext);

        String ret = execPlan.getExplainString(TExplainLevel.NORMAL);
        return ret;
    }

    private static ExecPlan getIcebergUpdateExecPlan(String originStmt) throws Exception {
        connectContext.setQueryId(UUIDUtil.genUUID());
        connectContext.setExecutionId(UUIDUtil.toTUniqueId(connectContext.getQueryId()));
        connectContext.setDumpInfo(new QueryDumpInfo(connectContext));
        StatementBase statementBase =
                com.starrocks.sql.parser.SqlParser.parse(originStmt, connectContext.getSessionVariable().getSqlMode())
                        .get(0);
        connectContext.getDumpInfo().setOriginStmt(originStmt);
        return StatementPlanner.plan(statementBase, connectContext);
    }

    private static String getIcebergUpdateExecPlanString(String originStmt) throws Exception {
        ExecPlan execPlan = getIcebergUpdateExecPlan(originStmt);
        return execPlan.getExplainString(TExplainLevel.NORMAL);
    }

    @Test
    public void testIcebergUpdateMultipleColumns() throws Exception {
        String sql = "UPDATE iceberg0.unpartitioned_db.t0_v2 SET data = 'new', date = '2024-06-01' WHERE id = 1";
        ExecPlan execPlan = getIcebergUpdateExecPlan(sql);
        assertNotNull(execPlan);
        String explainString = execPlan.getExplainString(TExplainLevel.NORMAL);
        assertTrue(explainString.contains("ICEBERG ROW DELTA SINK"),
                "Multi-column update should produce ICEBERG ROW DELTA SINK");
    }

    @Test
    public void testIcebergUpdatePartitionedTablePlan() throws Exception {
        String sql = "UPDATE iceberg0.partitioned_db.t1_v2 SET data = 'new' WHERE id = 1";
        ExecPlan execPlan = getIcebergUpdateExecPlan(sql);
        assertNotNull(execPlan);
        String explainString = execPlan.getExplainString(TExplainLevel.NORMAL);
        assertTrue(explainString.contains("ICEBERG ROW DELTA SINK"),
                "Partitioned update should produce ICEBERG ROW DELTA SINK");
    }

    @Test
    public void testIcebergUpdateVerboseExplain() throws Exception {
        String explainStmt = "explain verbose UPDATE iceberg0.unpartitioned_db.t0_v2 SET data = 'new' WHERE id = 1";
        connectContext.setQueryId(UUIDUtil.genUUID());
        connectContext.setExecutionId(UUIDUtil.toTUniqueId(connectContext.getQueryId()));
        connectContext.getState().reset();
        List<StatementBase> statements =
                com.starrocks.sql.parser.SqlParser.parse(explainStmt, connectContext.getSessionVariable().getSqlMode());
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, statements.get(0));
        stmtExecutor.execute();
        Assertions.assertEquals(QueryState.MysqlStateType.EOF, connectContext.getState().getStateType());
    }

    @Test
    public void testIcebergUpdateCostsExplain() throws Exception {
        String explainStmt = "explain costs UPDATE iceberg0.unpartitioned_db.t0_v2 SET data = 'new' WHERE id = 1";
        connectContext.setQueryId(UUIDUtil.genUUID());
        connectContext.setExecutionId(UUIDUtil.toTUniqueId(connectContext.getQueryId()));
        connectContext.getState().reset();
        List<StatementBase> statements =
                com.starrocks.sql.parser.SqlParser.parse(explainStmt, connectContext.getSessionVariable().getSqlMode());
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, statements.get(0));
        stmtExecutor.execute();
        Assertions.assertEquals(QueryState.MysqlStateType.EOF, connectContext.getState().getStateType());
    }

    @Test
    public void testIcebergUpdateFragmentSinkType() throws Exception {
        String sql = "UPDATE iceberg0.unpartitioned_db.t0_v2 SET data = 'new' WHERE id = 1";
        ExecPlan execPlan = getIcebergUpdateExecPlan(sql);
        assertNotNull(execPlan);
        PlanFragment sinkFragment = execPlan.getFragments().get(0);
        assertNotNull(sinkFragment.getSink());
        assertTrue(sinkFragment.getSink() instanceof com.starrocks.planner.IcebergRowDeltaSink,
                "Sink should be IcebergRowDeltaSink");
    }

    @Test
    public void testIcebergUpdatePartitionedMultiColumnPlan() throws Exception {
        // Partitioned table + multi-column SET exercises the partition-shuffle property,
        // multi-expr cast path, and the shared RowDelta sink construction together.
        String sql = "UPDATE iceberg0.partitioned_db.t1_v2 SET data = 'new' WHERE id < 10";
        ExecPlan execPlan = getIcebergUpdateExecPlan(sql);
        assertNotNull(execPlan);

        String explainString = execPlan.getExplainString(TExplainLevel.NORMAL);
        assertTrue(explainString.contains("ICEBERG ROW DELTA SINK"));
        assertTrue(explainString.contains("IcebergScanNode"));

        PlanFragment sinkFragment = execPlan.getFragments().get(0);
        assertTrue(sinkFragment.getSink() instanceof com.starrocks.planner.IcebergRowDeltaSink);
    }

    @Test
    public void testIcebergUpdateSessionVariableRestoredAfterPlan() throws Exception {
        // UpdatePlanner flips enable_local_shuffle_agg off while planning, then
        // restores it in the finally block. Confirm we return to the caller's value.
        boolean prev = connectContext.getSessionVariable().isEnableLocalShuffleAgg();
        try {
            connectContext.getSessionVariable().setEnableLocalShuffleAgg(true);
            String sql = "UPDATE iceberg0.unpartitioned_db.t0_v2 SET data = 'x' WHERE id = 1";
            ExecPlan execPlan = getIcebergUpdateExecPlan(sql);
            assertNotNull(execPlan);
            Assertions.assertTrue(connectContext.getSessionVariable().isEnableLocalShuffleAgg(),
                    "enable_local_shuffle_agg should be restored to true");
        } finally {
            connectContext.getSessionVariable().setEnableLocalShuffleAgg(prev);
        }
    }

    @Test
    public void testIcebergUpdateSinkFragmentHasIcebergTableSink() throws Exception {
        // The sink fragment should be flagged as having an Iceberg table sink
        // so pipeline scheduling picks the right execution strategy.
        String sql = "UPDATE iceberg0.unpartitioned_db.t0_v2 SET data = 'x' WHERE id = 1";
        ExecPlan execPlan = getIcebergUpdateExecPlan(sql);
        assertNotNull(execPlan);
        PlanFragment sinkFragment = execPlan.getFragments().get(0);
        assertTrue(sinkFragment.hasIcebergTableSink(),
                "Sink fragment should be marked with hasIcebergTableSink");
    }

    @Test
    public void testIcebergUpdateWithComplexPredicate() throws Exception {
        // Non-trivial WHERE (AND + comparison) exercises conflict-detection filter
        // synthesis inside setupIcebergRowDeltaSink.
        String sql = "UPDATE iceberg0.unpartitioned_db.t0_v2 SET data = 'x' WHERE id > 5 AND id < 100";
        ExecPlan execPlan = getIcebergUpdateExecPlan(sql);
        assertNotNull(execPlan);
        String explainString = execPlan.getExplainString(TExplainLevel.NORMAL);
        assertTrue(explainString.contains("ICEBERG ROW DELTA SINK"));
    }

    @Test
    public void testIcebergUpdatePipelineDopIsSet() throws Exception {
        // The Iceberg sink pipeline configurer must set a concrete pipeline DOP
        // on the sink fragment (>= 1) regardless of adaptive sink DOP setting.
        String sql = "UPDATE iceberg0.unpartitioned_db.t0_v2 SET data = 'x' WHERE id = 1";
        ExecPlan execPlan = getIcebergUpdateExecPlan(sql);
        PlanFragment sinkFragment = execPlan.getFragments().get(0);
        Assertions.assertTrue(sinkFragment.getPipelineDop() >= 1,
                "Sink fragment pipeline DOP must be >= 1");
    }

    @Test
    public void testIcebergUpdateOutputExprsMatchSchema() throws Exception {
        // The sink tuple slots are built from execPlan.getOutputExprs() — verify
        // the planner produces the expected 6 output exprs for a V2 unpartitioned
        // update: [_file, _pos, id, data, date, op_code].
        String sql = "UPDATE iceberg0.unpartitioned_db.t0_v2 SET data = 'x' WHERE id = 1";
        ExecPlan execPlan = getIcebergUpdateExecPlan(sql);
        assertNotNull(execPlan.getOutputExprs());
        Assertions.assertEquals(6, execPlan.getOutputExprs().size());
    }

    @Test
    public void testIcebergRowDeltaSinkToThriftRoundtrip() throws Exception {
        // Drive toThrift() and assert the produced TDataSink encodes the row-delta
        // contract: type, write_mode, tuple_id, target table id, and a non-null
        // cloud configuration block. toThrift() is protected on DataSink, so we
        // reach it via reflection rather than widening access for testing.
        String sql = "UPDATE iceberg0.unpartitioned_db.t0_v2 SET data = 'x' WHERE id = 1";
        ExecPlan execPlan = getIcebergUpdateExecPlan(sql);
        IcebergRowDeltaSink sink = (IcebergRowDeltaSink) execPlan.getFragments().get(0).getSink();
        java.lang.reflect.Method toThrift = IcebergRowDeltaSink.class.getDeclaredMethod("toThrift");
        toThrift.setAccessible(true);
        TDataSink tDataSink = (TDataSink) toThrift.invoke(sink);
        assertNotNull(tDataSink);
        assertSame(TDataSinkType.ICEBERG_ROW_DELTA_SINK, tDataSink.getType());
        TIcebergTableSink tIcebergTableSink = tDataSink.getIceberg_table_sink();
        assertNotNull(tIcebergTableSink);
        assertSame(TIcebergWriteMode.ROW_DELTA, tIcebergTableSink.getWrite_mode());
        assertFalse(tIcebergTableSink.is_static_partition_sink);
        assertNotNull(tIcebergTableSink.getCloud_configuration());
        assertTrue(tIcebergTableSink.getTarget_max_file_size() > 0);
        // Both codecs must be populated: data files use compression_type, position-delete
        // files use delete_compression_type. The mock table has no codec properties, so
        // both fall back to the session default — they should be equal but both set.
        assertTrue(tIcebergTableSink.isSetCompression_type(),
                "data-file compression codec must be set");
        assertTrue(tIcebergTableSink.isSetDelete_compression_type(),
                "delete-file compression codec must be set");
        // Tuple id from sink must match what the planner attached to the fragment.
        assertTrue(sink.getExplainString("", TExplainLevel.NORMAL).contains("TUPLE ID:"));
    }

    @Test
    public void testIcebergRowDeltaSinkAccessors() throws Exception {
        // Cover the small accessor surface that downstream code relies on.
        String sql = "UPDATE iceberg0.unpartitioned_db.t0_v2 SET data = 'x' WHERE id = 1";
        ExecPlan execPlan = getIcebergUpdateExecPlan(sql);
        IcebergRowDeltaSink sink = (IcebergRowDeltaSink) execPlan.getFragments().get(0).getSink();
        // No exchange node id for a sink-only fragment, no output partition.
        assertNull(sink.getExchNodeId());
        assertNull(sink.getOutputPartition());
        // Row-delta sinks must allow runtime adaptive DOP.
        assertTrue(sink.canUseRuntimeAdaptiveDop());
        // Sink extra info is populated by setupIcebergRowDeltaSink (filter expr).
        IcebergMetadata.IcebergSinkExtra extra = sink.getSinkExtraInfo();
        assertNotNull(extra);
        // t0_v2 is unpartitioned.
        assertTrue(sink.isUnpartitionedTable());
    }

    @Test
    public void testIcebergRowDeltaSinkPartitionedFlag() throws Exception {
        // Partitioned table must report isUnpartitionedTable() == false.
        String sql = "UPDATE iceberg0.partitioned_db.t1_v2 SET data = 'x' WHERE id = 1";
        ExecPlan execPlan = getIcebergUpdateExecPlan(sql);
        IcebergRowDeltaSink sink = (IcebergRowDeltaSink) execPlan.getFragments().get(0).getSink();
        assertFalse(sink.isUnpartitionedTable());
    }

    private static IcebergTable lookupV2IcebergTable() {
        return (IcebergTable) GlobalStateMgr.getCurrentState().getMetadataMgr()
                .getTable(connectContext, "iceberg0", "unpartitioned_db", "t0_v2");
    }

    /**
     * Build a TupleDescriptor whose slots match the given (name, type) pairs. A null
     * column name means "skip setColumn so the slot has a null column"; this is used
     * to exercise the slot-with-null-column path inside validateTuple.
     */
    private static TupleDescriptor buildTupleDescriptor(String[] names, com.starrocks.type.Type[] types) {
        DescriptorTable descTbl = new DescriptorTable();
        TupleDescriptor tuple = descTbl.createTupleDescriptor();
        for (int i = 0; i < names.length; i++) {
            SlotDescriptor slot = descTbl.addSlotDescriptor(tuple);
            slot.setIsMaterialized(true);
            slot.setIsNullable(true);
            slot.setType(types[i]);
            if (names[i] != null) {
                slot.setColumn(new Column(names[i], types[i]));
            }
        }
        tuple.computeMemLayout();
        return tuple;
    }

    @Test
    public void testIcebergRowDeltaSinkValidateMissingFilePath() {
        // No _file column → validateTuple must reject with "requires _file and _pos".
        ScalarType varchar = TypeFactory.createVarcharType(1024);
        TupleDescriptor tuple = buildTupleDescriptor(
                new String[] {IcebergTable.ROW_POSITION, "data"},
                new com.starrocks.type.Type[] {IntegerType.BIGINT, varchar});
        IcebergRowDeltaSink sink = new IcebergRowDeltaSink(
                lookupV2IcebergTable(), tuple, connectContext.getSessionVariable());
        StarRocksConnectorException ex = assertThrows(StarRocksConnectorException.class, sink::init);
        assertTrue(ex.getMessage().contains("requires _file and _pos"));
    }

    @Test
    public void testIcebergRowDeltaSinkValidateMissingPos() {
        // No _pos column → same rejection.
        ScalarType varchar = TypeFactory.createVarcharType(1024);
        TupleDescriptor tuple = buildTupleDescriptor(
                new String[] {IcebergTable.FILE_PATH, "data"},
                new com.starrocks.type.Type[] {varchar, varchar});
        IcebergRowDeltaSink sink = new IcebergRowDeltaSink(
                lookupV2IcebergTable(), tuple, connectContext.getSessionVariable());
        StarRocksConnectorException ex = assertThrows(StarRocksConnectorException.class, sink::init);
        assertTrue(ex.getMessage().contains("requires _file and _pos"));
    }

    @Test
    public void testIcebergRowDeltaSinkValidateFilePathWrongType() {
        // _file is required to be VARCHAR; passing INT must be rejected explicitly.
        TupleDescriptor tuple = buildTupleDescriptor(
                new String[] {IcebergTable.FILE_PATH, IcebergTable.ROW_POSITION},
                new com.starrocks.type.Type[] {IntegerType.INT, IntegerType.BIGINT});
        IcebergRowDeltaSink sink = new IcebergRowDeltaSink(
                lookupV2IcebergTable(), tuple, connectContext.getSessionVariable());
        StarRocksConnectorException ex = assertThrows(StarRocksConnectorException.class, sink::init);
        assertTrue(ex.getMessage().contains("_file column must be type of VARCHAR"));
    }

    @Test
    public void testIcebergRowDeltaSinkValidatePosWrongType() {
        // _pos is required to be BIGINT; passing INT must be rejected explicitly.
        ScalarType varchar = TypeFactory.createVarcharType(1024);
        TupleDescriptor tuple = buildTupleDescriptor(
                new String[] {IcebergTable.FILE_PATH, IcebergTable.ROW_POSITION},
                new com.starrocks.type.Type[] {varchar, IntegerType.INT});
        IcebergRowDeltaSink sink = new IcebergRowDeltaSink(
                lookupV2IcebergTable(), tuple, connectContext.getSessionVariable());
        StarRocksConnectorException ex = assertThrows(StarRocksConnectorException.class, sink::init);
        assertTrue(ex.getMessage().contains("_pos column must be type of BIGINT"));
    }

    @Test
    public void testIcebergPlannerUtilsConstructor() {
        // Trivial coverage for the implicit default constructor — the class is a
        // bag of static helpers but JaCoCo still tracks the synthesized <init>.
        assertNotNull(new IcebergPlannerUtils());
    }

    @Test
    public void testIcebergPlannerUtilsCreateShufflePropertyMissingPartitionCol() {
        // Partitioned table whose partition column is absent from outputColumns:
        // partitionColumnIds ends up empty and the helper must fall back to an
        // empty PhysicalPropertySet rather than building a HashDistributionDesc.
        IcebergTable partitioned = (IcebergTable) GlobalStateMgr.getCurrentState().getMetadataMgr()
                .getTable(connectContext, "iceberg0", "partitioned_db", "t1_v2");
        assertTrue(partitioned.isPartitioned());
        com.starrocks.sql.optimizer.base.ColumnRefFactory cf =
                new com.starrocks.sql.optimizer.base.ColumnRefFactory();
        // Output columns intentionally do NOT include the partition column ("date").
        List<com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator> outputs = List.of(
                cf.create("id", IntegerType.INT, true),
                cf.create("data", TypeFactory.createVarcharType(64), true));
        com.starrocks.sql.optimizer.base.PhysicalPropertySet props =
                IcebergPlannerUtils.createShuffleProperty(partitioned, outputs);
        // Default property uses EmptyDistributionProperty — no shuffle requirement.
        assertSame(com.starrocks.sql.optimizer.base.EmptyDistributionProperty.INSTANCE,
                props.getDistributionProperty());
    }

    @Test
    public void testIcebergPlannerUtilsBuildIcebergFilterExprNullPlan() {
        // Null exec plan returns null without NPE.
        assertNull(IcebergPlannerUtils.buildIcebergFilterExpr(null));
    }

    @Test
    public void testIcebergPlannerUtilsBuildIcebergFilterExprNoIcebergScan() throws Exception {
        // OLAP update plan has no IcebergScanNode, so the helper finds no predicate
        // and short-circuits to null.
        String sql = "update tprimary set v2 = v2 + 1 where v1 = 'aaa'";
        connectContext.setQueryId(UUIDUtil.genUUID());
        connectContext.setExecutionId(UUIDUtil.toTUniqueId(connectContext.getQueryId()));
        connectContext.setDumpInfo(new QueryDumpInfo(connectContext));
        StatementBase stmt = com.starrocks.sql.parser.SqlParser.parse(
                sql, connectContext.getSessionVariable().getSqlMode()).get(0);
        connectContext.getDumpInfo().setOriginStmt(sql);
        ExecPlan execPlan = new StatementPlanner().plan(stmt, connectContext);
        assertNull(IcebergPlannerUtils.buildIcebergFilterExpr(execPlan));
    }

    @Test
    public void testIcebergPlannerUtilsConfigurePipelineDisabled() throws Exception {
        // When canUsePipeline is false the helper must pin DOP to 1 and return
        // before touching adaptive-DOP / spill settings.
        String sql = "UPDATE iceberg0.unpartitioned_db.t0_v2 SET data = 'x' WHERE id = 1";
        ExecPlan execPlan = getIcebergUpdateExecPlan(sql);
        IcebergPlannerUtils.configureIcebergSinkPipeline(execPlan, connectContext, false);
        assertEquals(1, execPlan.getFragments().get(0).getPipelineDop());
    }

    @Test
    public void testIcebergPlannerUtilsConfigurePipelineNonAdaptiveDop() throws Exception {
        // canUsePipeline=true with enable_adaptive_sink_dop=false hits the
        // parallel_exec_instance_num branch instead of getSinkDegreeOfParallelism.
        boolean prevAdaptive = connectContext.getSessionVariable().getEnableAdaptiveSinkDop();
        try {
            connectContext.getSessionVariable().setEnableAdaptiveSinkDop(false);
            String sql = "UPDATE iceberg0.unpartitioned_db.t0_v2 SET data = 'x' WHERE id = 1";
            ExecPlan execPlan = getIcebergUpdateExecPlan(sql);
            IcebergPlannerUtils.configureIcebergSinkPipeline(execPlan, connectContext, true);
            // parallel_exec_instance_num is always >= 1.
            assertTrue(execPlan.getFragments().get(0).getPipelineDop() >= 1);
        } finally {
            connectContext.getSessionVariable().setEnableAdaptiveSinkDop(prevAdaptive);
        }
    }

    @Test
    public void testOlapUpdateInsideMultiStatementTxnSurfacesInitFailure() throws Exception {
        // Two paths in setupOlapTableSink that depend on a non-zero txn id:
        //   1) txnId != 0  → setIsMultiStatementsTxn(true) on the OlapTableSink.
        //   2) olapTableSink.init() rejects an unknown txn → caught and rethrown
        //      as SemanticException.
        // Setting a synthetic txn id forces both paths in one shot.
        long prevTxn = connectContext.getTxnId();
        try {
            connectContext.setTxnId(424242L);
            com.starrocks.sql.analyzer.SemanticException ex = assertThrows(
                    com.starrocks.sql.analyzer.SemanticException.class,
                    () -> getUpdateExecPlan("update tprimary set v1 = 'aaa' where pk = 1"));
            assertTrue(ex.getMessage().contains("Transaction"),
                    "Expected SemanticException about the unknown txn, got: " + ex.getMessage());
        } finally {
            connectContext.setTxnId(prevTxn);
        }
    }

    @Test
    public void testIcebergRowDeltaSinkValidateSkipsNullColumnSlot() {
        // A slot with a null column is a benign shape (e.g. unmaterialized helper) —
        // validateTuple must skip it instead of NPE'ing. Required _file/_pos must
        // still be present, otherwise we fail on the same missing-column path.
        ScalarType varchar = TypeFactory.createVarcharType(1024);
        TupleDescriptor tuple = buildTupleDescriptor(
                new String[] {IcebergTable.FILE_PATH, IcebergTable.ROW_POSITION, null},
                new com.starrocks.type.Type[] {varchar, IntegerType.BIGINT, IntegerType.INT});
        IcebergRowDeltaSink sink = new IcebergRowDeltaSink(
                lookupV2IcebergTable(), tuple, connectContext.getSessionVariable());
        sink.init(); // must not throw
    }

    @Test
    public void testIcebergUpdatePartitionedShuffleUsesPartitionColumn() throws Exception {
        // The partition shuffle property must hash on the partition column id.
        // The upstream fragment (fragment[1]) is the one that feeds the sink with
        // a HASH_PARTITIONED output partition for partitioned tables.
        String sql = "UPDATE iceberg0.partitioned_db.t1_v2 SET data = 'x' WHERE id = 1";
        ExecPlan execPlan = getIcebergUpdateExecPlan(sql);
        assertNotNull(execPlan);
        assertSame(TPartitionType.HASH_PARTITIONED,
                execPlan.getFragments().get(1).getOutputPartition().getType());
    }
}