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

package com.starrocks.planner;

import com.staros.proto.PlacementPolicy;
import com.starrocks.catalog.ColocateRange;
import com.starrocks.catalog.ColocateTableIndex;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Tuple;
import com.starrocks.catalog.Variant;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.FeConstants;
import com.starrocks.common.Range;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DefaultCoordinator;
import com.starrocks.qe.PrepareStmtContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.SetExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.PrepareStmtPlanner;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.SetStmtAnalyzer;
import com.starrocks.sql.ast.DeallocateStmt;
import com.starrocks.sql.ast.ExecuteStmt;
import com.starrocks.sql.ast.PrepareStmt;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SetCatalogStmt;
import com.starrocks.sql.ast.SetStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.SystemVariable;
import com.starrocks.sql.ast.UseCatalogStmt;
import com.starrocks.sql.ast.UseDbStmt;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.ast.warehouse.SetWarehouseStmt;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.base.DistributionSpec;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.RangeScanExecutionValidator;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.type.IntegerType;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.function.Consumer;

/** Exercises the planner contract using real lake tablets in two aligned ranges and two partitions. */
public class RangeParallelScanPlanTest {
    private static final String DB = "range_parallel_scan";
    private static final String TWO_PARTITIONS =
            "partition by range(k2) (partition p1 values less than ('1000'), partition p2 values less than ('2000')) ";

    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;
    private static boolean savedEnableRangeDistribution;

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        savedEnableRangeDistribution = Config.enable_range_distribution;
        Config.enable_range_distribution = true;
        connectContext.getSessionVariable().setEnableRangeDistribution(true);
        connectContext.getSessionVariable().setEnableLocalShuffleAgg(false);

        starRocksAssert.withDatabase(DB).useDatabase(DB);
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB);

        // Creates group rca_grp with the single ColocateRange [MIN, MAX). Splitting the group below leaves this
        // table unaligned, so it is dropped once the aligned tables exist.
        starRocksAssert.withTable("create table seed (k1 int, k2 int, v int) order by(k1, k2) "
                + "properties('replication_num' = '1', 'colocate_with' = 'rca_grp:k1')");
        OlapTable seed = getTable("seed");
        ColocateTableIndex colocateIndex = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        long grpId = colocateIndex.getGroup(seed.getId()).grpId;
        long lowPackGroup = colocateIndex.getColocateRangeMgr().getColocateRanges(grpId).get(0).getShardGroupId();
        long highPackGroup = GlobalStateMgr.getCurrentState().getStarOSAgent()
                .createShardGroup(db.getId(), seed.getId(), 0L, 0L, PlacementPolicy.PACK);
        colocateIndex.getColocateRangeMgr().setColocateRanges(grpId, List.of(
                new ColocateRange(Range.lt(intTuple(100)), lowPackGroup),
                new ColocateRange(Range.ge(intTuple(100)), highPackGroup)));

        // Created after the split: each partition gets one tablet per ColocateRange.
        for (String name : List.of("t", "u")) {
            starRocksAssert.withTable("create table " + name + " (k1 int, k2 int, v int, s varchar(20)) duplicate key(k1, k2) "
                    + TWO_PARTITIONS + "order by(k1, k2) "
                    + "properties('replication_num' = '1', 'colocate_with' = 'rca_grp:k1')");
        }
        // FORCE: a plain drop parks the table in the recycle bin, where it stays a group member.
        starRocksAssert.dropTable("seed force");

        starRocksAssert.withTable("create table h (k1 int, k2 int, v int, s varchar(20)) duplicate key(k1, k2) "
                + "distributed by hash(k1) buckets 3 properties('replication_num' = '1')");
    }

    @AfterAll
    public static void afterClass() {
        Config.enable_range_distribution = savedEnableRangeDistribution;
    }

    private static Tuple intTuple(int... values) {
        return new Tuple(Arrays.stream(values)
                .mapToObj(value -> Variant.of(IntegerType.INT, String.valueOf(value)))
                .toList());
    }

    private static OlapTable getTable(String name) {
        return (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(DB, name);
    }

    private static String explain(String sql) throws Exception {
        return UtFrameUtils.getPlanAndFragment(connectContext, sql).second.getExplainString(TExplainLevel.NORMAL);
    }

    private static void mode(String value) throws Exception {
        SystemVariable variable = new SystemVariable(
                "range_scan_execution_mode", new StringLiteral(value));
        SetStmtAnalyzer.analyze(
                new SetStmt(List.of(variable)), connectContext);
        Assertions.assertTrue(connectContext.modifySystemVariable(variable, true));
    }

    @Test
    public void analyzedSetAndStatementHintUseRegisteredMode() throws Exception {
        for (String value : List.of("PaRaLlEl", "COLOCATE")) {
            mode(value);
            Assertions.assertEquals(value.toLowerCase(java.util.Locale.ROOT),
                    connectContext.getSessionVariable().getRangeScanExecutionMode());
        }
        for (String value : List.of("auto", "unknown", "")) {
            DdlException failure = Assertions.assertThrows(DdlException.class, () -> mode(value));
            Assertions.assertEquals("Variable 'range_scan_execution_mode' can't be set to the value of '" + value + "'",
                    failure.getMessage());
            Assertions.assertEquals("colocate", connectContext.getSessionVariable().getRangeScanExecutionMode());
        }
        SystemVariable global = new SystemVariable(com.starrocks.sql.ast.SetType.GLOBAL,
                "range_scan_execution_mode", new StringLiteral("parallel"));
        SetStmtAnalyzer.analyze(new SetStmt(List.of(global)), connectContext);
        Assertions.assertThrows(Exception.class, () -> connectContext.modifySystemVariable(global, false));
        String plan = explain("select /*+ SET_VAR(range_scan_execution_mode=parallel) */ k1, count(*) from t group by k1");
        Assertions.assertTrue(plan.contains("SCAN EXECUTION MODE: PARALLEL_SCAN"), plan);
        Assertions.assertEquals("colocate", connectContext.getSessionVariable().getRangeScanExecutionMode());
    }

    private static void executePlannedSet(String name, String value) throws Exception {
        SetStmt statement = new SetStmt(List.of(new SystemVariable(name, new StringLiteral(value))));
        Assertions.assertNull(StatementPlanner.plan(statement, connectContext));
        new SetExecutor(connectContext, statement).execute();
    }

    @Test
    public void plannedSetCanConfigureAndExitParallelMode() throws Exception {
        SessionVariable saved = connectContext.getSessionVariable().clone();
        try {
            executePlannedSet("range_scan_execution_mode", "parallel");
            executePlannedSet("pipeline_dop", "4");
            Assertions.assertEquals(4, connectContext.getSessionVariable().getPipelineDop());
            Assertions.assertTrue(connectContext.getSessionVariable().isParallelRangeScan());
            for (String value : List.of("auto", "unknown")) {
                DdlException failure = Assertions.assertThrows(DdlException.class,
                        () -> executePlannedSet("range_scan_execution_mode", value));
                Assertions.assertEquals("Variable 'range_scan_execution_mode' can't be set to the value of '" + value + "'",
                        failure.getMessage());
                Assertions.assertTrue(connectContext.getSessionVariable().isParallelRangeScan());
            }
            SetStmt global = new SetStmt(List.of(new SystemVariable(com.starrocks.sql.ast.SetType.GLOBAL,
                    "range_scan_execution_mode", new StringLiteral("parallel"))));
            Assertions.assertNull(StatementPlanner.plan(global, connectContext));
            Assertions.assertThrows(DdlException.class, () -> new SetExecutor(connectContext, global).execute());
            Assertions.assertTrue(connectContext.getSessionVariable().isParallelRangeScan());
            SetStmt subquery = (SetStmt) UtFrameUtils.parseStmtWithNewParser(
                    "set @parallel_unsupported = (select v from t limit 1)", connectContext);
            Assertions.assertNull(StatementPlanner.plan(subquery, connectContext));
            Exception failure = Assertions.assertThrows(com.starrocks.sql.analyzer.SemanticException.class,
                    () -> new SetExecutor(connectContext, subquery).execute());
            Assertions.assertTrue(failure.getMessage().contains("range_scan_execution_mode"), failure.toString());
            Assertions.assertFalse(connectContext.getUserVariables().containsKey("parallel_unsupported"));
            executePlannedSet("range_scan_execution_mode", "colocate");
            Assertions.assertFalse(connectContext.getSessionVariable().isParallelRangeScan());
        } finally {
            connectContext.setSessionVariable(saved);
        }
    }

    @Test
    public void parallelModeAllowsSessionSelectionAndPreparedCleanup() throws Exception {
        SessionVariable saved = connectContext.getSessionVariable().clone();
        try {
            mode("parallel");
            for (StatementBase statement : List.of(
                    new UseDbStmt("default_catalog", DB, NodePosition.ZERO),
                    new UseCatalogStmt("default_catalog", NodePosition.ZERO),
                    new SetCatalogStmt("default_catalog", NodePosition.ZERO),
                    new SetWarehouseStmt("default_warehouse"),
                    new DeallocateStmt("unused_parallel_statement"))) {
                Assertions.assertNull(StatementPlanner.plan(statement, connectContext), statement.getClass().getName());
                Assertions.assertTrue(connectContext.getSessionVariable().isParallelRangeScan());
            }
        } finally {
            connectContext.setSessionVariable(saved);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"SET ROLE ALL", "BEGIN", "COMMIT", "ROLLBACK", "KILL QUERY 123"})
    public void parallelModeAllowsRoleTransactionAndCancellationControls(String sql) throws Exception {
        SessionVariable saved = connectContext.getSessionVariable().clone();
        try {
            mode("parallel");
            StatementBase statement = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            Assertions.assertNull(StatementPlanner.plan(statement, connectContext));
            Assertions.assertTrue(connectContext.getSessionVariable().isParallelRangeScan());
        } finally {
            connectContext.setSessionVariable(saved);
        }
    }

    @Test
    public void parallelPrepareValidatesItsAggregateQuery() throws Exception {
        SessionVariable saved = connectContext.getSessionVariable().clone();
        try {
            connectContext.getSessionVariable().setEnablePrepareStmt(true);
            mode("parallel");
            StatementBase supported = UtFrameUtils.parseStmtWithNewParser(
                    "prepare range_parallel_prepare from select k1, count(*) from t group by k1", connectContext);
            Assertions.assertNull(StatementPlanner.plan(supported, connectContext));
            StatementBase unsupported = UtFrameUtils.parseStmtWithNewParser(
                    "prepare range_parallel_point from select * from t where k1 = 1", connectContext);
            Exception failure = Assertions.assertThrows(com.starrocks.sql.analyzer.SemanticException.class,
                    () -> StatementPlanner.plan(unsupported, connectContext));
            Assertions.assertTrue(failure.getMessage().contains("an aggregate is required"), failure.toString());
        } finally {
            connectContext.setSessionVariable(saved);
        }
    }

    @Test
    public void parallelRequiresNetworkMerge() throws Exception {
        mode("parallel");
        try {
            for (String sql : List.of("select k1, count(*) from t group by k1",
                    "select k1, k2, sum(v) from t group by k1, k2",
                    "select k1, count(distinct v) from t group by k1")) {
                String plan = explain(sql);
                Assertions.assertTrue(plan.contains("HASH_PARTITIONED"), plan);
                Assertions.assertTrue(plan.contains("SCAN EXECUTION MODE: PARALLEL_SCAN"), plan);
                Assertions.assertTrue(plan.contains("merge "), plan);
            }
        } finally {
            mode("colocate");
        }
    }

    @Test
    public void defaultRetainsColocation() throws Exception {
        String plan = explain("select k1, count(*) from t group by k1");
        Assertions.assertFalse(plan.contains("HASH_PARTITIONED"), plan);
        Assertions.assertTrue(plan.contains("update finalize"), plan);
    }

    private static List<Operator> operators(OptExpression root) {
        List<Operator> result = new ArrayList<>();
        result.add(root.getOp());
        root.getInputs().forEach(child -> result.addAll(operators(child)));
        return result;
    }

    private static PhysicalOlapScanOperator physicalScan(ExecPlan plan) {
        return operators(plan.getPhysicalPlan()).stream().filter(PhysicalOlapScanOperator.class::isInstance)
                .map(PhysicalOlapScanOperator.class::cast).findFirst().orElseThrow();
    }

    private static OlapScanNode nativeScan(ExecPlan plan) {
        return (OlapScanNode) plan.getScanNodes().get(0);
    }

    @Test
    public void fixedModeAndPruningSurviveBothPlanningPaths() throws Exception {
        SessionVariable saved = connectContext.getSessionVariable().clone();
        try {
            for (boolean dbLock : List.of(false, true)) {
                connectContext.getSessionVariable().setCboUseDBLock(dbLock);
                for (int dop : List.of(1, 4)) {
                    connectContext.getSessionVariable().setPipelineDop(dop);
                    String sql = "select k1, sum(v) from t where k1 < 50 group by k1";
                    mode("colocate");
                    ExecPlan ordinary = UtFrameUtils.getPlanAndFragment(connectContext, sql).second;
                    mode("PaRaLlEl");
                    ExecPlan parallel = UtFrameUtils.getPlanAndFragment(connectContext, sql).second;
                    PhysicalOlapScanOperator physical = physicalScan(parallel);
                    Assertions.assertTrue(physical.isParallelRangeScan());
                    Assertions.assertEquals(DistributionSpec.DistributionType.ANY,
                            physical.getDistributionSpec().getType());
                    List<LogicalOlapScanOperator> logical = operators(parallel.getLogicalPlan().getRoot()).stream()
                            .filter(LogicalOlapScanOperator.class::isInstance)
                            .map(LogicalOlapScanOperator.class::cast).toList();
                    Assertions.assertEquals(1, logical.size());
                    Assertions.assertTrue(logical.get(0).isParallelRangeScan());
                    Assertions.assertEquals(DistributionSpec.DistributionType.ANY,
                            logical.get(0).getDistributionSpec().getType());
                    Assertions.assertEquals(new HashSet<>(physicalScan(ordinary).getSelectedTabletId()),
                            new HashSet<>(physical.getSelectedTabletId()));
                    Assertions.assertEquals(2, physical.getSelectedTabletId().size());
                    OlapScanNode scan = nativeScan(parallel);
                    Assertions.assertTrue(scan.isParallelRangeScan());
                    Assertions.assertFalse(scan.getFragment().isUseGroupExecution());
                    Assertions.assertFalse(scan.getFragment().isColocateGroupFragment());
                    Assertions.assertTrue(scan.getBucketSeqToLocations().isEmpty());
                    var wire = scan.treeToThrift().getNodes().get(0).getLake_scan_node();
                    Assertions.assertFalse(wire.isOutput_chunk_by_bucket());
                    Assertions.assertFalse(wire.isSorted_by_keys_per_tablet());
                    Assertions.assertTrue(scan.getScanRangeLocations(0).stream()
                            .allMatch(range -> !range.getScan_range().getInternal_scan_range().isSetBucket_sequence()));
                    // Mode belongs to the plan, independent of subsequent session edits.
                    mode("colocate");
                    Assertions.assertTrue(scan.isParallelRangeScan());
                    Assertions.assertTrue(physical.isParallelRangeScan());
                }
            }
        } finally {
            connectContext.setSessionVariable(saved);
        }
    }

    @Test
    public void preparedAggregateRecapturesModeOnEveryExecute() throws Exception {
        SessionVariable saved = connectContext.getSessionVariable().clone();
        String name = "range_parallel_aggregate";
        try {
            mode("colocate");
            connectContext.getSessionVariable().setEnablePrepareStmt(true);
            PrepareStmt statement = (PrepareStmt) UtFrameUtils.parseStmtWithNewParser(
                    "prepare " + name + " from select k1, sum(v) from t where k1 < ? group by k1", connectContext);
            PrepareStmtContext prepared = new PrepareStmtContext(statement, connectContext, null);
            connectContext.putPreparedStmt(name, prepared);
            ExecPlan previous = null;
            for (String value : List.of("colocate", "parallel", "colocate", "parallel")) {
                mode(value);
                connectContext.setQueryId(UUIDUtil.genUUID());
                connectContext.setExecutionId(UUIDUtil.toTUniqueId(connectContext.getQueryId()));
                connectContext.setThreadLocalInfo();
                List<Expr> params = List.of(new IntLiteral(50, IntegerType.INT));
                ExecuteStmt execute = new ExecuteStmt(name, params);
                Analyzer.analyze(execute, connectContext);
                ExecPlan plan = PrepareStmtPlanner.plan(execute, statement.assignValues(params), connectContext);
                boolean parallel = value.equals("parallel");
                Assertions.assertEquals(parallel, physicalScan(plan).isParallelRangeScan());
                Assertions.assertEquals(parallel, nativeScan(plan).isParallelRangeScan());
                Assertions.assertEquals(parallel,
                        plan.getExplainString(TExplainLevel.NORMAL).contains("HASH_PARTITIONED"));
                Assertions.assertEquals(2, physicalScan(plan).getSelectedTabletId().size());
                Assertions.assertFalse(prepared.isCached());
                if (previous != null) {
                    Assertions.assertNotSame(previous.getPhysicalPlan(), plan.getPhysicalPlan());
                    Assertions.assertEquals(!parallel, physicalScan(previous).isParallelRangeScan());
                    Assertions.assertEquals(!parallel, nativeScan(previous).isParallelRangeScan());
                }
                previous = plan;
            }
        } finally {
            connectContext.removePreparedStmt(name);
            connectContext.setSessionVariable(saved);
        }
    }

    @Test
    public void singleNodeInventoryDoesNotRemoveParallelNetworkMerge() throws Exception {
        SessionVariable saved = connectContext.getSessionVariable().clone();
        try {
            connectContext.getSessionVariable().setEnableLocalShuffleAgg(true);
            mode("parallel");
            Assertions.assertTrue(explain("select k1, count(*) from t group by k1").contains("HASH_PARTITIONED"));
            mode("colocate");
            Assertions.assertFalse(explain("select k1, count(*) from t group by k1").contains("HASH_PARTITIONED"));
        } finally {
            connectContext.setSessionVariable(saved);
        }
    }

    @Test
    public void exactDistinctAndEmptyInputKeepNormalSemantics() throws Exception {
        mode("parallel");
        try {
            String plan = explain("select count(distinct v) from t");
            Assertions.assertTrue(plan.contains("UNPARTITIONED"), plan);
            Assertions.assertTrue(plan.contains("merge finalize"), plan);
            for (String sql : List.of("select k1, count(*) from t where k2 > 5000 group by k1",
                    "select count(*) from t where k2 > 5000",
                    "select count(distinct v) from t where 1=0")) {
                ExecPlan empty = UtFrameUtils.getPlanAndFragment(connectContext, sql).second;
                Assertions.assertTrue(empty.getScanNodes().isEmpty(), empty.getExplainString(TExplainLevel.NORMAL));
                UtFrameUtils.getPlanAndStartScheduling(connectContext, sql);
            }
        } finally {
            mode("colocate");
        }
    }

    private static void reject(String sql) {
        Exception failure = Assertions.assertThrows(Exception.class, () -> explain(sql));
        Assertions.assertTrue(failure.getMessage().contains("range_scan_execution_mode"), failure.toString());
    }

    @Test
    public void parallelFragmentUsesOrdinaryOneShotAssignmentWithoutColocation() throws Exception {
        SessionVariable saved = connectContext.getSessionVariable().clone();
        connectContext.getSessionVariable().setPipelineDop(4);
        // Capture the plan with singleton inventory, then grow the resource before worker capture/assignment.
        var captured = UtFrameUtils.getScheduler(connectContext,
                "select /*+ SET_VAR(range_scan_execution_mode=parallel) */ k1, count(*) from t group by k1");
        Assertions.assertTrue(captured.getJobSpec().getFragments().stream()
                .anyMatch(fragment -> fragment.getExplainString(TExplainLevel.NORMAL).contains("HASH_PARTITIONED")));
        var added = UtFrameUtils.addMockComputeNode(10002);
        try {
            captured.execWithoutDeploy();
            assertOrdinaryParallelSource(captured, 4);
            for (int dop : List.of(1, 4)) {
                for (boolean incremental : List.of(true, false)) {
                    mode("parallel");
                    connectContext.getSessionVariable().setPipelineDop(dop);
                    connectContext.getSessionVariable().setEnableConnectorIncrementalScanRanges(incremental);
                    var scheduler = UtFrameUtils.getPlanAndStartScheduling(connectContext,
                            "select k1, count(*) from t group by k1").second;
                    assertOrdinaryParallelSource(scheduler, dop);
                    Assertions.assertEquals(incremental,
                            connectContext.getSessionVariable().isEnableConnectorIncrementalScanRanges());
                }
            }
        } finally {
            GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().dropComputeNode(added);
            GlobalStateMgr.getCurrentState().getStarOSAgent().removeWorker(
                    added.getHost() + ":" + added.getStarletPort(), added.getWorkerGroupId());
            connectContext.setSessionVariable(saved);
        }
    }

    private static void assertOrdinaryParallelSource(DefaultCoordinator scheduler, int dop) {
        Assertions.assertFalse(scheduler.getJobSpec().isIncrementalScanRanges());
        var source = scheduler.getExecutionDAG().getFragmentsInPreorder().stream()
                .filter(fragment -> !fragment.getScanNodes().isEmpty()).findFirst().orElseThrow();
        Assertions.assertFalse(source.isColocated());
        Assertions.assertNull(source.getColocatedAssignment());
        Assertions.assertFalse(source.getPlanFragment().isUseGroupExecution());
        var expected = source.getScanNodes().iterator().next().getScanRangeLocations(0).stream()
                .map(range -> range.getScan_range()).sorted().toList();
        var actual = source.getInstances().stream().flatMap(instance -> {
            Assertions.assertTrue(instance.getBucketSeqs().isEmpty());
            Assertions.assertTrue(instance.getNode2DriverSeqToScanRanges().isEmpty());
            Assertions.assertEquals(dop, instance.getPipelineDop());
            return instance.getNode2ScanRanges().values().stream().flatMap(List::stream);
        }).map(param -> param.getScan_range()).sorted().toList();
        Assertions.assertEquals(4, actual.size());
        Assertions.assertEquals(expected, actual);
        Assertions.assertEquals(source.getScanRangeAssignment().size(), source.getInstances().size());
    }

    @Test
    public void unsupportedShapesFailExplicitly() throws Exception {
        mode("parallel");
        try {
            for (String sql : List.of("select * from t", "select count(*) from h",
                    "select count(*) from t where k2 < 500",
                    "select count(*) from t join u on t.k1=u.k1",
                    "select count(*) from (select * from t) a",
                    "select count(*) from t union all select count(*) from u",
                    "with a as (select * from t) select count(*) from a",
                    "select k1, sum(v), row_number() over(order by k1) from t group by k1",
                    "select k1, count(*) from t group by rollup(k1)",
                    "select count(*) from t where v in (select v from u)",
                    "select count(*) from t [_META_]", "select count(*) from t [_CACHE_STATS_]",
                    "insert into u select * from t")) {
                reject(sql);
                Assertions.assertTrue(connectContext.getSessionVariable().isParallelRangeScan());
            }
        } finally {
            mode("colocate");
        }
    }

    @Test
    public void capturedVersionOverrideRejectsParallelExecution() throws Exception {
        var saved = connectContext.getScanVersionOverride();
        mode("parallel");
        try {
            connectContext.setScanVersionOverride(java.util.Map.of(1L, 1L));
            reject("select k1, count(*) from t group by k1");
        } finally {
            connectContext.setScanVersionOverride(saved);
            mode("colocate");
        }
    }

    @Test
    public void unsupportedSettingsAreNotSilentlyMutated() throws Exception {
        SessionVariable saved = connectContext.getSessionVariable().clone();
        try {
            for (Consumer<SessionVariable> setting : List.<Consumer<SessionVariable>>of(
                    variables -> variables.setEnableQueryCache(true),
                    variables -> variables.setEnablePhasedScheduler(true),
                    variables -> variables.setSingleNodeExecPlan(true),
                    variables -> variables.setEnablePipelineEngine(false),
                    variables -> variables.setParallelExecInstanceNum(2),
                    variables -> variables.setEnableLakePreparedPhysicalSplitScan(true))) {
                connectContext.setSessionVariable(saved.clone());
                mode("parallel");
                setting.accept(connectContext.getSessionVariable());
                String before = connectContext.getSessionVariable().getNonDefaultVariablesJson();
                reject("select k1, count(*) from t group by k1");
                Assertions.assertEquals(before, connectContext.getSessionVariable().getNonDefaultVariablesJson());
            }
        } finally {
            connectContext.setSessionVariable(saved);
        }
    }

    private static void rejectFinal(OptExpression plan, QueryStatement statement, String reason) {
        Exception failure = Assertions.assertThrows(Exception.class,
                () -> RangeScanExecutionValidator.validatePhysicalPlan(plan, statement, connectContext));
        Assertions.assertTrue(failure.getMessage().contains("range_scan_execution_mode"), failure.toString());
        Assertions.assertTrue(failure.getMessage().contains(reason), failure.toString());
    }

    @Test
    public void finalGuardRejectsLostModeReplacementAndMultipleScans() throws Exception {
        mode("parallel");
        try {
            String sql = "select k1, count(*) from t group by k1";
            QueryStatement query = (QueryStatement) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            PhysicalOlapScanOperator scan = physicalScan(UtFrameUtils.getPlanAndFragment(connectContext, sql).second);
            PhysicalOlapScanOperator lost = PhysicalOlapScanOperator.builder().withOperator(scan).build();
            lost.setParallelRangeScan(false);
            rejectFinal(OptExpression.create(lost), query, "lost parallel mode");
            LogicalOlapScanOperator original = LogicalOlapScanOperator.builder()
                    .withOperator(new LogicalOlapScanOperator(getTable("t")))
                    .setSelectedIndexId(getTable("t").getBaseIndexMetaId()).setParallelRangeScan(true)
                    .setDistributionSpec(DistributionSpec.createAnyDistributionSpec()).build();
            for (LogicalOlapScanOperator replacement : List.of(
                    LogicalOlapScanOperator.builder().withOperator(original).setTable(getTable("u")).build(),
                    LogicalOlapScanOperator.builder().withOperator(original)
                            .setSelectedIndexId(getTable("t").getBaseIndexMetaId() + 999).build())) {
                rejectFinal(OptExpression.create(new PhysicalOlapScanOperator(replacement)), query, "source replacement");
            }
            rejectFinal(OptExpression.create(scan, OptExpression.create(scan)), query, "exactly one final native scan");
            PhysicalOlapScanOperator oneTablet = PhysicalOlapScanOperator.builder().withOperator(scan).build();
            oneTablet.setSelectedTabletId(List.of(scan.getSelectedTabletId().get(0)));
            rejectFinal(OptExpression.create(oneTablet), query, "two selected real tablets");
        } finally {
            mode("colocate");
        }
    }

    @Test
    public void dictionaryReconstructionPreservesParallelMode() throws Exception {
        SessionVariable saved = connectContext.getSessionVariable().clone();
        boolean savedMock = FeConstants.USE_MOCK_DICT_MANAGER;
        try {
            FeConstants.USE_MOCK_DICT_MANAGER = true;
            connectContext.getSessionVariable().setUseLowCardinalityOptimizeOnLake(true);
            connectContext.getSessionVariable().setUseLowCardinalityOptimizeV2(false);
            mode("parallel");
            ExecPlan plan = UtFrameUtils.getPlanAndFragment(connectContext,
                    "select s, count(*) from t group by s").second;
            Assertions.assertTrue(plan.getExplainString(TExplainLevel.NORMAL).contains("Decode"));
            Assertions.assertTrue(physicalScan(plan).isParallelRangeScan());
            Assertions.assertFalse(physicalScan(plan).getGlobalDicts().isEmpty());
        } finally {
            FeConstants.USE_MOCK_DICT_MANAGER = savedMock;
            connectContext.setSessionVariable(saved);
        }
    }
}
