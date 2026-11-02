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
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Tuple;
import com.starrocks.catalog.Variant;
import com.starrocks.common.Config;
import com.starrocks.common.Range;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.PrepareStmtContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.PrepareStmtPlanner;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.ast.ExecuteStmt;
import com.starrocks.sql.ast.PrepareStmt;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.QueryGroupDistributionSpec;
import com.starrocks.sql.optimizer.operator.AggType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalRepeatOperator;
import com.starrocks.sql.optimizer.rule.transformation.materialization.OptExpressionDuplicator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.PlanTestNoneDBBase;
import com.starrocks.sql.plan.RangeScanExecutionValidator;
import com.starrocks.thrift.TExplainLevel;
import com.starrocks.thrift.TPlanNodeType;
import com.starrocks.type.IntegerType;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

public class RangeGroupedScanPlanTest {
    private static ConnectContext context;
    private static boolean savedRange;
    private static final String SQL = "select exp_id, exp_group, user_tag, count(distinct uid) from t "
            + "where exp_id = 123 and dt >= 0 group by exp_id, exp_group, user_tag";

    @BeforeAll
    public static void setup() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        context = UtFrameUtils.createDefaultCtx();
        savedRange = Config.enable_range_distribution;
        Config.enable_range_distribution = true;
        context.getSessionVariable().setEnableRangeDistribution(true);
        context.getSessionVariable().setEnableGroupExecution(true);
        context.getSessionVariable().setPipelineDop(4);
        StarRocksAssert fixture = new StarRocksAssert(context);
        fixture.withDatabase("range_grouped_scan").useDatabase("range_grouped_scan");
        String columns = " (exp_id bigint not null, exp_group bigint not null, user_tag bigint not null, "
                + "dt bigint not null, uid bigint null) duplicate key(exp_id, exp_group, user_tag) ";
        String props = " order by(exp_id, exp_group, user_tag) properties('replication_num'='1', "
                + "'colocate_with'='grouped_test:exp_id,exp_group,user_tag')";
        fixture.withTable("create table seed" + columns + props);
        var db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("range_grouped_scan");
        OlapTable seed = (OlapTable) db.getTable("seed");
        var index = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        long group = index.getGroup(seed.getId()).grpId;
        long low = index.getColocateRangeMgr().getColocateRanges(group).get(0).getShardGroupId();
        long high = GlobalStateMgr.getCurrentState().getStarOSAgent()
                .createShardGroup(db.getId(), seed.getId(), 0L, 0L, PlacementPolicy.PACK);
        Tuple cut = new Tuple(List.of(Variant.of(IntegerType.BIGINT, "123"),
                Variant.of(IntegerType.BIGINT, "50"), Variant.of(IntegerType.BIGINT, "0")));
        index.getColocateRangeMgr().setColocateRanges(group, List.of(
                new ColocateRange(Range.lt(cut), low), new ColocateRange(Range.ge(cut), high)));
        fixture.withTable("create table t" + columns + " partition by range(dt) "
                + "(partition p1 values less than ('1000'), partition p2 values less than ('2000'))" + props);
        fixture.withTable("create table implicit_sort" + columns + " partition by range(dt) "
                + "(partition p1 values less than ('1000'), partition p2 values less than ('2000'))"
                + props.replace(" order by(exp_id, exp_group, user_tag)", ""));
        fixture.dropTable("seed force");
        fixture.withTable("create table h" + columns + " distributed by hash(exp_id) buckets 2 "
                + "properties('replication_num'='1')");
        fixture.withTable("create table single_group" + columns + " partition by range(dt) "
                + "(partition p1 values less than ('1000'), partition p2 values less than ('2000'))"
                + props.replace("grouped_test", "single_group"));
        fixture.withTable("create table bad_type" + columns.replace("exp_id bigint", "exp_id int")
                + props.replace("grouped_test", "bad_type"));
        fixture.withTable("create table nullable_key" + columns.replace("exp_id bigint not null", "exp_id bigint")
                + props.replace("grouped_test", "nullable_key"));
        fixture.withTable("create table short_key" + columns
                + props.replace("grouped_test", "short_key").replace("exp_id,exp_group,user_tag", "exp_id,exp_group"));
    }

    @AfterAll
    public static void cleanup() {
        Config.enable_range_distribution = savedRange;
    }

    private static ExecPlan plan(String sql) throws Exception {
        context.getSessionVariable().setRangeScanExecutionMode("grouped");
        return UtFrameUtils.getPlanAndFragment(context, sql).second;
    }

    private static void assertGrouped(ExecPlan plan) {
        String explain = plan.getExplainString(TExplainLevel.NORMAL);
        Assertions.assertFalse(explain.contains("HASH_PARTITIONED"), explain);
        Assertions.assertFalse(explain.contains("merge finalize"), explain);
        Assertions.assertTrue(explain.contains("update finalize"), explain);
        Assertions.assertTrue(explain.contains("multi_distinct_count"), explain);
        Assertions.assertTrue(explain.contains("GROUPED_SCAN"), explain);
        Assertions.assertEquals(1, plan.getScanNodes().size());
        OlapScanNode scan = (OlapScanNode) plan.getScanNodes().get(0);
        Assertions.assertTrue(scan.isGroupedRangeScan());
        QueryGroupManifest manifest = scan.getQueryGroupManifest();
        Assertions.assertEquals(3, manifest.getKeys().orderedCUniqueIds().size());
        Assertions.assertEquals(4, manifest.getSourceCount());
        Assertions.assertEquals(2, manifest.getGroupCount());
        Assertions.assertEquals(4, manifest.getTaskCount());
        Assertions.assertTrue(scan.getFragment().isUseGroupExecution());
        var wire = scan.treeToThrift().getNodes().get(0);
        Assertions.assertEquals(TPlanNodeType.LAKE_GROUPED_SCAN_NODE, wire.getNode_type());
        Assertions.assertFalse(wire.getLake_scan_node().isSorted_by_keys_per_tablet());
        Assertions.assertFalse(wire.getLake_scan_node().isOutput_chunk_by_bucket());
        Assertions.assertFalse(wire.getLake_scan_node().isSetOutput_asc_hint());
        for (var task : manifest.getTasks()) {
            Assertions.assertTrue(manifest.toScanRange(task).isSetGrouped_range_task());
        }
        var aggregates = scan.getFragment().getPlanRoot().treeToThrift().getNodes().stream()
                .filter(n -> n.isSetAgg_node()).toList();
        Assertions.assertEquals(1, aggregates.size());
        var agg = aggregates.get(0).getAgg_node();
        Assertions.assertTrue(agg.isNeed_finalize());
        Assertions.assertFalse(agg.isUse_streaming_preaggregation());
        Assertions.assertFalse(agg.isUse_sort_agg());
        Assertions.assertFalse(agg.isUse_per_bucket_optimize());
    }

    @Test
    public void completeKeysLiteralAliasesAndFinalScalarConsumers() throws Exception {
        for (String sql : List.of(SQL, SQL.replace("exp_id = 123", "exp_id = '123'"),
                SQL.replace("group by exp_id, exp_group, user_tag",
                "group by user_tag, exp_id, exp_group"),
                SQL.replace("select exp_id,", "select exp_id as experiment,"),
                SQL.replace("select exp_id,", "select 123 as exp_id,"),
                SQL.replace("select exp_id, exp_group, user_tag,", "select"),
                SQL + " having count(distinct uid) > 0 order by user_tag limit 10")) {
            assertGrouped(plan(sql));
        }
    }

    @Test
    public void autoAggregationWithKnownStatisticsKeepsOneFinalizedConsumer() throws Exception {
        SessionVariable saved = context.getSessionVariable().clone();
        StarRocksAssert fixture = new StarRocksAssert(context);
        try {
            context.getSessionVariable().setRangeScanExecutionMode("colocate");
            fixture.withTable("create table auto_stats (exp_id bigint not null, exp_group bigint not null, "
                    + "user_tag bigint not null, dt bigint not null, uid bigint null) "
                    + "duplicate key(exp_id, exp_group, user_tag) partition by range(dt) "
                    + "(partition p1 values less than ('1000'), partition p2 values less than ('2000')) "
                    + "order by(exp_id, exp_group, user_tag) properties('replication_num'='1', "
                    + "'colocate_with'='grouped_test:exp_id,exp_group,user_tag')");
            var db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("range_grouped_scan");
            OlapTable table = (OlapTable) db.getTable("auto_stats");
            PlanTestNoneDBBase.setTableStatistics(table, 32768);
            var statistics = GlobalStateMgr.getCurrentState().getStatisticStorage();
            statistics.addColumnStatistic(table, "exp_id", new ColumnStatistic(123, 123, 0, 8, 1));
            statistics.addColumnStatistic(table, "exp_group", new ColumnStatistic(0, 99, 0, 8, 16));
            statistics.addColumnStatistic(table, "user_tag", new ColumnStatistic(0, 511, 0, 8, 512));
            statistics.addColumnStatistic(table, "dt", new ColumnStatistic(0, 1999, 0, 8, 4));
            statistics.addColumnStatistic(table, "uid", new ColumnStatistic(1, 4, 0, 8, 4));
            context.getSessionVariable().setNewPlanerAggStage(0);
            assertGrouped(plan(SQL.replace("from t ", "from auto_stats ")));
            assertGrouped(plan(SQL.replace("from t where exp_id = 123 and dt >= 0", "from auto_stats")));
            Assertions.assertEquals(0, context.getSessionVariable().getNewPlannerAggStage());
        } finally {
            context.getSessionVariable().setRangeScanExecutionMode("colocate");
            fixture.dropTable("auto_stats force");
            context.setSessionVariable(saved);
        }
    }

    @Test
    public void groupedAutoDeclinesOptionalSplitsAndPreservesMandatorySplits() {
        SessionVariable saved = context.getSessionVariable().clone();
        try {
            var db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("range_grouped_scan");
            OlapTable table = (OlapTable) db.getTable("t");
            var scan = LogicalOlapScanOperator.builder()
                    .withOperator(new LogicalOlapScanOperator(table, Map.of(), Map.of(), null, -1, null))
                    .setSelectedTabletId(List.of(1L, 2L)).build();
            OptExpression input = OptExpression.create(scan);
            input.deriveLogicalPropertyItself();
            Assertions.assertFalse(input.getLogicalProperty().oneTabletProperty().supportOneTabletOpt);
            var aggregate = new LogicalAggregationOperator(AggType.GLOBAL, List.of(), Map.of());
            var repeat = new LogicalRepeatOperator(List.of(), List.of(), List.of(), Map.of());
            for (String mode : List.of("colocate", "parallel", "grouped")) {
                context.getSessionVariable().setRangeScanExecutionMode(mode);
                context.getSessionVariable().setNewPlanerAggStage(0);
                Assertions.assertEquals(!mode.equals("grouped"),
                        Utils.couldGenerateMultiStageAggregate(input.getLogicalProperty(), aggregate, scan), mode);
                Assertions.assertEquals(0, context.getSessionVariable().getNewPlannerAggStage());
                Assertions.assertTrue(Utils.couldGenerateMultiStageAggregate(
                        input.getLogicalProperty(), aggregate, repeat), mode);
                context.getSessionVariable().setNewPlanerAggStage(1);
                Assertions.assertFalse(Utils.couldGenerateMultiStageAggregate(
                        input.getLogicalProperty(), aggregate, scan), mode);
                Assertions.assertTrue(Utils.couldGenerateMultiStageAggregate(
                        input.getLogicalProperty(), aggregate, repeat), mode);
            }
        } finally {
            context.setSessionVariable(saved);
        }
    }

    @Test
    public void optimizerProvenEmptyKeepsNormalSemantics() throws Exception {
        Assertions.assertTrue(plan(SQL.replace("exp_id = 123", "1 = 0")).getScanNodes().isEmpty());
        Assertions.assertTrue(plan(SQL.replace("dt >= 0", "dt > 5000")).getScanNodes().isEmpty());
    }

    @Test
    public void unsupportedShapesAreExplicit() {
        for (String sql : List.of(SQL.replace("from t", "from h"),
                SQL.replace("from t", "from single_group"), SQL.replace("from t", "from bad_type"),
                SQL.replace("from t", "from nullable_key"), SQL.replace("from t", "from short_key"),
                SQL.replace("count(distinct uid)", "count(*)"),
                SQL.replace("count(distinct uid)", "count(distinct uid + 1)"),
                SQL.replace("count(distinct uid)", "sum(distinct uid)"),
                SQL.replace("count(distinct uid)", "count(distinct uid), sum(uid)"),
                SQL.replace("count(distinct uid)", "count(distinct uid), row_number() over(order by exp_id)"),
                SQL.replace("group by exp_id, exp_group, user_tag", "group by rollup(exp_id, exp_group, user_tag)"),
                SQL.replace("select exp_id, exp_group, user_tag,", "select exp_id, exp_group,")
                        .replace("group by exp_id, exp_group, user_tag", "group by exp_id, exp_group"),
                SQL.replace("dt >= 0", "dt < 1000"),
                "select count(distinct t.uid) from t join t u on t.uid=u.uid",
                "with a as (select * from t) select count(*) from a",
                SQL + " union all " + SQL)) {
            Exception failure = Assertions.assertThrows(Exception.class, () -> plan(sql));
            Assertions.assertTrue(failure.getMessage().contains("range_scan_execution_mode=grouped"), failure.toString());
        }
    }

    @Test
    public void incompatibleSettingsRejectWithoutMutatingSession() throws Exception {
        SessionVariable saved = context.getSessionVariable().clone();
        try {
            for (Consumer<SessionVariable> setting : List.<Consumer<SessionVariable>>of(
                    v -> v.setEnableGroupExecution(false), v -> v.setEnableQueryCache(true),
                    v -> v.setEnablePipelineEngine(false), v -> v.setNewPlanerAggStage(2),
                    v -> v.setNewPlanerAggStage(3), v -> v.setNewPlanerAggStage(4))) {
                context.setSessionVariable(saved.clone());
                setting.accept(context.getSessionVariable());
                Exception failure = Assertions.assertThrows(Exception.class, () -> plan(SQL));
                Assertions.assertTrue(failure.getMessage().contains("range_scan_execution_mode=grouped"), failure.toString());
            }
        } finally {
            context.setSessionVariable(saved);
        }
    }
    @Test
    public void optimizationOptionsRetainHashAndAllPhysicalKeys() throws Exception {
        SessionVariable saved = context.getSessionVariable().clone();
        try {
            context.getSessionVariable().setEnableSortAggregate(true);
            Assertions.assertTrue(context.getSessionVariable().isEnablePerBucketComputeOptimize());
            context.getSessionVariable().setEnableConstantExecuteInFE(true);
            context.getSessionVariable().setEnableLocalShuffleAgg(true);
            assertGrouped(plan(SQL));
            assertGrouped(plan(SQL.replace("select exp_id, exp_group, user_tag,", "select")));
        } finally {
            context.setSessionVariable(saved);
        }
    }

    @Test
    public void runtimeEmptyResidualDoesNotDropSourceSnapshots() throws Exception {
        ExecPlan plan = plan(SQL.replace("dt >= 0", "dt >= 0 and uid < -9223372036854775807"));
        assertGrouped(plan);
        OlapScanNode scan = (OlapScanNode) plan.getScanNodes().get(0);
        var manifest = scan.getQueryGroupManifest();
        var first = manifest.getSources().get(0);
        long version = first.capturedVersion();
        first.original().getScan_range().getInternal_scan_range().setVersion("999999");
        Assertions.assertEquals(Long.toString(version), manifest.toScanRange(manifest.getTasks().get(0))
                .getInternal_scan_range().getVersion());
        Assertions.assertSame(manifest, scan.getQueryGroupManifest());
    }

    @Test
    public void preparedAggregateCapturesEveryModeTransition() throws Exception {
        SessionVariable saved = context.getSessionVariable().clone();
        String name = "grouped_mode_transition";
        try {
            context.getSessionVariable().setRangeScanExecutionMode("colocate");
            context.getSessionVariable().setEnablePrepareStmt(true);
            var statement = (PrepareStmt) UtFrameUtils.parseStmtWithNewParser(
                    "prepare " + name + " from " + SQL.replace("exp_id = 123", "exp_id = ?"), context);
            var prepared = new PrepareStmtContext(statement, context, null);
            context.putPreparedStmt(name, prepared);
            ExecPlan previous = null;
            for (String mode : List.of("colocate", "grouped", "parallel", "grouped", "colocate")) {
                context.getSessionVariable().setRangeScanExecutionMode(mode);
                List<Expr> args = List.of(
                        new IntLiteral(123, IntegerType.BIGINT));
                var execute = new ExecuteStmt(name, args);
                Analyzer.analyze(execute, context);
                ExecPlan current = PrepareStmtPlanner.plan(execute, statement.assignValues(args), context);
                var scan = (OlapScanNode) current.getScanNodes().get(0);
                Assertions.assertEquals(mode.equals("grouped"), scan.isGroupedRangeScan());
                Assertions.assertEquals(mode.equals("parallel"), scan.isParallelRangeScan());
                Assertions.assertFalse(prepared.isCached());
                if (mode.equals("grouped")) {
                    assertGrouped(current);
                }
                if (previous != null) {
                    Assertions.assertNotSame(previous.getPhysicalPlan(), current.getPhysicalPlan());
                }
                previous = current;
            }
        } finally {
            context.removePreparedStmt(name);
            context.setSessionVariable(saved);
        }
    }

    @Test
    public void sessionControlsRemainUsable() throws Exception {
        context.getSessionVariable().setRangeScanExecutionMode("grouped");
        for (String sql : List.of("set range_scan_execution_mode='colocate'", "use range_grouped_scan")) {
            var statement = UtFrameUtils.parseStmtWithNewParser(sql, context);
            RangeScanExecutionValidator.validateStatement(statement, context);
        }
    }
    @Test
    public void implicitDuplicateKeysAreValidPhysicalSortKeys() throws Exception {
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getDb("range_grouped_scan").getTable("implicit_sort");
        var metadata = table.getIndexMetaByMetaId(table.getBaseIndexMetaId());
        Assertions.assertNull(metadata.getSortKeyUniqueIds());
        Assertions.assertNull(metadata.getSortKeyIdxes());
        assertGrouped(plan(SQL.replace("from t", "from implicit_sort")));
    }

    @Test
    public void absentSortUniqueIdsUsePositionOrDuplicateKeyFallback() throws Exception {
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getDb("range_grouped_scan").getTable("t");
        var metadata = table.getIndexMetaByMetaId(table.getBaseIndexMetaId());
        List<Integer> savedIds = metadata.getSortKeyUniqueIds();
        List<Integer> savedIndexes = metadata.getSortKeyIdxes();
        try {
            for (List<Integer> uniqueIds : java.util.Arrays.<List<Integer>>asList(null, List.of())) {
                for (List<Integer> indexes : java.util.Arrays.asList(savedIndexes, null)) {
                    metadata.setSortKeyUniqueIds(uniqueIds);
                    metadata.setSortKeyIdxes(indexes);
                    assertGrouped(plan(SQL));
                }
            }
        } finally {
            metadata.setSortKeyUniqueIds(savedIds);
            metadata.setSortKeyIdxes(savedIndexes);
        }
    }

    @Test
    public void malformedAndMismatchedPhysicalSortMetadataRejectsWithModeReason() throws Exception {
        context.getSessionVariable().setRangeScanExecutionMode("grouped");
        var statement = UtFrameUtils.parseStmtWithNewParser(SQL, context);
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getDb("range_grouped_scan").getTable("t");
        var metadata = table.getIndexMetaByMetaId(table.getBaseIndexMetaId());
        List<Integer> savedIds = metadata.getSortKeyUniqueIds();
        List<Integer> savedIndexes = metadata.getSortKeyIdxes();
        try {
            // All IDs exist, but their order disagrees with the C definition.
            metadata.setSortKeyUniqueIds(List.of(savedIds.get(1), savedIds.get(0), savedIds.get(2)));
            Exception mismatch = Assertions.assertThrows(Exception.class,
                    () -> RangeScanExecutionValidator.validateStatement(statement, context));
            Assertions.assertTrue(mismatch.getMessage().contains("range_scan_execution_mode=grouped"), mismatch.toString());
            metadata.setSortKeyUniqueIds(null);
            metadata.setSortKeyIdxes(List.of(0, 1, 9001));
            Exception invalidIndex = Assertions.assertThrows(Exception.class,
                    () -> RangeScanExecutionValidator.validateStatement(statement, context));
            Assertions.assertTrue(invalidIndex.getMessage().contains("range_scan_execution_mode=grouped"),
                    invalidIndex.toString());
            // Empty positional range metadata does not prove a three-column logical C definition.
            metadata.setSortKeyIdxes(List.of());
            Exception emptyRangeKeys = Assertions.assertThrows(Exception.class,
                    () -> RangeScanExecutionValidator.validateStatement(statement, context));
            Assertions.assertTrue(emptyRangeKeys.getMessage().contains("range_scan_execution_mode=grouped"),
                    emptyRangeKeys.toString());
        } finally {
            metadata.setSortKeyUniqueIds(savedIds);
            metadata.setSortKeyIdxes(savedIndexes);
        }
    }

    @Test
    public void inconsistentBaseSortKeyIdentityRejectsBeforeDeployment() throws Exception {
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getDb("range_grouped_scan").getTable("t");
        var metadata = table.getIndexMetaByMetaId(table.getBaseIndexMetaId());
        List<Integer> saved = metadata.getSortKeyUniqueIds();
        try {
            metadata.setSortKeyUniqueIds(List.of(9001, 9002, 9003));
            Exception failure = Assertions.assertThrows(Exception.class, () -> plan(SQL));
            Assertions.assertTrue(failure.getMessage().contains("range_scan_execution_mode=grouped"), failure.toString());
        } finally {
            metadata.setSortKeyUniqueIds(saved);
        }
    }

    @Test
    public void finalFragmentGuardRejectsLostBlockingOrGroupProof() throws Exception {
        ExecPlan plan = plan(SQL);
        OlapScanNode scan = (OlapScanNode) plan.getScanNodes().get(0);
        var groups = plan.getExecGroups();
        var owner = groups.stream().filter(group -> group.contains(scan)).findFirst().orElseThrow();
        owner.disableColocateGroup(scan);
        Exception failure = Assertions.assertThrows(Exception.class,
                () -> RangeScanExecutionValidator.validateFragments(plan));
        Assertions.assertTrue(failure.getMessage().contains("share one execution group"), failure.toString());
    }
    @Test
    public void ownerCapacityIsDeferredUntilWorkerAssignment() throws Exception {
        SessionVariable saved = context.getSessionVariable().clone();
        try {
            var field = SessionVariable.class.getDeclaredField("groupExecutionMaxGroups");
            field.setAccessible(true);
            field.setInt(context.getSessionVariable(), 1);
            assertGrouped(plan(SQL));
        } finally {
            context.setSessionVariable(saved);
        }
    }

    @Test
    public void finalFragmentRejectsIntermediateAggregateState() throws Exception {
        ExecPlan plan = plan(SQL);
        OlapScanNode scan = (OlapScanNode) plan.getScanNodes().get(0);
        AggregationNode aggregation = (AggregationNode) scan.getFragment().getPlanRoot();
        aggregation.unsetNeedsFinalize();
        Exception failure = Assertions.assertThrows(Exception.class,
                () -> RangeScanExecutionValidator.validateFragments(plan));
        Assertions.assertTrue(failure.getMessage().contains("blocking finalized HASH"), failure.toString());
    }
    @Test
    public void duplicatedScanRemapsScalarReferencesAndRetainsPhysicalIdentities() throws Exception {
        ExecPlan plan = plan(SQL.replace("select exp_id, exp_group, user_tag,", "select"));
        var original = logicalScan(plan.getLogicalPlan().getRoot());
        var duplicator = new OptExpressionDuplicator(
                plan.getColumnRefFactory(), null);
        var copied = duplicator.duplicate(original);
        var scan = (LogicalOlapScanOperator) copied.getOp();
        var oldScan = (LogicalOlapScanOperator) original.getOp();
        Assertions.assertTrue(scan.isGroupedRangeScan());
        Assertions.assertEquals(oldScan.getOrderedPhysicalCUniqueIds(), scan.getOrderedPhysicalCUniqueIds());
        var oldSpec = (QueryGroupDistributionSpec) oldScan.getDistributionSpec();
        var newSpec = (QueryGroupDistributionSpec) scan.getDistributionSpec();
        Assertions.assertEquals(oldSpec.getOrderedPhysicalCUniqueIds(), newSpec.getOrderedPhysicalCUniqueIds());
        Assertions.assertNotEquals(oldSpec.getOrderedCColumns(), newSpec.getOrderedCColumns());
        for (int i = 0; i < 3; i++) {
            var oldRef = plan.getColumnRefFactory().getColumnRef(oldSpec.getOrderedCColumns().get(i).getColId());
            Assertions.assertEquals(duplicator.getColumnMapping().get(oldRef).getId(),
                    newSpec.getOrderedCColumns().get(i).getColId());
        }
    }

    private static OptExpression logicalScan(OptExpression root) {
        if (root.getOp() instanceof LogicalOlapScanOperator) {
            return root;
        }
        return root.getInputs().stream().map(RangeGroupedScanPlanTest::logicalScan)
                .filter(Objects::nonNull).findFirst().orElse(null);
    }
}
