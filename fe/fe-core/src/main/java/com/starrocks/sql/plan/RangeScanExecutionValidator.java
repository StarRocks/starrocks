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
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.RangeDistributionInfo;
import com.starrocks.planner.OlapScanNode;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.DeallocateStmt;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.ast.KillStmt;
import com.starrocks.sql.ast.PrepareStmt;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.SetCatalogStmt;
import com.starrocks.sql.ast.SetRoleStmt;
import com.starrocks.sql.ast.SetStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.ast.UseCatalogStmt;
import com.starrocks.sql.ast.UseDbStmt;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.ast.txn.BeginStmt;
import com.starrocks.sql.ast.txn.CommitStmt;
import com.starrocks.sql.ast.txn.RollbackStmt;
import com.starrocks.sql.ast.warehouse.SetWarehouseStmt;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.base.DistributionSpec;
import com.starrocks.sql.optimizer.base.HashDistributionDesc;
import com.starrocks.sql.optimizer.base.HashDistributionSpec;
import com.starrocks.sql.optimizer.base.QueryGroupDistributionSpec;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalDecodeOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalDistributionOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalFilterOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashAggregateOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalLimitOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalProjectOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalTopNOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalValuesOperator;
import com.starrocks.thrift.TPlanNodeType;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/** Fail closed for explicit experimental range execution requests, before fragment deployment. */
public final class RangeScanExecutionValidator {
    private RangeScanExecutionValidator() {
    }

    public static void validateStatement(StatementBase statement, ConnectContext context) {
        SessionVariable variables = context.getSessionVariable();
        if (!variables.isRangeScanExecutionOverride()) {
            return;
        }
        // Non-scan controls retain their normal analysis, authorization and execution.
        if (statement instanceof SetStmt || statement instanceof UseDbStmt || statement instanceof UseCatalogStmt
                || statement instanceof SetCatalogStmt || statement instanceof SetWarehouseStmt
                || statement instanceof DeallocateStmt || statement instanceof SetRoleStmt
                || statement instanceof BeginStmt || statement instanceof CommitStmt
                || statement instanceof RollbackStmt || statement instanceof KillStmt) {
            return;
        }
        if (statement instanceof PrepareStmt prepare) {
            validateStatement(prepare.getInnerStmt(), context);
            return;
        }
        require(variables, statement instanceof QueryStatement, "only read-only SELECT aggregates are supported");
        QueryStatement query = (QueryStatement) statement;
        require(variables, !query.hasOutFileClause(), "OUTFILE is unsupported");
        require(variables, query.getQueryRelation() instanceof SelectRelation, "set operations are unsupported");
        SelectRelation select = (SelectRelation) query.getQueryRelation();
        require(variables, select.getCteRelations().isEmpty(), "CTEs are unsupported");
        require(variables, select.getRelation() instanceof TableRelation, "only a direct single-table aggregate is supported");
        require(variables, select.hasAggregation(), "an aggregate is required");
        require(variables, !select.hasAnalyticInfo(), "analytic functions are unsupported");
        require(variables, select.getGroupingSetsList() == null || select.getGroupingSetsList().isEmpty(),
                "grouping sets are unsupported");
        List<Expr> expressions = new ArrayList<>(select.getOutputExpression());
        expressions.add(select.getPredicate());
        expressions.add(select.getHaving());
        expressions.addAll(select.getGroupBy());
        select.getOrderBy().forEach(order -> expressions.add(order.getExpr()));
        for (Expr expression : expressions) {
            if (expression != null) {
                AnalyzerUtils.verifyNoSubQuery(expression, "range_scan_execution_mode=" + variables.getRangeScanExecutionMode());
            }
        }
        require(variables, select.getAggregate().stream().noneMatch(call -> call.isDistinct()
                && !"count".equalsIgnoreCase(call.getFunctionName())),
                "only COUNT DISTINCT is supported for distinct aggregates");
        TableRelation relation = (TableRelation) select.getRelation();
        require(variables, relation.getTable() instanceof OlapTable && relation.getTable().isCloudNativeTable()
                && RunMode.isSharedDataMode(), "a native shared-data range-colocate table is required");
        OlapTable table = (OlapTable) relation.getTable();
        require(variables, table.getDefaultDistributionInfo() instanceof RangeDistributionInfo
                && GlobalStateMgr.getCurrentState().getColocateTableIndex().isColocateTable(table.getId()),
                "native range distribution and colocate membership are required");
        require(variables, relation.getTableHints().isEmpty() && relation.getTabletIds().isEmpty()
                && relation.getReplicaIds().isEmpty() && relation.getSampleClause() == null
                && relation.getQueryPeriod() == null && relation.getTvrVersionRange() == null
                && relation.getGtid() == 0,
                "special, hinted, historical and sampled scans are unsupported");
        if (variables.isGroupedRangeScan()) {
            require(variables, table.getKeysType() == KeysType.DUP_KEYS, "DUP tables are required");
            List<Column> keys;
            List<Column> physicalSortKeys;
            try {
                keys = MetaUtils.getRangeDistributionColumns(table);
                var metadata = table.getIndexMetaByMetaId(table.getBaseIndexMetaId());
                physicalSortKeys = MetaUtils.resolveEffectiveSortKeyColumns(metadata.getSchema(),
                        metadata.getSortKeyUniqueIds(), metadata.getSortKeyIdxes());
            } catch (IllegalArgumentException | IndexOutOfBoundsException | NullPointerException e) {
                throw new SemanticException("range_scan_execution_mode=grouped: invalid base-index sort key metadata: "
                        + e.getMessage());
            }
            var colocateIndex = GlobalStateMgr.getCurrentState().getColocateTableIndex();
            var schema = colocateIndex.getGroupSchema(colocateIndex.getGroup(table.getId()));
            require(variables, keys.size() == 3 && schema != null && schema.getColocateColumnCount() == 3
                    && keys.stream().allMatch(column -> column.getType().isBigint() && !column.isAllowNull()),
                    "K=C must be exactly three BIGINT NOT NULL columns");
            require(variables, keys.stream().map(Column::getUniqueId).toList()
                    .equals(physicalSortKeys.stream().map(Column::getUniqueId).toList()),
                    "base-index sort key unique IDs must match ordered C identities");
            require(variables, select.getGroupBy().size() == 3 && select.getGroupBy().stream()
                    .allMatch(expr -> expr instanceof SlotRef)
                    && new HashSet<>(select.getGroupBy().stream().map(expr -> ((SlotRef) expr).getColumnName()
                            .toLowerCase(java.util.Locale.ROOT)).toList()).equals(new HashSet<>(keys.stream()
                                    .map(column -> column.getName().toLowerCase(java.util.Locale.ROOT)).toList())),
                    "GROUP BY must contain exactly the three direct C columns");
            require(variables, select.getAggregate().size() == 1, "exactly one COUNT DISTINCT is required");
            var call = select.getAggregate().get(0);
            require(variables, call.isDistinct() && "count".equalsIgnoreCase(call.getFunctionName())
                    && call.getChildren().size() == 1 && call.getChild(0) instanceof SlotRef
                    && call.getChild(0).getType().isBigint(),
                    "one COUNT DISTINCT over a direct BIGINT column is required");
            require(variables, variables.isEnableGroupExecution(), "Group Execution is required");
            require(variables, variables.getNewPlannerAggStage() <= 1, "forced multi-stage aggregation is unsupported");
        }
        require(variables, variables.isEnablePipelineEngine(), "pipeline execution is required");
        require(variables, !variables.isSingleNodeExecPlan(), "single_node_exec_plan is unsupported");
        require(variables, !variables.isEnableQueryCache(), "Query Cache is unsupported");
        require(variables, !variables.enablePhasedScheduler(), "phased scheduling is unsupported");
        require(variables, variables.getParallelExecInstanceNum() == 1, "parallel_fragment_exec_instance_num must be 1");
    }

    public static void validatePhysicalPlan(OptExpression plan, QueryStatement statement, ConnectContext context) {
        SessionVariable variables = context.getSessionVariable();
        if (!variables.isRangeScanExecutionOverride()) {
            return;
        }
        // Consult the original analyzed relation even if a rewrite discarded every execution-mode flag.
        validateStatement(statement, context);
        OlapTable original = (OlapTable) ((TableRelation) ((SelectRelation) statement.getQueryRelation())
                .getRelation()).getTable();
        List<PhysicalOlapScanOperator> scans = new ArrayList<>();
        collect(plan, scans, variables);
        require(variables, scans.size() <= 1, "exactly one final native scan is supported");
        if (scans.isEmpty()) {
            // Normal optimizer-eliminated empty input (including COUNT=0) needs no assignment.
            return;
        }
        PhysicalOlapScanOperator scan = scans.get(0);
        require(variables, scan.getTable().getId() == original.getId()
                && scan.getSelectedIndexMetaId() == original.getBaseIndexMetaId(),
                "MV, rollup or source replacement is unsupported");
        if (variables.isGroupedRangeScan()) {
            require(variables, scan.isGroupedRangeScan(), "the final scan lost grouped mode");
            require(variables, scan.getDistributionSpec() instanceof QueryGroupDistributionSpec,
                    "the final scan lost complete-C distribution");
            require(variables, scan.getOrderedPhysicalCUniqueIds().equals(MetaUtils.getRangeDistributionColumns(original)
                    .stream().map(Column::getUniqueId).toList()), "the final scan lost physical C identities");
            List<PhysicalHashAggregateOperator> aggregates = new ArrayList<>();
            collectGroupedAggregates(plan, aggregates, variables);
            require(variables, aggregates.size() == 1 && aggregates.get(0).isOnePhaseAgg()
                    && !aggregates.get(0).isWithLocalShuffle() && !aggregates.get(0).isUseSortAgg()
                    && !aggregates.get(0).isUsePerBucketOptmize(),
                    "exactly one blocking one-phase HASH aggregate is required");
            var calls = aggregates.get(0).getAggregations().values();
            require(variables, calls.size() == 1 && calls.stream().allMatch(call ->
                    FunctionSet.MULTI_DISTINCT_COUNT.equalsIgnoreCase(call.getFnName())
                            || (FunctionSet.COUNT.equalsIgnoreCase(call.getFnName()) && call.isDistinct())),
                    "the final aggregate must retain one exact COUNT DISTINCT");
            require(variables, aggregates.get(0).getGroupBys().size() == 3,
                    "the final aggregate lost complete-C grouping keys");
        } else {
            require(variables, scan.isParallelRangeScan(), "the final scan lost parallel mode");
            require(variables, scan.getDistributionSpec().getType() == DistributionSpec.DistributionType.ANY,
                    "the final scan must have effective ANY distribution");
        }
        require(variables, scan.getGtid() == 0 && scan.getSample() == null && scan.getHintsReplicaId().isEmpty(),
                "special final scan artifacts are unsupported");
        if (!variables.isGroupedRangeScan() && !scan.getSelectedTabletId().isEmpty()) {
            require(variables, new HashSet<>(scan.getSelectedPartitionId()).size() >= 2,
                    "at least two selected logical partitions are required");
            require(variables, new HashSet<>(scan.getSelectedTabletId()).size() >= 2,
                    "at least two selected real tablets are required");
        }
    }

    private static void collect(OptExpression plan, List<PhysicalOlapScanOperator> scans, SessionVariable variables) {
        Operator operator = plan.getOp();
        require(variables, operator instanceof PhysicalOlapScanOperator || operator instanceof PhysicalHashAggregateOperator
                || operator instanceof PhysicalDistributionOperator || operator instanceof PhysicalProjectOperator
                || operator instanceof PhysicalFilterOperator || operator instanceof PhysicalLimitOperator
                || operator instanceof PhysicalTopNOperator || operator instanceof PhysicalDecodeOperator
                || operator instanceof PhysicalValuesOperator,
                "unsupported final operator: " + operator.getOpType());
        if (operator instanceof PhysicalOlapScanOperator scan) {
            scans.add(scan);
        }
        plan.getInputs().forEach(child -> collect(child, scans, variables));
    }

    public static void validateFragments(ExecPlan plan) {
        SessionVariable variables = plan.getConnectContext().getSessionVariable();
        if (!variables.isGroupedRangeScan() || plan.getScanNodes().isEmpty()) {
            return;
        }
        require(variables, !plan.isShortCircuit(), "short-circuit execution is unsupported");
        require(variables, plan.getScanNodes().size() == 1 && plan.getScanNodes().get(0) instanceof OlapScanNode,
                "exactly one native grouped scan is required");
        OlapScanNode scan = (OlapScanNode) plan.getScanNodes().get(0);
        require(variables, scan.isGroupedRangeScan() && scan.getQueryGroupManifest() != null,
                "the final scan lost its captured manifest");
        var fragment = scan.getFragment();
        var nodes = fragment.getPlanRoot().treeToThrift().getNodes();
        var aggregates = nodes.stream().filter(node -> node.isSetAgg_node()).toList();
        require(variables, aggregates.size() == 1 && !fragment.isWithLocalShuffle(),
                "exactly one HASH consumer in the scan fragment is required");
        var aggregate = aggregates.get(0);
        var wire = aggregate.getAgg_node();
        require(variables, wire.isNeed_finalize() && !wire.isUse_streaming_preaggregation()
                && !wire.isUse_sort_agg() && !wire.isUse_per_bucket_optimize(),
                "blocking finalized HASH execution is required");
        require(variables, fragment.isUseGroupExecution() && plan.getExecGroups().stream()
                .anyMatch(group -> group.isColocateExecGroup() && group.contains(scan)
                        && group.contains(aggregate.getNode_id())),
                "scan and HASH consumer must share one execution group");
        var scanWire = scan.treeToThrift().getNodes().get(0);
        require(variables, scanWire.getNode_type() == TPlanNodeType.LAKE_GROUPED_SCAN_NODE
                && !scanWire.getLake_scan_node().isSorted_by_keys_per_tablet()
                && !scanWire.getLake_scan_node().isOutput_chunk_by_bucket()
                && !scanWire.getLake_scan_node().isSetOutput_asc_hint(),
                "grouped scan must retain node46 and unordered complete-driver input");
    }

    private static void collectGroupedAggregates(OptExpression plan, List<PhysicalHashAggregateOperator> aggregates,
                                                 SessionVariable variables) {
        if (plan.getOp() instanceof PhysicalHashAggregateOperator aggregate) {
            aggregates.add(aggregate);
            var inputProperty = plan.inputAt(0).getOutputProperty();
            require(variables, inputProperty != null
                    && inputProperty.getDistributionProperty().getSpec() instanceof QueryGroupDistributionSpec
                    && inputProperty.getDistributionProperty().getSpec().isSatisfy(new HashDistributionSpec(
                            new HashDistributionDesc(aggregate.getGroupBys().stream()
                                    .map(column -> column.getId()).toList(), HashDistributionDesc.SourceType.SHUFFLE_AGG))),
                    "the final aggregate lost the complete-C input proof");
        }
        if (plan.getOp() instanceof PhysicalDistributionOperator distribution) {
            require(variables, distribution.getDistributionSpec().getType() == DistributionSpec.DistributionType.GATHER,
                    "only finalized scalar gather is supported");
        }
        plan.getInputs().forEach(child -> collectGroupedAggregates(child, aggregates, variables));
    }

    private static void require(SessionVariable variables, boolean condition, String reason) {
        if (!condition) {
            throw new SemanticException("range_scan_execution_mode=" + variables.getRangeScanExecutionMode() + ": " + reason);
        }
    }
}
