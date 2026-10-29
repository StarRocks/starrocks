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

import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.RangeDistributionInfo;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.DeallocateStmt;
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
import com.starrocks.sql.ast.txn.BeginStmt;
import com.starrocks.sql.ast.txn.CommitStmt;
import com.starrocks.sql.ast.txn.RollbackStmt;
import com.starrocks.sql.ast.warehouse.SetWarehouseStmt;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.base.DistributionSpec;
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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/** Fail closed for an explicit experimental parallel request, before fragment deployment. */
public final class RangeScanExecutionValidator {
    private RangeScanExecutionValidator() {
    }

    public static void validateStatement(StatementBase statement, ConnectContext context) {
        SessionVariable variables = context.getSessionVariable();
        if (!variables.isParallelRangeScan()) {
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
        require(statement instanceof QueryStatement, "only read-only SELECT aggregates are supported");
        QueryStatement query = (QueryStatement) statement;
        require(!query.hasOutFileClause(), "OUTFILE is unsupported");
        require(query.getQueryRelation() instanceof SelectRelation, "set operations are unsupported");
        SelectRelation select = (SelectRelation) query.getQueryRelation();
        require(select.getCteRelations().isEmpty(), "CTEs are unsupported");
        require(select.getRelation() instanceof TableRelation, "only a direct single-table aggregate is supported");
        require(select.hasAggregation(), "an aggregate is required");
        require(!select.hasAnalyticInfo(), "analytic functions are unsupported");
        require(select.getGroupingSetsList() == null || select.getGroupingSetsList().isEmpty(),
                "grouping sets are unsupported");
        List<Expr> expressions = new ArrayList<>(select.getOutputExpression());
        expressions.add(select.getPredicate());
        expressions.add(select.getHaving());
        expressions.addAll(select.getGroupBy());
        select.getOrderBy().forEach(order -> expressions.add(order.getExpr()));
        for (Expr expression : expressions) {
            if (expression != null) {
                AnalyzerUtils.verifyNoSubQuery(expression, "range_scan_execution_mode=parallel");
            }
        }
        require(select.getAggregate().stream().noneMatch(call -> call.isDistinct()
                && !"count".equalsIgnoreCase(call.getFunctionName())),
                "only COUNT DISTINCT is supported for distinct aggregates");
        TableRelation relation = (TableRelation) select.getRelation();
        require(relation.getTable() instanceof OlapTable && relation.getTable().isCloudNativeTable()
                && RunMode.isSharedDataMode(), "a native shared-data range-colocate table is required");
        OlapTable table = (OlapTable) relation.getTable();
        require(table.getDefaultDistributionInfo() instanceof RangeDistributionInfo
                && GlobalStateMgr.getCurrentState().getColocateTableIndex().isColocateTable(table.getId()),
                "native range distribution and colocate membership are required");
        require(relation.getTableHints().isEmpty() && relation.getTabletIds().isEmpty()
                && relation.getReplicaIds().isEmpty() && relation.getSampleClause() == null
                && relation.getQueryPeriod() == null && relation.getTvrVersionRange() == null
                && relation.getGtid() == 0
                && (context.getScanVersionOverride() == null || context.getScanVersionOverride().isEmpty()),
                "special, hinted, historical and sampled scans are unsupported");
        require(variables.isEnablePipelineEngine(), "pipeline execution is required");
        require(!variables.isSingleNodeExecPlan(), "single_node_exec_plan is unsupported");
        require(!variables.isEnableQueryCache(), "Query Cache is unsupported");
        require(!variables.enablePhasedScheduler(), "phased scheduling is unsupported");
        require(!variables.isEnableLakePreparedPhysicalSplitScan(), "prepared physical split scans are unsupported");
        require(variables.getParallelExecInstanceNum() == 1, "parallel_fragment_exec_instance_num must be 1");
    }

    public static void validatePhysicalPlan(OptExpression plan, QueryStatement statement, ConnectContext context) {
        if (!context.getSessionVariable().isParallelRangeScan()) {
            return;
        }
        // Consult the original analyzed relation even if a rewrite discarded every parallel flag.
        validateStatement(statement, context);
        OlapTable original = (OlapTable) ((TableRelation) ((SelectRelation) statement.getQueryRelation())
                .getRelation()).getTable();
        List<PhysicalOlapScanOperator> scans = new ArrayList<>();
        collect(plan, scans);
        require(scans.size() <= 1, "exactly one final native scan is supported");
        if (scans.isEmpty()) {
            // Normal optimizer-eliminated empty input (including COUNT=0) needs no assignment.
            return;
        }
        PhysicalOlapScanOperator scan = scans.get(0);
        require(scan.getTable().getId() == original.getId()
                && scan.getSelectedIndexMetaId() == original.getBaseIndexMetaId(),
                "MV, rollup or source replacement is unsupported");
        require(scan.isParallelRangeScan(), "the final scan lost parallel mode");
        require(scan.getDistributionSpec().getType() == DistributionSpec.DistributionType.ANY,
                "the final scan must have effective ANY distribution");
        require(scan.getGtid() == 0 && scan.getSample() == null && scan.getHintsReplicaId().isEmpty(),
                "special final scan artifacts are unsupported");
        if (!scan.getSelectedTabletId().isEmpty()) {
            require(new HashSet<>(scan.getSelectedPartitionId()).size() >= 2,
                    "at least two selected logical partitions are required");
            require(new HashSet<>(scan.getSelectedTabletId()).size() >= 2,
                    "at least two selected real tablets are required");
        }
    }

    private static void collect(OptExpression plan, List<PhysicalOlapScanOperator> scans) {
        Operator operator = plan.getOp();
        require(operator instanceof PhysicalOlapScanOperator || operator instanceof PhysicalHashAggregateOperator
                || operator instanceof PhysicalDistributionOperator || operator instanceof PhysicalProjectOperator
                || operator instanceof PhysicalFilterOperator || operator instanceof PhysicalLimitOperator
                || operator instanceof PhysicalTopNOperator || operator instanceof PhysicalDecodeOperator
                || operator instanceof PhysicalValuesOperator,
                "unsupported final operator: " + operator.getOpType());
        if (operator instanceof PhysicalOlapScanOperator scan) {
            scans.add(scan);
        }
        plan.getInputs().forEach(child -> collect(child, scans));
    }

    private static void require(boolean condition, String reason) {
        if (!condition) {
            throw new SemanticException("range_scan_execution_mode=parallel: " + reason);
        }
    }
}
