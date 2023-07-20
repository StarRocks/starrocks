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


package com.starrocks.sql.optimizer;

import com.google.common.base.Preconditions;
import com.starrocks.analysis.ParseNode;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.HudiTable;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.JDBCTable;
import com.starrocks.catalog.MysqlTable;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.JoinRelation;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.SubqueryRelation;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.ast.UnionRelation;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalAssertOneRowOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEAnchorOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEConsumeOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEProduceOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalEsScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalExceptOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalHiveScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalHudiScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalIcebergScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalIntersectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJDBCScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalLimitOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalMetaScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalMysqlScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalRepeatOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalSchemaScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTableFunctionOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTopNOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalValuesOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalWindowOperator;
import com.starrocks.sql.optimizer.operator.logical.MockOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalAssertOneRowOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalCTEAnchorOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalCTEConsumeOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalCTEProduceOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalDistributionOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalEsScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalExceptOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalFilterOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashAggregateOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHiveScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHudiScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalIcebergScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalIntersectOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalJDBCScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalLimitOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalMetaScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalMysqlScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalNestLoopJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalNoCTEOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalProjectOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalRepeatOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalSchemaScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalTableFunctionOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalTopNOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalUnionOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalValuesOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalWindowOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.rule.Rule;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class OptimizerTraceUtil {
    private static final Logger LOG = LogManager.getLogger(OptimizerTraceUtil.class);

    // In order to completely avoid the overhead caused by assembling strings when enable_optimizer_trace_log is false.
    // We specifically implement the log interface for some classes

    public static void logOptExpression(ConnectContext ctx, String format, OptExpression optExpression) {
        if (ctx.getSessionVariable().isEnableOptimizerTraceLog()) {
            log(ctx, String.format(format, optExpression.explain()));
        }
    }

    public static void logQueryStatement(ConnectContext ctx, String format, QueryStatement statement) {
        if (ctx.getSessionVariable().isEnableOptimizerTraceLog()) {
            log(ctx, String.format(format, statement.accept(new RelationTracePrinter(), "")));
        }
    }

    public static void log(ConnectContext ctx, Object object) {
        if (ctx.getSessionVariable().isEnableOptimizerTraceLog()) {
            log(ctx, String.valueOf(object));
        }
    }

    public static void log(ConnectContext ctx, String format, Object... object) {
        if (ctx.getSessionVariable().isEnableOptimizerTraceLog()) {
            log(ctx, String.format(format, object));
        }
    }

<<<<<<< HEAD
=======
    public static void logMVPrepare(String format, Object... object) {
        logMVPrepare(ConnectContext.get(), null, format, object);
    }

    public static void logMVPrepare(ConnectContext ctx, String format, Object... object) {
        logMVPrepare(ctx, null, format, object);
    }

    public static void logMVPrepare(ConnectContext ctx, MaterializedView mv,
                                    String format, Object... object) {
        if (ctx.getSessionVariable().isEnableMVOptimizerTraceLog()) {
            LOG.info("[MV TRACE] [PREPARE {}] {}", ctx.getQueryId(), String.format(format, object));
        }
        // Only record trace when mv is not null.
        if (mv != null) {
            PlannerProfile.LogTracer tracer = PlannerProfile.getLogTracer(mv.getName());
            if (tracer != null) {
                tracer.log(String.format(format, object));
            }
        }
    }

    public static void logMVRewrite(MvRewriteContext mvRewriteContext, String format, Object... object) {
        MaterializationContext mvContext = mvRewriteContext.getMaterializationContext();
        if (mvContext.getOptimizerContext().getSessionVariable().isEnableMVOptimizerTraceLog()) {
            // QueryID-Rule-MVName log
            LOG.info("[MV TRACE] [REWRITE {} {} {}] {}",
                    mvContext.getOptimizerContext().getTraceInfo().getQueryId(),
                    mvRewriteContext.getRule().type().name(),
                    mvContext.getMv().getName(),
                    String.format(format, object));
        }

        // Trace log if needed.
        PlannerProfile.LogTracer tracer = PlannerProfile.getLogTracer(mvContext.getMv().getName());
        if (tracer != null) {
            tracer.log(String.format("[%s] %s",   mvRewriteContext.getRule().type().name(),
                    String.format(format, object)));
        }
    }

    public static void logMVRewrite(OptimizerContext optimizerContext, Rule rule,
                                     String format, Object... object) {
        if (optimizerContext.getSessionVariable().isEnableMVOptimizerTraceLog()) {
            // QueryID-Rule log
            LOG.info("[MV TRACE] [REWRITE {} {}] {}",
                    optimizerContext.getTraceInfo().getQueryId(),
                    rule.type().name(),
                    String.format(format, object));
        }
    }

>>>>>>> 656e543e84 ([BugFix] fix mv rewrite for join predicate pushdown (#27632))
    private static void log(ConnectContext ctx, String message) {
        Preconditions.checkState(ctx.getSessionVariable().isEnableOptimizerTraceLog());
        LOG.info("[TRACE QUERY {}] {}", ctx.getQueryId(), message);
    }

    public static void logApplyRule(SessionVariable sessionVariable,
                                    OptimizerTraceInfo traceInfo, Rule rule,
                                    OptExpression oldExpression, List<OptExpression> newExpressions) {
        if (sessionVariable.isEnableOptimizerTraceLog()) {
            StringBuilder sb = new StringBuilder();
            sb.append(String.format("[TRACE QUERY %s] APPLY RULE %s\n", traceInfo.getQueryId(), rule));
            sb.append("Original Expression:\n").append(oldExpression.explain());
            sb.append("New Expression:\n");
            if (newExpressions.isEmpty()) {
                sb.append("Empty\n");
            } else {
                for (int i = 0; i < newExpressions.size(); i++) {
                    sb.append(i).append(":\n").append(newExpressions.get(i).explain());
                }
            }
            LOG.info(sb.toString());
            traceInfo.recordAppliedRule(rule.toString());
        }
    }

    public static class OperatorTracePrinter extends OperatorVisitor<String, Void> {
        @Override
        public String visitOperator(Operator op, Void context) {
            return op.toString();
        }

        @Override
        public String visitLogicalTableScan(LogicalScanOperator node, Void context) {
            return "LogicalScanOperator" + " {" +
                    "table='" + node.getTable().getId() + '\'' +
                    ", outputColumns='" + new ArrayList<>(node.getColRefToColumnMetaMap().keySet()) + '\'' +
                    '}';
        }

        @Override
        public String visitLogicalSchemaScan(LogicalSchemaScanOperator node, Void context) {
            return super.visitLogicalSchemaScan(node, context);
        }

        @Override
        public String visitLogicalOlapScan(LogicalOlapScanOperator node, Void context) {
            return "LogicalOlapScanOperator" + " {" + "table=" + node.getTable().getId() +
                    ", selectedPartitionId=" + node.getSelectedPartitionId() +
                    ", outputColumns=" + new ArrayList<>(node.getColRefToColumnMetaMap().keySet()) +
                    ", predicate=" + node.getPredicate() +
                    ", limit=" + node.getLimit() +
                    "}";
        }

        @Override
        public String visitLogicalHiveScan(LogicalHiveScanOperator node, Void context) {
            return "LogicalHiveScanOperator" + " {" + "table=" + ((HiveTable) node.getTable()).getTableName() +
                    ", outputColumns=" + new ArrayList<>(node.getColRefToColumnMetaMap().keySet()) +
                    ", predicates=" + node.getScanOperatorPredicates() +
                    ", limit=" + node.getLimit() +
                    "}";
        }

        @Override
        public String visitLogicalIcebergScan(LogicalIcebergScanOperator node, Void context) {
            StringBuilder sb = new StringBuilder("LogicalIcebergScanOperator");
            sb.append(" {").append("table=").append(((IcebergTable) node.getTable()).getRemoteTableName())
                    .append(", outputColumns=").append(new ArrayList<>(node.getColRefToColumnMetaMap().keySet()))
                    .append(", predicates=").append(node.getScanOperatorPredicates())
                    .append("}");
            return sb.toString();
        }

        @Override
        public String visitLogicalHudiScan(LogicalHudiScanOperator node, Void context) {
            return "LogicalHudiScanOperator" + " {" + "table=" + ((HudiTable) node.getTable()).getTableName() +
                    ", outputColumns=" + new ArrayList<>(node.getColRefToColumnMetaMap().keySet()) +
                    ", predicates=" + node.getScanOperatorPredicates() +
                    ", limit=" + node.getLimit() +
                    "}";
        }

        @Override
        public String visitLogicalMysqlScan(LogicalMysqlScanOperator node, Void context) {
            return "LogicalMysqlScanOperator" + " {" + "table=" + ((MysqlTable) node.getTable()).getMysqlTableName() +
                    ", outputColumns=" + new ArrayList<>(node.getColRefToColumnMetaMap().keySet()) +
                    ", predicate=" + node.getPredicate() +
                    ", limit=" + node.getLimit() +
                    "}";
        }

        @Override
        public String visitLogicalMetaScan(LogicalMetaScanOperator node, Void context) {
            return super.visitLogicalMetaScan(node, context);
        }

        @Override
        public String visitLogicalEsScan(LogicalEsScanOperator node, Void context) {
            return "LogicalEsScanOperator" + " {" + "selectedIndex=" + node.getSelectedIndex() +
                    ", outputColumns=" + new ArrayList<>(node.getColRefToColumnMetaMap().keySet()) +
                    ", predicate=" + node.getPredicate() +
                    ", limit=" + node.getLimit() +
                    "}";
        }

        @Override
        public String visitLogicalJDBCScan(LogicalJDBCScanOperator node, Void context) {
            return "LogicalJDBCScanOperator" + " {" + "table=" + ((JDBCTable) node.getTable()).getJdbcTable() +
                    ", outputColumns=" + new ArrayList<>(node.getColRefToColumnMetaMap().keySet()) +
                    ", predicate=" + node.getPredicate() +
                    ", limit=" + node.getLimit() +
                    "}";
        }

        @Override
        public String visitLogicalProject(LogicalProjectOperator node, Void context) {
            StringBuilder sb = new StringBuilder("LogicalProjectOperator {projection=");
            sb.append(new ArrayList<>(node.getColumnRefMap().values()));
            sb.append("}");
            return sb.toString();
        }

        @Override
        public String visitLogicalJoin(LogicalJoinOperator node, Void context) {
            return super.visitLogicalJoin(node, context);
        }

        @Override
        public String visitLogicalAggregation(LogicalAggregationOperator node, Void context) {
            return "LogicalAggregation" + " {type=" + node.getType() +
                    " ,aggregations=" + node.getAggregations() +
                    " ,groupKeys=" + node.getGroupingKeys() + "}";
        }

        @Override
        public String visitLogicalTopN(LogicalTopNOperator node, Void context) {
            return "LogicalTopNOperator" + " {phase=" + node.getSortPhase().toString() +
                    ", orderBy=" + node.getOrderByElements() +
                    ", limit=" + node.getLimit() +
                    ", offset=" + node.getOffset() +
                    "}";
        }

        @Override
        public String visitLogicalAssertOneRow(LogicalAssertOneRowOperator node, Void context) {
            return super.visitLogicalAssertOneRow(node, context);
        }

        @Override
        public String visitLogicalAnalytic(LogicalWindowOperator node, Void context) {
            StringBuilder sb = new StringBuilder("LogicalWindowOperator");
            sb.append(" {window=").append(node.getWindowCall());
            sb.append(", partitions=").append(node.getPartitionExpressions());
            sb.append(", orderBy=").append(node.getOrderByElements());
            sb.append(", enforceSort").append(node.getEnforceSortColumns());
            sb.append("}");
            return sb.toString();
        }

        @Override
        public String visitLogicalUnion(LogicalUnionOperator node, Void context) {
            return getSetOperationBuilder("LogicalUnionOperator", node.getOutputColumnRefOp(),
                    node.getChildOutputColumns());
        }

        @Override
        public String visitLogicalExcept(LogicalExceptOperator node, Void context) {
            return getSetOperationBuilder("LogicalExceptOperator", node.getOutputColumnRefOp(),
                    node.getChildOutputColumns());
        }

        @Override
        public String visitLogicalIntersect(LogicalIntersectOperator node, Void context) {
            return getSetOperationBuilder("LogicalIntersectOperator", node.getOutputColumnRefOp(),
                    node.getChildOutputColumns());
        }

        @Override
        public String visitLogicalValues(LogicalValuesOperator node, Void context) {
            return super.visitLogicalValues(node, context);
        }

        @Override
        public String visitLogicalRepeat(LogicalRepeatOperator node, Void context) {
            return super.visitLogicalRepeat(node, context);
        }

        @Override
        public String visitLogicalFilter(LogicalFilterOperator node, Void context) {
            return "LogicalFilterOperator" + " {" + "predicate=" + node.getPredicate() + "}";
        }

        @Override
        public String visitLogicalTableFunction(LogicalTableFunctionOperator node, Void context) {
            return super.visitLogicalTableFunction(node, context);
        }

        @Override
        public String visitLogicalLimit(LogicalLimitOperator node, Void context) {
            return "LogicalLimitOperator" + " {limit=" + node.getLimit() +
                    ", offset=" + node.getOffset() +
                    "}";
        }

        @Override
        public String visitLogicalCTEAnchor(LogicalCTEAnchorOperator node, Void context) {
            return super.visitLogicalCTEAnchor(node, context);
        }

        @Override
        public String visitLogicalCTEConsume(LogicalCTEConsumeOperator node, Void context) {
            return super.visitLogicalCTEConsume(node, context);
        }

        @Override
        public String visitLogicalCTEProduce(LogicalCTEProduceOperator node, Void context) {
            return super.visitLogicalCTEProduce(node, context);
        }

        @Override
        public String visitMockOperator(MockOperator node, Void context) {
            return super.visitMockOperator(node, context);
        }

        @Override
        public String visitPhysicalDistribution(PhysicalDistributionOperator node, Void context) {
            return "PhysicalDistributionOperator" + " {distributionSpec=" + node.getDistributionSpec() +
                    " ,globalDict=" + node.getGlobalDicts() +
                    "}";
        }

        @Override
        public String visitPhysicalProject(PhysicalProjectOperator node, Void context) {
            StringBuilder sb = new StringBuilder("PhysicalProjectOperator {projection=");
            sb.append(new ArrayList<>(node.getColumnRefMap().values()));
            sb.append("}");
            return sb.toString();
        }

        @Override
        public String visitPhysicalHashAggregate(PhysicalHashAggregateOperator node, Void context) {
            return "PhysicalHashAggregate" + " {type=" + node.getType() +
                    ", groupBy=" + node.getGroupBys() +
                    ", partitionBy=" + node.getPartitionByColumns() +
                    " ,aggregations=" + node.getAggregations() +
                    "}";
        }

        @Override
        public String visitPhysicalHashJoin(PhysicalHashJoinOperator node, Void context) {
            return super.visitPhysicalHashJoin(node, context);
        }

        @Override
        public String visitPhysicalNestLoopJoin(PhysicalNestLoopJoinOperator node, Void context) {
            return node.toString();
        }

        @Override
        public String visitPhysicalOlapScan(PhysicalOlapScanOperator node, Void context) {
            return "PhysicalOlapScanOperator" + " {" + "table=" + node.getTable().getId() +
                    ", selectedPartitionId=" + node.getSelectedPartitionId() +
                    ", outputColumns=" + node.getOutputColumns() +
                    ", projection=" + node.getProjection() +
                    ", predicate=" + node.getPredicate() +
                    ", limit=" + node.getLimit() +
                    "}";
        }

        @Override
        public String visitPhysicalHiveScan(PhysicalHiveScanOperator node, Void context) {
            return "PhysicalHiveScanOperator" + " {" + "table=" + ((HiveTable) node.getTable()).getTableName() +
                    ", outputColumns=" + new ArrayList<>(node.getColRefToColumnMetaMap().keySet()) +
                    ", predicates=" + node.getScanOperatorPredicates() +
                    ", limit=" + node.getLimit() +
                    "}";
        }

        @Override
        public String visitPhysicalIcebergScan(PhysicalIcebergScanOperator node, Void context) {
            StringBuilder sb = new StringBuilder("PhysicalIcebergScanOperator");
            sb.append(" {").append("table=").append(((IcebergTable) node.getTable()).getRemoteTableName())
                    .append(", outputColumns=").append(new ArrayList<>(node.getColRefToColumnMetaMap().keySet()))
                    .append(", predicates=").append(node.getScanOperatorPredicates())
                    .append("}");
            return sb.toString();
        }

        @Override
        public String visitPhysicalHudiScan(PhysicalHudiScanOperator node, Void context) {
            return "PhysicalHudiScanOperator" + " {" + "table=" + ((HudiTable) node.getTable()).getTableName() +
                    ", outputColumns=" + new ArrayList<>(node.getColRefToColumnMetaMap().keySet()) +
                    ", predicates=" + node.getScanOperatorPredicates() +
                    ", limit=" + node.getLimit() +
                    "}";
        }

        @Override
        public String visitPhysicalSchemaScan(PhysicalSchemaScanOperator node, Void context) {
            return super.visitPhysicalSchemaScan(node, context);
        }

        @Override
        public String visitPhysicalMysqlScan(PhysicalMysqlScanOperator node, Void context) {
            return "PhysicalMysqlScanOperator" + " {" + "table=" + ((MysqlTable) node.getTable()).getMysqlTableName() +
                    ", outputColumns=" + new ArrayList<>(node.getColRefToColumnMetaMap().keySet()) +
                    ", predicate=" + node.getPredicate() +
                    ", limit=" + node.getLimit() +
                    "}";
        }

        @Override
        public String visitPhysicalEsScan(PhysicalEsScanOperator node, Void context) {
            return "PhysicalEsScanOperator" + " {" + "selectedIndex=" + node.getSelectedIndex() +
                    ", outputColumns=" + new ArrayList<>(node.getColRefToColumnMetaMap().keySet()) +
                    ", predicate=" + node.getPredicate() +
                    ", limit=" + node.getLimit() +
                    "}";
        }

        @Override
        public String visitPhysicalMetaScan(PhysicalMetaScanOperator node, Void context) {
            return super.visitPhysicalMetaScan(node, context);
        }

        @Override
        public String visitPhysicalJDBCScan(PhysicalJDBCScanOperator node, Void context) {
            return "PhysicalJDBCScanOperator" + " {" + "table=" + ((JDBCTable) node.getTable()).getJdbcTable() +
                    ", outputColumns=" + new ArrayList<>(node.getColRefToColumnMetaMap().keySet()) +
                    ", predicate=" + node.getPredicate() +
                    ", limit=" + node.getLimit() +
                    "}";
        }

        @Override
        public String visitPhysicalTopN(PhysicalTopNOperator node, Void context) {
            return "PhysicalTopNOperator" + " {phase=" + node.getSortPhase() +
                    ", orderBy=" + node.getOrderSpec() +
                    ", limit=" + node.getLimit() +
                    ", offset=" + node.getOffset() +
                    "}";
        }

        @Override
        public String visitPhysicalAssertOneRow(PhysicalAssertOneRowOperator node, Void context) {
            return super.visitPhysicalAssertOneRow(node, context);
        }

        @Override
        public String visitPhysicalAnalytic(PhysicalWindowOperator node, Void context) {
            return super.visitPhysicalAnalytic(node, context);
        }

        @Override
        public String visitPhysicalUnion(PhysicalUnionOperator node, Void context) {
            return getSetOperationBuilder("PhysicalUnionOperator", node.getOutputColumnRefOp(),
                    node.getChildOutputColumns());
        }

        @Override
        public String visitPhysicalExcept(PhysicalExceptOperator node, Void context) {
            return getSetOperationBuilder("PhysicalExceptOperator", node.getOutputColumnRefOp(),
                    node.getChildOutputColumns());
        }

        @Override
        public String visitPhysicalIntersect(PhysicalIntersectOperator node, Void context) {
            return getSetOperationBuilder("PhysicalIntersectOperator", node.getOutputColumnRefOp(),
                    node.getChildOutputColumns());
        }

        @NotNull
        private String getSetOperationBuilder(String name, List<ColumnRefOperator> outputColumnRefOp,
                                              List<List<ColumnRefOperator>> childOutputColumns) {
            StringBuilder sb = new StringBuilder(name);
            sb.append("{");
            sb.append("output=[").append(outputColumnRefOp.stream().map(ColumnRefOperator::toString)
                    .collect(Collectors.joining(", "))).append("], ");

            String child = childOutputColumns.stream()
                    .map(l -> l.stream().map(ColumnRefOperator::toString).collect(Collectors.joining(", ")))
                    .collect(Collectors.joining(", "));

            sb.append(child).append("}");
            return sb.toString();
        }

        @Override
        public String visitPhysicalValues(PhysicalValuesOperator node, Void context) {
            return super.visitPhysicalValues(node, context);
        }

        @Override
        public String visitPhysicalRepeat(PhysicalRepeatOperator node, Void context) {
            return super.visitPhysicalRepeat(node, context);
        }

        @Override
        public String visitPhysicalFilter(PhysicalFilterOperator node, Void context) {
            return super.visitPhysicalFilter(node, context);
        }

        @Override
        public String visitPhysicalTableFunction(PhysicalTableFunctionOperator node, Void context) {
            return super.visitPhysicalTableFunction(node, context);
        }

        @Override
        public String visitPhysicalLimit(PhysicalLimitOperator node, Void context) {
            return "PhysicalLimitOperator" + " {limit=" + node.getLimit() +
                    ", offset=" + node.getOffset() +
                    "}";
        }

        @Override
        public String visitPhysicalCTEAnchor(PhysicalCTEAnchorOperator node, Void context) {
            return super.visitPhysicalCTEAnchor(node, context);
        }

        @Override
        public String visitPhysicalCTEProduce(PhysicalCTEProduceOperator node, Void context) {
            return super.visitPhysicalCTEProduce(node, context);
        }

        @Override
        public String visitPhysicalCTEConsume(PhysicalCTEConsumeOperator node, Void context) {
            return super.visitPhysicalCTEConsume(node, context);
        }

        @Override
        public String visitPhysicalNoCTE(PhysicalNoCTEOperator node, Void context) {
            return super.visitPhysicalNoCTE(node, context);
        }
    }

    public static class RelationTracePrinter extends AstVisitor<String, String> {
        @Override
        public String visit(ParseNode node) {
            return node == null ? "null" : node.toSql();
        }

        @Override
        public String visit(ParseNode node, String indent) {
            return node == null ? "null" : node.accept(this, indent);
        }

        @Override
        public String visitQueryStatement(QueryStatement statement, String indent) {
            return "QueryStatement{\n" + indent + "  queryRelation=" +
                    visit(statement.getQueryRelation(), indent + "  ") +
                    "\n" +
                    indent + "}";
        }

        @Override
        public String visitSelect(SelectRelation node, String indent) {
            return "SelectRelation{\n" + indent + "  selectList=" + node.getSelectList() + "\n" +
                    indent + "  fromRelation=" +
                    visit(node.getRelation(), indent + "  ") + "\n" +
                    indent + "  predicate=" +
                    visit(node.getPredicate()) + "\n" +
                    indent + "  groupByClause=" +
                    visit(node.getGroupByClause()) + "\n" +
                    indent + "  having=" +
                    visit(node.getHaving()) + "\n" +
                    indent + "  sortClause=" +
                    node.getOrderBy() + "\n" +
                    indent + "  limit=" +
                    visit(node.getLimit()) + "\n" +
                    indent + "}";
        }

        @Override
        public String visitJoin(JoinRelation node, String indent) {
            return "JoinRelation{" + "joinType=" + node.getJoinOp() +
                    ", left=" + visit(node.getLeft(), indent) +
                    ", right=" + visit(node.getRight(), indent) +
                    ", onPredicate=" + node.getOnPredicate() +
                    "}";
        }

        @Override
        public String visitSubquery(SubqueryRelation node, String indent) {
            return "SubqueryRelation{\n" + indent + "  alias=" +
                    (node.getAlias() == null ? "anonymous" : node.getAlias()) + "\n" +
                    indent + "  query=" +
                    visit(node.getQueryStatement(), indent + "  ") + "\n" +
                    indent + "}";
        }

        @Override
        public String visitUnion(UnionRelation node, String indent) {
            StringBuilder sb = new StringBuilder("UnionRelation{\n");
            sb.append(indent).append("relations=\n");
            for (QueryRelation relation : node.getRelations()) {
                sb.append(indent).append("  ").append(visit(relation, indent + "  ")).append("\n");
            }
            sb.append("}");
            return sb.toString();
        }

        @Override
        public String visitTable(TableRelation node, String indent) {
            return node.toString();
        }
    }
}
