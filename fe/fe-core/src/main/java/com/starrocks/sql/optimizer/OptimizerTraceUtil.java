// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

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
            sb.append("Original Expression:\n" + oldExpression.explain());
            sb.append("New Expression:\n");
            if (newExpressions.isEmpty()) {
                sb.append("Empty\n");
            } else {
                for (int i = 0; i < newExpressions.size(); i++) {
                    sb.append(i + ":\n" + newExpressions.get(i).explain());
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
            StringBuilder sb = new StringBuilder("LogicalOlapScanOperator");
            sb.append(" {").append("table=").append(node.getTable().getId())
                    .append(", selectedParitionId=").append(node.getSelectedPartitionId())
                    .append(", outputColumns=").append(new ArrayList<>(node.getColRefToColumnMetaMap().keySet()))
                    .append(", predicate=").append(node.getPredicate())
                    .append(", limit=").append(node.getLimit())
                    .append("}");
            return sb.toString();
        }

        @Override
        public String visitLogicalHiveScan(LogicalHiveScanOperator node, Void context) {
            StringBuilder sb = new StringBuilder("LogicalHiveScanOperator");
            sb.append(" {").append("table=").append(((HiveTable) node.getTable()).getTableName())
                    .append(", outputColumns=").append(new ArrayList<>(node.getColRefToColumnMetaMap().keySet()))
                    .append(", predicates=").append(node.getScanOperatorPredicates())
                    .append(", limit=").append(node.getLimit())
                    .append("}");
            return sb.toString();
        }

        @Override
        public String visitLogicalIcebergScan(LogicalIcebergScanOperator node, Void context) {
            StringBuilder sb = new StringBuilder("LogicalIcebergScanOperator");
            sb.append(" {").append("table=").append(((IcebergTable) node.getTable()).getTable())
                    .append(", outputColumns=").append(new ArrayList<>(node.getColRefToColumnMetaMap().keySet()))
                    .append(", conjuncts=").append(node.getConjuncts())
                    .append(", minmaxConjuncts=").append(node.getMinMaxConjuncts())
                    .append("}");
            return sb.toString();
        }

        @Override
        public String visitLogicalHudiScan(LogicalHudiScanOperator node, Void context) {
            StringBuilder sb = new StringBuilder("LogicalHudiScanOperator");
            sb.append(" {").append("table=").append(((HudiTable) node.getTable()).getTable())
                    .append(", outputColumns=").append(new ArrayList<>(node.getColRefToColumnMetaMap().keySet()))
                    .append(", predicates=").append(node.getScanOperatorPredicates())
                    .append(", limit=").append(node.getLimit())
                    .append("}");
            return sb.toString();
        }

        @Override
        public String visitLogicalMysqlScan(LogicalMysqlScanOperator node, Void context) {
            StringBuilder sb = new StringBuilder("LogicalMysqlScanOperator");
            sb.append(" {").append("table=").append(((MysqlTable) node.getTable()).getMysqlTableName())
                    .append(", outputColumns=").append(new ArrayList<>(node.getColRefToColumnMetaMap().keySet()))
                    .append(", predicate=").append(node.getPredicate())
                    .append(", limit=").append(node.getLimit())
                    .append("}");
            return sb.toString();
        }

        @Override
        public String visitLogicalMetaScan(LogicalMetaScanOperator node, Void context) {
            return super.visitLogicalMetaScan(node, context);
        }

        @Override
        public String visitLogicalEsScan(LogicalEsScanOperator node, Void context) {
            StringBuilder sb = new StringBuilder("LogicalEsScanOperator");
            sb.append(" {").append("selectedIndex=").append(node.getSelectedIndex())
                    .append(", outputColumns=").append(new ArrayList<>(node.getColRefToColumnMetaMap().keySet()))
                    .append(", predicate=").append(node.getPredicate())
                    .append(", limit=").append(node.getLimit())
                    .append("}");
            return sb.toString();
        }

        @Override
        public String visitLogicalJDBCScan(LogicalJDBCScanOperator node, Void context) {
            StringBuilder sb = new StringBuilder("LogicalJDBCScanOperator");
            sb.append(" {").append("table=").append(((JDBCTable) node.getTable()).getJdbcTable())
                    .append(", outputColumns=").append(new ArrayList<>(node.getColRefToColumnMetaMap().keySet()))
                    .append(", predicate=").append(node.getPredicate())
                    .append(", limit=").append(node.getLimit())
                    .append("}");
            return sb.toString();
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
            StringBuilder sb = new StringBuilder("LogicalAggregation");
            sb.append(" {type=").append(node.getType());
            sb.append(" ,aggregations=").append(node.getAggregations());
            sb.append(" ,groupKeys=").append(node.getGroupingKeys()).append("}");
            return sb.toString();
        }

        @Override
        public String visitLogicalTopN(LogicalTopNOperator node, Void context) {
            StringBuilder sb = new StringBuilder("LogicalTopNOperator");
            sb.append(" {phase=").append(node.getSortPhase().toString())
                    .append(", orderBy=").append(node.getOrderByElements())
                    .append(", limit=").append(node.getLimit())
                    .append(", offset=").append(node.getOffset());
            sb.append("}");
            return sb.toString();
        }

        @Override
        public String visitLogicalAssertOneRow(LogicalAssertOneRowOperator node, Void context) {
            return super.visitLogicalAssertOneRow(node, context);
        }

        @Override
        public String visitLogicalAnalytic(LogicalWindowOperator node, Void context) {
            return super.visitLogicalAnalytic(node, context);
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
            StringBuilder sb = new StringBuilder("LogicalFilterOperator");
            sb.append(" {").append("predicate=").append(node.getPredicate()).append("}");
            return sb.toString();
        }

        @Override
        public String visitLogicalTableFunction(LogicalTableFunctionOperator node, Void context) {
            return super.visitLogicalTableFunction(node, context);
        }

        @Override
        public String visitLogicalLimit(LogicalLimitOperator node, Void context) {
            StringBuilder sb = new StringBuilder("LogicalLimitOperator");
            sb.append(" {limit=").append(node.getLimit())
                    .append(", offset=").append(node.getOffset())
                    .append("}");
            return sb.toString();
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
            StringBuilder sb = new StringBuilder("PhysicalDistributionOperator");
            sb.append(" {distributionSpec=").append(node.getDistributionSpec())
                    .append(" ,globalDict=").append(node.getGlobalDicts());
            sb.append("}");
            return sb.toString();
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
            StringBuilder sb = new StringBuilder("PhysicalHashAggregate");
            sb.append(" {type=").append(node.getType())
                    .append(", groupBy=").append(node.getGroupBys())
                    .append(", partitionBy=").append(node.getPartitionByColumns())
                    .append(" ,aggregations=").append(node.getAggregations());
            sb.append("}");
            return sb.toString();
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
            StringBuilder sb = new StringBuilder("PhysicalOlapScanOperator");
            sb.append(" {").append("table=").append(node.getTable().getId())
                    .append(", selectedPartitionId=").append(node.getSelectedPartitionId())
                    .append(", outputColumns=").append(node.getOutputColumns())
                    .append(", projection=").append(node.getProjection())
                    .append(", predicate=").append(node.getPredicate())
                    .append(", limit=").append(node.getLimit())
                    .append("}");
            return sb.toString();
        }

        @Override
        public String visitPhysicalHiveScan(PhysicalHiveScanOperator node, Void context) {
            StringBuilder sb = new StringBuilder("PhysicalHiveScanOperator");
            sb.append(" {").append("table=").append(((HiveTable) node.getTable()).getTableName())
                    .append(", outputColumns=").append(new ArrayList<>(node.getColRefToColumnMetaMap().keySet()))
                    .append(", predicates=").append(node.getScanOperatorPredicates())
                    .append(", limit=").append(node.getLimit())
                    .append("}");
            return sb.toString();
        }

        @Override
        public String visitPhysicalIcebergScan(PhysicalIcebergScanOperator node, Void context) {
            StringBuilder sb = new StringBuilder("PhysicalIcebergScanOperator");
            sb.append(" {").append("table=").append(((IcebergTable) node.getTable()).getTable())
                    .append(", outputColumns=").append(new ArrayList<>(node.getColRefToColumnMetaMap().keySet()))
                    .append(", conjuncts=").append(node.getConjuncts())
                    .append(", minmaxConjuncts=").append(node.getMinMaxConjuncts())
                    .append("}");
            return sb.toString();
        }

        @Override
        public String visitPhysicalHudiScan(PhysicalHudiScanOperator node, Void context) {
            StringBuilder sb = new StringBuilder("PhysicalHudiScanOperator");
            sb.append(" {").append("table=").append(((HudiTable) node.getTable()).getTable())
                    .append(", outputColumns=").append(new ArrayList<>(node.getColRefToColumnMetaMap().keySet()))
                    .append(", predicates=").append(node.getScanOperatorPredicates())
                    .append(", limit=").append(node.getLimit())
                    .append("}");
            return sb.toString();
        }

        @Override
        public String visitPhysicalSchemaScan(PhysicalSchemaScanOperator node, Void context) {
            return super.visitPhysicalSchemaScan(node, context);
        }

        @Override
        public String visitPhysicalMysqlScan(PhysicalMysqlScanOperator node, Void context) {
            StringBuilder sb = new StringBuilder("PhysicalMysqlScanOperator");
            sb.append(" {").append("table=").append(((MysqlTable) node.getTable()).getMysqlTableName())
                    .append(", outputColumns=").append(new ArrayList<>(node.getColRefToColumnMetaMap().keySet()))
                    .append(", predicate=").append(node.getPredicate())
                    .append(", limit=").append(node.getLimit())
                    .append("}");
            return sb.toString();
        }

        @Override
        public String visitPhysicalEsScan(PhysicalEsScanOperator node, Void context) {
            StringBuilder sb = new StringBuilder("PhysicalEsScanOperator");
            sb.append(" {").append("selectedIndex=").append(node.getSelectedIndex())
                    .append(", outputColumns=").append(new ArrayList<>(node.getColRefToColumnMetaMap().keySet()))
                    .append(", predicate=").append(node.getPredicate())
                    .append(", limit=").append(node.getLimit())
                    .append("}");
            return sb.toString();
        }

        @Override
        public String visitPhysicalMetaScan(PhysicalMetaScanOperator node, Void context) {
            return super.visitPhysicalMetaScan(node, context);
        }

        @Override
        public String visitPhysicalJDBCScan(PhysicalJDBCScanOperator node, Void context) {
            StringBuilder sb = new StringBuilder("PhysicalJDBCScanOperator");
            sb.append(" {").append("table=").append(((JDBCTable) node.getTable()).getJdbcTable())
                    .append(", outputColumns=").append(new ArrayList<>(node.getColRefToColumnMetaMap().keySet()))
                    .append(", predicate=").append(node.getPredicate())
                    .append(", limit=").append(node.getLimit())
                    .append("}");
            return sb.toString();
        }

        @Override
        public String visitPhysicalTopN(PhysicalTopNOperator node, Void context) {
            StringBuilder sb = new StringBuilder("PhysicalTopNOperator");
            sb.append(" {phase=").append(node.getSortPhase())
                    .append(", orderBy=").append(node.getOrderSpec())
                    .append(", limit=").append(node.getLimit())
                    .append(", offset=").append(node.getOffset());
            sb.append("}");
            return sb.toString();
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
            StringBuilder sb = new StringBuilder("PhysicalLimitOperator");
            sb.append(" {limit=").append(node.getLimit())
                    .append(", offset=").append(node.getOffset())
                    .append("}");
            return sb.toString();
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
            StringBuilder sb = new StringBuilder("QueryStatement{\n");
            sb.append(indent).append("  queryRelation=")
                    .append(visit(statement.getQueryRelation(), indent + "  "))
                    .append("\n");
            sb.append(indent).append("}");
            return sb.toString();
        }

        @Override
        public String visitSelect(SelectRelation node, String indent) {
            StringBuilder sb = new StringBuilder("SelectRelation{\n");
            sb.append(indent).append("  selectList=").append(node.getSelectList()).append("\n");
            sb.append(indent).append("  fromRelation=")
                    .append(visit(node.getRelation(), indent + "  ")).append("\n");
            sb.append(indent).append("  predicate=")
                    .append(visit(node.getPredicate())).append("\n");
            sb.append(indent).append("  groupByClause=")
                    .append(visit(node.getGroupByClause())).append("\n");
            sb.append(indent).append("  having=")
                    .append(visit(node.getHaving())).append("\n");
            sb.append(indent).append("  sortClause=")
                    .append(node.getSortClause()).append("\n");
            sb.append(indent).append("  limit=")
                    .append(visit(node.getLimit())).append("\n");
            sb.append(indent).append("}");
            return sb.toString();
        }

        @Override
        public String visitJoin(JoinRelation node, String indent) {
            StringBuilder sb = new StringBuilder("JoinRelation{");
            sb.append("joinType=").append(node.getJoinOp());
            sb.append(", left=").append(visit(node.getLeft(), indent));
            sb.append(", right=").append(visit(node.getRight(), indent));
            sb.append(", onPredicate=").append(node.getOnPredicate());
            sb.append("}");
            return sb.toString();
        }

        @Override
        public String visitSubquery(SubqueryRelation node, String indent) {
            StringBuilder sb = new StringBuilder("SubqueryRelation{\n");
            sb.append(indent).append("  alias=")
                    .append(node.getAlias() == null ? "anonymous" : node.getAlias()).append("\n");
            sb.append(indent).append("  query=")
                    .append(visit(node.getQueryStatement(), indent + "  ")).append("\n");
            sb.append(indent).append("}");
            return sb.toString();
        }

        @Override
        public String visitUnion(UnionRelation node, String indent) {
            StringBuilder sb = new StringBuilder("UnionRelation{\n");
            sb.append(indent).append("relations=\n");
            for (QueryRelation relation : node.getRelations()) {
                sb.append(indent + "  ").append(visit(relation, indent + "  ")).append("\n");
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
