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

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.JDBCTable;
import com.starrocks.connector.jdbc.JDBCPushDownSQLBuilder;
import com.starrocks.sql.ast.expression.ExprUtils;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJDBCScanOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.CanPushDownPredicateVisitor;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.type.Type;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class PushDownAggToJDBCScanRule extends TransformationRule {
    private static final String JDBC_AGG_ALIAS_PREFIX = "jdbc_agg_";
    private static final Set<String> SUPPORTED_JDBC_AGG_FUNCTIONS = Set.of(
            FunctionSet.COUNT,
            FunctionSet.SUM,
            FunctionSet.MIN,
            FunctionSet.MAX,
            FunctionSet.AVG);

    public PushDownAggToJDBCScanRule() {
        super(RuleType.TF_PUSH_DOWN_AGG_TO_JDBC_SCAN, Pattern.create(OperatorType.LOGICAL_AGGR)
                .addChildren(Pattern.create(OperatorType.LOGICAL_JDBC_SCAN)));
    }

    private LogicalJDBCScanOperator getScanOperator(OptExpression input) {
        return input.inputAt(0).getOp().cast();
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        if (!context.getSessionVariable().isEnableJdbcAggPushDown()) {
            return false;
        }
        LogicalAggregationOperator aggregationOperator = input.getOp().cast();
        LogicalJDBCScanOperator scanOperator = getScanOperator(input);
        // Cheap structural gate only; the full feasibility analysis runs once in transform().
        return canPushDownAggregate(aggregationOperator, scanOperator);
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalAggregationOperator aggregationOperator = input.getOp().cast();
        LogicalJDBCScanOperator scanOperator = getScanOperator(input);
        AggregatePushDown pushDown = buildAggregatePushDown(aggregationOperator, scanOperator);
        if (pushDown == null) {
            // An empty result means "not applied" — returning the input would count as a
            // change and keep an iterative rewrite pass spinning.
            return Lists.newArrayList();
        }

        Map<ColumnRefOperator, Column> newColRefToColumnMetaMap = Maps.newLinkedHashMap();
        Map<Column, ColumnRefOperator> newColumnMetaToColRefMap = Maps.newLinkedHashMap();
        List<Column> outputColumns = Lists.newArrayList();
        List<ScalarOperator> selectItems = Lists.newArrayList();
        List<String> selectAliases = Lists.newArrayList();
        Set<String> outputColumnNames = Sets.newHashSet();
        int aggregateAliasIndex = 0;

        for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : pushDown.outputColumnRefToExpr.entrySet()) {
            ColumnRefOperator outputColumnRef = entry.getKey();
            ScalarOperator outputExpr = entry.getValue();
            String aggregateAlias = null;
            if (outputExpr instanceof CallOperator) {
                do {
                    aggregateAlias = JDBC_AGG_ALIAS_PREFIX + aggregateAliasIndex++;
                } while (outputColumnNames.contains(aggregateAlias.toLowerCase(Locale.ROOT)));
            }
            String columnName = outputExpr instanceof CallOperator ? aggregateAlias : outputColumnRef.getName();
            Column outputColumn = JDBCPushDownRuleUtils.createOutputColumn(
                    outputColumnRef, outputExpr, scanOperator, columnName);

            newColRefToColumnMetaMap.put(outputColumnRef, outputColumn);
            newColumnMetaToColRefMap.put(outputColumn, outputColumnRef);
            outputColumns.add(outputColumn);
            outputColumnNames.add(outputColumn.getName().toLowerCase(Locale.ROOT));
            selectItems.add(outputExpr);
            selectAliases.add(outputColumn.getName());
        }

        JDBCTable jdbcTable = (JDBCTable) scanOperator.getTable();
        String pushDownQuery = JDBCPushDownSQLBuilder.buildScalarSelectQuery(
                scanOperator, selectItems, selectAliases,
                pushDown.groupByExprs,
                Utils.extractConjuncts(pushDown.havingPredicate));
        JDBCTable queryTable = new JDBCTable(jdbcTable);
        queryTable.setNewFullSchema(outputColumns);
        queryTable.setPushDownQuery(pushDownQuery);

        LogicalJDBCScanOperator newScanOperator = new LogicalJDBCScanOperator.Builder()
                .withOperator(scanOperator)
                .setTable(queryTable)
                .setLimit(aggregationOperator.getLimit())
                .setPredicate(null)
                .setProjection(aggregationOperator.getProjection())
                .setColRefToColumnMetaMap(newColRefToColumnMetaMap)
                .setColumnMetaToColRefMap(newColumnMetaToColRefMap)
                .build();
        return Lists.newArrayList(OptExpression.create(newScanOperator));
    }

    private AggregatePushDown buildAggregatePushDown(
            LogicalAggregationOperator aggregationOperator,
            LogicalJDBCScanOperator scanOperator) {

        Map<ColumnRefOperator, ScalarOperator> outputColumnRefToExpr = Maps.newLinkedHashMap();
        List<ScalarOperator> groupByExprs = Lists.newArrayList();
        for (ColumnRefOperator groupBy : aggregationOperator.getGroupingKeys()) {
            ScalarOperator groupByExpr = groupBy;
            if (!isSimpleJDBCColumnRef(groupByExpr, scanOperator)) {
                return null;
            }
            outputColumnRefToExpr.put(groupBy, groupByExpr);
            groupByExprs.add(groupByExpr);
        }

        for (Map.Entry<ColumnRefOperator, CallOperator> aggregation :
                aggregationOperator.getAggregations().entrySet()) {
            ScalarOperator rewrittenAggregateCall = aggregation.getValue();
            if (!(rewrittenAggregateCall instanceof CallOperator)) {
                return null;
            }
            CallOperator aggregateCall = normalizeJDBCAggregate((CallOperator) rewrittenAggregateCall, scanOperator);
            if (aggregateCall == null) {
                return null;
            }
            if (!isSupportedJDBCAggregate(aggregateCall, scanOperator)) {
                return null;
            }
            outputColumnRefToExpr.put(aggregation.getKey(), aggregateCall);
        }

        if (outputColumnRefToExpr.isEmpty()) {
            return null;
        }

        ScalarOperator havingPredicate = null;
        if (aggregationOperator.getPredicate() != null) {
            ReplaceColumnRefRewriter havingRewriter = new ReplaceColumnRefRewriter(outputColumnRefToExpr, true);
            havingPredicate = havingRewriter.rewrite(aggregationOperator.getPredicate());
            JDBCTable.ProtocolType dialect = ((JDBCTable) scanOperator.getTable()).getProtocolType();
            if (!CanPushDownPredicateVisitor.canPushDown(havingPredicate, dialect)) {
                return null;
            }
        }
        return new AggregatePushDown(outputColumnRefToExpr, groupByExprs, havingPredicate);
    }

    private boolean canPushDownAggregate(LogicalAggregationOperator aggregationOperator,
                                         LogicalJDBCScanOperator scanOperator) {
        boolean isPartitionBySameWithGroupBy =
                Objects.equals(aggregationOperator.getPartitionByColumns(), aggregationOperator.getGroupingKeys());
        if (aggregationOperator.hasRemoveDistinctFunc() ||
                aggregationOperator.isTopNLocalAgg() ||
                aggregationOperator.getAggTopnSortInfo() != null || !isPartitionBySameWithGroupBy ||
                !JDBCPushDownRuleUtils.isColumnPruningOnly(scanOperator.getProjection())) {
            return false;
        }
        return true;
    }

    private boolean isSimpleJDBCColumnRef(ScalarOperator scalarOperator, LogicalJDBCScanOperator scanOperator) {
        return scalarOperator instanceof ColumnRefOperator &&
                scanOperator.getColRefToColumnMetaMap().containsKey((ColumnRefOperator) scalarOperator);
    }

    private CallOperator normalizeJDBCAggregate(CallOperator aggregation, LogicalJDBCScanOperator scanOperator) {
        String fnName = aggregation.getFnName().toLowerCase(Locale.ROOT);
        if (!FunctionSet.MULTI_DISTINCT_COUNT.equals(fnName)) {
            return aggregation;
        }
        if (aggregation.getArguments().size() != 1 || !isSimpleJDBCColumnRef(aggregation.getChild(0), scanOperator)) {
            return null;
        }

        Function countFn = ExprUtils.getBuiltinFunction(FunctionSet.COUNT,
                new Type[] {aggregation.getChild(0).getType()}, Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        return new CallOperator(FunctionSet.COUNT, aggregation.getType(), aggregation.getArguments(), countFn, true);
    }

    private boolean isSupportedJDBCAggregate(CallOperator aggregation, LogicalJDBCScanOperator scanOperator) {
        if (!aggregation.isAggregate() || aggregation.isRemovedDistinct() || aggregation.getIgnoreNulls()) {
            return false;
        }
        String fnName = aggregation.getFnName().toLowerCase(Locale.ROOT);
        if (!SUPPORTED_JDBC_AGG_FUNCTIONS.contains(fnName)) {
            return false;
        }
        if (aggregation.isDistinct()) {
            return FunctionSet.COUNT.equals(fnName) && !aggregation.isCountStar() &&
                    aggregation.getArguments().size() == 1 &&
                    isSimpleJDBCColumnRef(aggregation.getChild(0), scanOperator);
        }
        if (aggregation.isCountStar()) {
            return true;
        }
        return aggregation.getArguments().size() == 1 &&
                (isSimpleJDBCColumnRef(aggregation.getChild(0), scanOperator) ||
                        aggregation.getChild(0) instanceof ConstantOperator);
    }

    private static class AggregatePushDown {
        private final Map<ColumnRefOperator, ScalarOperator> outputColumnRefToExpr;
        private final List<ScalarOperator> groupByExprs;
        private final ScalarOperator havingPredicate;

        private AggregatePushDown(Map<ColumnRefOperator, ScalarOperator> outputColumnRefToExpr,
                                  List<ScalarOperator> groupByExprs,
                                  ScalarOperator havingPredicate) {
            this.outputColumnRefToExpr = outputColumnRefToExpr;
            this.groupByExprs = groupByExprs;
            this.havingPredicate = havingPredicate;
        }
    }
}
