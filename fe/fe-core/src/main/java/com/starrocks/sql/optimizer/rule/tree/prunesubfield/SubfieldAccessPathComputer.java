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

package com.starrocks.sql.optimizer.rule.tree.prunesubfield;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.ColumnAccessPath;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorBuilderFactory;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEConsumeOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;

import java.util.List;
import java.util.Map;
import java.util.Set;

/*
 * compute all complex column access path in whole plan, we only need check the expression
 * which one in project and predicate (the rule execute order promise this)
 */
class SubfieldAccessPathComputer extends OptExpressionVisitor<OptExpression, Void> {
    private final Map<ScalarOperator, ColumnRefSet> allComplexColumns = Maps.newHashMap();

    private void visitPredicate(OptExpression optExpression) {
        if (optExpression.getOp().getPredicate() == null) {
            return;
        }

        SubfieldExpressionCollector collector = new SubfieldExpressionCollector();
        optExpression.getOp().getPredicate().accept(collector, null);
        for (ScalarOperator expr : collector.getComplexExpressions()) {
            allComplexColumns.put(expr, expr.getUsedColumns());
        }
    }

    private OptExpression visitChildren(OptExpression optExpression, Void context) {
        for (int i = optExpression.getInputs().size() - 1; i >= 0; i--) {
            OptExpression child = optExpression.inputAt(i);
            optExpression.setChild(i, child.getOp().accept(this, child, context));
        }
        return optExpression;
    }

    @Override
    public OptExpression visit(OptExpression optExpression, Void context) {
        visitPredicate(optExpression);
        return visitChildren(optExpression, context);
    }

    @Override
    public OptExpression visitLogicalProject(OptExpression optExpression, Void context) {
        LogicalProjectOperator lpo = optExpression.getOp().cast();
        rewriteColumnsInPaths(lpo.getColumnRefMap(), true);

        // collect complex column
        SubfieldExpressionCollector collector = new SubfieldExpressionCollector();
        for (ScalarOperator value : lpo.getColumnRefMap().values()) {
            value.accept(collector, null);
        }

        ColumnRefSet allRefs = new ColumnRefSet();
        allComplexColumns.values().forEach(allRefs::union);

        for (ScalarOperator expr : collector.getComplexExpressions()) {
            // check repeat put complex column, like that
            //      project( columnB: structA.b.c.d )
            //         |
            //      project( structA: structA ) -- structA use for `structA.b.c.d`, so we don't need put it again
            //         |
            //       .....
            if (expr.isColumnRef() && allRefs.contains((ColumnRefOperator) expr)) {
                continue;
            }
            allComplexColumns.put(expr, expr.getUsedColumns());
        }

        return visitChildren(optExpression, context);
    }

    private void rewriteColumnsInPaths(Map<ColumnRefOperator, ScalarOperator> mappingRefs, boolean deleteOrigin) {
        // check & rewrite nest expression
        Set<ColumnRefOperator> keys = mappingRefs.keySet();
        List<ScalarOperator> complexColumns = Lists.newArrayList(allComplexColumns.keySet());
        ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(mappingRefs, true);
        for (ScalarOperator expr : complexColumns) {
            ColumnRefSet usedColumns = allComplexColumns.get(expr);

            // need rewrite
            if (usedColumns.containsAny(keys)) {
                if (deleteOrigin) {
                    allComplexColumns.remove(expr);
                }
                expr = rewriter.rewrite(expr);
                allComplexColumns.put(expr, expr.getUsedColumns());
            }
        }
    }

    @Override
    public OptExpression visitLogicalJoin(OptExpression optExpression, Void context) {
        visitPredicate(optExpression);
        LogicalJoinOperator join = optExpression.getOp().cast();
        if (join.getOnPredicate() != null) {
            SubfieldExpressionCollector collector = new SubfieldExpressionCollector();
            join.getOnPredicate().accept(collector, null);
            for (ScalarOperator expr : collector.getComplexExpressions()) {
                allComplexColumns.put(expr, expr.getUsedColumns());
            }
        }
        return visitChildren(optExpression, context);
    }

    @Override
    public OptExpression visitLogicalAggregate(OptExpression optExpression, Void context) {
        visitPredicate(optExpression);
        LogicalAggregationOperator agg = optExpression.getOp().cast();
        SubfieldExpressionCollector collector = new SubfieldExpressionCollector();
        agg.getGroupingKeys().forEach(s -> s.accept(collector, null));
        for (ScalarOperator expr : collector.getComplexExpressions()) {
            allComplexColumns.put(expr, expr.getUsedColumns());
        }
        return visitChildren(optExpression, context);
    }

    @Override
    public OptExpression visitLogicalTableScan(OptExpression optExpression, Void context) {
        visitPredicate(optExpression);

        // normalize access path
        LogicalScanOperator scan = optExpression.getOp().cast();
        Set<ColumnRefOperator> scanColumns = scan.getColRefToColumnMetaMap().keySet();

        SubfieldAccessPathNormalizer normalizer = new SubfieldAccessPathNormalizer();

        allComplexColumns.forEach((k, v) -> {
            if (v.containsAny(scanColumns)) {
                normalizer.add(k);
            }
        });

        List<ColumnAccessPath> accessPaths = Lists.newArrayList();

        for (ColumnRefOperator ref : scanColumns) {
            if (!normalizer.hasPath(ref)) {
                continue;
            }
            String columnName = scan.getColRefToColumnMetaMap().get(ref).getName();
            ColumnAccessPath p = normalizer.normalizePath(ref, columnName);

            if (p.hasChildPath()) {
                accessPaths.add(p);
            }
        }

        if (accessPaths.isEmpty()) {
            return optExpression;
        }

        LogicalScanOperator.Builder builder = OperatorBuilderFactory.build(scan);
        Operator newScan = builder.withOperator(scan).setColumnAccessPaths(accessPaths).build();
        return OptExpression.create(newScan, optExpression.getInputs());
    }

    @Override
    public OptExpression visitLogicalCTEConsume(OptExpression optExpression, Void context) {
        LogicalCTEConsumeOperator cte = optExpression.getOp().cast();
        visitPredicate(optExpression);
        Map<ColumnRefOperator, ScalarOperator> projectMap = Maps.newHashMap();
        projectMap.putAll(cte.getCteOutputColumnRefMap());

        rewriteColumnsInPaths(projectMap, false);

        return visitChildren(optExpression, context);
    }

    @Override
    public OptExpression visitLogicalUnion(OptExpression optExpression, Void context) {
        visitPredicate(optExpression);

        LogicalUnionOperator union = optExpression.getOp().cast();
        for (List<ColumnRefOperator> childOutputColumn : union.getChildOutputColumns()) {
            Map<ColumnRefOperator, ScalarOperator> project = Maps.newHashMap();
            for (int i = 0; i < union.getOutputColumnRefOp().size(); i++) {
                project.put(union.getOutputColumnRefOp().get(i), childOutputColumn.get(i));
            }

            rewriteColumnsInPaths(project, false);
        }
        return visitChildren(optExpression, context);
    }

    @Override
    public OptExpression visitLogicalExcept(OptExpression optExpression, Void context) {
        // except/intersect should check whole value, can't prune subfield
        allComplexColumns.clear();
        return visitChildren(optExpression, context);
    }

    @Override
    public OptExpression visitLogicalIntersect(OptExpression optExpression, Void context) {
        allComplexColumns.clear();
        return visitChildren(optExpression, context);
    }

}
