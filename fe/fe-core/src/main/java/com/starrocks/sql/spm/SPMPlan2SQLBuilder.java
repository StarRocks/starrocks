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

package com.starrocks.sql.spm;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.sql.Expr2SQLPrinter;
import com.starrocks.sql.ast.HintNode;
import com.starrocks.sql.ast.expression.AnalyticWindow;
import com.starrocks.sql.common.UnsupportedException;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.base.DistributionSpec;
import com.starrocks.sql.optimizer.base.HashDistributionDesc;
import com.starrocks.sql.optimizer.base.HashDistributionSpec;
import com.starrocks.sql.optimizer.base.Ordering;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.SortPhase;
import com.starrocks.sql.optimizer.operator.physical.PhysicalCTEAnchorOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalCTEConsumeOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalDistributionOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalFilterOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashAggregateOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalLimitOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalRepeatOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalSetOperation;
import com.starrocks.sql.optimizer.operator.physical.PhysicalTableFunctionOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalTopNOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalValuesOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalWindowOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

// translate physical tree plan to SQL
public class SPMPlan2SQLBuilder {
    private long tableId = 0;

    private final ExprSQLBuilder exprSQLBuilder = new ExprSQLBuilder();

    private final SQLBuilder planSQLBuilder = new SQLBuilder();

    public String toSQL(List<HintNode> hints, OptExpression plan, List<ColumnRefOperator> outputColumn) {
        SQLRelation relation = plan.getOp().accept(planSQLBuilder, plan, null);
        if (hints != null && !hints.isEmpty()) {
            relation.hints = hints.stream().map(HintNode::toSql).collect(Collectors.joining(", "));
        }
        // keep select order
        List<Integer> outputIds = outputColumn.stream().map(ColumnRefOperator::getId).toList();
        List<Integer> selectIds = relation.selects.stream().map(Pair::getKey).toList();
        if (CollectionUtils.isEmpty(selectIds) || outputIds.equals(selectIds)) {
            return relation.toSQL(outputColumn);
        } else {
            String f = relation.toSQL();
            String select = outputColumn.stream()
                    .map(c -> relation.columnNames.get(c.getId()))
                    .collect(Collectors.joining(", "));
            return "SELECT " + select + " FROM (" + f + ") t" + (tableId++);
        }
    }

    private class SQLRelation {
        private final Map<Integer, String> columnNames = Maps.newHashMap();
        private List<String> cte = null;
        //        private Map<Integer, String> selects = Map.of();
        private List<Pair<Integer, String>> selects = List.of();
        private String hints = "";
        private String from = "";
        private String where = "";
        private String groupBy = "";
        private String having = "";
        private String orderBy = "";
        private String limit = "";
        private String groupings = "";
        private String relationName = null;
        // record table unused column name, avoid conflict
        private List<String> reserveNames = null;
        private boolean assertRows = false;

        private String registerRef(Integer cid, String alias) {
            columnNames.put(cid, alias);
            return alias;
        }

        private String registerRef(Integer cid) {
            String ref = "c_" + cid;
            columnNames.put(cid, ref);
            return ref;
        }

        private String newAlias() {
            relationName = "t_" + (tableId++);
            return relationName;
        }

        private String getRelationAlias() {
            return relationName == null ? from : relationName;
        }

        private String toRelationSQL() {
            if (relationName == null) {
                return from;
            }
            if (assertRows) {
                return "ASSERT_ROWS (" + toSQL() + ") " + relationName;
            }
            return "(" + toSQL() + ") " + relationName;
        }

        private String toSQL() {
            return toSQL(Collections.emptyList());
        }

        private String toSQL(List<ColumnRefOperator> outputs) {
            StringBuilder sql = new StringBuilder();
            if (!CollectionUtils.isEmpty(cte)) {
                sql.append("WITH ");
                sql.append(String.join(", ", cte));
                sql.append(" ");
            }
            sql.append("SELECT ");
            if (!StringUtils.isBlank(hints)) {
                sql.append(hints);
            }

            if (CollectionUtils.isEmpty(outputs)) {
                sql.append(CollectionUtils.isEmpty(selects) ? "*" :
                        selects.stream().map(Pair::getValue).collect(Collectors.joining(", ")));
            } else {
                Map<Integer, String> temp = CollectionUtils.isEmpty(selects) ?
                        columnNames :
                        selects.stream().collect(Collectors.toMap(Pair::getKey, Pair::getValue));
                sql.append(outputs.stream().map(p -> temp.get(p.getId())).collect(Collectors.joining(", ")));
            }

            sql.append(" FROM ");
            sql.append(from);
            if (!StringUtils.isBlank(where)) {
                sql.append(" WHERE ");
                sql.append(where);
            }
            if (!StringUtils.isBlank(groupBy)) {
                sql.append(" GROUP BY ");
                sql.append(groupBy);
            }
            if (!StringUtils.isBlank(having)) {
                sql.append(" HAVING ");
                sql.append(having);
            }
            if (!StringUtils.isBlank(orderBy)) {
                sql.append(" ORDER BY ");
                sql.append(orderBy);
            }
            if (!StringUtils.isBlank(limit)) {
                sql.append(" LIMIT ");
                sql.append(limit);
            }
            return sql.toString();
        }
    }

    private class SQLBuilder extends OptExpressionVisitor<SQLRelation, Void> {
        private final Map<Integer, String> cteColumnNames = Maps.newHashMap();
        private final Map<Integer, String> cteRelationNames = Maps.newHashMap();

        @Override
        public SQLRelation visit(OptExpression optExpression, Void context) {
            UnsupportedException.unsupportedException(
                    "SQLPlanManager doesn't support: " + optExpression.getOp().getOpType());
            return null;
        }

        public SQLRelation process(OptExpression optExpression) {
            return optExpression.getOp().accept(this, optExpression, null);
        }

        @Override
        public SQLRelation visitPhysicalJoin(OptExpression optExpression, Void context) {
            SQLRelation left = process(optExpression.getInputs().get(0));
            SQLRelation right = process(optExpression.getInputs().get(1));

            SQLRelation joinRelation = new SQLRelation();
            PhysicalJoinOperator join = optExpression.getOp().cast();
            String hints = StringUtils.isBlank(join.getJoinHint()) ? getJoinDistributionHints(optExpression) :
                    join.getJoinHint();

            // check column name conflicts
            if (StringUtils.equalsIgnoreCase(left.getRelationAlias(), right.getRelationAlias())) {
                left.newAlias();
                right.newAlias();
            }

            boolean columnConflicts = !Collections.disjoint(left.columnNames.values(), right.columnNames.values());
            if (!columnConflicts && left.reserveNames != null) {
                columnConflicts = !Collections.disjoint(left.reserveNames, right.columnNames.values());
            }
            if (!columnConflicts && right.reserveNames != null) {
                columnConflicts = !Collections.disjoint(right.reserveNames, left.columnNames.values());
            }

            if (columnConflicts) {
                left.columnNames.forEach((k, v) -> joinRelation.columnNames.put(k, left.getRelationAlias() + "." + v));
                right.columnNames.forEach(
                        (k, v) -> joinRelation.columnNames.put(k, right.getRelationAlias() + "." + v));
            } else {
                joinRelation.columnNames.putAll(left.columnNames);
                joinRelation.columnNames.putAll(right.columnNames);
            }
            joinRelation.from = left.toRelationSQL() + " " + join.getJoinType().toString() + "[" + hints + "] "
                    + right.toRelationSQL();
            if (join.getOnPredicate() != null) {
                joinRelation.from += " ON " + exprSQLBuilder.print(join.getOnPredicate(), joinRelation);
            }
            joinRelation.where = exprSQLBuilder.print(join.getPredicate(), joinRelation);

            if (join.getProjection() != null) {
                List<Integer> forces = columnConflicts ? Lists.newArrayList(joinRelation.columnNames.keySet()) :
                        Collections.emptyList();
                visitProjection(join.getProjection(), joinRelation, forces);
            } else if (columnConflicts) {
                List<Pair<Integer, String>> selects = Lists.newArrayList();
                for (Integer key : joinRelation.columnNames.keySet()) {
                    selects.add(Pair.of(key,
                            joinRelation.columnNames.get(key) + " AS " + joinRelation.registerRef(key)));
                }
                joinRelation.selects = selects;
            }
            joinRelation.newAlias();
            return joinRelation;
        }

        @Override
        public SQLRelation visitPhysicalHashAggregate(OptExpression optExpression, Void context) {
            PhysicalHashAggregateOperator agg = optExpression.getOp().cast();
            SQLRelation childRelation = process(optExpression.inputAt(0));
            if (agg.getType().isLocal() || agg.getType().isDistinctLocal()) {
                // update aggregate outputs function name
                for (var entry : agg.getAggregations().entrySet()) {
                    ColumnRefOperator key = entry.getKey();
                    CallOperator aggFn = entry.getValue();
                    String aggFnStr = exprSQLBuilder.print(aggFn, childRelation);
                    childRelation.registerRef(key.getId(), aggFnStr);
                }
                return childRelation;
            }

            // group by grouping sets
            SQLRelation aggRelation;
            List<Integer> aliasIds = Lists.newArrayList();
            List<Pair<Integer, String>> selects = Lists.newArrayList();

            if (!StringUtils.isEmpty(childRelation.groupings)) {
                aggRelation = childRelation;
                for (ColumnRefOperator groupBy : agg.getGroupBys()) {
                    aliasIds.add(groupBy.getId());
                    if (childRelation.columnNames.containsKey(groupBy.getId())) {
                        // grouping_id doesn't contains
                        selects.add(Pair.of(groupBy.getId(), exprSQLBuilder.print(groupBy, childRelation)));
                        aggRelation.registerRef(groupBy.getId(), childRelation.columnNames.get(groupBy.getId()));
                    }
                }
                aggRelation.groupBy = aggRelation.groupings;
            } else {
                aggRelation = new SQLRelation();
                aggRelation.from = childRelation.toRelationSQL();
                for (ColumnRefOperator groupBy : agg.getGroupBys()) {
                    Preconditions.checkState(childRelation.columnNames.containsKey(groupBy.getId()));
                    selects.add(Pair.of(groupBy.getId(), exprSQLBuilder.print(groupBy, childRelation)));
                    aggRelation.registerRef(groupBy.getId(), childRelation.columnNames.get(groupBy.getId()));
                }
                aggRelation.groupBy = selects.stream().map(Pair::getValue).collect(Collectors.joining(", "));
            }

            for (var entry : agg.getAggregations().entrySet()) {
                ColumnRefOperator key = entry.getKey();
                CallOperator aggFn = entry.getValue();
                String fn;
                if (childRelation.columnNames.containsKey(key.getId())) {
                    fn = exprSQLBuilder.print(key, childRelation);
                } else {
                    fn = exprSQLBuilder.print(aggFn, childRelation);
                }
                aggRelation.registerRef(key.getId(), fn);
                selects.add(Pair.of(key.getId(), fn));
                aliasIds.add(key.getId());
            }

            aggRelation.having = exprSQLBuilder.print(agg.getPredicate(), aggRelation);
            aggRelation.selects = selects;
            visitProjection(agg.getProjection(), aggRelation, aliasIds);
            aggRelation.newAlias();
            return aggRelation;
        }

        @Override
        public SQLRelation visitPhysicalDistribution(OptExpression optExpression, Void context) {
            return process(optExpression.inputAt(0));
        }

        @Override
        public SQLRelation visitPhysicalValues(OptExpression optExpression, Void context) {
            PhysicalValuesOperator values = optExpression.getOp().cast();
            SQLRelation relation = new SQLRelation();
            relation.from = values.getRows().stream()
                    .map(l -> "(" + l.stream().map(v -> exprSQLBuilder.print(v, relation))
                            .collect(Collectors.joining(", ")) + ")").collect(Collectors.joining(", "));
            relation.from = "(VALUES " + relation.from + ") AS t";
            relation.from += "(" + values.getColumnRefSet().stream().map(c -> relation.registerRef(c.getId()))
                    .collect(Collectors.joining(", ")) + ")";

            if (values.getRows() != null) {
                visitProjection(values.getProjection(), relation, Collections.emptyList());
                relation.newAlias();
            }
            return relation;
        }

        private SQLRelation visitPhysicalSet(OptExpression optExpression, String op) {
            PhysicalSetOperation set = optExpression.getOp().cast();
            SQLRelation setRelation = new SQLRelation();

            set.getOutputColumnRefOp().forEach(c -> setRelation.registerRef(c.getId()));
            List<String> children = Lists.newArrayList();
            for (int i = 0; i < optExpression.getInputs().size(); i++) {
                OptExpression child = optExpression.inputAt(i);
                SQLRelation relation = process(child);
                String childSQL = "";

                List<ColumnRefOperator> childOutputs = set.getChildOutputColumns().get(i);
                List<Pair<Integer, String>> childSelects = Lists.newArrayList();
                for (int index = 0; index < childOutputs.size(); index++) {
                    String alias = relation.columnNames.get(childOutputs.get(index).getId()) + " AS "
                            + setRelation.columnNames.get(set.getOutputColumnRefOp().get(index).getId());
                    childSelects.add(Pair.of(childOutputs.get(index).getId(), alias));
                }

                if (CollectionUtils.isEmpty(relation.selects)) {
                    relation.selects = childSelects;
                    childSQL += relation.toSQL();
                } else {
                    childSQL += "SELECT ";
                    childSQL += childSelects.stream().map(Pair::getValue).collect(Collectors.joining(", "));
                    childSQL += " FROM ";
                    childSQL += relation.toRelationSQL(); // don't need new relation
                }
                children.add(childSQL);
            }
            setRelation.from = "(" + String.join(" " + op + " ", children) + ") " + setRelation.newAlias();
            setRelation.newAlias();
            return setRelation;
        }

        @Override
        public SQLRelation visitPhysicalUnion(OptExpression optExpression, Void context) {
            return visitPhysicalSet(optExpression, "UNION ALL");
        }

        @Override
        public SQLRelation visitPhysicalExcept(OptExpression optExpression, Void context) {
            return visitPhysicalSet(optExpression, "EXCEPT");
        }

        @Override
        public SQLRelation visitPhysicalIntersect(OptExpression optExpression, Void context) {
            return visitPhysicalSet(optExpression, "INTERSECT");
        }

        @Override
        public SQLRelation visitPhysicalTableFunction(OptExpression optExpression, Void context) {
            SQLRelation child = process(optExpression.getInputs().get(0));
            PhysicalTableFunctionOperator tableFunction = optExpression.getOp().cast();

            SQLRelation result = new SQLRelation();

            StringBuilder sb = new StringBuilder();
            sb.append(child.toRelationSQL()).append(", ");
            sb.append(tableFunction.getFn().functionName());
            sb.append("(").append(tableFunction.getFnParamColumnRefs().stream()
                    .map(c -> child.columnNames.get(c.getId()))
                    .collect(Collectors.joining(", "))).append(")");
            sb.append(" AS ").append(result.newAlias());
            sb.append("(").append(tableFunction.getFnResultColRefs().stream()
                    .map(ColumnRefOperator::getName)
                    .collect(Collectors.joining(", "))).append(")");

            result.from = sb.toString();
            result.columnNames.putAll(child.columnNames);
            for (ColumnRefOperator ref : tableFunction.getFnResultColRefs()) {
                result.columnNames.put(ref.getId(), result.registerRef(ref.getId(), ref.getName()));
            }
            return result;
        }

        @Override
        public SQLRelation visitPhysicalLimit(OptExpression optExpression, Void context) {
            SQLRelation child = process(optExpression.getInputs().get(0));
            PhysicalLimitOperator limit = optExpression.getOp().cast();
            SQLRelation limitRelation;
            if (StringUtils.isEmpty(child.limit)) {
                limitRelation = child;
            } else {
                limitRelation = new SQLRelation();
                limitRelation.from = child.toRelationSQL();
                limitRelation.columnNames.putAll(child.columnNames);
                limitRelation.newAlias();
            }

            limitRelation.limit = limit.hasOffset() ? limit.getOffset() + ", " : "";
            limitRelation.limit += limit.getLimit();
            return limitRelation;
        }

        @Override
        public SQLRelation visitPhysicalTopN(OptExpression optExpression, Void context) {
            SQLRelation child = process(optExpression.getInputs().get(0));
            PhysicalTopNOperator topN = optExpression.getOp().cast();
            if (!CollectionUtils.isEmpty(topN.getPartitionByColumns()) || !StringUtils.isEmpty(child.groupings)
                    || topN.getSortPhase() == SortPhase.PARTIAL) {
                return child;
            }

            SQLRelation relation;
            if (StringUtils.isEmpty(child.limit) && StringUtils.isEmpty(child.orderBy)) {
                relation = child;
            } else {
                relation = new SQLRelation();
                relation.from = child.toRelationSQL();
                relation.columnNames.putAll(child.columnNames);
                relation.newAlias();
            }

            relation.limit = topN.getOffset() > 0 ? topN.getOffset() + ", " : "";
            relation.limit += topN.hasLimit() ? topN.getLimit() : "";
            List<String> orderBys = Lists.newArrayList();
            for (Ordering orderDesc : topN.getOrderSpec().getOrderDescs()) {
                orderBys.add(exprSQLBuilder.print(orderDesc.getColumnRef(), relation) + " " + (orderDesc.isAscending() ?
                        "ASC" : "DESC"));
            }
            relation.orderBy = String.join(", ", orderBys);
            return relation;
        }

        @Override
        public SQLRelation visitPhysicalAssertOneRow(OptExpression optExpression, Void context) {
            SQLRelation child = process(optExpression.getInputs().get(0));
            Preconditions.checkState(child.columnNames.size() == 1);

            child.assertRows = true;
            if (CollectionUtils.isEmpty(child.selects)) {
                // must one column
                List<Pair<Integer, String>> selects = Lists.newArrayList();
                child.columnNames.forEach((k, v) -> selects.add(Pair.of(k, v)));
                child.selects = selects;
            }
            child.newAlias();
            return child;
        }

        @Override
        public SQLRelation visitPhysicalFilter(OptExpression optExpression, Void context) {
            SQLRelation child = process(optExpression.getInputs().get(0));
            PhysicalFilterOperator filter = optExpression.getOp().cast();
            SQLRelation relation = new SQLRelation();
            relation.from = child.toRelationSQL();
            relation.where = exprSQLBuilder.print(filter.getPredicate(), child);
            relation.columnNames.putAll(child.columnNames);
            relation.newAlias();
            return relation;
        }

        @Override
        public SQLRelation visitPhysicalCTEAnchor(OptExpression optExpression, Void context) {
            PhysicalCTEAnchorOperator anchor = optExpression.getOp().cast();

            SQLRelation produce = process(optExpression.getInputs().get(0));
            produce.newAlias();
            cteRelationNames.put(anchor.getCteId(), produce.getRelationAlias());
            cteColumnNames.putAll(produce.columnNames);

            SQLRelation consume = process(optExpression.getInputs().get(1));
            if (consume.cte == null) {
                consume.cte = Lists.newArrayList();
            }
            consume.cte.add(produce.getRelationAlias() + " AS (" + produce.toSQL() + ")");
            return consume;
        }

        @Override
        public SQLRelation visitPhysicalCTEConsume(OptExpression optExpression, Void context) {
            PhysicalCTEConsumeOperator consume = optExpression.getOp().cast();
            SQLRelation relation = new SQLRelation();
            relation.from = cteRelationNames.get(consume.getCteId());
            consume.getCteOutputColumnRefMap()
                    .forEach((k, v) -> relation.registerRef(k.getId(), cteColumnNames.get(v.getId())));
            relation.where = exprSQLBuilder.print(consume.getPredicate(), relation);
            visitProjection(consume.getProjection(), relation, Collections.emptyList());
            if (consume.getPredicate() != null || consume.getProjection() != null) {
                relation.newAlias();
            }
            return relation;
        }

        @Override
        public SQLRelation visitPhysicalCTEProduce(OptExpression optExpression, Void context) {
            return process(optExpression.getInputs().get(0));
        }

        @Override
        public SQLRelation visitPhysicalNoCTE(OptExpression optExpression, Void context) {
            return process(optExpression.getInputs().get(0));
        }

        private void visitProjection(Projection projection, SQLRelation relation, List<Integer> forceAlias) {
            if (projection == null) {
                if (CollectionUtils.isEmpty(forceAlias)) {
                    return;
                }
                // 1. update force alias
                List<Pair<Integer, String>> selects = Lists.newArrayList();
                for (Integer key : relation.columnNames.keySet()) {
                    String v = relation.columnNames.get(key);
                    if (forceAlias.contains(key)) {
                        selects.add(Pair.of(key, v + " AS " + relation.registerRef(key)));
                    } else {
                        selects.add(Pair.of(key, v));
                    }
                }
                relation.selects = selects;
                return;
            }

            Map<ColumnRefOperator, ScalarOperator> project = projection.getColumnRefMap();
            List<Integer> saveColumnIds = Lists.newArrayList();
            List<String> selects = Lists.newArrayList();
            List<String> alias = Lists.newArrayList();

            // 1. print first
            project.forEach((k, v) -> {
                saveColumnIds.add(k.getId());
                selects.add(exprSQLBuilder.print(v, relation));
            });

            // 2. register & override alias
            project.forEach((k, v) -> {
                if (!k.equals(v) || forceAlias.contains(k.getId())) {
                    alias.add(" AS " + relation.registerRef(k.getId()));
                } else {
                    alias.add("");
                }
            });

            // 3. combine to select
            Preconditions.checkState(saveColumnIds.size() == selects.size());
            Preconditions.checkState(saveColumnIds.size() == alias.size());

            relation.selects = Lists.newArrayList();
            for (int i = 0; i < saveColumnIds.size(); i++) {
                relation.selects.add(Pair.of(saveColumnIds.get(i), selects.get(i) + alias.get(i)));
            }

            relation.columnNames.entrySet().removeIf(e -> !saveColumnIds.contains(e.getKey()));
        }

        @Override
        public SQLRelation visitPhysicalAnalytic(OptExpression optExpression, Void context) {
            SQLRelation child = process(optExpression.inputAt(0));
            PhysicalWindowOperator window = optExpression.getOp().cast();

            //            Preconditions.checkState(!StringUtils.isEmpty(child.orderBy));
            Preconditions.checkState(window.getPredicate() == null);
            child.orderBy = "";

            SQLRelation relation = new SQLRelation();
            relation.from = child.toRelationSQL();

            String frame = "";
            if (!CollectionUtils.isEmpty(window.getPartitionExpressions())) {
                frame += "PARTITION BY " + window.getPartitionExpressions().stream()
                        .map(p -> exprSQLBuilder.print(p, child)).collect(Collectors.joining(", "));
                frame += " ";
            }
            if (!CollectionUtils.isEmpty(window.getOrderByElements())) {
                frame += "ORDER BY " + window.getOrderByElements().stream()
                        .map(o -> exprSQLBuilder.print(o.getColumnRef(), child) + " " + (o.isAscending() ? "ASC" :
                                "DESC")).collect(Collectors.joining(", "));
                frame += " ";
            }
            if (window.getAnalyticWindow() != null && !AnalyticWindow.DEFAULT_WINDOW.equals(
                    window.getAnalyticWindow())) {
                frame += window.getAnalyticWindow().toSql();
            }
            frame = " OVER (" + frame + ")";

            List<Integer> analytics = Lists.newArrayList();
            relation.columnNames.putAll(child.columnNames);
            for (var entry : window.getAnalyticCall().entrySet()) {
                ColumnRefOperator key = entry.getKey();
                CallOperator value = entry.getValue();
                relation.registerRef(key.getId(), exprSQLBuilder.print(value, child) + frame);
                analytics.add(key.getId());
            }

            visitProjection(window.getProjection(), relation, analytics);
            relation.newAlias();
            return relation;
        }

        @Override
        public SQLRelation visitPhysicalRepeat(OptExpression optExpression, Void context) {
            SQLRelation relation = process(optExpression.getInputs().get(0));
            PhysicalRepeatOperator repeat = optExpression.getOp().cast();

            SQLRelation groupingRelation = new SQLRelation();
            groupingRelation.from = relation.toRelationSQL();
            groupingRelation.columnNames.putAll(relation.columnNames);
            for (ColumnRefOperator grouping : repeat.getOutputGrouping()) {
                if ("GROUPING_ID".equals(grouping.getName())) {
                    continue;
                }
                Preconditions.checkState("GROUPING".equals(grouping.getName()));
                Preconditions.checkState(repeat.getGroupingsFnArgs().containsKey(grouping));
                String fn = "GROUPING(" + repeat.getGroupingsFnArgs().get(grouping).stream()
                        .map(p -> exprSQLBuilder.print(p, relation)).collect(Collectors.joining(", ")) + ")";
                groupingRelation.registerRef(grouping.getId(), fn);
            }
            List<String> groupings = Lists.newArrayList();
            for (var group : repeat.getRepeatColumnRef()) {
                groupings.add("(" + group.stream().map(c -> exprSQLBuilder.print(c, relation))
                        .collect(Collectors.joining(", ")) + ")");
            }
            groupingRelation.groupings = "GROUPING SETS(" + String.join(", ", groupings) + ")";
            return groupingRelation;
        }

        @Override
        public SQLRelation visitPhysicalScan(OptExpression optExpression, Void context) {
            SQLRelation relation = new SQLRelation();
            PhysicalScanOperator scan = optExpression.getOp().cast();
            relation.from = scan.getTable().getName();
            scan.getColRefToColumnMetaMap().forEach((k, v) -> relation.registerRef(k.getId(), v.getName()));
            relation.where = exprSQLBuilder.print(scan.getPredicate(), relation);
            visitProjection(scan.getProjection(), relation, Collections.emptyList());

            if (scan.getPredicate() != null || scan.getProjection() != null) {
                relation.newAlias();
                return relation;
            }
            relation.reserveNames = Lists.newArrayList();
            scan.getTable().getColumns().forEach(c -> relation.reserveNames.add(c.getName()));
            return relation;
        }

        @Override
        public SQLRelation visitPhysicalOlapScan(OptExpression optExpression, Void context) {
            SQLRelation relation = visitPhysicalScan(optExpression, context);
            PhysicalOlapScanOperator olap = optExpression.getOp().cast();
            if (CollectionUtils.isEmpty(olap.getPrunedPartitionPredicates())) {
                return relation;
            }
            // prune partition predicate
            String predicate = olap.getPrunedPartitionPredicates().stream().map(p -> exprSQLBuilder.print(p, relation))
                    .collect(Collectors.joining(" AND "));
            if (StringUtils.isEmpty(relation.where)) {
                relation.where = predicate;
            } else {
                relation.where = relation.where + " AND " + predicate;
            }
            return relation;
        }

        // 1. left: exchange, right: exchange
        //  a. hash shuffle
        // 2. left: exchange, right: local
        //  a. shuffle bucket
        // 3. left: local, right: exchange
        //  a. broadcast
        //  b. local bucket
        // 4. left: local, right: local
        //  a. colocate
        //  b. shuffle bucket
        public String getJoinDistributionHints(OptExpression optExpression) {
            boolean leftExchange = optExpression.inputAt(0).getOp().getOpType() == OperatorType.PHYSICAL_DISTRIBUTION;
            boolean rightExchange = optExpression.inputAt(1).getOp().getOpType() == OperatorType.PHYSICAL_DISTRIBUTION;
            if (leftExchange && rightExchange) {
                PhysicalDistributionOperator left = optExpression.inputAt(0).getOp().cast();
                PhysicalDistributionOperator right = optExpression.inputAt(1).getOp().cast();
                Preconditions.checkState(
                        left.getDistributionSpec().getType() == DistributionSpec.DistributionType.SHUFFLE);
                Preconditions.checkState(
                        right.getDistributionSpec().getType() == DistributionSpec.DistributionType.SHUFFLE);
                return HintNode.HINT_JOIN_SHUFFLE;
            } else if (leftExchange) {
                return HintNode.HINT_JOIN_SHUFFLE;
            } else if (rightExchange) {
                PhysicalDistributionOperator right = optExpression.inputAt(1).getOp().cast();
                if (right.getDistributionSpec().getType() == DistributionSpec.DistributionType.BROADCAST) {
                    return HintNode.HINT_JOIN_BROADCAST;
                } else {
                    Preconditions.checkState(
                            right.getDistributionSpec().getType() == DistributionSpec.DistributionType.SHUFFLE);
                    return HintNode.HINT_JOIN_BUCKET;
                }
            } else {
                Preconditions.checkState(optExpression.getRequiredProperties().stream()
                        .allMatch(p -> p.getDistributionProperty().isShuffle()));
                if (optExpression.getRequiredProperties().stream().allMatch(p -> {
                    HashDistributionSpec spec = p.getDistributionProperty().getSpec().cast();
                    return HashDistributionDesc.SourceType.LOCAL.equals(spec.getHashDistributionDesc().getSourceType());
                })) {
                    return HintNode.HINT_JOIN_COLOCATE;
                } else {
                    return HintNode.HINT_JOIN_SHUFFLE;
                }
            }
        }
    }

    private static class ExprSQLBuilder extends Expr2SQLPrinter<SQLRelation> {
        @Override
        public String print(ScalarOperator scalarOperator) {
            UnsupportedException.unsupportedException("SQLPlanManager doesn't support: " + scalarOperator);
            return null;
        }

        @Override
        public String print(ScalarOperator scalarOperator, SQLRelation context) {
            if (scalarOperator == null) {
                return "";
            }
            return super.print(scalarOperator, context);
        }

        @Override
        public String visitVariableReference(ColumnRefOperator variable, SQLRelation context) {
            return context.columnNames.get(variable.getId());
        }

        @Override
        public String visitCall(CallOperator call, SQLRelation context) {
            if (SPMFunctions.isSPMFunctions(call)) {
                List<String> children =
                        call.getChildren().stream().map(c -> print(c, context)).collect(Collectors.toList());
                return SPMFunctions.toSQL(call.getFnName(), children);
            }
            return super.visitCall(call, context);
        }
    }
}
