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
import com.starrocks.analysis.AnalyticWindow;
import com.starrocks.analysis.HintNode;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.sql.ExpressionPrinter;
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
import com.starrocks.sql.optimizer.operator.physical.PhysicalTopNOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalValuesOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalWindowOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

// translate physical tree plan to SQL
public class SPMPlan2SQLBuilder {
    private long tableId = 0;

    private final ExprSQLBuilder exprSQLBuilder = new ExprSQLBuilder();

    private final SQLBuilder planSQLBuilder = new SQLBuilder();

    public String toSQL(List<HintNode> hints, OptExpression plan) {
        SQLRelation relation = plan.getOp().accept(planSQLBuilder, plan, null);
        if (hints != null && !hints.isEmpty()) {
            relation.hints = hints.stream().map(HintNode::toSql).collect(Collectors.joining(", "));
        }
        return relation.toSQL();
    }

    private class SQLRelation {
        private final Map<Integer, String> columnNames = Maps.newHashMap();
        private final List<String> cte = Lists.newArrayList();
        private List<Integer> groupingIds = Lists.newArrayList();
        private String select = "";
        private String hints = "";
        private String from = "";
        private String where = "";
        private String groupBy = "";
        private String having = "";
        private String orderBy = "";
        private String limit = "";
        private String groupings = "";
        private String relationName = null;

        private String registerRef(Integer cid, String alias) {
            columnNames.put(cid, alias);
            return alias;
        }

        private String registerRef(Integer cid) {
            String ref = "c_" + cid;
            columnNames.put(cid, ref);
            return ref;
        }

        private void newAlias() {
            relationName = "t_" + (tableId++);
        }

        private String toRelationSQL() {
            if (relationName == null) {
                return from;
            }
            return "(" + toSQL() + ") " + relationName;
        }

        private String toSQL() {
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
            sql.append(StringUtils.isBlank(select) ? "*" : select);
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
            SQLRelation mergeRelation = new SQLRelation();
            PhysicalJoinOperator join = optExpression.getOp().cast();
            String hints = StringUtils.isBlank(join.getJoinHint()) ? getJoinDistributionHints(optExpression) :
                    join.getJoinHint();

            // check column name conflicts
            if (StringUtils.equalsIgnoreCase(left.relationName, right.relationName)) {
                left.newAlias();
                right.newAlias();
            }

            boolean columnConflicts = !Collections.disjoint(left.columnNames.values(), right.columnNames.values());
            if (columnConflicts) {
                left.columnNames.forEach((k, v) -> {
                    mergeRelation.columnNames.put(k, left.relationName + "." + v);
                    joinRelation.registerRef(k);
                });
                right.columnNames.forEach((k, v) -> {
                    mergeRelation.columnNames.put(k, right.relationName + "." + v);
                    joinRelation.registerRef(k);
                });
            } else {
                mergeRelation.columnNames.putAll(left.columnNames);
                mergeRelation.columnNames.putAll(right.columnNames);
                joinRelation.columnNames.putAll(mergeRelation.columnNames);
            }

            joinRelation.from = left.toRelationSQL() + " " + join.getJoinType().toString() + "[" + hints + "] "
                    + right.toRelationSQL();
            if (join.getOnPredicate() != null) {
                joinRelation.from += " ON " + exprSQLBuilder.print(join.getOnPredicate(), mergeRelation);
            }
            joinRelation.where = exprSQLBuilder.print(join.getPredicate(), mergeRelation);

            if (join.getProjection() != null) {
                List<String> selects = Lists.newArrayList();
                Map<ColumnRefOperator, ScalarOperator> project = join.getProjection().getColumnRefMap();
                project.forEach((k, v) -> {
                    if (k.equals(v)) {
                        String alias = columnConflicts ? " AS " + joinRelation.columnNames.get(k.getId()) : "";
                        selects.add(exprSQLBuilder.print(v, mergeRelation) + alias);
                    } else {
                        String alias = joinRelation.registerRef(k.getId());
                        selects.add(exprSQLBuilder.print(v, mergeRelation) + " AS " + alias);
                    }
                });
                joinRelation.select = String.join(", ", selects);
            } else if (columnConflicts) {
                List<String> selects = Lists.newArrayList();
                left.columnNames.forEach((k, v) -> selects.add(v + " AS " + joinRelation.columnNames.get(k)));
                right.columnNames.forEach((k, v) -> selects.add(v + " AS " + joinRelation.columnNames.get(k)));
                joinRelation.select = String.join(", ", selects);
            }
            joinRelation.newAlias();
            return joinRelation;
        }

        @Override
        public SQLRelation visitPhysicalHashAggregate(OptExpression optExpression, Void context) {
            PhysicalHashAggregateOperator agg = optExpression.getOp().cast();
            SQLRelation childRelation = process(optExpression.inputAt(0));
            if (!agg.getType().isGlobal() && !agg.getType().isDistinctGlobal()) {
                return childRelation;
            }

            // group by grouping sets
            SQLRelation aggRelation;
            List<String> selects = Lists.newArrayList();

            if (!CollectionUtils.isEmpty(childRelation.groupingIds)) {
                aggRelation = childRelation;

                aggRelation.groupBy = aggRelation.groupings;
                
            } else {
                aggRelation = new SQLRelation();
                aggRelation.from = childRelation.toRelationSQL();
                for (ColumnRefOperator groupBy : agg.getGroupBys()) {
                    Preconditions.checkState(childRelation.columnNames.containsKey(groupBy.getId()));
                    selects.add(exprSQLBuilder.print(groupBy, childRelation));
                    aggRelation.registerRef(groupBy.getId(), childRelation.columnNames.get(groupBy.getId()));
                }
                aggRelation.groupBy = String.join(", ", selects);
            }

            for (var entry : agg.getAggregations().entrySet()) {
                ColumnRefOperator key = entry.getKey();
                CallOperator aggFn = entry.getValue();
                String alias = aggRelation.registerRef(key.getId());
                selects.add(exprSQLBuilder.print(aggFn, childRelation) + " AS " + alias);
            }

            aggRelation.having = exprSQLBuilder.print(agg.getPredicate(), aggRelation);
            aggRelation.select = String.join(", ", selects);

            if (agg.getProjection() == null) {
                aggRelation.newAlias();
                return aggRelation;
            }
            SQLRelation projectRelation = new SQLRelation();
            projectRelation.from = aggRelation.toRelationSQL();
            visitProjection(agg.getProjection(), aggRelation);
            projectRelation.newAlias();
            return projectRelation;
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
            relation.newAlias();
            return relation;
        }

        private SQLRelation visitPhysicalSet(OptExpression optExpression, String op) {
            PhysicalSetOperation set = optExpression.getOp().cast();
            SQLRelation setRelation = new SQLRelation();

            List<String> children = Lists.newArrayList();
            for (int i = 0; i < optExpression.getInputs().size(); i++) {
                OptExpression child = optExpression.inputAt(i);
                List<ColumnRefOperator> childOutputs = set.getChildOutputColumns().get(i);

                SQLRelation relation = process(child);
                String childSQL = "SELECT " + childOutputs.stream().map(c -> relation.columnNames.get(c.getId()))
                        .collect(Collectors.joining(", ")) + " FROM ";
                childSQL += relation.toRelationSQL();
                children.add(childSQL);
            }

            setRelation.newAlias();
            setRelation.from = "(" + String.join(" " + op + " ", children) + ") " + setRelation.relationName;
            setRelation.select = set.getOutputColumnRefOp().stream().map(c -> setRelation.registerRef(c.getId()))
                    .collect(Collectors.joining(", "));
            setRelation.newAlias();
            return setRelation;
        }

        @Override
        public SQLRelation visitPhysicalUnion(OptExpression optExpression, Void context) {
            return visitPhysicalSet(optExpression, "UNION");
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
            if (!CollectionUtils.isEmpty(topN.getPartitionByColumns()) || topN.getSortPhase() == SortPhase.PARTIAL) {
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
        public SQLRelation visitPhysicalFilter(OptExpression optExpression, Void context) {
            SQLRelation child = process(optExpression.getInputs().get(0));
            PhysicalFilterOperator filter = optExpression.getOp().cast();
            SQLRelation relation = new SQLRelation();
            relation.from = child.toRelationSQL();
            relation.where = exprSQLBuilder.print(filter.getPredicate(), child);
            relation.columnNames.putAll(child.columnNames);
            return relation;
        }

        @Override
        public SQLRelation visitPhysicalCTEAnchor(OptExpression optExpression, Void context) {
            PhysicalCTEAnchorOperator anchor = optExpression.getOp().cast();

            SQLRelation produce = process(optExpression.getInputs().get(0));
            produce.newAlias();
            cteRelationNames.put(anchor.getCteId(), produce.relationName);
            cteColumnNames.putAll(produce.columnNames);

            SQLRelation consume = process(optExpression.getInputs().get(1));
            consume.cte.add(produce.relationName + " AS (" + produce.toSQL() + ")");
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
            visitProjection(consume.getProjection(), relation);

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

        private void visitProjection(Projection projection, SQLRelation relation) {
            if (projection == null) {
                return;
            }
            List<String> selects = Lists.newArrayList();
            Map<ColumnRefOperator, ScalarOperator> project = projection.getColumnRefMap();
            if (project.entrySet().stream().anyMatch(e -> !e.getKey().equals(e.getValue()))) {
                project.forEach((k, v) -> {
                    if (k.equals(v)) {
                        selects.add(exprSQLBuilder.print(v, relation));
                    } else {
                        String alias = relation.registerRef(k.getId());
                        selects.add(exprSQLBuilder.print(v, relation) + " AS " + alias);
                    }
                });
                relation.select = String.join(", ", selects);
            }
        }

        @Override
        public SQLRelation visitPhysicalAnalytic(OptExpression optExpression, Void context) {
            SQLRelation child = process(optExpression.inputAt(0));
            PhysicalWindowOperator window = optExpression.getOp().cast();

            Preconditions.checkState(!StringUtils.isEmpty(child.orderBy));
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

            List<String> selects = Lists.newArrayList();
            selects.addAll(child.columnNames.values());
            relation.columnNames.putAll(child.columnNames);

            for (var entry : window.getAnalyticCall().entrySet()) {
                ColumnRefOperator key = entry.getKey();
                CallOperator value = entry.getValue();
                String alias = relation.registerRef(key.getId());
                selects.add(exprSQLBuilder.print(value, child) + frame + " AS " + alias);
            }
            relation.select = String.join(", ", selects);
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

            return groupingRelation;
        }

        @Override
        public SQLRelation visitPhysicalScan(OptExpression optExpression, Void context) {
            SQLRelation relation = new SQLRelation();
            PhysicalScanOperator scan = optExpression.getOp().cast();
            relation.from = scan.getTable().getName();
            scan.getColRefToColumnMetaMap().forEach((k, v) -> relation.registerRef(k.getId(), v.getName()));
            relation.where = exprSQLBuilder.print(scan.getPredicate(), relation);
            visitProjection(scan.getProjection(), relation);

            if (scan.getPredicate() != null || scan.getProjection() != null) {
                relation.newAlias();
            }
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
                return JoinOperator.HINT_SHUFFLE;
            } else if (leftExchange) {
                return JoinOperator.HINT_SHUFFLE;
            } else if (rightExchange) {
                PhysicalDistributionOperator right = optExpression.inputAt(1).getOp().cast();
                if (right.getDistributionSpec().getType() == DistributionSpec.DistributionType.BROADCAST) {
                    return JoinOperator.HINT_BROADCAST;
                } else {
                    Preconditions.checkState(
                            right.getDistributionSpec().getType() == DistributionSpec.DistributionType.SHUFFLE);
                    return JoinOperator.HINT_BUCKET;
                }
            } else {
                Preconditions.checkState(optExpression.getRequiredProperties().stream()
                        .anyMatch(p -> !p.getDistributionProperty().isShuffle()));
                if (optExpression.getRequiredProperties().stream().allMatch(p -> {
                    HashDistributionSpec spec = p.getDistributionProperty().getSpec().cast();
                    return HashDistributionDesc.SourceType.LOCAL.equals(spec.getHashDistributionDesc().getSourceType());
                })) {
                    return JoinOperator.HINT_COLOCATE;
                } else {
                    return JoinOperator.HINT_SHUFFLE;
                }
            }
        }
    }

    private static class ExprSQLBuilder extends ExpressionPrinter<SQLRelation> {
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
                        call.getChildren().stream().map(c -> visit(c, context)).collect(Collectors.toList());
                return SPMFunctions.toSQL(call.getFnName(), children);
            }
            return super.visitCall(call, context);
        }
    }
}
