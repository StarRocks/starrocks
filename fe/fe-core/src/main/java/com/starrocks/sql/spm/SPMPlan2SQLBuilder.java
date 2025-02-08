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
import com.starrocks.analysis.JoinOperator;
import com.starrocks.sql.ExpressionPrinter;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.DistributionSpec;
import com.starrocks.sql.optimizer.base.HashDistributionDesc;
import com.starrocks.sql.optimizer.base.HashDistributionSpec;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.physical.PhysicalDistributionOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import org.apache.commons.lang3.StringUtils;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

// translate physical tree plan to SQL
public class SPMPlan2SQLBuilder {
    private long tableId = 0;

    private final ExprSQLBuilder exprSQLBuilder = new ExprSQLBuilder();

    private final SQLBuilder planSQLBuilder = new SQLBuilder();

    public String toSQL(ColumnRefFactory factory, OptExpression plan) {
        PlanToSQLContext context = new PlanToSQLContext();
        SQLRelation relation = plan.getOp().accept(planSQLBuilder, plan, context);
        return relation.toSQL();
    }

    private static class SQLRelation {
        String cte = "";
        String select = "";
        String from = "";
        String where = "";
        String groupBy = "";
        String having = "";
        String orderBy = "";
        String limit = "";
        String relationName = null;

        private boolean isOnlyScan() {
            return StringUtils.isAllBlank(cte, select, where, groupBy, having, orderBy, limit);
        }

        private String toRelationSQL() {
            if (isOnlyScan()) {
                return from + (from.equals(relationName) ? "" : " " + relationName);
            }
            return "(" + toSQL() + ") " + relationName;
        }

        private String toSQL() {
            StringBuilder sql = new StringBuilder();
            sql.append("SELECT ");
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

    private static class PlanToSQLContext {
        private SQLRelation current;

        private final LinkedList<SQLRelation> relationStack = new LinkedList<>();

        private final Map<Integer, String> columnNames = Maps.newHashMap();

        private SQLRelation registerRelation() {
            relationStack.addLast(new SQLRelation());
            current = relationStack.getLast();
            return relationStack.getLast();
        }

        private void registerRef(Integer cid, String alias) {
            columnNames.put(cid, alias);
        }
    }

    private class SQLBuilder extends OptExpressionVisitor<SQLRelation, PlanToSQLContext> {

        private String getTableAlias() {
            return "t_" + (tableId++);
        }

        private String getColumnAlias(long ref) {
            return "c_" + ref;
        }

        @Override
        public SQLRelation visit(OptExpression optExpression, PlanToSQLContext context) {
            Preconditions.checkState(false);
            return null;
        }

        public SQLRelation process(OptExpression optExpression, PlanToSQLContext context) {
            return optExpression.getOp().accept(this, optExpression, context);
        }

        @Override
        public SQLRelation visitPhysicalJoin(OptExpression optExpression, PlanToSQLContext context) {
            PlanToSQLContext leftContext = new PlanToSQLContext();
            PlanToSQLContext rightContext = new PlanToSQLContext();

            SQLRelation left = process(optExpression.getInputs().get(0), leftContext);
            SQLRelation right = process(optExpression.getInputs().get(1), rightContext);

            SQLRelation joinRelation = context.registerRelation();
            PhysicalJoinOperator join = optExpression.getOp().cast();
            String hints = StringUtils.isBlank(join.getJoinHint()) ? getJoinDistributionHints(optExpression) :
                    join.getJoinHint();

            // check column name conflicts
            if (StringUtils.equalsIgnoreCase(left.relationName, right.relationName)) {
                left.relationName = getTableAlias();
                right.relationName = getTableAlias();
            }

            PlanToSQLContext joinContext = new PlanToSQLContext();
            boolean columnConflicts =
                    !Collections.disjoint(leftContext.columnNames.values(), rightContext.columnNames.values());
            if (columnConflicts) {
                leftContext.columnNames.forEach((k, v) -> {
                    joinContext.columnNames.put(k, left.relationName + "." + v);
                    context.registerRef(k, getColumnAlias(k));
                });
                rightContext.columnNames.forEach((k, v) -> {
                    joinContext.columnNames.put(k, right.relationName + "." + v);
                    context.registerRef(k, getColumnAlias(k));
                });
            } else {
                joinContext.columnNames.putAll(leftContext.columnNames);
                joinContext.columnNames.putAll(rightContext.columnNames);
                context.columnNames.putAll(joinContext.columnNames);
            }

            joinRelation.from = left.toRelationSQL() + " " +
                    join.getJoinType().toString() + "[" + hints + "] " +
                    right.toRelationSQL() + " ON " + exprSQLBuilder.print(join.getOnPredicate(), joinContext);
            joinRelation.where = exprSQLBuilder.print(join.getPredicate(), joinContext);

            if (join.getProjection() != null) {
                List<String> selects = Lists.newArrayList();
                Map<ColumnRefOperator, ScalarOperator> project = join.getProjection().getColumnRefMap();
                project.forEach((k, v) -> {
                    if (k.equals(v)) {
                        String alias = columnConflicts ? " AS " + context.columnNames.get(k.getId()) : "";
                        selects.add(exprSQLBuilder.print(v, joinContext) + alias);
                    } else {
                        String alias = getColumnAlias(k.getId());
                        context.registerRef(k.getId(), alias);
                        selects.add(exprSQLBuilder.print(v, joinContext) + " AS " + alias);
                    }
                });
                joinRelation.select = String.join(", ", selects);
            } else if (columnConflicts) {
                List<String> selects = Lists.newArrayList();
                leftContext.columnNames.forEach((k, v) -> selects.add(v + " AS " + context.columnNames.get(k)));
                rightContext.columnNames.forEach((k, v) -> selects.add(v + " AS " + context.columnNames.get(k)));
                joinRelation.select = String.join(", ", selects);
            }

            return joinRelation;
        }

        @Override
        public SQLRelation visitPhysicalDistribution(OptExpression optExpression, PlanToSQLContext context) {
            return process(optExpression.inputAt(0), context);
        }

        private void visitCommon(PhysicalOperator operator, PlanToSQLContext context) {
            SQLRelation relation = context.current;
            relation.where = exprSQLBuilder.print(operator.getPredicate(), context);
            if (operator.getProjection() != null) {
                List<String> selects = Lists.newArrayList();
                Map<ColumnRefOperator, ScalarOperator> project = operator.getProjection().getColumnRefMap();
                if (project.entrySet().stream().anyMatch(e -> !e.getKey().equals(e.getValue()))) {
                    project.forEach((k, v) -> {
                        if (k.equals(v)) {
                            selects.add(exprSQLBuilder.print(v, context));
                        } else {
                            String alias = getColumnAlias(k.getId());
                            context.registerRef(k.getId(), alias);
                            selects.add(exprSQLBuilder.print(v, context) + " AS " + alias);
                        }
                    });
                    relation.select = String.join(", ", selects);
                }
            }
            if (!relation.isOnlyScan()) {
                relation.relationName = getTableAlias();
            }
        }

        @Override
        public SQLRelation visitPhysicalScan(OptExpression optExpression, PlanToSQLContext context) {
            SQLRelation relation = context.registerRelation();
            PhysicalScanOperator scan = optExpression.getOp().cast();
            relation.from = scan.getTable().getName();
            relation.relationName = relation.from;
            scan.getColRefToColumnMetaMap().forEach((k, v) -> context.registerRef(k.getId(), v.getName()));
            visitCommon(scan, context);
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
        public static String getJoinDistributionHints(OptExpression optExpression) {
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

    private static class ExprSQLBuilder extends ExpressionPrinter<PlanToSQLContext> {
        @Override
        public String print(ScalarOperator scalarOperator) {
            Preconditions.checkState(false);
            return null;
        }

        @Override
        public String print(ScalarOperator scalarOperator, PlanToSQLContext context) {
            if (scalarOperator == null) {
                return "";
            }
            return super.print(scalarOperator, context);
        }

        @Override
        public String visitVariableReference(ColumnRefOperator variable, PlanToSQLContext context) {
            return context.columnNames.get(variable.getId());
        }

        @Override
        public String visitCall(CallOperator call, PlanToSQLContext context) {
            if (SPMFunctions.isSPMFunctions(call)) {
                List<String> children = call.getChildren().stream().map(c -> visit(c, context)).toList();
                return SPMFunctions.toSQL(call.getFnName(), children);
            }
            return super.visitCall(call, context);
        }
    }
}
