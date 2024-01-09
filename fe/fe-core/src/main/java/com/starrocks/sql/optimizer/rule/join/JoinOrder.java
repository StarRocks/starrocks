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

package com.starrocks.sql.optimizer.rule.join;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.catalog.Type;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.Group;
import com.starrocks.sql.optimizer.JoinHelper;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.RowOutputInfo;
import com.starrocks.sql.optimizer.UKFKConstraintsCollector;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.ColumnOutputInfo;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorBuilderFactory;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.UKFKConstraints;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.statistics.StatisticsCalculator;
import com.starrocks.sql.optimizer.statistics.StatisticsEstimateCoefficient;
import com.starrocks.sql.optimizer.validate.InputDependenciesChecker;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public abstract class JoinOrder {

    private static final Logger LOGGER = LogManager.getLogger(JoinOrder.class);
    /**
     * Like {@link OptExpression} or {@link com.starrocks.sql.optimizer.GroupExpression} ,
     * Description of an expression in the join order environment
     * left and right child of join expressions point to child groups
     */
    static class ExpressionInfo {
        public ExpressionInfo(OptExpression expr) {
            this.expr = expr;
        }

        public ExpressionInfo(OptExpression expr,
                              GroupInfo leftChild,
                              GroupInfo rightChild) {
            this.expr = expr;
            this.leftChildExpr = leftChild;
            this.rightChildExpr = rightChild;
        }

        OptExpression expr;
        GroupInfo leftChildExpr;
        GroupInfo rightChildExpr;

        public double getCost() {
            return cost;
        }

        double cost = -1L;
        double rowCount = -1L;

        @Override
        public int hashCode() {
            return Objects.hash(expr.getOp().hashCode(), leftChildExpr, rightChildExpr);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof JoinOrder)) {
                return false;
            }

            ExpressionInfo other = (ExpressionInfo) obj;
            return Objects.equals(expr, other.expr)
                    && Objects.equals(leftChildExpr, other.leftChildExpr)
                    && Objects.equals(rightChildExpr, other.rightChildExpr);
        }
    }

    /**
     * Like {@link Group}, the atoms bitset could identify one group
     */
    static class GroupInfo {
        public GroupInfo(BitSet atoms) {
            this.atoms = atoms;
        }

        final BitSet atoms;
        ExpressionInfo bestExprInfo = null;
        double lowestExprCost = Double.MAX_VALUE;

        @Override
        public int hashCode() {
            return atoms.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }

            GroupInfo other = (GroupInfo) obj;
            return atoms.equals(other.atoms);
        }
    }

    /**
     * The join level from bottom to top
     * For A Join B Join C Join D
     * Level 1 groups are: A, B, C, D
     * Level 2 groups are: AB, AC, AD, BC ...
     * Level 3 groups are: ABC, ABD, BCD ...
     * Level 4 groups are: ABCD
     */
    static class JoinLevel {
        final int level;
        List<GroupInfo> groups = Lists.newArrayList();

        public JoinLevel(int level) {
            this.level = level;
        }
    }

    /**
     * The Edge represents the join on predicate
     * For A.id = B.id
     * The predicate is A.id = B.id,
     * The vertexes are A and B
     */
    static class Edge {
        final BitSet vertexes = new BitSet();
        final ScalarOperator predicate;

        public Edge(ScalarOperator predicate) {
            this.predicate = predicate;
        }
    }

    public JoinOrder(OptimizerContext context) {
        this.context = context;
    }

    protected final OptimizerContext context;
    protected int atomSize;
    protected final List<JoinLevel> joinLevels = Lists.newArrayList();
    protected final Map<BitSet, GroupInfo> bitSetToGroupInfo = Maps.newHashMap();

    protected int edgeSize;
    protected final List<Edge> edges = Lists.newArrayList();

    // Because there may be expression mapping between joins,
    // in the process of reordering, it may need to be re-allocated,
    // so in the initialization process, these expressions will be stored in expressionMap first
    Map<ColumnRefOperator, ScalarOperator> expressionMap;

    // Atom: A child of the Multi join. This could be a table or some
    // other operator like a group by or a full outer join.
    void init(List<OptExpression> atoms, List<ScalarOperator> predicates,
              Map<ColumnRefOperator, ScalarOperator> expressionMap) {

        // 1. calculate statistics for each atom expression
        for (OptExpression atom : atoms) {
            calculateStatistics(atom);
        }

        // 2. build join graph
        atomSize = atoms.size();
        edgeSize = predicates.size();
        for (ScalarOperator predicate : predicates) {
            edges.add(new Edge(predicate));
        }

        this.expressionMap = expressionMap;
        computeEdgeCover(atoms, expressionMap);

        // 3. init join levels
        // For human read easily, the join level start with 1, not 0.
        for (int i = 0; i <= atomSize; ++i) {
            joinLevels.add(new JoinLevel(i));
        }

        // 4.init join group info
        JoinLevel atomLevel = joinLevels.get(1);
        for (int i = 0; i < atomSize; ++i) {
            BitSet atomBit = new BitSet();
            atomBit.set(i);
            ExpressionInfo atomExprInfo = new ExpressionInfo(atoms.get(i));
            computeCost(atomExprInfo);

            GroupInfo groupInfo = new GroupInfo(atomBit);
            groupInfo.bestExprInfo = atomExprInfo;
            groupInfo.lowestExprCost = atomExprInfo.cost;
            atomLevel.groups.add(groupInfo);
        }
    }

    public void reorder(List<OptExpression> atoms, List<ScalarOperator> predicates,
                        Map<ColumnRefOperator, ScalarOperator> expressionMap) {
        init(atoms, predicates, expressionMap);
        enumerate();
    }

    // Different join order algorithms should have different implementations
    protected abstract void enumerate();

    //Get reorder result
    public abstract List<OptExpression> getResult();

    // Use graph to represent the join expression:
    // The vertex represent the join node,
    // The edge represent the join predicate
    protected void computeEdgeCover(List<OptExpression> vertexes,
                                    Map<ColumnRefOperator, ScalarOperator> expressionMap) {
        for (int i = 0; i < edgeSize; ++i) {
            ScalarOperator predicate = edges.get(i).predicate;
            ColumnRefSet predicateColumn = predicate.getUsedColumns();
            for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : expressionMap.entrySet()) {
                if (predicate.getUsedColumns().contains(entry.getKey())) {
                    predicateColumn.union(entry.getValue().getUsedColumns());
                }
            }

            for (int j = 0; j < atomSize; ++j) {
                OptExpression atom = vertexes.get(j);
                ColumnRefSet outputColumns = atom.getOutputColumns();
                if (predicateColumn.isIntersect(outputColumns)) {
                    edges.get(i).vertexes.set(j);
                }
            }
        }
    }

    protected List<GroupInfo> getGroupForLevel(int level) {
        return joinLevels.get(level).groups;
    }

    protected void calculateStatistics(OptExpression expr) {
        // Avoid repeated calculate
        if (expr.getStatistics() != null) {
            return;
        }

        for (OptExpression child : expr.getInputs()) {
            calculateStatistics(child);
        }

        ExpressionContext expressionContext = new ExpressionContext(expr);
        StatisticsCalculator statisticsCalculator = new StatisticsCalculator(
                expressionContext, context.getColumnRefFactory(), context);
        statisticsCalculator.estimatorStats();
        expr.setStatistics(expressionContext.getStatistics());
    }

    protected void computeCost(ExpressionInfo exprInfo) {
        double cost = exprInfo.expr.getStatistics().getOutputRowCount();
        exprInfo.rowCount = cost;
        if (exprInfo.leftChildExpr != null) {
            cost = cost > (StatisticsEstimateCoefficient.MAXIMUM_COST - exprInfo.leftChildExpr.bestExprInfo.cost) ?
                    StatisticsEstimateCoefficient.MAXIMUM_COST : cost + exprInfo.leftChildExpr.bestExprInfo.cost;

            cost = cost > (StatisticsEstimateCoefficient.MAXIMUM_COST - exprInfo.rightChildExpr.bestExprInfo.cost) ?
                    StatisticsEstimateCoefficient.MAXIMUM_COST : cost + exprInfo.rightChildExpr.bestExprInfo.cost;

            LogicalJoinOperator joinOperator = (LogicalJoinOperator) exprInfo.expr.getOp();
            if (joinOperator.getJoinType().isCrossJoin()) {
                // punish cross join
                long crossJoinCostPenalty = ConnectContext.get().getSessionVariable().getCrossJoinCostPenalty();
                cost = cost > (StatisticsEstimateCoefficient.MAXIMUM_COST / crossJoinCostPenalty) ?
                        StatisticsEstimateCoefficient.MAXIMUM_COST :
                        cost * crossJoinCostPenalty;
            } else if (!existsEqOnPredicate(exprInfo.expr)) {
                // punish nestloop join
                cost = cost > (StatisticsEstimateCoefficient.MAXIMUM_COST /
                        StatisticsEstimateCoefficient.EXECUTE_COST_PENALTY) ?
                        StatisticsEstimateCoefficient.MAXIMUM_COST :
                        cost * StatisticsEstimateCoefficient.EXECUTE_COST_PENALTY;
            }
        }
        exprInfo.cost = cost;
    }

    protected Optional<ExpressionInfo> buildJoinExpr(GroupInfo leftGroup, GroupInfo rightGroup) {
        ExpressionInfo leftExprInfo = leftGroup.bestExprInfo;
        ExpressionInfo rightExprInfo = rightGroup.bestExprInfo;
        ScalarOperator onPredicates = buildInnerJoinPredicate(leftGroup.atoms, rightGroup.atoms);

        LogicalJoinOperator newJoin;

        if (onPredicates != null) {
            newJoin = new LogicalJoinOperator(JoinOperator.INNER_JOIN, onPredicates);
        } else {
            newJoin = new LogicalJoinOperator(JoinOperator.CROSS_JOIN, null);
        }

        Map<ColumnRefOperator, ScalarOperator> leftExpression = new HashMap<>();
        Map<ColumnRefOperator, ScalarOperator> rightExpression = new HashMap<>();
        if (onPredicates != null) {
            ColumnRefSet useColumns = onPredicates.getUsedColumns();

            for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : expressionMap.entrySet()) {
                if (!useColumns.contains(entry.getKey())) {
                    continue;
                }

                if (entry.getValue().isConstantRef()) {
                    // constant always on left
                    leftExpression.put(entry.getKey(), entry.getValue());
                    continue;
                }

                ColumnRefSet valueUseColumns = entry.getValue().getUsedColumns();
                if (leftExprInfo.expr.getOutputColumns().containsAll(valueUseColumns)) {
                    leftExpression.put(entry.getKey(), entry.getValue());
                } else if (rightExprInfo.expr.getOutputColumns().containsAll(valueUseColumns)) {
                    rightExpression.put(entry.getKey(), entry.getValue());
                }
            }
        }

        pushRequiredColumns(leftExprInfo, leftExpression);
        pushRequiredColumns(rightExprInfo, rightExpression);

        UKFKConstraints.JoinProperty joinProperty = null;
        SessionVariable sessionVariable = ConnectContext.get().getSessionVariable();
        if (sessionVariable.isEnableUKFKOpt() && sessionVariable.isEnableUKFKJoinReorder()) {
            UKFKConstraintsCollector.collectColumnConstraints(leftExprInfo.expr);
            UKFKConstraintsCollector.collectColumnConstraints(rightExprInfo.expr);
            UKFKConstraints constraint = UKFKConstraintsCollector.buildJoinColumnConstraint(newJoin,
                    newJoin.getJoinType(), newJoin.getOnPredicate(), leftExprInfo.expr, rightExprInfo.expr);
            joinProperty = constraint.getJoinProperty();
        }

        // use small table as right child
        OptExpression joinExpr;
        boolean reverse = false;
        if (joinProperty != null && joinProperty.ukConstraint.isIntact) {
            if (joinProperty.isLeftUK) {
                if (cannotLetFKAsRightTable(leftExprInfo, rightExprInfo)) {
                    // If the uk table is too small, then use it as right table
                    reverse = true;
                    joinExpr = OptExpression.create(newJoin, rightExprInfo.expr,
                            leftExprInfo.expr);
                } else {
                    // Otherwise, use the fk table as the right table
                    newJoin.setFKRight(true);
                    joinExpr = OptExpression.create(newJoin, leftExprInfo.expr,
                            rightExprInfo.expr);
                }
            } else {
                if (cannotLetFKAsRightTable(rightExprInfo, leftExprInfo)) {
                    // If the uk table is too small, then use it as right table
                    joinExpr = OptExpression.create(newJoin, leftExprInfo.expr,
                            rightExprInfo.expr);
                } else {
                    // Otherwise, use the fk table as the right table
                    reverse = true;
                    newJoin.setFKRight(true);
                    joinExpr = OptExpression.create(newJoin, rightExprInfo.expr,
                            leftExprInfo.expr);
                }
            }
        } else {
            if (leftExprInfo.rowCount < rightExprInfo.rowCount) {
                reverse = true;
                joinExpr = OptExpression.create(newJoin, rightExprInfo.expr,
                        leftExprInfo.expr);
            } else {
                joinExpr = OptExpression.create(newJoin, leftExprInfo.expr,
                        rightExprInfo.expr);
            }
        }

        try {
            InputDependenciesChecker.getInstance().validate(joinExpr, context.getTaskContext());
        } catch (Exception e) {
            LOGGER.debug("the reorder result is not a valid plan.", e);
            return Optional.empty();
        }

        if (reverse) {
            return Optional.of(new ExpressionInfo(joinExpr, rightGroup, leftGroup));
        } else {
            return Optional.of(new ExpressionInfo(joinExpr, leftGroup, rightGroup));
        }
    }

    private boolean cannotLetFKAsRightTable(ExpressionInfo ukExprInfo, ExpressionInfo fkExprInfo) {
        RowOutputInfo ukRowOutputInfo = ukExprInfo.expr.getRowOutputInfo();
        RowOutputInfo fkRowOutputInfo = fkExprInfo.expr.getRowOutputInfo();

        double ukNormalizedRows = ukExprInfo.rowCount;
        double ukTypeSize = ukRowOutputInfo.getColumnOutputInfo().stream()
                .map(ColumnOutputInfo::getColumnRef)
                .map(ColumnRefOperator::getType)
                .map(Type::getTypeSize)
                .reduce(1, Integer::sum);
        double fkTypeSize = fkRowOutputInfo.getColumnOutputInfo().stream()
                .map(ColumnOutputInfo::getColumnRef)
                .map(ColumnRefOperator::getType)
                .map(Type::getTypeSize)
                .reduce(1, Integer::sum);
        double fkNormalizedRows = fkExprInfo.rowCount * fkTypeSize / ukTypeSize;

        return ConnectContext.get().getSessionVariable().getUkfkJoinThreshold() * ukNormalizedRows < fkNormalizedRows;
    }

    private void pushRequiredColumns(ExpressionInfo exprInfo, Map<ColumnRefOperator, ScalarOperator> expression) {
        if (expression.isEmpty()) {
            return;
        }

        Map<ColumnRefOperator, ScalarOperator> projection = Maps.newHashMap();
        if (exprInfo.expr.getOp().getProjection() != null) {
            projection.putAll(exprInfo.expr.getOp().getProjection().getColumnRefMap());
        } else {
            exprInfo.expr.getOutputColumns().getStream().map(context.getColumnRefFactory()::getColumnRef)
                    .forEach(c -> projection.put(c, c));
        }

        // merge two projection, keep same logical from MergeTwoProjectRule
        ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(projection);
        Map<ColumnRefOperator, ScalarOperator> resultMap = Maps.newHashMap();
        for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : expression.entrySet()) {
            resultMap.put(entry.getKey(), rewriter.rewrite(entry.getValue()));
        }
        resultMap.putAll(projection);

        Operator.Builder builder = OperatorBuilderFactory.build(exprInfo.expr.getOp());
        exprInfo.expr = OptExpression.create(
                builder.withOperator(exprInfo.expr.getOp()).setLimit(Operator.DEFAULT_LIMIT)
                        .setProjection(new Projection(resultMap)).build(),
                exprInfo.expr.getInputs());
        exprInfo.expr.deriveLogicalPropertyItself();
    }

    private ScalarOperator buildInnerJoinPredicate(BitSet left, BitSet right) {
        List<ScalarOperator> onPredicates = Lists.newArrayList();
        BitSet joinBitSet = new BitSet();
        joinBitSet.or(left);
        joinBitSet.or(right);
        for (int i = 0; i < edgeSize; ++i) {
            Edge edge = edges.get(i);
            if (contains(joinBitSet, edge.vertexes) &&
                    left.intersects(edge.vertexes) &&
                    right.intersects(edge.vertexes)) {
                onPredicates.add(edge.predicate);
            }
        }
        return Utils.compoundAnd(onPredicates);
    }

    public boolean canBuildInnerJoinPredicate(GroupInfo leftGroup, GroupInfo rightGroup) {
        BitSet left = leftGroup.atoms;
        BitSet right = rightGroup.atoms;
        BitSet joinBitSet = new BitSet();
        joinBitSet.or(left);
        joinBitSet.or(right);

        for (int i = 0; i < edgeSize; ++i) {
            Edge edge = edges.get(i);
            if (!Utils.isEqualBinaryPredicate(edge.predicate)) {
                continue;
            }
            if (contains(joinBitSet, edge.vertexes) &&
                    left.intersects(edge.vertexes) &&
                    right.intersects(edge.vertexes)) {
                return true;
            }
        }
        return false;
    }

    private boolean contains(BitSet left, BitSet right) {
        return right.stream().allMatch(left::get);
    }

    private boolean existsEqOnPredicate(OptExpression optExpression) {
        LogicalJoinOperator joinOp = optExpression.getOp().cast();
        List<ScalarOperator> onPredicates = Utils.extractConjuncts(joinOp.getOnPredicate());

        ColumnRefSet leftChildColumns = optExpression.inputAt(0).getOutputColumns();
        ColumnRefSet rightChildColumns = optExpression.inputAt(1).getOutputColumns();

        List<BinaryPredicateOperator> eqOnPredicates = JoinHelper.getEqualsPredicate(
                leftChildColumns, rightChildColumns, onPredicates);
        return !eqOnPredicates.isEmpty();
    }
}
