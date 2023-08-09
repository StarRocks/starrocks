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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.ColumnOutputInfo;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ArrayOperator;
import com.starrocks.sql.optimizer.operator.scalar.ArraySliceOperator;
import com.starrocks.sql.optimizer.operator.scalar.BetweenPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CaseWhenOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.CollectionElementOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ExistsPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.LikePredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.BaseScalarOperatorShuttle;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriter;
import com.starrocks.sql.optimizer.rewrite.scalar.NormalizePredicateRule;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.rule.join.JoinReorderHelper;
import com.starrocks.sql.optimizer.rule.join.JoinReorderProperty;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;

import static com.starrocks.sql.optimizer.rule.join.JoinReorderProperty.ASSOCIATIVITY_BOTTOM_MASK;
import static com.starrocks.sql.optimizer.rule.join.JoinReorderProperty.ASSOCIATIVITY_TOP_MASK;
import static com.starrocks.sql.optimizer.rule.join.JoinReorderProperty.LEFT_ASSCOM_BOTTOM_MASK;
import static com.starrocks.sql.optimizer.rule.join.JoinReorderProperty.LEFT_ASSCOM_TOP_MASK;

/*       Join            Join
 *      /    \          /    \
 *     Join   C  =>    A     Join
 *    /    \                /    \
 *   A      B              B      C
 */

public class JoinAssociativityRule extends JoinAssociateBaseRule {
    public static final JoinAssociativityRule INNER_JOIN_ASSOCIATIVITY_RULE = new JoinAssociativityRule(
            RuleType.TF_JOIN_ASSOCIATIVITY_INNER, true);

    public static final JoinAssociativityRule OUTER_JOIN_ASSOCIATIVITY_RULE = new JoinAssociativityRule(
            RuleType.TF_JOIN_ASSOCIATIVITY_OUTER, false);

    private JoinAssociativityRule(RuleType ruleType, boolean isInnerMode) {
        super(ruleType, Pattern.create(OperatorType.LOGICAL_JOIN)
                .addChildren(Pattern.create(OperatorType.LOGICAL_JOIN)
                        .addChildren(Pattern.create(OperatorType.PATTERN_LEAF, OperatorType.PATTERN_MULTI_LEAF))
                        .addChildren(Pattern.create(OperatorType.PATTERN_LEAF, OperatorType.PATTERN_MULTI_LEAF)))
                .addChildren(Pattern.create(OperatorType.PATTERN_LEAF)), JoinAssociateBaseRule.ASSOCIATE_MODE,
                isInnerMode);
    }

    @Override
    public boolean check(final OptExpression input, OptimizerContext context) {
        LogicalJoinOperator topJoin = (LogicalJoinOperator) input.getOp();
        LogicalJoinOperator bottomJoin = (LogicalJoinOperator) input.inputAt(0).getOp();
        if ((topJoin.getTransformMask() & (ASSOCIATIVITY_TOP_MASK | LEFT_ASSCOM_TOP_MASK)) > 0 &&
                (bottomJoin.getTransformMask() & (ASSOCIATIVITY_BOTTOM_MASK | LEFT_ASSCOM_BOTTOM_MASK)) > 0) {
            return false;
        }
        if (StringUtils.isNotEmpty(topJoin.getJoinHint()) || StringUtils.isNotEmpty(bottomJoin.getJoinHint())) {
            return false;
        }

        if (bottomJoin.hasLimit()) {
            return false;
        }

        if (JoinReorderProperty.getAssociativityProperty(bottomJoin.getJoinType(), topJoin.getJoinType(), isInnerMode)
                != JoinReorderProperty.SUPPORTED) {
            return false;
        }

        return JoinReorderHelper.isAssoc(input.inputAt(0), input);
    }

    @Override
    public ScalarOperator rewriteNewTopOnCondition(JoinOperator topJoinType, ProjectionSplitter splitter,
                                                   ScalarOperator newTopOnCondition,
                                                   ColumnRefSet newBotJoinOutputCols,
                                                   ColumnRefFactory columnRefFactory) {
        if (JoinOperator.INNER_JOIN == topJoinType && newTopOnCondition != null) {
            // rewrite on condition like 'tblA.col = tblB.col + tblC.col' to 'tblA.col = add'
            // and add the add->tblB.col + tblC.col map to projectMap
            JoinOnConditionShuttle shuttle = new JoinOnConditionShuttle(newBotJoinOutputCols, columnRefFactory);
            newTopOnCondition = shuttle.rewriteOnCondition(newTopOnCondition);
            splitter.getBotJoinCols().addAll(shuttle.getColumnEntries());
        }
        return newTopOnCondition;
    }

    @Override
    public OptExpression createNewTopJoinExpr(LogicalJoinOperator newTopJoin, OptExpression newTopJoinChild,
                                              OptExpression newBotJoinExpr) {
        return OptExpression.create(newTopJoin, newTopJoinChild, newBotJoinExpr);
    }

    @Override
    public int createTransformMask(boolean isTop) {
        return isTop ? ASSOCIATIVITY_TOP_MASK : ASSOCIATIVITY_BOTTOM_MASK;
    }

    /*
     * JoinOnConditionShuttle is used for rewrite the join on condition when the expr in this condition can be
     * pushed to its child. For example select t1.v1 from t1 join t2 joint t3 on t1.v1 = t2.v1 + t3.v1, we can rewrite
     * this sql to select t1.v1 from t1 join (select t2.v1 + t3.v1 as add from t2, t3) t on t1.v1 = t.add
     */
    private class JoinOnConditionShuttle extends BaseScalarOperatorShuttle {
        private final ColumnRefFactory columnRefFactory;

        private final ColumnRefSet newBotJoinOutputCols;

        private final Map<ScalarOperator, ColumnRefOperator> exprToColumnRefMap;

        public JoinOnConditionShuttle(ColumnRefSet newBotJoinOutputCols, ColumnRefFactory columnRefFactory) {
            this.columnRefFactory = columnRefFactory;
            this.exprToColumnRefMap = Maps.newHashMap();
            this.newBotJoinOutputCols = newBotJoinOutputCols;
        }

        public List<ColumnOutputInfo> getColumnEntries() {
            List<ColumnOutputInfo> entryList = Lists.newArrayList();
            exprToColumnRefMap.entrySet().stream()
                    .forEach(e -> entryList.add(new ColumnOutputInfo(e.getValue(), e.getKey())));
            return entryList;
        }

        public ScalarOperator rewriteOnCondition(ScalarOperator onCondition) {
            ScalarOperator copy = onCondition.clone();
            ScalarOperator newOnCondition = copy.accept(this, null);
            ScalarOperatorRewriter scalarOperatorRewriter = new ScalarOperatorRewriter();
            newOnCondition =
                    scalarOperatorRewriter.rewrite(newOnCondition, ImmutableList.of(new NormalizePredicateRule()));
            return newOnCondition;
        }

        @Override
        public ScalarOperator visitArray(ArrayOperator array, Void context) {
            return array;
        }

        @Override
        public ScalarOperator visitCollectionElement(CollectionElementOperator collectionElementOp, Void context) {
            return collectionElementOp;
        }

        @Override
        public ScalarOperator visitArraySlice(ArraySliceOperator array, Void context) {
            return array;
        }

        @Override
        public ScalarOperator visitCall(CallOperator call, Void context) {
            if (canDerivedFromNewBotJoinOutput(call.getUsedColumns())) {
                return addExprToColumnRefMap(call);
            }
            return super.visitCall(call, context);
        }

        @Override
        public ScalarOperator visitCaseWhenOperator(CaseWhenOperator operator, Void context) {
            return operator;
        }

        @Override
        public ScalarOperator visitCastOperator(CastOperator operator, Void context) {
            if (canDerivedFromNewBotJoinOutput(operator.getUsedColumns())) {
                return addExprToColumnRefMap(operator);
            }
            return super.visitCastOperator(operator, context);
        }

        @Override
        public ScalarOperator visitBetweenPredicate(BetweenPredicateOperator predicate, Void context) {
            return predicate;
        }

        @Override
        public ScalarOperator visitBinaryPredicate(BinaryPredicateOperator predicate, Void context) {
            if (predicate.getBinaryType() != BinaryPredicateOperator.BinaryType.EQ) {
                return predicate;
            } else {
                return super.visitBinaryPredicate(predicate, context);
            }
        }

        @Override
        public ScalarOperator visitCompoundPredicate(CompoundPredicateOperator predicate, Void context) {
            return predicate;
        }

        @Override
        public ScalarOperator visitExistsPredicate(ExistsPredicateOperator predicate, Void context) {
            return predicate;
        }

        @Override
        public ScalarOperator visitInPredicate(InPredicateOperator predicate, Void context) {
            return predicate;
        }

        @Override
        public ScalarOperator visitIsNullPredicate(IsNullPredicateOperator predicate, Void context) {
            return predicate;
        }

        @Override
        public ScalarOperator visitLikePredicateOperator(LikePredicateOperator predicate, Void context) {
            return predicate;
        }

        private boolean canDerivedFromNewBotJoinOutput(ColumnRefSet columnRefSet) {
            if (columnRefSet.isEmpty()) {
                return false;
            }
            return newBotJoinOutputCols.containsAll(columnRefSet);
        }

        private ScalarOperator addExprToColumnRefMap(ScalarOperator operator) {
            if (!exprToColumnRefMap.containsKey(operator)) {
                ColumnRefOperator columnRefOperator = createColumnRefOperator(operator);
                exprToColumnRefMap.put(operator, columnRefOperator);
                return columnRefOperator;
            } else {
                return exprToColumnRefMap.get(operator);
            }
        }

        private ColumnRefOperator createColumnRefOperator(ScalarOperator operator) {
            return columnRefFactory.create(operator, operator.getType(), operator.isNullable());
        }
    }
}
