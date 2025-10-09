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
import com.starrocks.catalog.Type;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.ast.expression.JoinOperator;
import com.starrocks.sql.common.LargeInPredicateException;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalRawValuesOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.LargeInPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

import static com.starrocks.sql.ast.HintNode.HINT_JOIN_BROADCAST;

/**
 * Transform large IN/NOT IN predicates to semi-join/anti-join for better performance.
 *
 * <p><b>Transformation:</b>
 * 
 * <p>Before (IN):
 * <pre>
 *   Filter(col IN (v1, v2, ..., vN))
 *     |
 *   Child
 * </pre>
 *
 * <p>After (IN):
 * <pre>
 *   LeftSemiJoin(col = const_col)
 *     |         |
 *   Child    RawValues(const_col: v1, v2, ..., vN)
 * </pre>
 *
 * <p>Before (NOT IN):
 * <pre>
 *   Filter(col NOT IN (v1, v2, ..., vN))
 *     |
 *   Child
 * </pre>
 *
 * <p>After (NOT IN):
 * <pre>
 *   LeftAntiJoin(col = const_col)
 *     |         |
 *   Child    RawValues(const_col: v1, v2, ..., vN)
 * </pre>
 * 
 * <p><b>Transformation Restrictions:</b>
 * This rule has specific limitations to ensure correctness:
 * <ul>
 *   <li>Only ONE LargeInPredicate is allowed per query. If multiple LargeInPredicates are detected,
 *       transformation is rejected.</li>
 *   <li>OR compound predicates are NOT supported. LargeInPredicate cannot coexist with OR in the 
 *       same filter predicate tree.</li>
 * </ul>
 * 
 * <p><b>Exception Handling and Query Retry:</b>
 * When the transformation cannot proceed due to unsupported scenarios (multiple LargeInPredicates, 
 * OR predicates, or type mismatches), it will throw {@link com.starrocks.sql.common.LargeInPredicateException}.
 * This exception is caught by upper layers (StmtExecutor), which triggers a query retry from the 
 * parser stage with {@code enable_large_in_predicate} disabled. The retry ensures the query executes 
 * via the normal IN predicate path, guaranteeing correctness at the cost of potentially higher 
 * FE processing overhead.
 * 
 * <p><b>Performance Benefits:</b>
 * For queries with extremely large IN lists (e.g., 100,000+ constants), this transformation significantly
 * reduces FE memory usage and planning time by using {@link com.starrocks.planner.RawValuesNode}
 * instead of creating individual expression nodes for each constant.
 * 
 * @see com.starrocks.sql.ast.expression.LargeInPredicate
 * @see com.starrocks.planner.RawValuesNode
 * @see com.starrocks.sql.common.LargeInPredicateException
 */
public class LargeInPredicateToJoinRule extends TransformationRule {
    private static final Logger LOG = LogManager.getLogger(LargeInPredicateToJoinRule.class);

    private static class PredicateAnalysis {
        boolean hasOrPredicate = false;
        List<ScalarOperator> largeInPredicates = new ArrayList<>();
    }

    public LargeInPredicateToJoinRule() {
        super(RuleType.TF_LARGE_IN_PREDICATE_TO_JOIN, Pattern.create(OperatorType.LOGICAL_FILTER)
                .addChildren(Pattern.create(OperatorType.PATTERN_LEAF)));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        if (!context.getSessionVariable().enableLargeInPredicate()) {
            return false;
        }

        LogicalFilterOperator filterOp = (LogicalFilterOperator) input.getOp();
        ScalarOperator predicate = filterOp.getPredicate();
        
        PredicateAnalysis analysis = analyzePredicate(predicate);
        
        if (analysis.largeInPredicates.isEmpty()) {
            return false;
        }
        
        if (analysis.hasOrPredicate) {
            throw new LargeInPredicateException("LargeInPredicate does not support OR compound predicates");
        }
        
        if (analysis.largeInPredicates.size() > 1) {
            throw new LargeInPredicateException(
                    "LargeInPredicate does not support multiple LargeInPredicate in one query, found: %d",
                    analysis.largeInPredicates.size());
        }

        return true;
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalFilterOperator filterOp = input.getOp().cast();
        ScalarOperator predicate = filterOp.getPredicate();

        PredicateAnalysis analysis = analyzePredicate(predicate);
        ScalarOperator inPredicate = analysis.largeInPredicates.get(0);

        OptExpression result = convertToSemiJoin(input, inPredicate, context);

        return Lists.newArrayList(result);
    }

    private PredicateAnalysis analyzePredicate(ScalarOperator predicate) {
        PredicateAnalysis analysis = new PredicateAnalysis();
        analyzePredicateRecursive(predicate, analysis);
        return analysis;
    }

    private void analyzePredicateRecursive(ScalarOperator predicate, PredicateAnalysis analysis) {
        if (predicate instanceof CompoundPredicateOperator compound) {
            if (compound.getCompoundType() == CompoundPredicateOperator.CompoundType.OR) {
                analysis.hasOrPredicate = true;
            }
        }

        if (predicate instanceof LargeInPredicateOperator) {
            analysis.largeInPredicates.add(predicate);
        }

        for (ScalarOperator child : predicate.getChildren()) {
            analyzePredicateRecursive(child, analysis);
        }
    }

    private OptExpression convertToSemiJoin(OptExpression input, ScalarOperator largeInPredicate,
                                           OptimizerContext context) {
        LargeInPredicateOperator largeIn = largeInPredicate.cast();
        LogicalRawValuesOperator rawValuesOp = createRawConstantTable(largeIn, context);
        OptExpression valuesExpr = OptExpression.create(rawValuesOp);

        ScalarOperator originalPredicate = input.getOp().getPredicate();
        ScalarOperator remainingPredicate = removeLargeInPredicate(originalPredicate, largeInPredicate);

        OptExpression leftChild;
        if (remainingPredicate != null && !remainingPredicate.equals(ConstantOperator.TRUE)) {
            LogicalFilterOperator newFilterOp = new LogicalFilterOperator(remainingPredicate);
            leftChild = OptExpression.create(newFilterOp, input.getInputs().get(0));
        } else {
            leftChild = input.getInputs().get(0);
        }

        ColumnRefOperator leftColumn = (ColumnRefOperator) largeInPredicate.getChild(0);
        ColumnRefOperator rightColumn = rawValuesOp.getColumnRefSet().get(0);

        ScalarOperator finalLeftColumn = leftColumn;
        if (!leftColumn.getType().matchesType(rightColumn.getType())) {
            finalLeftColumn = new CastOperator(rightColumn.getType(), leftColumn, false);
        }

        BinaryPredicateOperator joinPredicate = new BinaryPredicateOperator(BinaryType.EQ, finalLeftColumn, rightColumn);

        JoinOperator joinType = largeIn.isNotIn() ? JoinOperator.LEFT_ANTI_JOIN : JoinOperator.LEFT_SEMI_JOIN;

        LogicalJoinOperator joinOp = new LogicalJoinOperator.Builder()
                .setJoinType(joinType)
                .setJoinHint(HINT_JOIN_BROADCAST)
                .setOnPredicate(joinPredicate)
                .build();

        return OptExpression.create(joinOp, leftChild, valuesExpr);
    }

    private LogicalRawValuesOperator createRawConstantTable(LargeInPredicateOperator largeInPredicate,
                                                                    OptimizerContext context) {
        Type constantType = largeInPredicate.getConstantType();
        ColumnRefOperator column = context.getColumnRefFactory().create(
                "const_value", constantType, false);

        String rawConstantList = largeInPredicate.getRawConstantList();
        int constantCount = largeInPredicate.getConstantCount();
        
        return new LogicalRawValuesOperator(
                Lists.newArrayList(column),
                constantType,
                rawConstantList, 
                constantCount);
    }


    private ScalarOperator removeLargeInPredicate(ScalarOperator predicate, ScalarOperator toRemove) {
        if (predicate.equals(toRemove)) {
            return null;
        }

        List<ScalarOperator> conjuncts = Utils.extractConjuncts(predicate);
        
        List<ScalarOperator> remainingConjuncts = new ArrayList<>();
        for (ScalarOperator conjunct : conjuncts) {
            if (!conjunct.equals(toRemove)) {
                remainingConjuncts.add(conjunct);
            }
        }
        
        if (remainingConjuncts.isEmpty()) {
            return null;
        } else if (remainingConjuncts.size() == 1) {
            return remainingConjuncts.get(0);
        } else {
            return Utils.compoundAnd(remainingConjuncts);
        }
    }
}

