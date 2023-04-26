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


package com.starrocks.sql.optimizer.rule.implementation;

import com.google.common.collect.Lists;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalNestLoopJoinOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.rule.transformation.JoinCommutativityRule;
import org.apache.commons.collections.CollectionUtils;

import java.util.List;

public class NestLoopJoinImplementationRule extends JoinImplementationRule {
    private NestLoopJoinImplementationRule() {
        super(RuleType.IMP_JOIN_TO_NESTLOOP_JOIN);
    }

    private static final NestLoopJoinImplementationRule INSTANCE = new NestLoopJoinImplementationRule();

    private static final String UNSUPPORTED_JOIN_CLAUSE = "Unsupported '%s' clause with this ['%s'] correlated condition.";

    public static NestLoopJoinImplementationRule getInstance() {
        return INSTANCE;
    }

    // Only choose NestLoopJoin for such scenarios, which HashJoin could not handle
    // 1. No equal-conjuncts in join clause
    // 2. JoinType is INNER/CROSS/OUTER/LEFT ANTI/LEFT SEMI
    @Override
    public boolean check(final OptExpression input, OptimizerContext context) {
        List<BinaryPredicateOperator> eqPredicates = extractEqPredicate(input, context);
        if (CollectionUtils.isNotEmpty(eqPredicates)) {
            return false;
        } else {
            JoinOperator joinType = getJoinType(input);
            LogicalJoinOperator joinOperator = (LogicalJoinOperator) input.getOp();
            if (!supportJoinType(joinType)) {
                throw new SemanticException(UNSUPPORTED_JOIN_CLAUSE, joinType, joinOperator.getOnPredicate());
            }
            return true;
        }
    }

    private boolean supportJoinType(JoinOperator joinType) {
        return joinType.isCrossJoin() || joinType.isInnerJoin() || joinType.isOuterJoin() || joinType.isSemiAntiJoin();
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        // Transform right semi/anti into left semi/anti
        OptExpression commutedExpr = JoinCommutativityRule.commuteRightSemiAntiJoin(input);
        LogicalJoinOperator joinOperator = (LogicalJoinOperator) commutedExpr.getOp();

        PhysicalNestLoopJoinOperator physicalNestLoopJoin = new PhysicalNestLoopJoinOperator(
                joinOperator.getJoinType(),
                joinOperator.getOnPredicate(),
                joinOperator.getJoinHint(),
                joinOperator.getLimit(),
                joinOperator.getPredicate(),
                joinOperator.getProjection());
        OptExpression result = OptExpression.create(physicalNestLoopJoin, commutedExpr.getInputs());
        return Lists.newArrayList(result);
    }
}
