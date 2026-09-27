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

package com.starrocks.sql.optimizer.validate;

import com.starrocks.common.Pair;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalAIProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalApplyOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalIcebergEqualityDeleteScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTableFunctionOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTopNOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalValuesOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalViewScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalWindowOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalAIProjectOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalCTEConsumeOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalDecodeOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalDistributionOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashAggregateOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHiveScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalIcebergEqualityDeleteScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalIcebergScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalProjectOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalSplitConsumeOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalTopNOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalValuesOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalWindowOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.List;
import java.util.Map;

/**
 * Ensures that every AI call is evaluated at an explicit AI project boundary.
 *
 * <p>An AI project may expose an AI call directly from its output map. AI calls are
 * forbidden everywhere else, including nested below another scalar expression and
 * inside an AI project's common-expression map.
 */
public final class AIFunctionPlacementValidator {
    private static final Visitor VISITOR = new Visitor();
    private static final String INVALID_PLACEMENT =
            "Invalid AI function placement: AI calls must be direct outputs of an AI project operator";

    private AIFunctionPlacementValidator() {
    }

    public static void validate(OptExpression root) {
        root.getOp().accept(VISITOR, root, null);
    }

    private static final class Visitor extends OptExpressionVisitor<Void, Void> {
        @Override
        public Void visit(OptExpression optExpression, Void context) {
            validateOperator(optExpression.getOp());
            for (OptExpression input : optExpression.getInputs()) {
                input.getOp().accept(this, input, null);
            }
            return null;
        }
    }

    private static void validateOperator(Operator operator) {
        validateNoAI(operator.getPredicate());
        validateNoAI(operator.getPredicateCommonOperators());
        validateNoAI(operator.getProjection());

        if (operator instanceof LogicalAIProjectOperator aiProject) {
            validateDirectOutputs(aiProject.getColumnRefMap());
            validateNoAI(aiProject.getCommonSubOperatorMap());
            return;
        }
        if (operator instanceof PhysicalAIProjectOperator aiProject) {
            validateDirectOutputs(aiProject.getColumnRefMap());
            validateNoAI(aiProject.getCommonSubOperatorMap());
            return;
        }

        if (operator instanceof LogicalProjectOperator project) {
            validateNoAI(project.getColumnRefMap());
        } else if (operator instanceof PhysicalProjectOperator project) {
            validateNoAI(project.getColumnRefMap());
            validateNoAI(project.getCommonSubOperatorMap());
        } else if (operator instanceof LogicalAggregationOperator aggregate) {
            validateNoAI(aggregate.getAggregations());
        } else if (operator instanceof PhysicalHashAggregateOperator aggregate) {
            validateNoAI(aggregate.getAggregations());
        } else if (operator instanceof LogicalApplyOperator apply) {
            validateNoAI(apply.getSubqueryOperator());
            validateNoAI(apply.getCorrelationConjuncts());
        } else if (operator instanceof LogicalWindowOperator window) {
            validateNoAI(window.getWindowCall());
            validateNoAI(window.getPartitionExpressions());
            validateNoAI(window.getSkewColumn());
            validateNoAI(window.getSkewValues());
        } else if (operator instanceof PhysicalWindowOperator window) {
            validateNoAI(window.getAnalyticCall());
            validateNoAI(window.getPartitionExpressions());
            validateNoAI(window.getSkewColumn());
            validateNoAI(window.getSkewValues());
        } else if (operator instanceof LogicalJoinOperator join) {
            validateNoAI(join.getOnPredicate());
            validateNoAI(join.getOriginalOnPredicate());
            validateNoAI(join.getSkewColumn());
            validateNoAI(join.getSkewValues());
        } else if (operator instanceof PhysicalJoinOperator join) {
            validateNoAI(join.getOnPredicate());
            if (join instanceof PhysicalHashJoinOperator hashJoin) {
                validateNoAI(hashJoin.getSkewColumn());
                validateNoAI(hashJoin.getSkewValues());
            }
        } else if (operator instanceof LogicalValuesOperator values) {
            validateRows(values.getRows());
        } else if (operator instanceof PhysicalValuesOperator values) {
            validateRows(values.getRows());
        } else if (operator instanceof LogicalTopNOperator topN) {
            validateNoAI(topN.getPartitionPreAggCall());
        } else if (operator instanceof PhysicalTopNOperator topN) {
            validateNoAI(topN.getPreAggCall());
        } else if (operator instanceof LogicalTableFunctionOperator tableFunction) {
            for (Pair<?, ScalarOperator> argument : tableFunction.getFnParamColumnProject()) {
                validateNoAI(argument.second);
            }
        } else if (operator instanceof LogicalViewScanOperator viewScan) {
            validateNoAI(viewScan.getOriginalColumnRefToInlinedColumnRefMap());
        } else if (operator instanceof LogicalOlapScanOperator scan) {
            validateNoAI(scan.getPrunedPartitionPredicates());
        } else if (operator instanceof LogicalIcebergEqualityDeleteScanOperator scan) {
            validateNoAI(scan.getOriginPredicate());
        } else if (operator instanceof PhysicalDecodeOperator decode) {
            validateNoAI(decode.getStringFunctions());
        } else if (operator instanceof PhysicalCTEConsumeOperator consume) {
            validateNoAI(consume.getGlobalDictsExpr());
        } else if (operator instanceof PhysicalDistributionOperator distribution) {
            validateNoAI(distribution.getGlobalDictsExpr());
        } else if (operator instanceof PhysicalOlapScanOperator scan) {
            validateNoAI(scan.getGlobalDictsExpr());
            validateNoAI(scan.getPrunedPartitionPredicates());
        } else if (operator instanceof PhysicalHiveScanOperator scan) {
            validateNoAI(scan.getGlobalDictsExpr());
        } else if (operator instanceof PhysicalIcebergEqualityDeleteScanOperator scan) {
            validateNoAI(scan.getOriginPredicate());
        } else if (operator instanceof PhysicalIcebergScanOperator scan) {
            validateNoAI(scan.getGlobalDictsExpr());
        } else if (operator instanceof PhysicalSplitConsumeOperator consume) {
            validateNoAI(consume.getSplitPredicate());
        }
    }

    private static void validateDirectOutputs(Map<?, ? extends ScalarOperator> outputs) {
        for (ScalarOperator expression : outputs.values()) {
            if (isAICall(expression)) {
                validateNoAI(expression.getChildren());
            } else {
                validateNoAI(expression);
            }
        }
    }

    private static void validateNoAI(Projection projection) {
        if (projection == null) {
            return;
        }
        validateNoAI(projection.getColumnRefMap());
        validateNoAI(projection.getCommonSubOperatorMap());
    }

    private static void validateNoAI(Map<?, ? extends ScalarOperator> expressions) {
        if (expressions == null) {
            return;
        }
        expressions.values().forEach(AIFunctionPlacementValidator::validateNoAI);
    }

    private static void validateNoAI(List<? extends ScalarOperator> expressions) {
        if (expressions == null) {
            return;
        }
        expressions.forEach(AIFunctionPlacementValidator::validateNoAI);
    }

    private static void validateRows(List<List<ScalarOperator>> rows) {
        if (rows == null) {
            return;
        }
        rows.forEach(AIFunctionPlacementValidator::validateNoAI);
    }

    private static void validateNoAI(ScalarOperator expression) {
        if (expression == null) {
            return;
        }
        if (isAICall(expression)) {
            throw new StarRocksPlannerException(INVALID_PLACEMENT, ErrorType.INTERNAL_ERROR);
        }
        expression.getChildren().forEach(AIFunctionPlacementValidator::validateNoAI);
    }

    private static boolean isAICall(ScalarOperator expression) {
        return expression instanceof CallOperator call
                && call.getFunction() != null
                && call.getFunction().isAi();
    }
}
