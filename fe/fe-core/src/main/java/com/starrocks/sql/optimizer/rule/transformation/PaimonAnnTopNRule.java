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

import com.starrocks.catalog.Column;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.connector.paimon.PaimonAnnQuery;
import com.starrocks.connector.paimon.PaimonGlobalIndexMode;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.SortPhase;
import com.starrocks.sql.optimizer.operator.TopNType;
import com.starrocks.sql.optimizer.operator.logical.LogicalPaimonScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTopNOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.type.ArrayType;
import com.starrocks.type.FloatType;

import java.util.ArrayList;
import java.util.List;

/** Carries a vector Top-N intent to final scan planning while keeping the original SQL operators. */
public class PaimonAnnTopNRule extends TransformationRule {
    public PaimonAnnTopNRule() {
        super(RuleType.TF_PAIMON_VECTOR_REWRITE_RULE, Pattern.create(OperatorType.LOGICAL_TOPN)
                .addChildren(Pattern.create(OperatorType.LOGICAL_PAIMON_SCAN)));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        PaimonGlobalIndexMode mode = context.getSessionVariable().getPaimonGlobalIndexMode();
        if (mode != PaimonGlobalIndexMode.AUTO && mode != PaimonGlobalIndexMode.DISTRIBUTED) {
            return List.of();
        }
        LogicalTopNOperator topN = input.getOp().cast();
        LogicalPaimonScanOperator scan = input.inputAt(0).getOp().cast();
        if (scan.getAnnQuery() != null || scan.getProjection() == null || scan.getPredicate() != null
                || scan.hasLimit() || topN.getPredicate() != null || topN.getLimit() <= 0 || topN.getOffset() < 0
                || topN.getLimit() > Integer.MAX_VALUE - topN.getOffset() || topN.isSplit()
                || topN.getSortPhase() != SortPhase.FINAL || topN.getTopNType() != TopNType.ROW_NUMBER
                || topN.isPerPipeline() || topN.getOrderByElements().size() != 1
                || topN.getPartitionByColumns() != null && !topN.getPartitionByColumns().isEmpty()) {
            return List.of();
        }
        var ordering = topN.getOrderByElements().get(0);
        ScalarOperator expression = scan.getProjection().getColumnRefMap().get(ordering.getColumnRef());
        if (!(expression instanceof CallOperator score) || score.getChildren().size() != 2) {
            return List.of();
        }
        String metric = switch (score.getFnName()) {
            case FunctionSet.APPROX_L2_DISTANCE -> ordering.isAscending() ? "l2" : null;
            case FunctionSet.APPROX_COSINE_SIMILARITY -> ordering.isAscending() ? null : "cosine";
            case FunctionSet.APPROX_INNER_PRODUCT -> ordering.isAscending() ? null : "inner_product";
            default -> null;
        };
        if (metric == null) {
            return List.of();
        }
        ScalarOperator column = score.getChild(0);
        ScalarOperator literal = score.getChild(1);
        if (!(column instanceof ColumnRefOperator)) {
            column = score.getChild(1);
            literal = score.getChild(0);
        }
        if (!(column instanceof ColumnRefOperator reference)
                || !(reference.getType() instanceof ArrayType array) || !FloatType.FLOAT.equals(array.getItemType())
                || !RewriteToVectorPlanRule.isConstantArrayFloat(literal)) {
            return List.of();
        }
        Column tableColumn = scan.getColRefToColumnMetaMap().get(reference);
        if (tableColumn == null) {
            return List.of();
        }
        List<Float> vector;
        try {
            List<String> elements = new ArrayList<>();
            RewriteToVectorPlanRule.extractValuesFromConstantArray(literal, elements);
            vector = elements.stream().map(Float::parseFloat).toList();
        } catch (SemanticException | NumberFormatException unsupportedLiteral) {
            return List.of();
        }
        if (vector.isEmpty() || vector.size() > 4096 || vector.stream().anyMatch(value -> !Float.isFinite(value))
                || "cosine".equals(metric) && vector.stream().allMatch(value -> value == 0)) {
            return List.of();
        }
        int k = (int) (topN.getLimit() + topN.getOffset());
        var candidate = new LogicalPaimonScanOperator.Builder().withOperator(scan)
                .setAnnQuery(new PaimonAnnQuery(tableColumn.getName(), metric, vector, k)).build();
        return List.of(OptExpression.create(topN, OptExpression.create(candidate)));
    }
}
