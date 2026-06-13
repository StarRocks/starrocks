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
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalWindowOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.skew.DataSkew;
import com.starrocks.sql.optimizer.statistics.Statistics;

import java.util.Collections;
import java.util.List;

/*
 * Rule Objective:
 *
 * When a window analytic operator has a skewed partition key, set forceMergeSort = true.
 * This causes the RequiredPropertyDeriver to use GatherDistributionSpec instead of hash shuffle,
 * which sorts the data first using merge-exchange, ensuring that workload during sort is spread evenly.
 *
 * Two session variables control the behavior:
 *   - enable_window_skew_merge_sort: enables statistics-based skew detection
 *   - force_window_merge_sort: unconditionally forces the flag, bypassing statistics
 */
public class WindowSkewToMergeSortRule extends TransformationRule {

    private static final WindowSkewToMergeSortRule INSTANCE = new WindowSkewToMergeSortRule();

    private WindowSkewToMergeSortRule() {
        super(RuleType.TF_WINDOW_SKEW_TO_MERGE_SORT, Pattern.create(OperatorType.LOGICAL_WINDOW));
    }

    public static WindowSkewToMergeSortRule getInstance() {
        return INSTANCE;
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        if (input.getOp() instanceof LogicalWindowOperator lwo) {
            List<ScalarOperator> partitionExprs = lwo.getPartitionExpressions();
            return partitionExprs != null
                    && !partitionExprs.isEmpty()
                    && lwo.getOrderByElements() != null
                    && !lwo.getOrderByElements().isEmpty()
                    && !lwo.isForceMergeSort();
        }
        return false;
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalWindowOperator window = (LogicalWindowOperator) input.getOp();

        // Force mode: unconditionally set forceMergeSort, bypass statistics and partition count
        if (context.getSessionVariable().isForceWindowMergeSort()) {
            return buildResult(window, input);
        }

        // Enable mode: only applies when there are multiple partition expressions.
        // For single-partition windows, SplitWindowSkewToUnionRule is the preferred strategy.
        if (window.getPartitionExpressions().size() <= 1) {
            return Collections.emptyList();
        }

        // Enable mode: trigger if any individual partition column shows skew from child statistics.
        OptExpression child = input.inputAt(0);
        Statistics statistics = child.getStatistics();
        if (statistics == null) {
            return Collections.emptyList();
        }

        if (hasAnySkewedPartitionColumn(window, statistics, context)) {
            return buildResult(window, input);
        }

        return Collections.emptyList();
    }

    private boolean hasAnySkewedPartitionColumn(LogicalWindowOperator window, Statistics statistics,
                                                OptimizerContext context) {
        double threshold = context.getSessionVariable().getDataSkewRowPercentageThreshold();
        DataSkew.Thresholds thresholds = DataSkew.Thresholds.withRelativeRowThreshold(threshold);

        for (ScalarOperator partitionExpr : window.getPartitionExpressions()) {
            // Skip expressions we cannot analyze; a single un-analyzable column should not
            // prevent us from detecting skew on the remaining partition columns.
            if (!(partitionExpr instanceof ColumnRefOperator col)) {
                continue;
            }
            if (!statistics.getColumnStatistics().containsKey(col)) {
                continue;
            }

            if (DataSkew.isColumnSkewed(statistics, statistics.getColumnStatistic(col), thresholds)) {
                return true;
            }
        }
        return false;
    }

    private List<OptExpression> buildResult(LogicalWindowOperator originalWindow, OptExpression input) {
        LogicalWindowOperator newWindow = new LogicalWindowOperator.Builder()
                .withOperator(originalWindow)
                .setForceMergeSort(true)
                .setUseHashBasedPartition(false)
                .setIsSkewed(false)
                .setSkewColumn(null)
                .setSkewValues(Collections.emptyList())
                .build();
        return Lists.newArrayList(OptExpression.create(newWindow, input.getInputs()));
    }
}
