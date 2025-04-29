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

package com.starrocks.qe.feedback;

import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import com.google.common.collect.TreeRangeSet;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriteContext;
import com.starrocks.sql.optimizer.rewrite.scalar.MvNormalizePredicateRule;
import com.starrocks.sql.optimizer.rule.transformation.materialization.AndRangePredicate;
import com.starrocks.sql.optimizer.rule.transformation.materialization.ColumnRangePredicate;
import com.starrocks.sql.optimizer.rule.transformation.materialization.OrRangePredicate;
import com.starrocks.sql.optimizer.rule.transformation.materialization.PredicateExtractor;
import com.starrocks.sql.optimizer.rule.transformation.materialization.PredicateSplit;
import com.starrocks.sql.optimizer.rule.transformation.materialization.RangePredicate;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;

/**
 * Represents a predicate that can be parameterized for comparison and merging operations.
 * This class analyzes predicates to determine if they have compatible structures and
 * supports both comparing predicates and merging their range constraints.
 */
public class ParameterizedPredicate {
    // Whether the predicate has a structure suitable for comparison operations
    private boolean isValidForComparison;
    private boolean enableParameterizedMode = false;
    private final ScalarOperator originPredicate;

    // The part of the predicate that couldn't be converted to range predicates
    private ScalarOperator residualPredicate;
    private final Map<ColumnRefOperator, ColumnRangePredicate> columnRangePredicates = new HashMap<>();

    public ParameterizedPredicate(ScalarOperator predicate) {
        this.originPredicate = predicate;
        try {
            analyzePredicate();
        } catch (Exception e) {
            // ignore
        }

    }

    /**
     * Analyzes the predicate to determine if it's valid for comparison and extracts its components.
     */
    private void analyzePredicate() {
        PredicateSplit splitPredicate = PredicateSplit.splitPredicate(originPredicate);

        // Equal predicates are not supported for comparison
        if (splitPredicate.getEqualPredicates() != null) {
            this.isValidForComparison = false;
            return;
        }

        // Process the residual predicates
        this.residualPredicate = normalizedPredicate(splitPredicate.getResidualPredicates());

        // Process range predicates if present
        ScalarOperator rangePredicates = splitPredicate.getRangePredicates();
        if (rangePredicates == null) {
            return;
        }

        processRangePredicates(rangePredicates);
    }

    /**
     * Processes range predicates by normalizing, extracting, and validating them.
     *
     * @param rangePredicates The range predicates to process
     */
    private void processRangePredicates(ScalarOperator rangePredicates) {
        ScalarOperator normalizedRangePredicate = normalizedPredicate(rangePredicates);
        if (normalizedRangePredicate == null) {
            return;
        }

        RangePredicate rangePredicate = extractRangePredicate(normalizedRangePredicate);
        if (!isValidRangePredicate(rangePredicate)) {
            this.isValidForComparison = false;
            return;
        }

        extractColumnPredicatesRecursive(rangePredicate);
        this.isValidForComparison = true;
    }


    public boolean match(ParameterizedPredicate other) {
        if (!isValidForComparison || !other.isValidForComparison) {
            return false;
        }

        if (!Objects.equals(residualPredicate, other.residualPredicate)) {
            return false;
        }

        if (columnRangePredicates.isEmpty() && other.columnRangePredicates.isEmpty()) {
            return true;
        }

        if (columnRangePredicates.size() != other.columnRangePredicates.size()) {
            return false;
        }

        for (Map.Entry<ColumnRefOperator, ColumnRangePredicate> entry : columnRangePredicates.entrySet()) {
            ColumnRefOperator columnRefOperator = entry.getKey();
            ColumnRangePredicate columnRangePredicate = entry.getValue();
            ColumnRangePredicate otherColumnRangePredicate = other.columnRangePredicates.get(columnRefOperator);
            if (otherColumnRangePredicate == null) {
                return false;
            }

            if (!enableParameterizedMode && !otherColumnRangePredicate.enclose(columnRangePredicate)) {
                return false;
            }
        }

        return true;
    }

    /**
     * Merges the column ranges from the source predicate into this predicate.
     * This operation modifies the current predicate by expanding its ranges.
     *
     * @param source The source predicate whose ranges will be merged into this one
     */
    public void mergeColumnRange(ParameterizedPredicate source) {
        if (!isValidForComparison || !source.isValidForComparison) {
            return;
        }

        if (columnRangePredicates.isEmpty() || source.columnRangePredicates.isEmpty()) {
            return;
        }

        for (Map.Entry<ColumnRefOperator, ColumnRangePredicate> entry : columnRangePredicates.entrySet()) {
            ColumnRefOperator columnRefOperator = entry.getKey();
            ColumnRangePredicate columnRangePredicate = entry.getValue();
            ColumnRangePredicate otherColumnRangePredicate = source.columnRangePredicates.get(columnRefOperator);
            if (otherColumnRangePredicate == null) {
                continue;
            }

            ColumnRangePredicate mergedRangePredicate = mergeRanges(columnRangePredicate, otherColumnRangePredicate);
            columnRangePredicates.put(columnRefOperator, mergedRangePredicate);
        }

    }

    /**
     * Merges two column range predicates into a single column range predicate.
     * The merge strategy varies based on the types and relationships of the ranges:
     * 1. Single points merging (e.g., x=1 and x=5):
     *    - Creates a closed range between the points (e.g., 1 <= x <= 5)
     *    - If points are identical, keeps as a single point
     * 2. Point and range merging (e.g., x=3 and 1 <= x <= 10):
     *    - If point is within range, keeps the original range
     *    - If point is outside range, extends the range to include the point (e.g., 1 and [3, 5] becomes [1, 5])
     *    - For unbounded ranges, preserves unboundedness while including the point
     * 3. General ranges merging:
     *    - For overlapping/intersecting ranges, creates their union (e.g., [1,5] and [3,8] becomes [1,8])
     *    - For disjoint ranges, keeps both ranges separate (e.g., [1,5] and [8,10] stays as two ranges)
     *    - The TreeRangeSet automatically handles merging of adjacent or overlapping ranges
     * The method operates only on ranges for the same column reference, returning null
     * if column references don't match between predicates.
     *
     * @param target The target column range predicate
     * @param source The source column range predicate
     * @return A new column range predicate containing the merged ranges
     */
    public ColumnRangePredicate mergeRanges(ColumnRangePredicate target, ColumnRangePredicate source) {
        if (target == null || source == null) {
            return target;
        }

        if (!Objects.equals(target.getColumnRef(), source.getColumnRef())) {
            return null;
        }

        Set<Range<ConstantOperator>> targetRanges = target.getColumnRanges().asRanges();
        Set<Range<ConstantOperator>> sourceRanges = source.getColumnRanges().asRanges();

        // Handle simple case: single range in both predicates
        if (targetRanges.size() == 1 && sourceRanges.size() == 1) {
            return mergeSimpleRanges(
                    target.getExpression(),
                    targetRanges.iterator().next(),
                    sourceRanges.iterator().next()
            );
        }

        // Handle general case: multiple ranges
        return mergeComplexRanges(target.getExpression(), targetRanges, sourceRanges);
    }

    /**
     * Merges two simple ranges (one range per predicate).
     *
     * @param expression The scalar operator expression
     * @param targetRange The target range
     * @param sourceRange The source range
     * @return A new column range predicate with the merged range
     */
    private ColumnRangePredicate mergeSimpleRanges(
            ScalarOperator expression,
            Range<ConstantOperator> targetRange,
            Range<ConstantOperator> sourceRange) {

        if (isSinglePoint(targetRange) && isSinglePoint(sourceRange)) {
            return mergeSinglePoints(expression, targetRange, sourceRange);
        }

        if (isSinglePoint(targetRange) || isSinglePoint(sourceRange)) {
            return mergePointAndRange(expression, targetRange, sourceRange);
        }

        // General case: merge two ranges
        TreeRangeSet<ConstantOperator> resultRanges = TreeRangeSet.create();
        resultRanges.add(targetRange);
        resultRanges.add(sourceRange);
        return new ColumnRangePredicate(expression, resultRanges);
    }

    /**
     * Merges complex ranges (multiple ranges per predicate).
     *
     * @param expression The scalar operator expression
     * @param targetRanges The target ranges
     * @param sourceRanges The source ranges
     * @return A new column range predicate with the merged ranges
     */
    private ColumnRangePredicate mergeComplexRanges(
            ScalarOperator expression,
            Set<Range<ConstantOperator>> targetRanges,
            Set<Range<ConstantOperator>> sourceRanges) {

        TreeRangeSet<ConstantOperator> resultRanges = TreeRangeSet.create();
        resultRanges.addAll(targetRanges);
        resultRanges.addAll(sourceRanges);
        return new ColumnRangePredicate(expression, resultRanges);
    }

    private ColumnRangePredicate mergeSinglePoints(
            ScalarOperator expression,
            Range<ConstantOperator> targetRange,
            Range<ConstantOperator> sourceRange) {

        ConstantOperator targetPoint = targetRange.lowerEndpoint();
        ConstantOperator sourcePoint = sourceRange.lowerEndpoint();
        TreeRangeSet<ConstantOperator> resultRanges = TreeRangeSet.create();

        int compareResult = targetPoint.compareTo(sourcePoint);
        Range<ConstantOperator> mergedRange;

        if (compareResult == 0) {
            // Points are equal, keep one of them
            mergedRange = targetRange;
        } else if (compareResult < 0) {
            // Target point is smaller, create range from target to source
            mergedRange = Range.closed(targetPoint, sourcePoint);
        } else {
            // Source point is smaller, create range from source to target
            mergedRange = Range.closed(sourcePoint, targetPoint);
        }

        resultRanges.add(mergedRange);
        return new ColumnRangePredicate(expression, resultRanges);
    }

    /**
     * Merges a point range with a non-point range.
     *
     * @param expression The scalar operator expression
     * @param targetRange The first range
     * @param sourceRange The second range
     * @return A new column range predicate with the merged range
     */
    private ColumnRangePredicate mergePointAndRange(
            ScalarOperator expression,
            Range<ConstantOperator> targetRange,
            Range<ConstantOperator> sourceRange) {

        // Determine which range is the point and which is the normal range
        Range<ConstantOperator> pointRange = isSinglePoint(targetRange) ? targetRange : sourceRange;
        Range<ConstantOperator> normalRange = isSinglePoint(targetRange) ? sourceRange : targetRange;
        ConstantOperator point = pointRange.lowerEndpoint();
        TreeRangeSet<ConstantOperator> resultRanges = TreeRangeSet.create();

        // Handle unbounded ranges
        if (!normalRange.hasLowerBound() && !normalRange.hasUpperBound()) {
            resultRanges.add(normalRange);
            return new ColumnRangePredicate(expression, resultRanges);
        }

        if (!normalRange.hasLowerBound()) {
            if (normalRange.contains(point)) {
                resultRanges.add(normalRange);
            } else {
                resultRanges.add(Range.atMost(point));
            }
            return new ColumnRangePredicate(expression, resultRanges);
        }

        if (!normalRange.hasUpperBound()) {
            if (normalRange.contains(point)) {
                resultRanges.add(normalRange);
            } else {
                resultRanges.add(Range.atLeast(point));
            }
            return new ColumnRangePredicate(expression, resultRanges);
        }

        // Handle bounded ranges
        if (normalRange.contains(point)) {
            // Point is within the range, keep the original range
            resultRanges.add(normalRange);
        } else if (point.compareTo(normalRange.lowerEndpoint()) < 0) {
            // Point is below the range, extend the lower bound
            resultRanges.add(Range.range(
                    point, BoundType.CLOSED,
                    normalRange.upperEndpoint(), normalRange.upperBoundType()));
        } else {
            // Point is above the range, extend the upper bound
            resultRanges.add(Range.range(
                    normalRange.lowerEndpoint(), normalRange.lowerBoundType(),
                    point, BoundType.CLOSED));
        }

        return new ColumnRangePredicate(expression, resultRanges);
    }

    /**
     * Checks if a range represents a single point.
     * A single point is a closed range where lower and upper endpoints are equal.
     *
     * @param range The range to check
     * @return true if the range represents a single point, false otherwise
     */
    private boolean isSinglePoint(Range<ConstantOperator> range) {
        return range.hasLowerBound() && range.hasUpperBound()
                && range.lowerBoundType() == BoundType.CLOSED
                && range.upperBoundType() == BoundType.CLOSED
                && range.lowerEndpoint().equals(range.upperEndpoint());
    }

    private ScalarOperator normalizedPredicate(ScalarOperator predicate) {
        if (predicate == null) {
            return null;
        }
        return new MvNormalizePredicateRule().visit(predicate, new ScalarOperatorRewriteContext());
    }

    private RangePredicate extractRangePredicate(ScalarOperator predicate) {
        PredicateExtractor extractor = new PredicateExtractor();
        return predicate.accept(extractor, new PredicateExtractor.PredicateExtractorContext());
    }

    public boolean isValidRangePredicate(RangePredicate predicate) {
        Set<ColumnRefOperator> seenColumns = new HashSet<>();
        return validatePredicateStructure(predicate, seenColumns);
    }

    /**
     * Validates the structure of a range predicate.
     * A valid predicate must not contain OR predicates or duplicate column references.
     *
     * @param predicate The predicate to validate
     * @param seenColumns Set of columns already seen in the predicate tree
     * @return true if the predicate structure is valid, false otherwise
     */
    private boolean validatePredicateStructure(RangePredicate predicate, Set<ColumnRefOperator> seenColumns) {
        if (predicate == null) {
            return true;
        }

        if (predicate instanceof OrRangePredicate) {
            return false;
        }

        if (predicate instanceof ColumnRangePredicate) {
            ColumnRefOperator column = ((ColumnRangePredicate) predicate).getColumnRef();

            if (seenColumns.contains(column)) {
                return false;
            }

            seenColumns.add(column);
            return true;
        }

        if (predicate instanceof AndRangePredicate andPredicate) {
            for (RangePredicate child : andPredicate.getChildPredicates()) {
                if (!validatePredicateStructure(child, seenColumns)) {
                    return false;
                }
            }

            return true;
        }

        return true;
    }

    private void extractColumnPredicatesRecursive(RangePredicate predicate) {
        if (predicate == null) {
            return;
        }

        if (predicate instanceof ColumnRangePredicate columnRangePredicate) {
            columnRangePredicates.put(columnRangePredicate.getColumnRef(), columnRangePredicate);
        } else if (predicate instanceof AndRangePredicate) {
            for (RangePredicate child : predicate.getChildPredicates()) {
                extractColumnPredicatesRecursive(child);
            }
        }
    }

    public boolean isEnableParameterizedMode() {
        return enableParameterizedMode;
    }

    public void enableParameterizedMode() {
        this.enableParameterizedMode = true;
    }

    public void disableParameterizedMode() {
        this.enableParameterizedMode = false;
    }

    public Map<ColumnRefOperator, ColumnRangePredicate> getColumnRangePredicates() {
        return columnRangePredicates;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", ParameterizedPredicate.class.getSimpleName() + "[", "]")
                .add("isValidForComparison=" + isValidForComparison)
                .add("enableParameterizedMode=" + enableParameterizedMode)
                .add("originPredicate=" + originPredicate.toString())
                .add("residualPredicate=" + residualPredicate.toString())
                .add("columnRangePredicates=" + columnRangePredicates)
                .toString();
    }
}
