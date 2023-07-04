// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.transformation.materialization;

import com.google.common.base.Preconditions;
import com.google.common.collect.BoundType;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
<<<<<<< HEAD
=======
import com.google.common.collect.TreeRangeSet;
import com.starrocks.analysis.BinaryType;
>>>>>>> 053daa6d45 ([Enhancement] mv rewrite predicate split support multi range (#24880))
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.starrocks.sql.optimizer.rule.transformation.materialization.PredicateSplit.rangeSetToExpr;

@SuppressWarnings("UnstableApiUsage")
public class RangeSimplifier {
<<<<<<< HEAD
    private final List<ScalarOperator> srcPredicates;
=======
    protected static final Logger LOG = LogManager.getLogger(RangeSimplifier.class);

    private final ScalarOperator srcPredicate;
>>>>>>> 053daa6d45 ([Enhancement] mv rewrite predicate split support multi range (#24880))

    public RangeSimplifier(ScalarOperator srcPredicate) {
        this.srcPredicate = srcPredicate;
    }

    // check whether target range predicates are contained in srcPredicates
    // all ScalarOperator should be BinaryPredicateOperator,
    // left is ColumnRefOperator and right is ConstantOperator
    public ScalarOperator simplify(ScalarOperator target) {
        try {

            Map<ColumnRefOperator, TreeRangeSet<ConstantOperator>> srcColumnToRange =
                    extractColumnIdRangeMapping(srcPredicate);
            if (srcColumnToRange == null) {
                return null;
            }
            Map<ColumnRefOperator, TreeRangeSet<ConstantOperator>> targetColumnToRange =
                    extractColumnIdRangeMapping(target);
            if (targetColumnToRange == null) {
                return null;
            }

            Map<Integer, ColumnRefOperator> srcColumnIdMap = srcColumnToRange.keySet()
                    .stream().collect(Collectors.toMap(ColumnRefOperator::getId, Function.identity()));

            Map<Integer, ColumnRefOperator> targetColumnIdMap = targetColumnToRange.keySet()
                    .stream().collect(Collectors.toMap(ColumnRefOperator::getId, Function.identity()));

            List<ColumnRefOperator> resultColumns = Lists.newArrayList();
            for (Map.Entry<Integer, ColumnRefOperator> targetEntry : targetColumnIdMap.entrySet()) {
                ColumnRefOperator targetColumn = targetEntry.getValue();
                TreeRangeSet<ConstantOperator> targetRange = targetColumnToRange.get(targetColumn);

                // Source columnId range must contain any target columnId.
<<<<<<< HEAD
                if (!srcColumnIdToRange.containsKey(targetEntry.getKey())
                        && !targetEntry.getValue().hasUpperBound()
                        && !targetEntry.getValue().hasLowerBound()) {
                    return  null;
=======
                if (!srcColumnToRange.containsKey(targetColumn)
                        && !targetRange.asRanges().stream().allMatch(Range::hasUpperBound)
                        && !targetRange.asRanges().stream().allMatch(Range::hasLowerBound)) {
                    return null;
>>>>>>> 053daa6d45 ([Enhancement] mv rewrite predicate split support multi range (#24880))
                }
                TreeRangeSet<ConstantOperator> srcRange = srcColumnToRange.get(targetColumn);
                if (srcRange.equals(targetRange)) {
                    continue;
                } else if (targetRange.enclosesAll(srcRange)) {
                    resultColumns.add(targetColumn);
                } else {
                    // can not be rewritten
                    return null;
                }
            }

            for (Map.Entry<Integer, ColumnRefOperator> srcEntry : srcColumnIdMap.entrySet()) {
                if (!targetColumnToRange.containsKey(srcEntry.getValue())) {
                    resultColumns.add(srcEntry.getValue());
                }
            }

            if (resultColumns.isEmpty()) {
                return ConstantOperator.createBoolean(true);
            } else {
<<<<<<< HEAD
                List<ScalarOperator> rewrittenRangePredicates = Lists.newArrayList();
                for (int columnId : resultColumnIds) {
                    Range<ConstantOperator> columnRange = srcColumnIdToRange.get(columnId);
                    if (isSingleValueRange(columnRange)) {
                        List<ScalarOperator> columnPredicates = srcPredicates.stream().filter(
                                predicate -> isScalarForColumns(predicate, columnId)
                        ).collect(Collectors.toList());
                        Preconditions.checkState(!columnPredicates.isEmpty());
                        BinaryPredicateOperator binary = (BinaryPredicateOperator) columnPredicates.get(0);
                        BinaryPredicateOperator eqBinary = new BinaryPredicateOperator(
                                BinaryPredicateOperator.BinaryType.EQ,
                                binary.getChild(0), columnRange.lowerEndpoint());
                        rewrittenRangePredicates.add(eqBinary);
                    } else {
                        List<ScalarOperator> columnScalars = srcPredicates.stream().filter(
                                predicate -> isScalarForColumns(predicate, columnId)
                        ).collect(Collectors.toList());
                        columnScalars = filterScalarOperators(columnScalars, columnRange);
                        rewrittenRangePredicates.addAll(columnScalars);
                    }
=======
                List<ScalarOperator> result = new ArrayList<>();
                for (ColumnRefOperator column : resultColumns) {
                    TreeRangeSet<ConstantOperator> columnRange = srcColumnToRange.get(column);
                    result.add(rangeSetToExpr(columnRange, column));
>>>>>>> 053daa6d45 ([Enhancement] mv rewrite predicate split support multi range (#24880))
                }
                return Utils.compoundAnd(result);
            }
        } catch (Exception e) {
<<<<<<< HEAD
=======
            LOG.debug("Simplify scalar operator {} failed:", target, e);
>>>>>>> 053daa6d45 ([Enhancement] mv rewrite predicate split support multi range (#24880))
            return null;
        }
    }

    private Map<Integer, Range<ConstantOperator>> extractColumnIdRangeMapping(List<ScalarOperator> predicates) {
        Map<Integer, Range<ConstantOperator>> columnIdToRange = Maps.newHashMap();
        for (ScalarOperator rangePredicate : predicates) {
            Preconditions.checkState(rangePredicate instanceof BinaryPredicateOperator);
            Preconditions.checkState(rangePredicate.getChild(0) instanceof ColumnRefOperator);
            Preconditions.checkState(rangePredicate.getChild(1) instanceof ConstantOperator);
            BinaryPredicateOperator srcBinary = (BinaryPredicateOperator) rangePredicate;
            Preconditions.checkState(srcBinary.getBinaryType().isRange() || srcBinary.getBinaryType().isEqual());
            ColumnRefOperator srcColumn = (ColumnRefOperator) srcBinary.getChild(0);
            ConstantOperator srcConstant = (ConstantOperator) srcBinary.getChild(1);
            if (!columnIdToRange.containsKey(srcColumn.getId())) {
                columnIdToRange.put(srcColumn.getId(), Range.all());
            }
            Range<ConstantOperator> columnRange = columnIdToRange.get(srcColumn.getId());
            Range<ConstantOperator> range = range(srcBinary.getBinaryType(), srcConstant);
            columnRange = columnRange.intersection(range);
            if (columnRange.isEmpty()) {
                return null;
            }
            columnIdToRange.put(srcColumn.getId(), columnRange);
        }
        return columnIdToRange;
    }

    private List<ScalarOperator> filterScalarOperators(
            List<ScalarOperator> columnScalars, Range<ConstantOperator> validRange) {
        List<ScalarOperator> results = Lists.newArrayList();;
        for (ScalarOperator candidate : columnScalars) {
            if (isRedundantPredicate(candidate, validRange)) {
                continue;
            }
            results.add(candidate);
        }
        return results;
    }

    private boolean isRedundantPredicate(ScalarOperator scalarOperator, Range<ConstantOperator> validRange) {
        Preconditions.checkState(scalarOperator instanceof BinaryPredicateOperator);
        Preconditions.checkState(scalarOperator.getChild(0) instanceof ColumnRefOperator);
        Preconditions.checkState(scalarOperator.getChild(1) instanceof ConstantOperator);
        BinaryPredicateOperator binary = scalarOperator.cast();
        ConstantOperator right = binary.getChild(1).cast();
        Range predicateRange = range(binary.getBinaryType(), right);
        // only range border's predicate is non redundant
        if (predicateRange.hasLowerBound()
                && validRange.hasLowerBound()
                && predicateRange.lowerBoundType() == validRange.lowerBoundType()
                && predicateRange.lowerEndpoint().equals(validRange.lowerEndpoint())) {
            // predicate is lower border
            return false;
        }
        if (predicateRange.hasUpperBound()
                && validRange.hasUpperBound()
                && predicateRange.upperBoundType() == validRange.upperBoundType()
                && predicateRange.upperEndpoint().equals(validRange.upperEndpoint())) {
            // predicate is upper border
            return false;
        }
        return true;
    }

    private boolean isScalarForColumns(ScalarOperator predicate, int columnId) {
        BinaryPredicateOperator binary = (BinaryPredicateOperator) predicate;
        ColumnRefOperator targetColumn = (ColumnRefOperator) binary.getChild(0);
        return targetColumn.getId() == columnId;
    }

    private boolean isSingleValueRange(Range<ConstantOperator> range) {
        // 3 <= range <= 3
        return range.hasLowerBound()
                && range.hasUpperBound()
                && range.lowerBoundType() == BoundType.CLOSED
                && range.upperBoundType() == BoundType.CLOSED
                && range.lowerEndpoint() == range.upperEndpoint();
    }

    private <C extends Comparable<C>> Range<C> range(BinaryPredicateOperator.BinaryType type, C value) {
        switch (type) {
            case EQ:
                return Range.singleton(value);
            case GE:
                return Range.atLeast(value);
            case GT:
                return Range.greaterThan(value);
            case LE:
                return Range.atMost(value);
            case LT:
                return Range.lessThan(value);
            default:
                throw new UnsupportedOperationException("unsupported type:" + type);
        }
    }

    private Map<ColumnRefOperator, TreeRangeSet<ConstantOperator>> extractColumnIdRangeMapping(ScalarOperator predicate) {
        PredicateSplit.RangeExtractor extractor = new PredicateSplit.RangeExtractor();
        return predicate.accept(extractor, new PredicateSplit.RangeExtractorContext());
    }


}
