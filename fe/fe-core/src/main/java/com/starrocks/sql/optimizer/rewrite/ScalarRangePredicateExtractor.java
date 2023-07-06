// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.optimizer.rewrite;

import com.google.common.base.Preconditions;
import com.google.common.collect.BoundType;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator.BinaryType;

/**
 * Derive a expression's value range. such as:
 * select * from t0 where v1 > 1 AND v1 < 5 => v1 is (1, 5)
 * select * from t0 where (v1 > 1 AND v1 < 5) OR (v1 = 3 OR v1 = 6) => v1 is (1, 6]
 * select * from t0 where (v1 + 1) = 3 AND (v1 + 1) = 6 => (v1 + 1) must be null
 * <p>
 * The core of the algorithm:
 * 1. Take the common set of the predicates on children of or, and compute UNION set on the same expressions
 * 2. Take the full set of the predicates on children of AND, and compute INTERSECT set on the same expressions
 */
public class ScalarRangePredicateExtractor {
    public ScalarOperator rewriteOnlyColumn(ScalarOperator predicate) {
        return rewrite(predicate, true);
    }

    public ScalarOperator rewriteAll(ScalarOperator predicate) {
        return rewrite(predicate, false);
    }

    private ScalarOperator rewrite(ScalarOperator predicate, boolean onlyExtractColumnRef) {
        if (predicate.getOpType() != OperatorType.COMPOUND) {
            return predicate;
        }

        Set<ScalarOperator> conjuncts = Sets.newLinkedHashSet();
        conjuncts.addAll(Utils.extractConjuncts(predicate));

        Map<ScalarOperator, ValueDescriptor> extractMap = extractImpl(predicate);

        Set<ScalarOperator> result = Sets.newLinkedHashSet();
        extractMap.keySet().stream().filter(k -> !onlyExtractColumnRef || k.isColumnRef())
                .map(extractMap::get)
                .filter(d -> d.sourceCount > 1)
                .map(ValueDescriptor::toScalarOperator).forEach(result::addAll);

        List<ScalarOperator> decimalKeys =
                extractMap.keySet().stream().filter(k -> !onlyExtractColumnRef || k.isColumnRef())
                        .filter(k -> k.getType().isDecimalOfAnyVersion()).collect(Collectors.toList());
        if (!decimalKeys.isEmpty()) {
            for (ScalarOperator key : decimalKeys) {
                ValueDescriptor vd = extractMap.get(key);
                vd.toScalarOperator().forEach(s -> Preconditions.checkState(
                        s.getChildren().stream().allMatch(c -> c.getType().matchesType(key.getType()))));
            }
        }

        ScalarOperator extractExpr = Utils.compoundAnd(Lists.newArrayList(result));
        if (extractExpr == null) {
            return predicate;
        }

        predicate = Utils.compoundAnd(Lists.newArrayList(conjuncts));
        if (isOnlyOrCompound(predicate)) {
            Set<ColumnRefOperator> c = Sets.newHashSet(Utils.extractColumnRef(predicate));
            if (c.size() == extractMap.size() &&
                    extractMap.values().stream().allMatch(v -> v instanceof MultiValuesDescriptor)) {
                return extractExpr;
            }
        }

        if (isOnlyAndCompound(predicate)) {
            List<ScalarOperator> cs = Utils.extractConjuncts(predicate);
            Set<ColumnRefOperator> cf = new HashSet<>(Utils.extractColumnRef(predicate));

            if (extractMap.values().stream().allMatch(valueDescriptor -> valueDescriptor.sourceCount == cs.size())
                    && extractMap.size() == cf.size()) {
                return extractExpr;
            }
        }

        if (!conjuncts.contains(extractExpr)) {
            result.forEach(f -> f.setFromPredicateRangeDerive(true));
            result.stream().filter(predicateOperator -> !checkStatisticsEstimateValid(predicateOperator))
                    .forEach(f -> f.setNotEvalEstimate(true));
            // The newly extracted predicate will not be used to estimate the statistics,
            // which will cause the cardinality to be too small
            extractExpr.setFromPredicateRangeDerive(true);
            if (!checkStatisticsEstimateValid(extractExpr)) {
                extractExpr.setNotEvalEstimate(true);
            }
            // TODO: merge `setFromPredicateRangeDerive` into `setRedundant`
            result.forEach(f -> f.setRedundant(true));
            extractExpr.setRedundant(true);
            return Utils.compoundAnd(predicate, extractExpr);
        }

        return predicate;
    }

    private boolean checkStatisticsEstimateValid(ScalarOperator predicate) {
        for (ScalarOperator child : predicate.getChildren()) {
            if (!checkStatisticsEstimateValid(child)) {
                return false;
            }
        }
        // we can not estimate the char/varchar type Min/Max column statistics
        if (predicate.getType().isStringType()) {
            return false;
        }
        return true;
    }

    private Map<ScalarOperator, ValueDescriptor> extractImpl(ScalarOperator scalarOperator) {
        RangeExtractor re = new RangeExtractor();
        return re.apply(scalarOperator, null);
    }

    private static class RangeExtractor extends ScalarOperatorVisitor<Void, Void> {
        private final Map<ScalarOperator, ValueDescriptor> descMap = Maps.newHashMap();

        public Map<ScalarOperator, ValueDescriptor> apply(ScalarOperator scalarOperator, Void context) {
            scalarOperator.accept(this, context);
            return descMap;
        }

        @Override
        public Void visit(ScalarOperator scalarOperator, Void context) {
            return null;
        }

        @Override
        public Void visitBinaryPredicate(BinaryPredicateOperator predicate, Void context) {
            if (predicate.getChild(1).isConstantRef() && predicate.getBinaryType() != BinaryType.NE
                    && predicate.getBinaryType() != BinaryType.EQ_FOR_NULL) {

                if (predicate.getChild(0).getType().isStringType() && predicate.getBinaryType() != BinaryType.EQ) {
                    return visit(predicate.getChild(0), context);
                }
                Preconditions.checkState(!descMap.containsKey(predicate.getChild(0)));
                descMap.put(predicate.getChild(0), ValueDescriptor.range(predicate));
            }

            return visit(predicate, context);
        }

        @Override
        public Void visitInPredicate(InPredicateOperator predicate, Void context) {
            if (!predicate.isNotIn() && predicate.allValuesMatch(ScalarOperator::isConstantRef)) {
                Preconditions.checkState(!descMap.containsKey(predicate.getChild(0)));
                descMap.put(predicate.getChild(0), ValueDescriptor.in(predicate));
            }

            return visit(predicate, context);
        }

        @Override
        public Void visitCompoundPredicate(CompoundPredicateOperator predicate, Void context) {
            if (predicate.isNot()) {
                return visit(predicate, context);
            }

            Map<ScalarOperator, ValueDescriptor> leftMap =
                    new RangeExtractor().apply(predicate.getChild(0), context);
            Map<ScalarOperator, ValueDescriptor> rightMap =
                    new RangeExtractor().apply(predicate.getChild(1), context);

            HashMap<ScalarOperator, ValueDescriptor> intersectMap = Maps.newHashMap();
            Set<ScalarOperator> intersectKeys = Sets.intersection(leftMap.keySet(), rightMap.keySet());

            if (predicate.isOr()) {
                for (ScalarOperator s : intersectKeys) {
                    ValueDescriptor rangeDescriptor = leftMap.get(s);
                    intersectMap.put(s, rangeDescriptor.union(rightMap.get(s)));
                }

                descMap.clear();
                descMap.putAll(intersectMap);

                return visit(predicate, context);
            } else if (predicate.isAnd()) {
                for (ScalarOperator s : intersectKeys) {
                    ValueDescriptor rangeDescriptor = leftMap.get(s);
                    intersectMap.put(s, rangeDescriptor.intersect(rightMap.get(s)));
                }

                descMap.clear();
                descMap.putAll(leftMap);
                descMap.putAll(rightMap);
                descMap.putAll(intersectMap);
            }

            return visit(predicate, context);
        }
    }

    /**
     * Describes the possible values of the column
     */
    private abstract static class ValueDescriptor {
        protected ScalarOperator columnRef;
        protected int sourceCount = 1;

        public ValueDescriptor(ScalarOperator ref) {
            columnRef = ref;
        }

        protected ValueDescriptor(ValueDescriptor o1, ValueDescriptor o2) {
            Preconditions.checkState(o1.columnRef.equals(o2.columnRef));
            this.columnRef = o1.columnRef;
            this.sourceCount = o1.sourceCount + o2.sourceCount;
        }

        public abstract ValueDescriptor union(ValueDescriptor other);

        public abstract ValueDescriptor intersect(ValueDescriptor other);

        public abstract List<ScalarOperator> toScalarOperator();

        public static ValueDescriptor in(ScalarOperator operator) {
            MultiValuesDescriptor d = new MultiValuesDescriptor(operator.getChild(0));
            operator.getChildren().stream().skip(1).map(c -> (ConstantOperator) c)
                    .filter(c -> !c.isNull()).forEach(d.values::add);
            return d;
        }

        public static ValueDescriptor range(ScalarOperator operator) {
            BinaryType type = ((BinaryPredicateOperator) operator).getBinaryType();

            if (type == BinaryType.EQ) {
                MultiValuesDescriptor d = new MultiValuesDescriptor(operator.getChild(0));
                d.values.add((ConstantOperator) operator.getChild(1));
                return d;
            }

            RangeDescriptor d = new RangeDescriptor(operator.getChild(0));
            ConstantOperator value = (ConstantOperator) operator.getChild(1);

            // Must simpled in scalar rule
            Preconditions.checkState(!value.isNull());

            switch (type) {
                case GE: {
                    d.range = Range.atLeast(value);
                    break;
                }
                case GT: {
                    d.range = Range.greaterThan(value);
                    break;
                }
                case LE: {
                    d.range = Range.atMost(value);
                    break;
                }
                case LT: {
                    d.range = Range.lessThan(value);
                    break;
                }
                case NE:
                case EQ_FOR_NULL: {
                    break;
                }
            }
            return d;
        }

        protected static ValueDescriptor mergeValuesAndRange(MultiValuesDescriptor value, RangeDescriptor range) {
            RangeDescriptor result = new RangeDescriptor(value, range);

            if (value.values.isEmpty()) {
                result.range = range.range;
            } else if (range.range == null) {
                result.range = Range.encloseAll(value.values);
            } else {
                result.range = Range.encloseAll(value.values).span(range.range);
            }

            return result;
        }

        protected static ValueDescriptor intersectValuesAndRange(MultiValuesDescriptor value, RangeDescriptor range) {
            MultiValuesDescriptor result = new MultiValuesDescriptor(value, range);

            if (range.range == null) {
                return result;
            }
            value.values.stream().filter(x -> range.range.test(x)).forEach(result.values::add);
            return result;
        }
    }

    private static class MultiValuesDescriptor extends ValueDescriptor {
        protected Set<ConstantOperator> values = new LinkedHashSet<>();

        public MultiValuesDescriptor(ScalarOperator ref) {
            super(ref);
        }

        public MultiValuesDescriptor(ValueDescriptor o1, ValueDescriptor o2) {
            super(o1, o2);
        }

        @Override
        public ValueDescriptor union(ValueDescriptor other) {
            if (other instanceof MultiValuesDescriptor) {
                MultiValuesDescriptor result = new MultiValuesDescriptor(this, other);
                result.values.addAll(values);
                result.values.addAll(((MultiValuesDescriptor) other).values);
                return result;
            }

            return mergeValuesAndRange(this, (RangeDescriptor) other);
        }

        @Override
        public ValueDescriptor intersect(ValueDescriptor other) {
            if (other instanceof MultiValuesDescriptor) {
                MultiValuesDescriptor result = new MultiValuesDescriptor(this, other);
                result.values.addAll(values);
                result.values.retainAll(((MultiValuesDescriptor) other).values);
                return result;
            }

            return intersectValuesAndRange(this, (RangeDescriptor) other);
        }

        @Override
        public List<ScalarOperator> toScalarOperator() {
            Preconditions.checkState(values != null, "invalid scalar values predicate extract");

            if (values.isEmpty()) {
                return Lists.newArrayList(ConstantOperator.createNull(columnRef.getType()));
            } else if (values.size() == 1) {
                return Lists.newArrayList(
                        new BinaryPredicateOperator(BinaryType.EQ, columnRef, values.iterator().next()));
            } else {
                InPredicateOperator ipo = new InPredicateOperator(false, columnRef);
                ipo.getChildren().addAll(values);
                return Lists.newArrayList(ipo);
            }
        }
    }

    private static class RangeDescriptor extends ValueDescriptor {
        protected Range<ConstantOperator> range = null;

        public RangeDescriptor(ScalarOperator ref) {
            super(ref);
        }

        public RangeDescriptor(ValueDescriptor o1, ValueDescriptor o2) {
            super(o1, o2);
        }

        @Override
        public ValueDescriptor union(ValueDescriptor other) {
            if (other instanceof RangeDescriptor) {
                RangeDescriptor result = new RangeDescriptor(this, other);
                RangeDescriptor o = (RangeDescriptor) other;
                if (o.range == null) {
                    result.range = range;
                } else if (range == null) {
                    result.range = o.range;
                } else {
                    result.range = range.span(o.range);
                }
                return result;
            }

            return mergeValuesAndRange((MultiValuesDescriptor) other, this);
        }

        @Override
        public ValueDescriptor intersect(ValueDescriptor other) {
            if (other instanceof RangeDescriptor) {
                RangeDescriptor result = new RangeDescriptor(this, other);
                RangeDescriptor o = (RangeDescriptor) other;
                if (range == null || o.range == null) {
                    return result;
                }

                try {
                    result.range = range.intersection(o.range);
                } catch (Exception ignore) {
                    // empty range
                    return new MultiValuesDescriptor(this, other);
                }

                return result;
            }

            return intersectValuesAndRange((MultiValuesDescriptor) other, this);
        }

        @Override
        public List<ScalarOperator> toScalarOperator() {
            Preconditions.checkState(range != null, "invalid scalar range predicate extract");
            List<ScalarOperator> operators = Lists.newArrayList();
            if (range.hasLowerBound()) {
                BinaryType type = range.lowerBoundType() == BoundType.CLOSED ? BinaryType.GE : BinaryType.GT;
                operators.add(new BinaryPredicateOperator(type, columnRef, range.lowerEndpoint()));
            }

            if (range.hasUpperBound()) {
                BinaryType type = range.upperBoundType() == BoundType.CLOSED ? BinaryType.LE : BinaryType.LT;
                operators.add(new BinaryPredicateOperator(type, columnRef, range.upperEndpoint()));
            }

            return operators;
        }
    }

    private static boolean isOnlyAndCompound(ScalarOperator predicate) {
        if (predicate instanceof CompoundPredicateOperator) {
            CompoundPredicateOperator compoundPredicateOperator = (CompoundPredicateOperator) predicate;
            if (!compoundPredicateOperator.isAnd()) {
                return false;
            }

            return isOnlyAndCompound(compoundPredicateOperator.getChild(0)) &&
                    isOnlyAndCompound(compoundPredicateOperator.getChild(1));
        } else {
            return true;
        }
    }

    private static boolean isOnlyOrCompound(ScalarOperator predicate) {
        if (predicate instanceof CompoundPredicateOperator) {
            CompoundPredicateOperator compoundPredicateOperator = (CompoundPredicateOperator) predicate;
            if (!compoundPredicateOperator.isOr()) {
                return false;
            }

            return isOnlyOrCompound(predicate.getChild(0)) && isOnlyOrCompound(predicate.getChild(1));
        } else {
            return true;
        }
    }
}