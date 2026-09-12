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

package com.starrocks.sql.optimizer.property;

import com.google.common.base.Preconditions;
import com.google.common.collect.BoundType;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.common.LargeInPredicateException;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.LargeInPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RangeExtractor {
    public Map<ScalarOperator, ValueDescriptor> apply(ScalarOperator scalarOperator, Void context) {
        Map<ScalarOperator, ValueDescriptor> values = new RangeValueExtractor().apply(scalarOperator, context);
        Map<ScalarOperator, ValueDescriptor> relations =
                new RangeRelationExtractor(values).apply(scalarOperator, context);
        if (!values.isEmpty() && !relations.isEmpty()) {
            return mergeValues(false, values, relations);
        }
        return values;
    }

    private static class RangeValueExtractor extends ScalarOperatorVisitor<Void, Void> {
        protected Map<ScalarOperator, ValueDescriptor> descMap = Maps.newHashMap();

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
            if (predicate.getChild(1).isConstantRef() && predicate.getBinaryType() == BinaryType.EQ_FOR_NULL
                    && isNullAlternativeDeriveEnabled()) {
                ConstantOperator value = (ConstantOperator) predicate.getChild(1);
                Preconditions.checkState(!descMap.containsKey(predicate.getChild(0)));
                if (value.isNull()) {
                    // `E <=> NULL` is TRUE only when E is NULL
                    descMap.put(predicate.getChild(0), ValueDescriptor.isNull(predicate.getChild(0)));
                } else {
                    // for a non-null const, `E <=> const` means the same as `E = const`
                    descMap.put(predicate.getChild(0),
                            ValueDescriptor.range(predicate.getChild(0), value, BinaryType.EQ));
                }
                return visit(predicate, context);
            }

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
        public Void visitIsNullPredicate(IsNullPredicateOperator predicate, Void context) {
            if (!predicate.isNotNull() && isNullAlternativeDeriveEnabled()) {
                Preconditions.checkState(!descMap.containsKey(predicate.getChild(0)));
                descMap.put(predicate.getChild(0), ValueDescriptor.isNull(predicate.getChild(0)));
            }
            return visit(predicate, context);
        }

        private static boolean isNullAlternativeDeriveEnabled() {
            ConnectContext ctx = ConnectContext.get();
            return ctx != null && ctx.getSessionVariable().isCboDerivePredicateNullAlternative();
        }

        @Override
        public Void visitLargeInPredicate(LargeInPredicateOperator predicate, Void context) {
            throw new LargeInPredicateException("not support large in predicate in the RangeValueExtractor");
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
                    new RangeValueExtractor().apply(predicate.getChild(0), context);
            Map<ScalarOperator, ValueDescriptor> rightMap =
                    new RangeValueExtractor().apply(predicate.getChild(1), context);
            descMap = mergeValues(predicate.isOr(), leftMap, rightMap);
            return null;
        }
    }

    private static Map<ScalarOperator, ValueDescriptor> mergeValues(boolean isUnion,
                                                                    Map<ScalarOperator, ValueDescriptor> leftMap,
                                                                    Map<ScalarOperator, ValueDescriptor> rightMap) {
        Map<ScalarOperator, ValueDescriptor> result = Maps.newHashMap();
        HashMap<ScalarOperator, ValueDescriptor> intersectMap = Maps.newHashMap();
        Set<ScalarOperator> intersectKeys = Sets.intersection(leftMap.keySet(), rightMap.keySet());

        if (isUnion) {
            for (ScalarOperator s : intersectKeys) {
                ValueDescriptor rangeDescriptor = leftMap.get(s);
                intersectMap.put(s, rangeDescriptor.union(rightMap.get(s)));
            }

            result.putAll(intersectMap);
        } else {
            for (ScalarOperator s : intersectKeys) {
                ValueDescriptor rangeDescriptor = leftMap.get(s);
                intersectMap.put(s, rangeDescriptor.intersect(rightMap.get(s)));
            }

            result.putAll(leftMap);
            result.putAll(rightMap);
            result.putAll(intersectMap);
        }
        return result;
    }

    private static class RangeRelationExtractor extends ScalarOperatorVisitor<Void, Void> {
        private final Map<ScalarOperator, ValueDescriptor> baseMap;
        protected Map<ScalarOperator, ValueDescriptor> descMap = Maps.newHashMap();

        public RangeRelationExtractor(Map<ScalarOperator, ValueDescriptor> baseMap) {
            this.baseMap = baseMap;
        }

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
            if (!predicate.getBinaryType().isRange() || predicate.getChild(0).getType().isStringType()) {
                return null;
            }

            ScalarOperator left = predicate.getChild(0);
            ScalarOperator right = predicate.getChild(1);
            if (left.isConstant() || right.isConstant()) {
                return null;
            }
            BinaryType binaryType = predicate.getBinaryType();
            generateBound(left, binaryType, right);
            generateBound(right, binaryType.commutative(), left);
            return null;
        }

        private void generateBound(ScalarOperator op, BinaryType binaryType, ScalarOperator value) {
            if (!baseMap.containsKey(value)) {
                return;
            }
            ValueDescriptor sourceDesc = baseMap.get(value);
            Range<ConstantOperator> range = sourceDesc.toRange();
            if ((binaryType == BinaryType.GE || binaryType == BinaryType.GT) && range.hasLowerBound()) {
                // >=/> lower bound
                var desc = ValueDescriptor.range(op, range.lowerEndpoint(),
                        range.lowerBoundType() == BoundType.CLOSED ? binaryType : BinaryType.GT);
                desc.incrementSource(sourceDesc.getSourceCount());
                descMap.put(op, desc);
            } else if ((binaryType == BinaryType.LE || binaryType == BinaryType.LT) && range.hasUpperBound()) {
                // <=/< upper bound
                var desc = ValueDescriptor.range(op, range.upperEndpoint(),
                        range.upperBoundType() == BoundType.CLOSED ? binaryType : BinaryType.LT);
                desc.incrementSource(sourceDesc.getSourceCount());
                descMap.put(op, desc);
            }
        }

        @Override
        public Void visitCompoundPredicate(CompoundPredicateOperator predicate, Void context) {
            if (predicate.isNot()) {
                return visit(predicate, context);
            }

            Map<ScalarOperator, ValueDescriptor> leftMap =
                    new RangeRelationExtractor(baseMap).apply(predicate.getChild(0), context);
            Map<ScalarOperator, ValueDescriptor> rightMap =
                    new RangeRelationExtractor(baseMap).apply(predicate.getChild(1), context);
            descMap = mergeValues(predicate.isOr(), leftMap, rightMap);
            return null;
        }
    }

    /**
     * Describes the possible values of the column
     */
    public abstract static class ValueDescriptor {
        protected ScalarOperator columnRef;
        protected int sourceCount = 1;
        // true if NULL is also allowed besides the values/range, i.e. the source predicate has an
        // `E IS NULL` or `E <=> NULL` alternative. OR unions this flag, AND intersects it.
        // toRange() ignores it on purpose: the range describes only non-null values, and a
        // comparison built from the range is never TRUE on NULL anyway.
        protected boolean admitsNull = false;

        protected ValueDescriptor(ScalarOperator ref) {
            columnRef = ref;
        }

        protected ValueDescriptor(ValueDescriptor o1, ValueDescriptor o2) {
            Preconditions.checkState(o1.columnRef.equals(o2.columnRef));
            this.columnRef = o1.columnRef;
            this.sourceCount = o1.sourceCount + o2.sourceCount;
        }

        public ScalarOperator getColumnRef() {
            return columnRef;
        }

        public void incrementSource(int count) {
            this.sourceCount += count;
        }

        public abstract ValueDescriptor union(ValueDescriptor other);

        public abstract ValueDescriptor intersect(ValueDescriptor other);

        public abstract List<ScalarOperator> toScalarOperator();

        public abstract Range<ConstantOperator> toRange();

        public int getSourceCount() {
            return sourceCount;
        }

        public static ValueDescriptor in(ScalarOperator operator) {
            MultiValuesDescriptor d = new MultiValuesDescriptor(operator.getChild(0));
            operator.getChildren().stream().skip(1).map(c -> (ConstantOperator) c)
                    .filter(c -> !c.isNull()).forEach(d.values::add);
            return d;
        }

        public static ValueDescriptor isNull(ScalarOperator operator) {
            MultiValuesDescriptor d = new MultiValuesDescriptor(operator);
            d.admitsNull = true;
            return d;
        }

        public static ValueDescriptor range(ScalarOperator operator) {
            BinaryType type = ((BinaryPredicateOperator) operator).getBinaryType();
            ScalarOperator op = operator.getChild(0);
            ConstantOperator value = (ConstantOperator) operator.getChild(1);
            return range(op, value, type);
        }

        public static ValueDescriptor range(ScalarOperator op, ConstantOperator value, BinaryType type) {
            if (type == BinaryType.EQ) {
                MultiValuesDescriptor d = new MultiValuesDescriptor(op);
                d.values.add(value);
                return d;
            }

            RangeDescriptor d = new RangeDescriptor(op);
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
                default:
                    break;
            }
            return d;
        }

        protected static ValueDescriptor mergeValuesAndRange(MultiValuesDescriptor value, RangeDescriptor range) {
            RangeDescriptor result = new RangeDescriptor(value, range);
            result.admitsNull = value.admitsNull || range.admitsNull;

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
            result.admitsNull = value.admitsNull && range.admitsNull;

            if (range.range == null) {
                return result;
            }
            value.values.stream().filter(x -> range.range.test(x)).forEach(result.values::add);
            return result;
        }
    }

    public static class MultiValuesDescriptor extends ValueDescriptor {
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
                result.admitsNull = admitsNull || other.admitsNull;
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
                result.admitsNull = admitsNull && other.admitsNull;
                return result;
            }

            return intersectValuesAndRange(this, (RangeDescriptor) other);
        }

        @Override
        public List<ScalarOperator> toScalarOperator() {
            Preconditions.checkState(values != null, "invalid scalar values predicate extract");

            ScalarOperator valuesPart;
            if (values.isEmpty()) {
                // empty value set and no NULL alternative: the source predicate can never be
                // TRUE, so return a typed NULL constant which rejects every row
                valuesPart = admitsNull ? null : ConstantOperator.createNull(columnRef.getType());
            } else if (values.size() == 1) {
                valuesPart = new BinaryPredicateOperator(BinaryType.EQ, columnRef, values.iterator().next());
            } else {
                InPredicateOperator ipo = new InPredicateOperator(false, columnRef);
                ipo.getChildren().addAll(values);
                valuesPart = ipo;
            }

            if (!admitsNull) {
                return Lists.newArrayList(valuesPart);
            }
            IsNullPredicateOperator isNull = new IsNullPredicateOperator(columnRef);
            if (valuesPart == null) {
                return Lists.newArrayList((ScalarOperator) isNull);
            }
            return Lists.newArrayList(new CompoundPredicateOperator(
                    CompoundPredicateOperator.CompoundType.OR, valuesPart, isNull));
        }

        @Override
        public Range<ConstantOperator> toRange() {
            // An empty value set means the source predicate is contradictory (e.g. col > X AND col < X),
            // so there is no derivable range. Return Range.all() so callers (e.g. generateBound) derive
            // no extra bound instead of failing the !values.isEmpty() precondition.
            if (values.isEmpty()) {
                return Range.all();
            }
            ConstantOperator min = values.stream().min(ConstantOperator::compareTo).get();
            ConstantOperator max = values.stream().max(ConstantOperator::compareTo).get();
            return Range.closed(min, max);
        }
    }

    public static class RangeDescriptor extends ValueDescriptor {

        protected Range<ConstantOperator> range = null;

        public RangeDescriptor(ScalarOperator ref) {
            super(ref);
        }

        public RangeDescriptor(ValueDescriptor o1, ValueDescriptor o2) {
            super(o1, o2);
        }

        public Range<ConstantOperator> getRange() {
            return range;
        }

        @Override
        public ValueDescriptor union(ValueDescriptor other) {
            if (other instanceof RangeDescriptor o) {
                RangeDescriptor result = new RangeDescriptor(this, other);
                result.admitsNull = admitsNull || o.admitsNull;
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
            if (other instanceof RangeDescriptor o) {
                RangeDescriptor result = new RangeDescriptor(this, other);
                result.admitsNull = admitsNull && o.admitsNull;
                if (range == null || o.range == null) {
                    return result;
                }

                try {
                    result.range = range.intersection(o.range);
                } catch (Exception ignore) {
                    // empty range
                    return emptyIntersection(other);
                }
                if (result.range.isEmpty()) {
                    return emptyIntersection(other);
                }
                return result;
            }

            return intersectValuesAndRange((MultiValuesDescriptor) other, this);
        }

        private MultiValuesDescriptor emptyIntersection(ValueDescriptor other) {
            MultiValuesDescriptor empty = new MultiValuesDescriptor(this, other);
            empty.admitsNull = admitsNull && other.admitsNull;
            return empty;
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

            if (admitsNull && !operators.isEmpty()) {
                return Lists.newArrayList(new CompoundPredicateOperator(
                        CompoundPredicateOperator.CompoundType.OR,
                        Utils.compoundAnd(operators), new IsNullPredicateOperator(columnRef)));
            }
            return operators;
        }

        @Override
        public Range<ConstantOperator> toRange() {
            return range == null ? Range.all() : range;
        }
    }
}
