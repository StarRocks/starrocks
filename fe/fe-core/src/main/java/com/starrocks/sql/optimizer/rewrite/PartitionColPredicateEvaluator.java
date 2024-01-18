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


package com.starrocks.sql.optimizer.rewrite;

import com.google.common.collect.BoundType;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.starrocks.analysis.BinaryType;
import com.starrocks.analysis.DateLiteral;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.LargeIntLiteral;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.MaxLiteral;
import com.starrocks.analysis.NullLiteral;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.PartitionKeyDiscreteDomain;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.Type;
import com.starrocks.common.exception.AnalysisException;
import com.starrocks.sql.optimizer.operator.ColumnFilterConverter;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;
import com.starrocks.sql.optimizer.rewrite.scalar.FoldConstantsRule;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class PartitionColPredicateEvaluator {

    private static final Logger LOG = LogManager.getLogger(PartitionColPredicateEvaluator.class);

    private static final int IN_OPERANDS_LIMIT = 50;

    private final List<Long> candidatePartitions;

    private final List<Range<PartitionKey>> candidateRanges;

    private final Map<ScalarOperator, List<Range<PartitionKey>>> exprToCandidateRanges;

    private final int candidateNum;

    private final Column partitionColumn;

    public PartitionColPredicateEvaluator(RangePartitionInfo rangePartitionInfo, List<Long> candidatePartitions) {
        this.candidatePartitions = candidatePartitions;
        candidateNum = candidatePartitions.size();
        partitionColumn = rangePartitionInfo.getPartitionColumns().get(0);
        candidateRanges = Lists.newArrayList();
        for (long id : candidatePartitions) {
            candidateRanges.add(rangePartitionInfo.getIdToRange(false).get(id));
        }
        exprToCandidateRanges = Maps.newHashMap();
    }

    public List<Long> prunePartitions(PartitionColPredicateExtractor extractor, ScalarOperator predicates) {
        Evaluator evaluator = new Evaluator();
        List<Long> res = Lists.newArrayList();
        try {
            ScalarOperator newPredicates = extractor.extract(predicates);
            if (null == newPredicates) {
                return candidatePartitions;
            }
            BitSet predicatesRes = newPredicates.accept(evaluator, null);
            for (int i = 0; i < candidatePartitions.size(); i++) {
                if (predicatesRes.get(i)) {
                    res.add(candidatePartitions.get(i));
                }
            }
            return res;
        } catch (Exception e) {
            LOG.warn("evaluate range partitions failed.", e);
            return candidatePartitions;
        }
    }

    private class Evaluator extends ScalarOperatorVisitor<BitSet, Void> {

        @Override
        public BitSet visit(ScalarOperator scalarOperator, Void context) {
            return createAllTrueBitSet();
        }

        @Override
        public BitSet visitConstant(ConstantOperator literal, Void context) {
            return literal.getBoolean() ? createAllTrueBitSet() : new BitSet(candidateNum);
        }

        @Override
        public BitSet visitCall(CallOperator call, Void context) {
            if (!exprToCandidateRanges.containsKey(call)) {
                List<Range<PartitionKey>> mappingRanges = Lists.newArrayList();
                PrimitiveType returnType = call.getType().getPrimitiveType();
                for (Range<PartitionKey> range : candidateRanges) {
                    Range<PartitionKey> newRange;
                    if (!range.hasUpperBound() || range.upperEndpoint().getKeys().get(0) instanceof MaxLiteral) {
                        newRange = createFullScopeRange(returnType);
                    } else {
                        PartitionKey lowerKey = new PartitionKey();
                        PartitionKey upperKey = new PartitionKey();
                        LiteralExpr lowerBound = range.hasLowerBound() ? range.lowerEndpoint().getKeys().get(0)
                                : createInfinity(partitionColumn.getType(), false);
                        LiteralExpr upperBound = range.upperEndpoint().getKeys().get(0);
                        Optional<LiteralExpr> mappingLowerBound = mapRangeBoundValue(call, lowerBound);
                        Optional<LiteralExpr> mappingUpperBound = mapRangeBoundValue(call, upperBound);
                        if (mappingLowerBound.isPresent() && mappingUpperBound.isPresent()) {
                            LiteralExpr newLowerBound = mappingLowerBound.get();
                            LiteralExpr newUpperBound = mappingUpperBound.get();
                            // switch bound value
                            if (newLowerBound.compareTo(newUpperBound) > 0) {
                                LiteralExpr tmp = newLowerBound;
                                newLowerBound = newUpperBound;
                                newUpperBound = tmp;
                            }
                            lowerKey.pushColumn(newLowerBound, returnType);
                            upperKey.pushColumn(newUpperBound, returnType);
                            newRange = Range.range(lowerKey, BoundType.CLOSED, upperKey, BoundType.CLOSED);
                        } else {
                            newRange = createFullScopeRange(call.getType().getPrimitiveType());
                        }
                    }
                    mappingRanges.add(newRange);
                }

                exprToCandidateRanges.put(call, mappingRanges);
            }
            return null;
        }

        @Override
        public BitSet visitVariableReference(ColumnRefOperator variable, Void context) {
            exprToCandidateRanges.put(variable, candidateRanges);
            return null;
        }

        @Override
        public BitSet visitBinaryPredicate(BinaryPredicateOperator predicate, Void context) {
            BinaryType type = predicate.getBinaryType();
            predicate.getChild(0).accept(this, null);
            ConstantOperator constantOperator = (ConstantOperator) predicate.getChild(1);
            Type childType = predicate.getChild(0).getType();
            LiteralExpr literalExpr;
            try {
                if (constantOperator.isNull()) {
                    literalExpr = LiteralExpr.createInfinity(childType, false);
                } else {
                    literalExpr = ColumnFilterConverter.convertLiteral(constantOperator);
                }
            } catch (AnalysisException e) {
                return createAllTrueBitSet();
            }
            PartitionKey conditionKey = new PartitionKey();
            conditionKey.pushColumn(literalExpr, childType.getPrimitiveType());
            Range<PartitionKey> predicateRange;
            switch (type) {
                case EQ:
                case EQ_FOR_NULL:
                    predicateRange = Range.closed(conditionKey, conditionKey);
                    break;
                case GE:
                    predicateRange = Range.atLeast(conditionKey);
                    break;
                case GT:
                    predicateRange = Range.greaterThan(conditionKey);
                    break;
                case LE:
                    predicateRange = Range.atMost(conditionKey);
                    break;
                case LT:
                    predicateRange = Range.lessThan(conditionKey);
                    break;
                case NE:
                default:
                    predicateRange = Range.all();
            }
            
            BitSet res = evaluateRangeHitSet(predicate.getChild(0), predicateRange);
            if (type == BinaryType.EQ_FOR_NULL) {
                res.or(evaluateNullRangeHitSet(predicate.getChild(0), childType));
            }

            return res;
        }

        @Override
        public BitSet visitCompoundPredicate(CompoundPredicateOperator predicate, Void context) {
            BitSet first = predicate.getChild(0).accept(this, null);
            if (predicate.isAnd()) {
                BitSet second = predicate.getChild(1).accept(this, null);
                first.and(second);
            } else if (predicate.isOr()) {
                BitSet second = predicate.getChild(1).accept(this, null);
                first.or(second);
            } else {
                first = predicate.getChild(0).accept(this, null);
                first.flip(0, candidateNum);
            }
            return first;
        }

        // only process in scene
        @Override
        public BitSet visitInPredicate(InPredicateOperator predicate, Void context) {
            predicate.getChild(0).accept(this, null);
            List<ConstantOperator> constList = predicate.getChildren().stream()
                    .skip(1).map(ConstantOperator.class::cast)
                    .filter(e -> !e.isNull()).sorted().collect(Collectors.toList());
            BitSet res = new BitSet();
            boolean encounterEx = false;
            PrimitiveType childType = predicate.getChild(0).getType().getPrimitiveType();
            if (IN_OPERANDS_LIMIT >= constList.size()) {
                for (ConstantOperator constantOperator : constList) {
                    try {
                        LiteralExpr literalExpr = ColumnFilterConverter.convertLiteral(constantOperator);
                        PartitionKey conditionKey = new PartitionKey();
                        conditionKey.pushColumn(literalExpr, childType);
                        Range<PartitionKey> predicateRange = Range.closed(conditionKey, conditionKey);
                        res.or(evaluateRangeHitSet(predicate.getChild(0), predicateRange));
                    } catch (AnalysisException e) {
                        encounterEx = true;
                        break;
                    }
                }
            } else {
                try {
                    LiteralExpr min = ColumnFilterConverter.convertLiteral(constList.get(0));
                    LiteralExpr max = ColumnFilterConverter.convertLiteral(constList.get(constList.size() - 1));
                    PartitionKey minKey = new PartitionKey();
                    minKey.pushColumn(min, childType);
                    PartitionKey maxKey = new PartitionKey();
                    maxKey.pushColumn(max, childType);
                    Range<PartitionKey> coarseRange = Range.range(minKey, BoundType.CLOSED, maxKey, BoundType.CLOSED);
                    res.or(evaluateRangeHitSet(predicate.getChild(0), coarseRange));
                } catch (AnalysisException e) {
                    encounterEx = true;
                }
            }

            if (encounterEx) {
                res = createAllTrueBitSet();
            }
            return res;
        }

        @Override
        public BitSet visitIsNullPredicate(IsNullPredicateOperator predicate, Void context) {
            predicate.getChild(0).accept(this, null);
            PartitionKey conditionKey = new PartitionKey();
            Type columnType = predicate.getChild(0).getType();
            try {
                conditionKey.pushColumn(LiteralExpr.createInfinity(columnType, false), columnType.getPrimitiveType());
            } catch (AnalysisException e) {
                return createAllTrueBitSet();
            }
            Range<PartitionKey> predicateRange = Range.closed(conditionKey, conditionKey);
            BitSet res = evaluateRangeHitSet(predicate.getChild(0), predicateRange);
            res.or(evaluateNullRangeHitSet(predicate.getChild(0), columnType));
            return res;
        }

        private BitSet evaluateNullRangeHitSet(ScalarOperator scalarOperator, Type columnType) {
            PartitionKey nullKey = new PartitionKey();
            nullKey.pushColumn(NullLiteral.create(columnType), columnType.getPrimitiveType());
            Range<PartitionKey> nullRange = Range.closed(nullKey, nullKey);
            return evaluateRangeHitSet(scalarOperator, nullRange);
        }

        private BitSet evaluateRangeHitSet(ScalarOperator scalarOperator, Range<PartitionKey> predicateRange) {
            BitSet bitSet = new BitSet(candidateNum);
            if (!exprToCandidateRanges.containsKey(scalarOperator)) {
                return createAllTrueBitSet();
            }
            List<Range<PartitionKey>> ranges = exprToCandidateRanges.get(scalarOperator);
            for (int i = 0; i < candidateNum; i++) {
                Range<PartitionKey> range = ranges.get(i);
                if (range.isConnected(predicateRange) && !range.intersection(predicateRange).isEmpty()) {
                    if (isCanonicalType(scalarOperator.getType())) {
                        // try to canonical predicate
                        Range intersectedRange = range.intersection(predicateRange);
                        Range canonicalRange = intersectedRange.canonical(new PartitionKeyDiscreteDomain());
                        if (!canonicalRange.isEmpty()) {
                            bitSet.set(i);
                        }
                    } else {
                        bitSet.set(i);
                    }
                }
            }
            return bitSet;
        }

        private boolean isCanonicalType(Type columnType) {
            return columnType.isInt() || columnType.isLargeint() || columnType.isBigint();
        }

        private BitSet createAllTrueBitSet() {
            BitSet res = new BitSet(candidateNum);
            res.flip(0, candidateNum);
            return res;
        }

        private LiteralExpr createInfinity(Type type, boolean isMax) {
            if (isMax) {
                return MaxLiteral.MAX_VALUE;
            }

            switch (type.getPrimitiveType()) {
                case TINYINT:
                case SMALLINT:
                case INT:
                case BIGINT:
                    return IntLiteral.createMinValue(type);
                case LARGEINT:
                    return LargeIntLiteral.createMinValue();
                case DATE:
                case DATETIME:
                    return DateLiteral.createMinValue(type);
                default:
                    throw new IllegalArgumentException("Invalid data type for creating infinity: " + type);
            }
        }

        private Optional<LiteralExpr> mapRangeBoundValue(CallOperator callOperator, LiteralExpr literalExpr) {
            ColumnRefReplacer refReplacer = new ColumnRefReplacer(literalExpr);
            CallOperator newCall = (CallOperator) callOperator.accept(refReplacer, null);

            ScalarOperatorRewriter rewriter = new ScalarOperatorRewriter();
            ScalarOperator result = rewriter.rewrite(newCall,
                    Collections.singletonList(new FoldConstantsRule(true)));
            if (result.isConstantRef()) {
                LiteralExpr newLiteralExpr;
                try {
                    newLiteralExpr = ColumnFilterConverter.convertLiteral((ConstantOperator) result);
                } catch (Exception e) {
                    return Optional.empty();
                }
                return Optional.of(newLiteralExpr);
            } else {
                return Optional.empty();
            }
        }

        private Range<PartitionKey> createFullScopeRange(PrimitiveType type) {
            PartitionKey upperKey = new PartitionKey();
            upperKey.pushColumn(MaxLiteral.MAX_VALUE, type);
            return Range.lessThan(upperKey);
        }
    }

    private class ColumnRefReplacer extends BaseScalarOperatorShuttle {

        private final LiteralExpr literalExpr;

        public ColumnRefReplacer(LiteralExpr literalExpr) {
            this.literalExpr = literalExpr;
        }

        @Override
        public ScalarOperator visitVariableReference(ColumnRefOperator variable, Void context) {
            if (literalExpr instanceof NullLiteral) {
                return ConstantOperator.createNull(literalExpr.getType());
            } else if (literalExpr instanceof MaxLiteral) {
                return variable;
            } else {
                return ConstantOperator.createObject(literalExpr.getRealObjectValue(), literalExpr.getType());
            }
        }

    }
}
