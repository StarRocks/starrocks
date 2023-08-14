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

import com.google.common.base.Preconditions;
import com.google.common.collect.BoundType;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.starrocks.analysis.BinaryType;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.PartitionKeyDiscreteDomain;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.sql.optimizer.operator.ColumnFilterConverter;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.BitSet;
import java.util.List;
import java.util.stream.Collectors;

public class PartitionColPredicateEvaluator {

    private static final Logger LOG = LogManager.getLogger(PartitionColPredicateEvaluator.class);

    private static final int IN_OPERANDS_LIMIT = 50;

    private List<Long> candidatePartitions;

    private List<Range<PartitionKey>> candidateRanges;

    private int candidateNum = 0;

    private Column partitionColumn;

    public PartitionColPredicateEvaluator(RangePartitionInfo rangePartitionInfo, List<Long> candidatePartitions) {
        this.candidatePartitions = candidatePartitions;
        candidateNum = candidatePartitions.size();
        partitionColumn = rangePartitionInfo.getPartitionColumns().get(0);
        candidateRanges = Lists.newArrayList();
        for (long id : candidatePartitions) {
            candidateRanges.add(rangePartitionInfo.getIdToRange(false).get(id));
        }
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
            Preconditions.checkState(Type.BOOLEAN == literal.getType());
            return literal.getBoolean() ? createAllTrueBitSet() : new BitSet(candidateNum);
        }

        @Override
        public BitSet visitBinaryPredicate(BinaryPredicateOperator predicate, Void context) {
            BinaryType type = predicate.getBinaryType();
            ConstantOperator constantOperator = (ConstantOperator) predicate.getChild(1);
            LiteralExpr literalExpr;
            try {
                if (constantOperator.isNull()) {
                    Type columnType = Type.fromPrimitiveType(partitionColumn.getPrimitiveType());
                    literalExpr = LiteralExpr.createInfinity(columnType, false);
                } else {
                    literalExpr = ColumnFilterConverter.convertLiteral(constantOperator);

                }
            } catch (AnalysisException e) {
                return createAllTrueBitSet();
            }
            PartitionKey conditionKey = new PartitionKey();
            conditionKey.pushColumn(literalExpr, partitionColumn.getPrimitiveType());
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
            return evaluateRangeHitSet(predicateRange);
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
            List<ConstantOperator> constList = predicate.getChildren().stream()
                    .skip(1).map(ConstantOperator.class::cast)
                    .filter(e -> !e.isNull()).sorted().collect(Collectors.toList());
            BitSet res = new BitSet();
            boolean encounterEx = false;
            if (IN_OPERANDS_LIMIT >= constList.size()) {
                for (ConstantOperator constantOperator : constList) {
                    try {
                        LiteralExpr literalExpr = ColumnFilterConverter.convertLiteral(constantOperator);
                        PartitionKey conditionKey = new PartitionKey();
                        conditionKey.pushColumn(literalExpr, partitionColumn.getPrimitiveType());
                        Range<PartitionKey> predicateRange = Range.closed(conditionKey, conditionKey);
                        res.or(evaluateRangeHitSet(predicateRange));
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
                    minKey.pushColumn(min, partitionColumn.getPrimitiveType());
                    PartitionKey maxKey = new PartitionKey();
                    maxKey.pushColumn(max, partitionColumn.getPrimitiveType());
                    Range<PartitionKey> coarseRange = Range.range(minKey, BoundType.CLOSED, maxKey, BoundType.CLOSED);
                    res.or(evaluateRangeHitSet(coarseRange));
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
            PartitionKey conditionKey = new PartitionKey();
            Type columnType = Type.fromPrimitiveType(partitionColumn.getPrimitiveType());
            try {
                conditionKey.pushColumn(LiteralExpr.createInfinity(columnType, false), partitionColumn.getPrimitiveType());
            } catch (AnalysisException e) {
                return createAllTrueBitSet();
            }
            Range<PartitionKey> predicateRange = Range.closed(conditionKey, conditionKey);
            return evaluateRangeHitSet(predicateRange);
        }

        private BitSet evaluateRangeHitSet(Range<PartitionKey> predicateRange) {
            BitSet bitSet = new BitSet(candidateNum);
            for (int i = 0; i < candidateNum; i++) {
                Range<PartitionKey> range = candidateRanges.get(i);
                if (range.isConnected(predicateRange) && !range.intersection(predicateRange).isEmpty()) {
                    if (isCanonicalType(partitionColumn.getType())) {
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
    }
}
