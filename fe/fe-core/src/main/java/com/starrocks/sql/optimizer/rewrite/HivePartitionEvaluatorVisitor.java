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
import com.google.common.collect.Sets;
import com.starrocks.analysis.BinaryType;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.MaxLiteral;
import com.starrocks.analysis.NullLiteral;
import com.starrocks.catalog.Type;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.ColumnFilterConverter;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;
import com.starrocks.sql.optimizer.rewrite.scalar.FoldConstantsRule;
import com.starrocks.sql.plan.ScalarOperatorToExpr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;

public class HivePartitionEvaluatorVisitor extends ScalarOperatorVisitor<Set<Long>, Void> {

    private static final Logger LOG = LogManager.getLogger(HivePartitionEvaluatorVisitor.class);

    List<ColumnRefOperator> partitionColumnRefs;

    Map<ColumnRefOperator, Set<Long>> columnToNullPartitions;

    private final Set<Long> allPartitions;

    Map<ColumnRefOperator, ConcurrentNavigableMap<LiteralExpr, Set<Long>>> columnToPartitionValuesMap;

    public HivePartitionEvaluatorVisitor(List<ColumnRefOperator> partitionColumnRefs, Map<ColumnRefOperator,
            Set<Long>> columnToNullPartitions, Set<Long> allPartitions,
            Map<ColumnRefOperator, ConcurrentNavigableMap<LiteralExpr, Set<Long>>> columnToPartitionValuesMap) {
        this.partitionColumnRefs = partitionColumnRefs;
        this.columnToNullPartitions = columnToNullPartitions;
        this.allPartitions = allPartitions;
        this.columnToPartitionValuesMap = columnToPartitionValuesMap;
    }

    @Override
    public Set<Long> visit(ScalarOperator scalarOperator, Void context) {
        return null;
    }

    @Override
    public Set<Long> visitConstant(ConstantOperator literal, Void context) {
        return null;
    }


    @Override
    public Set<Long> visitBinaryPredicate(BinaryPredicateOperator binaryPredicate, Void context) {
        Preconditions.checkNotNull(binaryPredicate);
        ScalarOperator right = binaryPredicate.getChild(1);
        // eval the right child only if it is a constant or cast(constant)
        ConstantOperator rightChild = evaluateConstant(right);
        if (rightChild == null) {
            return null;
        }

        if (!isSinglePartitionColumn(binaryPredicate)) {
            return null;
        }
        ColumnRefOperator leftChild = Utils.extractColumnRef(binaryPredicate).get(0);

        Set<Long> matches = Sets.newHashSet();
        ConcurrentNavigableMap<LiteralExpr, Set<Long>> partitionValueMap = columnToPartitionValuesMap.get(leftChild);
        Set<Long> nullPartitions = columnToNullPartitions.get(leftChild);

        if (binaryPredicate.getChild(0) instanceof CastOperator && partitionValueMap != null) {
            // partitionValueMap need cast to target type
            partitionValueMap = getCastPartitionValueMap((CastOperator) binaryPredicate.getChild(0),
                    partitionValueMap);
        }

        if (binaryPredicate.getChild(0) instanceof CallOperator && partitionValueMap != null) {
            partitionValueMap = getCallPartitionValueMap((CallOperator) binaryPredicate.getChild(0),
                    partitionValueMap);
        }

        if (partitionValueMap == null || nullPartitions == null || partitionValueMap.isEmpty()) {
            return null;
        }

        ScalarOperatorToExpr.FormatterContext formatterContext =
                new ScalarOperatorToExpr.FormatterContext(new HashMap<>());
        LiteralExpr literal = (LiteralExpr) ScalarOperatorToExpr.buildExecExpression(rightChild, formatterContext);

        BinaryType type = binaryPredicate.getBinaryType();
        switch (type) {
            case EQ:
                // SlotRef = Literal
                if (partitionValueMap.containsKey(literal)) {
                    matches.addAll(partitionValueMap.get(literal));
                }
                return matches;
            case EQ_FOR_NULL:
                // SlotRef <=> Literal
                if (Expr.IS_NULL_LITERAL.apply(literal)) {
                    // null
                    matches.addAll(nullPartitions);
                } else {
                    // same as EQ
                    if (partitionValueMap.containsKey(literal)) {
                        matches.addAll(partitionValueMap.get(literal));
                    }
                }
                return matches;
            case NE:
                // SlotRef != Literal
                matches.addAll(allPartitions);
                // remove null partitions
                matches.removeAll(nullPartitions);
                // remove partition matches literal
                if (partitionValueMap.containsKey(literal)) {
                    matches.removeAll(partitionValueMap.get(literal));
                }
                return matches;
            case LE:
            case LT:
            case GE:
            case GT:
                NavigableMap<LiteralExpr, Set<Long>> rangeValueMap = null;
                LiteralExpr firstKey = partitionValueMap.firstKey();
                LiteralExpr lastKey = partitionValueMap.lastKey();
                boolean upperInclusive = false;
                boolean lowerInclusive = false;
                LiteralExpr upperBoundKey = null;
                LiteralExpr lowerBoundKey = null;

                if (type == BinaryType.LE || type == BinaryType.LT) {
                    // SlotRef <[=] Literal
                    if (literal.compareLiteral(firstKey) < 0) {
                        return Sets.newHashSet();
                    }
                    if (type == BinaryType.LE) {
                        upperInclusive = true;
                    }
                    if (literal.compareLiteral(lastKey) <= 0) {
                        upperBoundKey = literal;
                    } else {
                        upperBoundKey = lastKey;
                        upperInclusive = true;
                    }
                    lowerBoundKey = firstKey;
                    lowerInclusive = true;
                } else {
                    // SlotRef >[=] Literal
                    if (literal.compareLiteral(lastKey) > 0) {
                        return Sets.newHashSet();
                    }
                    if (type == BinaryType.GE) {
                        lowerInclusive = true;
                    }
                    if (literal.compareLiteral(firstKey) >= 0) {
                        lowerBoundKey = literal;
                    } else {
                        lowerBoundKey = firstKey;
                        lowerInclusive = true;
                    }
                    upperBoundKey = lastKey;
                    upperInclusive = true;
                }

                rangeValueMap = partitionValueMap.subMap(lowerBoundKey, lowerInclusive, upperBoundKey, upperInclusive);
                for (Set<Long> partitions : rangeValueMap.values()) {
                    if (partitions != null) {
                        matches.addAll(partitions);
                    }
                }
                return matches;
            default:
                break;
        }
        return null;
    }

    @Override
    public Set<Long> visitCompoundPredicate(CompoundPredicateOperator compoundPredicate, Void context) {
        Preconditions.checkNotNull(compoundPredicate);
        if (compoundPredicate.getCompoundType() == CompoundPredicateOperator.CompoundType.NOT) {
            return null;
        }

        Set<Long> lefts = compoundPredicate.getChild(0).accept(this, null);
        Set<Long> rights = compoundPredicate.getChild(1).accept(this, null);
        if (lefts == null && rights == null) {
            return null;
        }

        if (compoundPredicate.getCompoundType() == CompoundPredicateOperator.CompoundType.AND) {
            if (lefts == null) {
                return rights;
            } else if (rights == null) {
                return lefts;
            } else {
                lefts.retainAll(rights);
            }
        } else if (compoundPredicate.getCompoundType() == CompoundPredicateOperator.CompoundType.OR) {
            if (lefts == null || rights == null) {
                return null;
            } else {
                lefts.addAll(rights);
            }
        }
        return lefts;
    }

    @Override
    public Set<Long> visitInPredicate(InPredicateOperator inPredicate, Void context) {
        Preconditions.checkNotNull(inPredicate);
        if (!inPredicate.allValuesMatch(ScalarOperator::isConstantRef)) {
            return null;
        }
        if (inPredicate.getChild(0).isConstant()) {
            // If child(0) of the in predicate is a constant expression,
            // then other children of in predicate should not be used as a condition for partition prune.
            // Such as "where  'Hi' in ('Hi', 'hello') and ... "
            return null;
        }

        if (!isSinglePartitionColumn(inPredicate)) {
            return null;
        }
        ColumnRefOperator child = Utils.extractColumnRef(inPredicate).get(0);

        Set<Long> matches = Sets.newHashSet();
        ConcurrentNavigableMap<LiteralExpr, Set<Long>> partitionValueMap = columnToPartitionValuesMap.get(child);
        Set<Long> nullPartitions = columnToNullPartitions.get(child);
        if (partitionValueMap == null || nullPartitions == null || partitionValueMap.isEmpty()) {
            return null;
        }

        if (inPredicate.isNotIn()) {
            // Column NOT IN (Literal, ..., Literal)
            // If there is a NullLiteral, return an empty set.
            if (inPredicate.hasAnyNullValues()) {
                return Sets.newHashSet();
            }

            // all partitions but remove null partitions
            matches.addAll(allPartitions);
            matches.removeAll(nullPartitions);
        }

        if (inPredicate.getChild(0) instanceof CallOperator) {
            partitionValueMap = getCallPartitionValueMap((CallOperator) inPredicate.getChild(0), partitionValueMap);
        }

        for (int i = 1; i < inPredicate.getChildren().size(); ++i) {
            ScalarOperatorToExpr.FormatterContext formatterContext =
                    new ScalarOperatorToExpr.FormatterContext(new HashMap<>());
            LiteralExpr literal =
                    (LiteralExpr) ScalarOperatorToExpr.buildExecExpression(inPredicate.getChild(i), formatterContext);

            Set<Long> partitions = partitionValueMap.get(literal);
            if (partitions != null) {
                if (inPredicate.isNotIn()) {
                    matches.removeAll(partitions);
                } else {
                    matches.addAll(partitions);
                }
            }
        }

        return matches;
    }

    @Override
    public Set<Long> visitIsNullPredicate(IsNullPredicateOperator isNullPredicate, Void context) {
        Preconditions.checkNotNull(isNullPredicate);
        if (!isSinglePartitionColumn(isNullPredicate)) {
            return null;
        }
        ColumnRefOperator child = Utils.extractColumnRef(isNullPredicate).get(0);
        ConcurrentNavigableMap<LiteralExpr, Set<Long>> partitionValueMap = columnToPartitionValuesMap.get(child);

        Set<Long> matches = Sets.newHashSet();
        Set<Long> nullPartitions = columnToNullPartitions.get(child);

        if (partitionValueMap == null || nullPartitions == null || partitionValueMap.isEmpty()) {
            return null;
        }

        if (isNullPredicate.isNotNull()) {
            // is not null
            matches.addAll(allPartitions);
            matches.removeAll(nullPartitions);
        } else {
            // is null
            matches.addAll(nullPartitions);
        }

        if (isNullPredicate.getChild(0) instanceof CallOperator) {
            matches.clear();
            partitionValueMap = getCallPartitionValueMap((CallOperator) isNullPredicate.getChild(0), partitionValueMap);
            Set<Long> result = partitionValueMap.entrySet().stream()
                    .filter(entry -> entry.getKey() instanceof NullLiteral)
                            .flatMap(entry -> entry.getValue().stream()).collect(Collectors.toSet());
            // call(null) = null
            Optional<LiteralExpr> nullLiteral = computeCallPartitionValue((CallOperator) isNullPredicate.getChild(0),
                    new NullLiteral());
            if (isNullPredicate.isNotNull()) {
                matches.addAll(allPartitions);
                matches.removeAll(result);
                if (nullLiteral.isPresent() && nullLiteral.get() instanceof NullLiteral) {
                    matches.removeAll(nullPartitions);
                }
            } else {
                matches.addAll(result);
                if (nullLiteral.isPresent() && nullLiteral.get() instanceof NullLiteral) {
                    matches.addAll(nullPartitions);
                }
            }
        }

        return matches;
    }

    private ConstantOperator evaluateConstant(ScalarOperator operator) {
        if (operator.isConstantRef()) {
            return (ConstantOperator) operator;
        }
        if (operator instanceof CastOperator && operator.getChild(0).isConstantRef()) {
            ConstantOperator child = (ConstantOperator) operator.getChild(0);
            ScalarOperatorToExpr.FormatterContext formatterContext =
                    new ScalarOperatorToExpr.FormatterContext(new HashMap<>());
            LiteralExpr literal = (LiteralExpr) ScalarOperatorToExpr.buildExecExpression(child, formatterContext);
            try {
                literal = castLiteralExpr(literal, operator.getType());
                return ConstantOperator.createObject(literal.getRealObjectValue(), operator.getType());
            } catch (Exception e) {
                LOG.warn("can not cast literal value " + literal.getStringValue() +
                        " to target type " + operator.getType().prettyPrint());
                return null;
            }
        }
        return null;
    }

    private LiteralExpr castLiteralExpr(LiteralExpr literalExpr, Type type) {
        LiteralExpr result = null;
        String value = literalExpr.getStringValue();
        if (literalExpr.getType() == Type.DATE && type.isNumericType()) {
            value = String.valueOf(literalExpr.getLongValue() / 1000000);
        }
        try {
            result = LiteralExpr.create(value, type);
        } catch (Exception e) {
            LOG.warn(e);
            throw new StarRocksConnectorException("can not cast literal value " + literalExpr.getStringValue() +
                    " to target type " + type.prettyPrint());
        }
        return result;
    }

    private boolean isSinglePartitionColumn(ScalarOperator predicate) {
        List<ColumnRefOperator> columnRefOperatorList = Utils.extractColumnRef(predicate);
        if (columnRefOperatorList.size() == 1 && partitionColumnRefs.contains(columnRefOperatorList.get(0))) {
            // such int_part_column + 1 = 11 can't prune partition
            if (predicate.getChild(0).isColumnRef() ||
                    (predicate.getChild(0) instanceof CastOperator &&
                            predicate.getChild(0).getChild(0).isColumnRef()) ||
                                    predicate.getChild(0) instanceof CallOperator) {
                return true;
            }
        }
        return false;
    }

    private ConcurrentNavigableMap<LiteralExpr, Set<Long>> getCastPartitionValueMap(CastOperator castOperator,
            ConcurrentNavigableMap<LiteralExpr, Set<Long>> partitionValueMap) {
        ConcurrentNavigableMap<LiteralExpr, Set<Long>> newPartitionValueMap = new ConcurrentSkipListMap<>();

        for (Map.Entry<LiteralExpr, Set<Long>> entry : partitionValueMap.entrySet()) {
            LiteralExpr key = entry.getKey();
            LiteralExpr literalExpr = castLiteralExpr(key, castOperator.getType());
            Set<Long> partitions = newPartitionValueMap.computeIfAbsent(literalExpr, k -> Sets.newHashSet());
            partitions.addAll(entry.getValue());
        }
        return newPartitionValueMap;
    }

    private ConcurrentNavigableMap<LiteralExpr, Set<Long>> getCallPartitionValueMap(CallOperator callOperator,
            ConcurrentNavigableMap<LiteralExpr, Set<Long>> partitionValueMap) {
        ConcurrentNavigableMap<LiteralExpr, Set<Long>> newPartitionValueMap = new ConcurrentSkipListMap<>();

        for (Map.Entry<LiteralExpr, Set<Long>> entry : partitionValueMap.entrySet()) {
            LiteralExpr key = entry.getKey();
            Optional<LiteralExpr> literalExpr = computeCallPartitionValue(callOperator, key);
            Set<Long> partitions = newPartitionValueMap.computeIfAbsent(literalExpr.isPresent() ?
                    literalExpr.get() : key, k -> Sets.newHashSet());
            partitions.addAll(entry.getValue());
        }
        return newPartitionValueMap;
    }

    public static Optional<LiteralExpr> computeCallPartitionValue(CallOperator callOperator, LiteralExpr literalExpr) {
        ColumnRefReplacer refReplacer = new ColumnRefReplacer(literalExpr);
        CallOperator newCall = (CallOperator) callOperator.accept(refReplacer, null);

        ScalarOperatorRewriter rewriter = new ScalarOperatorRewriter();
        ScalarOperator result = rewriter.rewrite(newCall,
                Collections.singletonList(new FoldConstantsRule(false)));
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

    public static class ColumnRefReplacer extends BaseScalarOperatorShuttle {

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
