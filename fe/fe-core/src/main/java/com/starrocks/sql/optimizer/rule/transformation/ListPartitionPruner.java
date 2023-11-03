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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Pair;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.planner.PartitionPruner;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.plan.ScalarOperatorToExpr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;

public class ListPartitionPruner implements PartitionPruner {
    private static final Logger LOG = LogManager.getLogger(ListPartitionPruner.class);

    // example:
    // partition keys                    partition id
    // date_col=2021-01-01/int_col=0     0
    // date_col=2021-01-01/int_col=1     1
    // date_col=2021-01-01/int_col=2     2
    // date_col=2021-01-02/int_col=0     3
    // date_col=2021-01-02/int_col=null  4
    // date_col=null/int_col=1           5

    // partitionColumnName -> (LiteralExpr -> partitionIds)
    // no null partitions in this map
    //
    // "date_col" -> (2021-01-01 -> set(0,1,2),
    //                2021-01-02 -> set(3,4))
    // "int_col"  -> (0 -> set(0,3),
    //                1 -> set(1,5),
    //                2 -> set(2))

    private final Map<ColumnRefOperator, ConcurrentNavigableMap<LiteralExpr, Set<Long>>> columnToPartitionValuesMap;
    // Store partitions with null partition values separately
    // partitionColumnName -> null partitionIds
    //
    // "date_col" -> set(5)
    // "int_col"  -> set(4)
    private final Map<ColumnRefOperator, Set<Long>> columnToNullPartitions;
    private final List<ScalarOperator> partitionConjuncts;
    // Conjuncts that not eval in partition pruner, and will be sent to backend.
    private final List<ScalarOperator> noEvalConjuncts = Lists.newArrayList();

    private final Set<Long> allPartitions;
    private final List<ColumnRefOperator> partitionColumnRefs;
    private final List<Long> specifyPartitionIds;

    public ListPartitionPruner(
            Map<ColumnRefOperator, ConcurrentNavigableMap<LiteralExpr, Set<Long>>> columnToPartitionValuesMap,
            Map<ColumnRefOperator, Set<Long>> columnToNullPartitions,
            List<ScalarOperator> partitionConjuncts, List<Long> specifyPartitionIds) {
        this.columnToPartitionValuesMap = columnToPartitionValuesMap;
        this.columnToNullPartitions = columnToNullPartitions;
        this.partitionConjuncts = partitionConjuncts;
        this.allPartitions = getAllPartitions();
        this.partitionColumnRefs = getPartitionColumnRefs();
        this.specifyPartitionIds = specifyPartitionIds;
    }

    private Set<Long> getAllPartitions() {
        Set<Long> allPartitions = Sets.newHashSet();
        for (ConcurrentNavigableMap<LiteralExpr, Set<Long>> partitionValuesMap : columnToPartitionValuesMap.values()) {
            for (Set<Long> partitions : partitionValuesMap.values()) {
                allPartitions.addAll(partitions);
            }
        }
        for (Set<Long> partitions : columnToNullPartitions.values()) {
            allPartitions.addAll(partitions);
        }
        return allPartitions;
    }

    private List<ColumnRefOperator> getPartitionColumnRefs() {
        List<ColumnRefOperator> partitionColumnRefOperators = Lists.newArrayList();
        partitionColumnRefOperators.addAll(columnToPartitionValuesMap.keySet());
        return partitionColumnRefOperators;
    }

    public List<ScalarOperator> getNoEvalConjuncts() {
        return noEvalConjuncts;
    }

    public List<ScalarOperator> getPartitionConjuncts() {
        return partitionConjuncts;
    }

    /**
     * Return a list of partitions left after applying the conjuncts on partition columns.
     * Null is returned if all partitions.
     * An empty set is returned if no match partitions.
     */
    @Override
    public List<Long> prune() throws AnalysisException {
        Preconditions.checkNotNull(columnToPartitionValuesMap);
        Preconditions.checkNotNull(columnToNullPartitions);
        Preconditions.checkArgument(columnToPartitionValuesMap.size() == columnToNullPartitions.size());
        Preconditions.checkNotNull(partitionConjuncts);
        if (columnToPartitionValuesMap.isEmpty()) {
            // no partition columns, notEvalConjuncts is same with conjuncts
            noEvalConjuncts.addAll(partitionConjuncts);
            return null;
        }
        if (partitionConjuncts.isEmpty()) {
            // no conjuncts, notEvalConjuncts is empty
            return specifyPartitionIds;
        }

        Set<Long> matches = null;
        for (ScalarOperator operator : partitionConjuncts) {
            List<ColumnRefOperator> columnRefOperatorList = Utils.extractColumnRef(operator);
            if (columnRefOperatorList.retainAll(this.partitionColumnRefs)) {
                noEvalConjuncts.add(operator);
                continue;
            }

            Pair<Set<Long>, Boolean> matchesPair = evalPartitionPruneFilter(operator);
            Set<Long> conjunctMatches = matchesPair.first;
            Boolean existNoEvalConjuncts = matchesPair.second;
            LOG.debug("prune by expr: {}, partitions: {}", operator.toString(), conjunctMatches);
            if (conjunctMatches != null) {
                if (matches == null) {
                    matches = Sets.newHashSet(conjunctMatches);
                } else {
                    matches.retainAll(conjunctMatches);
                }
                if (existNoEvalConjuncts) {
                    noEvalConjuncts.add(operator);
                }
            } else {
                noEvalConjuncts.add(operator);
            }
        }
        // Null represents the full set. If a partition is specified,
        // the intersection of the full set and the specified partition is the specified partition.
        // If a match is found, the intersection data is taken.
        // If no partition is specified, the match result will be taken
        if (matches == null) {
            if (specifyPartitionIds != null && !specifyPartitionIds.isEmpty()) {
                return specifyPartitionIds;
            } else {
                return null;
            }
        } else {
            if (specifyPartitionIds != null && !specifyPartitionIds.isEmpty()) {
                // intersect
                return specifyPartitionIds.stream().filter(matches::contains).collect(Collectors.toList());
            } else {
                return new ArrayList<>(matches);
            }
        }
    }

    private Pair<Set<Long>, Boolean> evalPartitionPruneFilter(ScalarOperator operator) {
        Set<Long> matches = null;
        Boolean existNoEval = false;
        if (operator instanceof BinaryPredicateOperator) {
            matches = evalBinaryPredicate((BinaryPredicateOperator) operator);
        } else if (operator instanceof InPredicateOperator) {
            matches = evalInPredicate((InPredicateOperator) operator);
        } else if (operator instanceof IsNullPredicateOperator) {
            matches = evalIsNullPredicate((IsNullPredicateOperator) operator);
        } else if (operator instanceof CompoundPredicateOperator) {
            Pair<Set<Long>, Boolean> matchesPair = evalCompoundPredicate((CompoundPredicateOperator) operator);
            matches = matchesPair.first;
            existNoEval = matchesPair.second;
        }
        return matches == null ? Pair.create(null, true) : Pair.create(matches, existNoEval);
    }

    private boolean isSinglePartitionColumn(ScalarOperator predicate) {
        List<ColumnRefOperator> columnRefOperatorList = Utils.extractColumnRef(predicate);
        if (columnRefOperatorList.size() == 1 && partitionColumnRefs.contains(columnRefOperatorList.get(0))) {
            // such int_part_column + 1 = 11 can't prune partition
            if (predicate.getChild(0).isColumnRef() ||
                    (predicate.getChild(0) instanceof CastOperator &&
                            predicate.getChild(0).getChild(0).isColumnRef())) {
                return true;
            }
        }
        return false;
    }

    // generate new partition value map using cast operator' type.
    // eg. string partition value cast to int
    // string_col = '01'  1
    // string_col = '1'   2
    // string_col = '001' 3
    // will generate new partition value map
    // int_col = 1  [1, 2, 3]
    private ConcurrentNavigableMap<LiteralExpr, Set<Long>> getCastPartitionValueMap(CastOperator castOperator,
                                            ConcurrentNavigableMap<LiteralExpr, Set<Long>> partitionValueMap) {
        ConcurrentNavigableMap<LiteralExpr, Set<Long>> newPartitionValueMap = new ConcurrentSkipListMap<>();

        for (Map.Entry<LiteralExpr, Set<Long>> entry : partitionValueMap.entrySet()) {
            LiteralExpr literalExpr = null;
            LiteralExpr key = entry.getKey();
            try {
                literalExpr = LiteralExpr.create(key.getStringValue(), castOperator.getType());
            } catch (Exception e) {
                // ignore
            }
            if (literalExpr == null) {
                try {
                    literalExpr = (LiteralExpr) key.uncheckedCastTo(castOperator.getType());
                } catch (Exception e) {
                    LOG.error(e);
                    throw new StarRocksConnectorException("can not cast partition value" + key.getStringValue() +
                            "to target type " + castOperator.getType().prettyPrint());
                }
            }
            Set<Long> partitions = newPartitionValueMap.computeIfAbsent(literalExpr, k -> Sets.newHashSet());
            partitions.addAll(entry.getValue());
        }
        return newPartitionValueMap;
    }

    private Set<Long> evalBinaryPredicate(BinaryPredicateOperator binaryPredicate) {
        Preconditions.checkNotNull(binaryPredicate);
        ScalarOperator right = binaryPredicate.getChild(1);

        if (!(right.isConstantRef())) {
            return null;
        }
        ConstantOperator rightChild = (ConstantOperator) binaryPredicate.getChild(1);

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

        if (partitionValueMap == null || nullPartitions == null || partitionValueMap.isEmpty()) {
            return null;
        }

        ScalarOperatorToExpr.FormatterContext formatterContext =
                new ScalarOperatorToExpr.FormatterContext(new HashMap<>());
        LiteralExpr literal = (LiteralExpr) ScalarOperatorToExpr.buildExecExpression(rightChild, formatterContext);

        BinaryPredicateOperator.BinaryType type = binaryPredicate.getBinaryType();
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

                if (type == BinaryPredicateOperator.BinaryType.LE || type == BinaryPredicateOperator.BinaryType.LT) {
                    // SlotRef <[=] Literal
                    if (literal.compareLiteral(firstKey) < 0) {
                        return Sets.newHashSet();
                    }
                    if (type == BinaryPredicateOperator.BinaryType.LE) {
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
                    if (type == BinaryPredicateOperator.BinaryType.GE) {
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

    private Set<Long> evalInPredicate(InPredicateOperator inPredicate) {
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

    private Set<Long> evalIsNullPredicate(IsNullPredicateOperator isNullPredicate) {
        Preconditions.checkNotNull(isNullPredicate);
        if (!isSinglePartitionColumn(isNullPredicate)) {
            return null;
        }
        ColumnRefOperator child = Utils.extractColumnRef(isNullPredicate).get(0);

        Set<Long> matches = Sets.newHashSet();
        Set<Long> nullPartitions = columnToNullPartitions.get(child);
        if (nullPartitions == null) {
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
        return matches;
    }

    private Pair<Set<Long>, Boolean> evalCompoundPredicate(CompoundPredicateOperator compoundPredicate) {
        Preconditions.checkNotNull(compoundPredicate);
        if (compoundPredicate.getCompoundType() == CompoundPredicateOperator.CompoundType.NOT) {
            return Pair.create(null, true);
        }

        Pair<Set<Long>, Boolean> leftPair = evalPartitionPruneFilter(compoundPredicate.getChild(0));
        Set<Long> lefts = leftPair.first;

        Pair<Set<Long>, Boolean> rightPair = evalPartitionPruneFilter(compoundPredicate.getChild(1));
        Set<Long> rights = rightPair.first;

        Boolean existNoEval = leftPair.second || rightPair.second;

        if (lefts == null && rights == null) {
            return Pair.create(null, existNoEval);
        }

        if (compoundPredicate.getCompoundType() == CompoundPredicateOperator.CompoundType.AND) {
            if (lefts == null) {
                return Pair.create(rights, existNoEval);
            } else if (rights == null) {
                return Pair.create(lefts, existNoEval);
            } else {
                lefts.retainAll(rights);
            }
        } else if (compoundPredicate.getCompoundType() == CompoundPredicateOperator.CompoundType.OR) {
            if (lefts == null || rights == null) {
                return Pair.create(null, existNoEval);
            } else {
                lefts.addAll(rights);
            }
        }
        return Pair.create(lefts, existNoEval);
    }
}
