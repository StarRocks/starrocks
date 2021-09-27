// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/planner/ListPartitionPruner.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.planner;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.analysis.BinaryPredicate;
import com.starrocks.analysis.CompoundPredicate;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.InPredicate;
import com.starrocks.analysis.IsNullPredicate;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.NullLiteral;
import com.starrocks.analysis.SlotDescriptor;
import com.starrocks.analysis.SlotId;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.common.AnalysisException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

/**
 * ListPartitionPruner provides a mechanism to filter out partitions of the table that has list partitions
 * based on the conjuncts.
 * For example, hive table has one or more partition columns, and has one or more partitions per column.
 */
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
    private final Map<String, TreeMap<LiteralExpr, Set<Long>>> columnToPartitionValuesMap;
    // Store partitions with null partition values separately
    // partitionColumnName -> null partitionIds
    //
    // "date_col" -> set(5)
    // "int_col"  -> set(4)
    private final Map<String, Set<Long>> columnToNullPartitions;
    private final List<Expr> partitionConjuncts;
    private final TupleDescriptor tupleDesc;

    // Conjuncts that not eval in partition pruner, and will be sent to backend.
    private final List<Expr> noEvalConjuncts = Lists.newArrayList();

    private final Set<Long> allPartitions;
    private final List<SlotId> partitionSlotIds;

    public ListPartitionPruner(Map<String, TreeMap<LiteralExpr, Set<Long>>> columnToPartitionValuesMap,
                               Map<String, Set<Long>> columnToNullPartitions, List<Expr> partitionConjuncts,
                               TupleDescriptor tupleDesc) {
        this.columnToPartitionValuesMap = columnToPartitionValuesMap;
        this.columnToNullPartitions = columnToNullPartitions;
        this.partitionConjuncts = partitionConjuncts;
        this.tupleDesc = tupleDesc;
        this.allPartitions = getAllPartitions();
        this.partitionSlotIds = getPartitionSlotIds();
    }

    private Set<Long> getAllPartitions() {
        Set<Long> allPartitions = Sets.newHashSet();
        for (TreeMap<LiteralExpr, Set<Long>> partitionValuesMap : columnToPartitionValuesMap.values()) {
            for (Set<Long> partitions : partitionValuesMap.values()) {
                allPartitions.addAll(partitions);
            }
        }
        for (Set<Long> partitions : columnToNullPartitions.values()) {
            allPartitions.addAll(partitions);
        }
        return allPartitions;
    }

    private List<SlotId> getPartitionSlotIds() {
        List<SlotId> partitionSlotIds = Lists.newArrayList();
        for (String columnName : columnToPartitionValuesMap.keySet()) {
            SlotDescriptor slotDesc = tupleDesc.getColumnSlot(columnName);
            if (slotDesc == null) {
                continue;
            }

            partitionSlotIds.add(slotDesc.getId());
        }
        return partitionSlotIds;
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

        if (columnToPartitionValuesMap.isEmpty() && columnToNullPartitions.isEmpty()) {
            // no partition columns, notEvalConjuncts is same with conjuncts
            noEvalConjuncts.addAll(partitionConjuncts);
            return null;
        }
        if (partitionConjuncts.isEmpty()) {
            // no conjuncts, notEvalConjuncts is empty
            return null;
        }

        Set<Long> matches = null;
        for (Expr expr : partitionConjuncts) {
            if (!expr.isBound(partitionSlotIds)) {
                noEvalConjuncts.add(expr);
                continue;
            }
            Set<Long> conjunctMatches = evalSlotBindingFilter(expr);
            LOG.debug("prune by expr: {}, partitions: {}", expr.toSql(), conjunctMatches);
            if (conjunctMatches != null) {
                if (matches == null) {
                    matches = Sets.newHashSet(conjunctMatches);
                } else {
                    matches.retainAll(conjunctMatches);
                }
            } else {
                noEvalConjuncts.add(expr);
            }
        }
        return new ArrayList<>(matches);
    }

    public List<Expr> getNoEvalConjuncts() {
        return noEvalConjuncts;
    }

    private SlotDescriptor getBoundSlotDescriptor(Expr expr) {
        for (SlotId slotId : partitionSlotIds) {
            if (expr.isBound(slotId)) {
                return tupleDesc.getSlot(slotId.asInt());
            }
        }
        LOG.warn("expr {} has no bound slot.", expr.toSql());
        return null;
    }

    private Set<Long> evalSlotBindingFilter(Expr expr) {
        if (expr instanceof BinaryPredicate) {
            return evalBinaryPredicate((BinaryPredicate) expr);
        } else if (expr instanceof InPredicate) {
            return evalInPredicate((InPredicate) expr);
        } else if (expr instanceof IsNullPredicate) {
            return evalIsNullPredicate((IsNullPredicate) expr);
        } else if (expr instanceof CompoundPredicate) {
            return evalCompoundPredicate((CompoundPredicate) expr);
        }
        return null;
    }

    private Set<Long> evalBinaryPredicate(BinaryPredicate binaryPredicate) {
        Preconditions.checkNotNull(binaryPredicate);
        SlotDescriptor slotDesc = getBoundSlotDescriptor(binaryPredicate);
        if (slotDesc == null) {
            return null;
        }
        Expr slotBinding = binaryPredicate.getSlotBinding(slotDesc.getId());
        if (slotBinding == null || !slotBinding.isConstant() || !Expr.IS_LITERAL.apply(slotBinding)) {
            return null;
        }

        Set<Long> matches = Sets.newHashSet();
        String columnName = slotDesc.getColumn().getName();
        TreeMap<LiteralExpr, Set<Long>> partitionValueMap = columnToPartitionValuesMap.get(columnName);
        Set<Long> nullPartitions = columnToNullPartitions.get(columnName);

        LiteralExpr literal = (LiteralExpr) slotBinding;
        BinaryPredicate.Operator op = binaryPredicate.getOp();
        if (!binaryPredicate.slotIsLeft()) {
            op = op.commutative();
        }
        switch (op) {
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

                if (op == BinaryPredicate.Operator.LE || op == BinaryPredicate.Operator.LT) {
                    // SlotRef <[=] Literal
                    if (literal.compareLiteral(firstKey) < 0) {
                        return Sets.newHashSet();
                    }
                    if (op == BinaryPredicate.Operator.LE) {
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
                    if (op == BinaryPredicate.Operator.GE) {
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

    private Set<Long> evalInPredicate(InPredicate inPredicate) {
        Preconditions.checkNotNull(inPredicate);
        if (!inPredicate.isLiteralChildren()) {
            return null;
        }
        if (Expr.IS_LITERAL.apply(inPredicate.getChild(0).unwrapExpr(false))) {
            // If child(0) of the in predicate is a constant expression,
            // then other children of in predicate should not be used as a condition for partition prune.
            // Such as "where  'Hi' in ('Hi', 'hello') and ... "
            return null;
        }
        SlotDescriptor slotDesc = getBoundSlotDescriptor(inPredicate);
        if (slotDesc == null) {
            return null;
        }

        Set<Long> matches = Sets.newHashSet();
        String columnName = slotDesc.getColumn().getName();
        TreeMap<LiteralExpr, Set<Long>> partitionValueMap = columnToPartitionValuesMap.get(columnName);
        Set<Long> nullPartitions = columnToNullPartitions.get(columnName);

        if (inPredicate.isNotIn()) {
            // SlotRef NOT IN (Literal, ..., Literal)
            // If there is a NullLiteral, return an empty set.
            List<Expr> nullLiterals = Lists.newArrayList();
            inPredicate.collectAll(Predicates.instanceOf(NullLiteral.class), nullLiterals);
            if (!nullLiterals.isEmpty()) {
                return Sets.newHashSet();
            }

            // all partitions but remove null partitions
            matches.addAll(allPartitions);
            matches.removeAll(nullPartitions);
        }

        for (int i = 1; i < inPredicate.getChildren().size(); ++i) {
            LiteralExpr literal = (LiteralExpr) inPredicate.getChild(i);
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

    private Set<Long> evalIsNullPredicate(IsNullPredicate isNullPredicate) {
        Preconditions.checkNotNull(isNullPredicate);
        if (!isNullPredicate.isSlotRefChildren()) {
            return null;
        }
        SlotDescriptor slotDesc = getBoundSlotDescriptor(isNullPredicate);
        if (slotDesc == null) {
            return null;
        }

        Set<Long> matches = Sets.newHashSet();
        String columnName = slotDesc.getColumn().getName();
        Set<Long> nullPartitions = columnToNullPartitions.get(columnName);
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

    private Set<Long> evalCompoundPredicate(CompoundPredicate compoundPredicate) {
        Preconditions.checkNotNull(compoundPredicate);
        if (compoundPredicate.getOp() == CompoundPredicate.Operator.NOT) {
            return null;
        }

        Set<Long> lefts = evalSlotBindingFilter(compoundPredicate.getChild(0));
        Set<Long> rights = evalSlotBindingFilter(compoundPredicate.getChild(1));
        if (lefts == null && rights == null) {
            return null;
        } else if (lefts == null) {
            return rights;
        } else if (rights == null) {
            return lefts;
        }

        if (compoundPredicate.getOp() == CompoundPredicate.Operator.AND) {
            lefts.retainAll(rights);
        } else if (compoundPredicate.getOp() == CompoundPredicate.Operator.OR) {
            lefts.addAll(rights);
        }
        return lefts;
    }
}
