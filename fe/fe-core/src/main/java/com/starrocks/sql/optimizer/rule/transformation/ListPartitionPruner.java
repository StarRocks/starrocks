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
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.analysis.BinaryType;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.SlotRef;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ListPartitionInfo;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Pair;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.planner.PartitionPruner;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.ExpressionAnalyzer;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.OperatorFunctionChecker;
import com.starrocks.sql.optimizer.operator.scalar.PredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriter;
import com.starrocks.sql.optimizer.transformer.SqlToScalarOperatorTranslator;
import com.starrocks.sql.plan.ScalarOperatorToExpr;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Consumer;
import java.util.function.Function;
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
    private final ListPartitionInfo listPartitionInfo;

    private boolean deduceExtraConjuncts = false;
    private LogicalScanOperator scanOperator;

    public ListPartitionPruner(
            Map<ColumnRefOperator, ConcurrentNavigableMap<LiteralExpr, Set<Long>>> columnToPartitionValuesMap,
            Map<ColumnRefOperator, Set<Long>> columnToNullPartitions,
            List<ScalarOperator> partitionConjuncts, List<Long> specifyPartitionIds) {
        this(columnToPartitionValuesMap, columnToNullPartitions, partitionConjuncts, specifyPartitionIds, null);
    }

    public ListPartitionPruner(
            Map<ColumnRefOperator, ConcurrentNavigableMap<LiteralExpr, Set<Long>>> columnToPartitionValuesMap,
            Map<ColumnRefOperator, Set<Long>> columnToNullPartitions,
            List<ScalarOperator> partitionConjuncts, List<Long> specifyPartitionIds,
            ListPartitionInfo listPartitionInfo) {
        this.columnToPartitionValuesMap = columnToPartitionValuesMap;
        this.columnToNullPartitions = columnToNullPartitions;
        this.partitionConjuncts = partitionConjuncts;
        this.allPartitions = getAllPartitions();
        this.partitionColumnRefs = getPartitionColumnRefs();
        this.specifyPartitionIds = specifyPartitionIds;
        this.listPartitionInfo = listPartitionInfo;
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

        deduceExtraConjuncts();

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
            if (CollectionUtils.isNotEmpty(specifyPartitionIds)) {
                return specifyPartitionIds;
            } else {
                return null;
            }
        } else {
            if (CollectionUtils.isNotEmpty(specifyPartitionIds)) {
                // intersect
                return specifyPartitionIds.stream().filter(matches::contains).collect(Collectors.toList());
            } else {
                return new ArrayList<>(matches);
            }
        }
    }

    /**
     * TODO: support more cases
     * Only some simple conjuncts can be pruned
     */
    public static boolean canPruneWithConjunct(ScalarOperator conjunct) {
        if (conjunct instanceof BinaryPredicateOperator) {
            BinaryPredicateOperator bop = conjunct.cast();
            return bop.getBinaryType().isEqualOrRange() && evaluateConstant(bop.getChild(1)) != null;
        } else if (conjunct instanceof InPredicateOperator) {
            InPredicateOperator inOp = conjunct.cast();
            return !inOp.isNotIn() && inOp.getChildren().stream().skip(1).allMatch(ScalarOperator::isConstant);
        } else if (conjunct instanceof IsNullPredicateOperator) {
            return true;
        } else if (conjunct instanceof CompoundPredicateOperator) {
            CompoundPredicateOperator cop = conjunct.cast();
            // all children should be pruneable
            if (cop.getChildren().stream().anyMatch(conj -> !canPruneWithConjunct(conj))) {
                return false;
            }
            return true;
        }
        return false;
    }

    /**
     * Can we use this conjunct to deduce extract pruneable-conjuncts
     * Example:
     * - conjunct: dt >= '2024-01-01'
     * - generate-expr: month=date_trunc('MONTH', dt)
     */
    public static List<String> deduceGenerateColumns(LogicalScanOperator scanOperator) {
        List<String> partitionColumnNames = scanOperator.getTable().getPartitionColumnNames();
        if (CollectionUtils.isEmpty(partitionColumnNames)) {
            return Lists.newArrayList();
        }
        List<String> result = Lists.newArrayList(partitionColumnNames);

        Function<SlotRef, ColumnRefOperator> slotRefResolver = (slot) -> {
            return scanOperator.getColumnNameToColRefMap().get(slot.getColumnName());
        };
        Consumer<SlotRef> slotRefConsumer = (slot) -> {
            ColumnRefOperator ref = scanOperator.getColumnNameToColRefMap().get(slot.getColumnName());
            slot.setType(ref.getType());
        };
        for (String partitionColumn : partitionColumnNames) {
            Column column = scanOperator.getTable().getColumn(partitionColumn);
            if (column != null && column.isGeneratedColumn()) {
                Expr generatedExpr = column.getGeneratedColumnExpr(scanOperator.getTable().getBaseSchema());
                ExpressionAnalyzer.analyzeExpressionResolveSlot(generatedExpr, ConnectContext.get(), slotRefConsumer);
                ScalarOperator call =
                        SqlToScalarOperatorTranslator.translateWithSlotRef(generatedExpr, slotRefResolver);

                if (call instanceof CallOperator &&
                        OperatorFunctionChecker.onlyContainMonotonicFunctions((CallOperator) call).first) {
                    List<ColumnRefOperator> columnRefOperatorList = Utils.extractColumnRef(call);
                    for (ColumnRefOperator ref : columnRefOperatorList) {
                        result.add(ref.getName());
                    }
                }
            }
        }

        return result;
    }

    public void prepareDeduceExtraConjuncts(LogicalScanOperator scanOperator) {
        this.deduceExtraConjuncts = true;
        this.scanOperator = scanOperator;
    }

    // Infer equivalent partitions columns based on the partition-conjuncts.
    // Suppose the query has an expression c1 >= '2024-01-02', and the table is partitioned by
    // a GeneratedColumn c3=date_trunc('month', c1), so we can infer the expression:
    // c3 >= date_trunc('month', '2024-01-02')
    // This optimization is only applied to the case that the table's partition column is actually a GeneratedColumn
    // which is monotonic function
    private void deduceExtraConjuncts() {
        if (!deduceExtraConjuncts) {
            return;
        }
        Function<SlotRef, ColumnRefOperator> slotRefResolver = (slot) -> {
            return scanOperator.getColumnNameToColRefMap().get(slot.getColumnName());
        };
        // The GeneratedColumn doesn't have the correct type info, let's help it
        Consumer<SlotRef> slotRefConsumer = (slot) -> {
            ColumnRefOperator ref = scanOperator.getColumnNameToColRefMap().get(slot.getColumnName());
            slot.setType(ref.getType());
        };

        // Build a map of c1 -> c3, in which c3=fn(c1)
        Map<ColumnRefOperator, Pair<ColumnRefOperator, ScalarOperator>> refedGeneratedColumnMap = Maps.newHashMap();
        for (ColumnRefOperator partitionColumn : partitionColumnRefs) {
            Column column = scanOperator.getTable().getColumn(partitionColumn.getName());
            if (column != null && column.isGeneratedColumn()) {
                Expr generatedExpr = column.getGeneratedColumnExpr(scanOperator.getTable().getBaseSchema());
                ExpressionAnalyzer.analyzeExpressionResolveSlot(generatedExpr, ConnectContext.get(), slotRefConsumer);
                ScalarOperator call =
                        SqlToScalarOperatorTranslator.translateWithSlotRef(generatedExpr, slotRefResolver);

                if (call instanceof CallOperator &&
                        OperatorFunctionChecker.onlyContainMonotonicFunctions((CallOperator) call).first) {
                    List<ColumnRefOperator> columnRefOperatorList = Utils.extractColumnRef(call);

                    for (ColumnRefOperator ref : columnRefOperatorList) {
                        refedGeneratedColumnMap.put(ref, Pair.create(partitionColumn, call));
                    }
                }
            }
        }

        // No GeneratedColumn with partition column
        if (refedGeneratedColumnMap.isEmpty()) {
            return;
        }

        List<ScalarOperator> extraConjuncts = Lists.newArrayList();
        for (ScalarOperator conjunct : partitionConjuncts) {
            List<ColumnRefOperator> columnRefOperatorList = Utils.extractColumnRef(conjunct);
            if (!checkDeduceConjunct(conjunct, columnRefOperatorList)) {
                continue;
            }

            ColumnRefOperator referenced = columnRefOperatorList.get(0);
            Pair<ColumnRefOperator, ScalarOperator> pair = refedGeneratedColumnMap.get(referenced);
            if (pair == null) {
                // No GeneratedColumn
                continue;
            }
            ColumnRefOperator generatedColumn = pair.first;
            ScalarOperator generatedExpr = pair.second;
            ScalarOperator result = buildDeducedConjunct(conjunct, generatedExpr, generatedColumn);
            if (result != null) {
                extraConjuncts.add(result);
            }
        }

        partitionConjuncts.addAll(extraConjuncts);
    }

    private boolean checkDeduceConjunct(ScalarOperator conjunct, List<ColumnRefOperator> columnRefs) {
        // The conjunct should not contain partition-column
        if (partitionColumnRefs.containsAll(columnRefs)) {
            return false;
        }
        // Only one Column-Ref
        if (columnRefs.size() != 1) {
            return false;
        }
        // Only support predicate
        if (!(conjunct instanceof PredicateOperator)) {
            return false;
        }
        // Left child should be a column-ref
        if (!conjunct.getChild(0).isColumnRef()) {
            return false;
        }

        return true;
    }

    /**
     * Input:
     * - conjunct: c1 >= '2024-01-02'
     * - monoExpr: date_trunc('MONTH', c1)
     * - generatedColumn: c3
     * <p>
     * Deducted result: c3 >= date_trunc('MONTH', '2024-01-02')
     */
    private ScalarOperator buildDeducedConjunct(ScalarOperator conjunct,
                                                ScalarOperator monoExpr,
                                                ColumnRefOperator generatedColumn) {
        ScalarOperatorVisitor<ScalarOperator, Void> visitor = new ScalarOperatorVisitor<>() {

            private ScalarOperator replaceExpr(int index) {
                Map<ColumnRefOperator, ScalarOperator> mapping = Maps.newHashMap();
                ReplaceColumnRefRewriter rewriter = new ReplaceColumnRefRewriter(mapping);
                mapping.put(Utils.extractColumnRef(monoExpr).get(0), conjunct.getChild((index)));
                return rewriter.rewrite(monoExpr);
            }

            @Override
            public ScalarOperator visit(ScalarOperator scalarOperator, Void context) {
                // Not supported
                return null;
            }

            @Override
            public ScalarOperator visitBinaryPredicate(BinaryPredicateOperator operator, Void context) {
                BinaryPredicateOperator result = operator.normalizeNonStrictMonotonic();
                if (result == null) {
                    return null;
                }
                result.setChild(0, generatedColumn);
                result.setChild(1, replaceExpr(1));
                return result;
            }

            @Override
            public ScalarOperator visitInPredicate(InPredicateOperator operator, Void context) {
                ScalarOperator result = operator.clone();
                result.setChild(0, generatedColumn);
                for (int i = 1; i < operator.getChildren().size(); i++) {
                    result.setChild(i, replaceExpr(i));
                }
                return result;
            }

        };
        ScalarOperator result = conjunct.accept(visitor, null);
        if (result == null) {
            return null;
        }

        ScalarOperatorRewriter rewriter = new ScalarOperatorRewriter();
        // implicit cast
        result = rewriter.rewrite(result, ScalarOperatorRewriter.DEFAULT_TYPE_CAST_RULE);
        // fold constant
        result = rewriter.rewrite(result, ScalarOperatorRewriter.FOLD_CONSTANT_RULES);
        return result;
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
        return isSinglePartitionColumn(predicate, partitionColumnRefs);
    }

    private static boolean isSinglePartitionColumn(ScalarOperator predicate,
                                                   List<ColumnRefOperator> partitionColumnRefs) {
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

    private static LiteralExpr castLiteralExpr(LiteralExpr literalExpr, Type type) {
        LiteralExpr result = null;
        String value = literalExpr.getStringValue();
        if (literalExpr.getType() == Type.DATE && type.isNumericType()) {
            value = String.valueOf(literalExpr.getLongValue() / 1000000);
        }
        try {
            result = LiteralExpr.create(value, type);
        } catch (Exception e) {
            LOG.warn("Failed to execute LiteralExpr.create", e);
            throw new StarRocksConnectorException("can not cast literal value " + literalExpr.getStringValue() +
                    " to target type " + type.prettyPrint());
        }
        return result;
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
            LiteralExpr key = entry.getKey();
            LiteralExpr literalExpr = castLiteralExpr(key, castOperator.getType());
            Set<Long> partitions = newPartitionValueMap.computeIfAbsent(literalExpr, k -> Sets.newHashSet());
            partitions.addAll(entry.getValue());
        }
        return newPartitionValueMap;
    }

    private static ConstantOperator evaluateConstant(ScalarOperator operator) {
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

    private Set<Long> evalBinaryPredicate(BinaryPredicateOperator binaryPredicate) {
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
                    if (listPartitionInfo == null) {
                        // external table
                        matches.removeAll(partitionValueMap.get(literal));
                    } else {
                        Set<Long> partitionIds = partitionValueMap.get(literal);
                        for (Long id : partitionIds) {
                            if (listPartitionInfo.isSingleValuePartition(id)) {
                                matches.remove(id);
                            }
                        }
                    }
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

        if (inPredicate.getChild(0) instanceof CastOperator && partitionValueMap != null) {
            // partitionValueMap need cast to target type
            partitionValueMap = getCastPartitionValueMap((CastOperator) inPredicate.getChild(0),
                    partitionValueMap);
        }

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
                    // external table, one partition column for one partition can only have one value
                    if (listPartitionInfo == null) {
                        matches.removeAll(partitions);
                    } else {
                        // for olap table, if one partition is multi value partition like PARTITION pCalifornia VALUES IN ("Los Angeles","San Francisco","San Diego")
                        // and we have a not in predicate like city not in ("Los Angeles"), it's not safe to remove this partition
                        for (Long id : partitions) {
                            if (listPartitionInfo.isSingleValuePartition(id)) {
                                matches.remove(id);
                            }
                        }
                    }
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

    public static void collectOlapTablePartitionValuesMap(
            OlapTable olapTable,
            Set<Long> partitionIds,
            Map<Column, ColumnRefOperator> columnRefOperatorMap,
            Map<ColumnRefOperator, ConcurrentNavigableMap<LiteralExpr, Set<Long>>> columnToPartitionValuesMap,
            Map<ColumnRefOperator, Set<Long>> columnToNullPartitions,
            boolean isStrict) {
        PartitionInfo partitionInfo = olapTable.getPartitionInfo();
        if (!partitionInfo.isListPartition()) {
            return;
        }
        ListPartitionInfo listPartitionInfo = (ListPartitionInfo) partitionInfo;
        // single item list partition has only one column mapper
        Map<Long, List<LiteralExpr>> literalExprValuesMap = listPartitionInfo.getLiteralExprValues();
        List<Column> partitionColumns = listPartitionInfo.getPartitionColumns(olapTable.getIdToColumn());
        if (!CollectionUtils.sizeIsEmpty(literalExprValuesMap)) {
            Set<Long> nullPartitionIds = new HashSet<>();
            ConcurrentNavigableMap<LiteralExpr, Set<Long>> partitionValueToIds = new ConcurrentSkipListMap<>();
            for (Map.Entry<Long, List<LiteralExpr>> entry : literalExprValuesMap.entrySet()) {
                Long partitionId = entry.getKey();
                if (!partitionIds.contains(partitionId)) {
                    continue;
                }
                List<LiteralExpr> values = entry.getValue();
                if (CollectionUtils.isEmpty(values)) {
                    continue;
                }
                if (values.size() == 1 || !isStrict) {
                    for (LiteralExpr value : values) {
                        // store null partition value seperated from non-null partition values
                        if (value.isConstantNull()) {
                            nullPartitionIds.add(partitionId);
                        } else {
                            putValueMapItem(partitionValueToIds, partitionId, value);
                        }
                    }
                }
            }
            // single item list partition has only one column
            Column column = partitionColumns.get(0);
            ColumnRefOperator columnRefOperator = columnRefOperatorMap.get(column);
            columnToPartitionValuesMap.put(columnRefOperator, partitionValueToIds);
            columnToNullPartitions.put(columnRefOperator, nullPartitionIds);
        }

        // multiItem list partition mapper
        Map<Long, List<List<LiteralExpr>>> multiLiteralExprValues = listPartitionInfo.getMultiLiteralExprValues();
        if (multiLiteralExprValues != null && multiLiteralExprValues.size() > 0) {
            for (int i = 0; i < partitionColumns.size(); i++) {
                ConcurrentNavigableMap<LiteralExpr, Set<Long>> partitionValueToIds = new ConcurrentSkipListMap<>();
                Set<Long> nullPartitionIds = new HashSet<>();
                for (Map.Entry<Long, List<List<LiteralExpr>>> entry : multiLiteralExprValues.entrySet()) {
                    Long partitionId = entry.getKey();
                    if (!partitionIds.contains(partitionId)) {
                        continue;
                    }
                    List<List<LiteralExpr>> multiValues = entry.getValue();
                    if (CollectionUtils.isEmpty(multiValues)) {
                        continue;
                    }
                    if (multiValues.size() == 1 || !isStrict) {
                        for (List<LiteralExpr> values : multiValues) {
                            LiteralExpr value = values.get(i);
                            // store null partition value seperated from non-null partition values
                            if (value.isConstantNull()) {
                                nullPartitionIds.add(partitionId);
                            } else {
                                putValueMapItem(partitionValueToIds, partitionId, value);
                            }
                        }
                    }
                }
                Column column = partitionColumns.get(i);
                ColumnRefOperator columnRefOperator = columnRefOperatorMap.get(column);
                columnToPartitionValuesMap.put(columnRefOperator, partitionValueToIds);
                columnToNullPartitions.put(columnRefOperator, nullPartitionIds);
            }
        }
    }

    private static void putValueMapItem(ConcurrentNavigableMap<LiteralExpr, Set<Long>> partitionValueToIds,
                                        Long partitionId,
                                        LiteralExpr value) {
        partitionValueToIds.computeIfAbsent(value, ignored -> Sets.newHashSet())
                .add(partitionId);
    }
}
