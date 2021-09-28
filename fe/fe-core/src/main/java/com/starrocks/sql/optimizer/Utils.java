// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalApplyOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class Utils {
    public static int combineHash(int hash, int value) {
        return hash * 37 + value;
    }

    public static List<ScalarOperator> extractConjuncts(ScalarOperator root) {
        if (null == root) {
            return Lists.newArrayList();
        }

        LinkedList<ScalarOperator> list = new LinkedList<>();
        if (!OperatorType.COMPOUND.equals(root.getOpType())) {
            list.add(root);
            return list;
        }

        CompoundPredicateOperator cpo = (CompoundPredicateOperator) root;
        if (!cpo.isAnd()) {
            list.add(root);
            return list;
        }

        list.addAll(extractConjuncts(cpo.getChild(0)));
        list.addAll(extractConjuncts(cpo.getChild(1)));
        return list;
    }

    public static List<ScalarOperator> extractDisjunctive(ScalarOperator root) {
        if (null == root) {
            return Lists.newArrayList();
        }

        LinkedList<ScalarOperator> list = new LinkedList<>();
        if (!OperatorType.COMPOUND.equals(root.getOpType())) {
            list.add(root);
            return list;
        }

        CompoundPredicateOperator cpo = (CompoundPredicateOperator) root;

        if (cpo.isOr()) {
            list.addAll(extractDisjunctive(cpo.getChild(0)));
            list.addAll(extractDisjunctive(cpo.getChild(1)));
        } else {
            list.add(root);
        }
        return list;
    }

    public static List<ColumnRefOperator> extractColumnRef(ScalarOperator root) {
        if (null == root || !root.isVariable()) {
            return Collections.emptyList();
        }

        LinkedList<ColumnRefOperator> list = new LinkedList<>();
        if (OperatorType.VARIABLE.equals(root.getOpType())) {
            list.add((ColumnRefOperator) root);
            return list;
        }

        for (ScalarOperator child : root.getChildren()) {
            list.addAll(extractColumnRef(child));
        }

        return list;
    }

    public static int countColumnRef(ScalarOperator root) {
        return countColumnRef(root, 0);
    }

    private static int countColumnRef(ScalarOperator root, int count) {
        if (null == root || !root.isVariable()) {
            return 0;
        }

        if (OperatorType.VARIABLE.equals(root.getOpType())) {
            return 1;
        }

        for (ScalarOperator child : root.getChildren()) {
            count += countColumnRef(child, count);
        }

        return count;
    }

    public static List<ColumnRefOperator> extractScanColumn(GroupExpression groupExpression) {
        if (OperatorType.LOGICAL_OLAP_SCAN.equals(groupExpression.getOp().getOpType())) {
            LogicalOlapScanOperator loso = (LogicalOlapScanOperator) groupExpression.getOp();

            return ImmutableList.<ColumnRefOperator>builder().addAll(loso.getColRefToColumnMetaMap().keySet()).build();
        }

        List<Group> groups = groupExpression.getInputs();

        ImmutableList.Builder<ColumnRefOperator> builder = ImmutableList.builder();
        for (Group group : groups) {
            GroupExpression expression = group.getFirstLogicalExpression();
            builder.addAll(extractScanColumn(expression));
        }

        return builder.build();
    }

    public static void extractOlapScanOperator(GroupExpression groupExpression, List<LogicalOlapScanOperator> list) {
        if (OperatorType.LOGICAL_OLAP_SCAN.equals(groupExpression.getOp().getOpType())) {
            LogicalOlapScanOperator loso = (LogicalOlapScanOperator) groupExpression.getOp();
            list.add(loso);
            return;
        }

        List<Group> groups = groupExpression.getInputs();
        for (Group group : groups) {
            GroupExpression expression = group.getFirstLogicalExpression();
            extractOlapScanOperator(expression, list);
        }
    }

    // check the ApplyNode's children contains correlation subquery
    public static boolean containsCorrelationSubquery(GroupExpression groupExpression) {
        if (groupExpression.getOp().isLogical() && OperatorType.LOGICAL_APPLY
                .equals(groupExpression.getOp().getOpType())) {
            LogicalApplyOperator apply = (LogicalApplyOperator) groupExpression.getOp();

            if (apply.getCorrelationColumnRefs().isEmpty()) {
                return false;
            }

            // only check right child
            return checkPredicateContainColumnRef(apply.getCorrelationColumnRefs(),
                    groupExpression.getInputs().get(1).getFirstLogicalExpression());
        }
        return false;
    }

    // GroupExpression
    private static boolean checkPredicateContainColumnRef(List<ColumnRefOperator> cro,
                                                          GroupExpression groupExpression) {
        LogicalOperator logicalOperator = (LogicalOperator) groupExpression.getOp();

        if (containAnyColumnRefs(cro, logicalOperator.getPredicate())) {
            return true;
        }

        for (Group group : groupExpression.getInputs()) {
            if (checkPredicateContainColumnRef(cro, group.getFirstLogicalExpression())) {
                return true;
            }
        }

        return false;
    }

    public static boolean containAnyColumnRefs(List<ColumnRefOperator> refs, ScalarOperator operator) {
        if (refs.isEmpty() || null == operator) {
            return false;
        }

        if (operator.isColumnRef()) {
            return refs.contains(operator);
        }

        for (ScalarOperator so : operator.getChildren()) {
            if (containAnyColumnRefs(refs, so)) {
                return true;
            }
        }

        return false;
    }

    public static boolean containColumnRef(ScalarOperator operator, String column) {
        if (null == column || null == operator) {
            return false;
        }

        if (operator.isColumnRef()) {
            return ((ColumnRefOperator) operator).getName().equalsIgnoreCase(column);
        }

        for (ScalarOperator so : operator.getChildren()) {
            if (containColumnRef(so, column)) {
                return true;
            }
        }

        return false;
    }

    public static ScalarOperator compoundOr(List<ScalarOperator> nodes) {
        return createCompound(CompoundPredicateOperator.CompoundType.OR, nodes);
    }

    public static ScalarOperator compoundOr(ScalarOperator... nodes) {
        return createCompound(CompoundPredicateOperator.CompoundType.OR, Arrays.asList(nodes));
    }

    public static ScalarOperator compoundAnd(List<ScalarOperator> nodes) {
        return createCompound(CompoundPredicateOperator.CompoundType.AND, nodes);
    }

    public static ScalarOperator compoundAnd(ScalarOperator... nodes) {
        return createCompound(CompoundPredicateOperator.CompoundType.AND, Arrays.asList(nodes));
    }

    // Build a compound tree by bottom up
    //
    // Example: compoundType.OR
    // Initial state:
    //  a b c d e
    //
    // First iteration:
    //  or    or
    //  /\    /\   e
    // a  b  c  d
    //
    // Second iteration:
    //     or   e
    //    / \
    //  or   or
    //  /\   /\
    // a  b c  d
    //
    // Last iteration:
    //       or
    //      / \
    //     or  e
    //    / \
    //  or   or
    //  /\   /\
    // a  b c  d
    private static ScalarOperator createCompound(CompoundPredicateOperator.CompoundType type,
                                                 List<ScalarOperator> nodes) {
        LinkedList<ScalarOperator> link =
                nodes.stream().filter(Objects::nonNull).collect(Collectors.toCollection(Lists::newLinkedList));

        if (link.size() < 1) {
            return null;
        }

        if (link.size() == 1) {
            return link.get(0);
        }

        while (link.size() > 1) {
            LinkedList<ScalarOperator> buffer = new LinkedList<>();

            // combine pairs of elements
            while (link.size() >= 2) {
                buffer.add(new CompoundPredicateOperator(type, link.poll(), link.poll()));
            }

            // if there's and odd number of elements, just append the last one
            if (!link.isEmpty()) {
                buffer.add(link.remove());
            }

            // continue processing the pairs that were just built
            link = buffer;
        }
        return link.remove();
    }

    public static boolean isInnerOrCrossJoin(Operator operator) {
        if (operator instanceof LogicalJoinOperator) {
            LogicalJoinOperator joinOperator = (LogicalJoinOperator) operator;
            return joinOperator.isInnerOrCrossJoin();
        }
        return false;
    }

    public static int countInnerJoinNodeSize(OptExpression root) {
        int count = 0;
        Operator operator = root.getOp();
        for (OptExpression child : root.getInputs()) {
            if (isInnerOrCrossJoin(operator)) {
                count += countInnerJoinNodeSize(child);
            } else {
                count = Math.max(count, countInnerJoinNodeSize(child));
            }
        }

        if (isInnerOrCrossJoin(operator)) {
            count += 1;
        }
        return count;
    }

    public static boolean hasUnknownColumnsStats(OptExpression root) {
        Operator operator = root.getOp();
        if (operator instanceof LogicalScanOperator) {
            LogicalScanOperator scanOperator = (LogicalScanOperator) operator;
            List<String> colNames = scanOperator.getColRefToColumnMetaMap().values().stream().map(Column::getName).collect(
                    Collectors.toList());

            List<ColumnStatistic> columnStatisticList =
                    Catalog.getCurrentStatisticStorage().getColumnStatistics(scanOperator.getTable(), colNames);
            return columnStatisticList.stream().anyMatch(ColumnStatistic::isUnknown);
        }

        return root.getInputs().stream().anyMatch(Utils::hasUnknownColumnsStats);
    }

    public static long getLongFromDateTime(LocalDateTime dateTime) {
        return dateTime.atZone(ZoneId.systemDefault()).toInstant().getEpochSecond();
    }

    public static long convertBitSetToLong(BitSet bitSet, int length) {
        long gid = 0;
        for (int b = 0; b < length; ++b) {
            gid = gid * 2 + (bitSet.get(b) ? 1 : 0);
        }
        return gid;
    }

    public static ColumnRefOperator findSmallestColumnRef(List<ColumnRefOperator> columnRefOperatorList) {
        Preconditions.checkState(!columnRefOperatorList.isEmpty());
        ColumnRefOperator smallestColumnRef = columnRefOperatorList.get(0);
        int smallestColumnLength = Integer.MAX_VALUE;
        for (ColumnRefOperator columnRefOperator : columnRefOperatorList) {
            Type columnType = columnRefOperator.getType();
            if (columnType.isScalarType()) {
                int columnLength = columnType.getSlotSize();
                if (columnLength < smallestColumnLength) {
                    smallestColumnRef = columnRefOperator;
                    smallestColumnLength = columnLength;
                }
            }
        }
        return smallestColumnRef;
    }

    public static boolean canDoReplicatedJoin(OlapTable table, long selectedIndexId,
                                              Collection<Long> selectedPartitionId,
                                              Collection<Long> selectedTabletId) {
        int backendSize = Catalog.getCurrentSystemInfo().backendSize();
        int aliveBackendSize = Catalog.getCurrentSystemInfo().getBackendIds(true).size();
        int schemaHash = table.getSchemaHashByIndexId(selectedIndexId);
        for (Long partitionId : selectedPartitionId) {
            Partition partition = table.getPartition(partitionId);
            if (partition.getReplicaCount() < backendSize) {
                return false;
            }
            long visibleVersion = partition.getVisibleVersion();
            long visibleVersionHash = partition.getVisibleVersionHash();
            MaterializedIndex materializedIndex = partition.getIndex(selectedIndexId);
            // TODO(kks): improve this for loop
            for (Long id : selectedTabletId) {
                Tablet tablet = materializedIndex.getTablet(id);
                if (tablet != null && tablet.getQueryableReplicasSize(visibleVersion, visibleVersionHash, schemaHash)
                        != aliveBackendSize) {
                    return false;
                }
            }
        }
        return true;
    }
}
