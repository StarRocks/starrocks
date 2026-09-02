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
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DeltaLakeTable;
import com.starrocks.catalog.FlussTable;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.PaimonTable;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Pair;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.connector.ConnectorMetadataRequestContext;
import com.starrocks.connector.GetRemoteFilesParams;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.connector.elasticsearch.EsShardPartitions;
import com.starrocks.connector.elasticsearch.EsTablePartitions;
import com.starrocks.connector.paimon.PaimonRemoteFileDesc;
import com.starrocks.planner.PartitionColumnFilter;
import com.starrocks.planner.PartitionPruner;
import com.starrocks.planner.RangePartitionPruner;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.ast.expression.ExprUtils;
import com.starrocks.sql.ast.expression.LiteralExpr;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.ScanOperatorPredicates;
import com.starrocks.sql.optimizer.operator.logical.LogicalEsScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.LikePredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.transformation.ListPartitionPruner;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.Split;

import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;

import static com.starrocks.connector.PartitionUtil.createPartitionKey;
import static com.starrocks.connector.PartitionUtil.toPartitionValues;
import static com.starrocks.connector.paimon.PaimonMetadata.getRowCount;

public class OptExternalPartitionPruner {
    private static final Logger LOG = LogManager.getLogger(OptExternalPartitionPruner.class);
    private static final int MAX_IN_COMBINATIONS = 64;

    public static LogicalScanOperator prunePartitions(OptimizerContext context,
                                                      LogicalScanOperator logicalScanOperator) {
        return prunePartitionsImpl(context, logicalScanOperator);
    }

    public static LogicalScanOperator prunePartitionsImpl(OptimizerContext context,
                                                          LogicalScanOperator logicalScanOperator) {
        if (logicalScanOperator instanceof LogicalEsScanOperator) {
            LogicalEsScanOperator operator = (LogicalEsScanOperator) logicalScanOperator;
            EsTablePartitions esTablePartitions = operator.getEsTablePartitions();

            Collection<Long> partitionIds = null;
            try {
                partitionIds = partitionPrune(operator.getTable(),
                        esTablePartitions.getPartitionInfo(), operator.getColumnFilters());
            } catch (AnalysisException e) {
                LOG.warn("Es Table partition prune failed. ", e);
            }

            ArrayList<String> unPartitionedIndices = Lists.newArrayList();
            ArrayList<String> partitionedIndices = Lists.newArrayList();
            for (EsShardPartitions esShardPartitions : esTablePartitions.getUnPartitionedIndexStates().values()) {
                operator.getSelectedIndex().add(esShardPartitions);
                unPartitionedIndices.add(esShardPartitions.getIndexName());
            }
            if (partitionIds != null) {
                for (Long partitionId : partitionIds) {
                    EsShardPartitions indexState = esTablePartitions.getEsShardPartitions(partitionId);
                    operator.getSelectedIndex().add(indexState);
                    partitionedIndices.add(indexState.getIndexName());
                }
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("partition prune finished, unpartitioned index [{}], " + "partitioned index [{}]",
                        String.join(",", unPartitionedIndices), String.join(",", partitionedIndices));
            }
        } else {
            // partitionColumnName -> (LiteralExpr -> partition ids)
            // no null partitions in this map, used by ListPartitionPruner
            Map<ColumnRefOperator, ConcurrentNavigableMap<LiteralExpr, Set<Long>>> columnToPartitionValuesMap =
                    Maps.newConcurrentMap();
            // Store partitions with null partition values separately, used by ListPartitionPruner
            // partitionColumnName -> null partitionIds
            Map<ColumnRefOperator, Set<Long>> columnToNullPartitions = Maps.newConcurrentMap();

            try {
                initPartitionInfo(logicalScanOperator, context, columnToPartitionValuesMap, columnToNullPartitions);
                classifyConjuncts(logicalScanOperator, columnToPartitionValuesMap);
                computePartitionInfo(logicalScanOperator, context, columnToPartitionValuesMap, columnToNullPartitions);
            } catch (Exception e) {
                LOG.warn("HMS table partition prune failed : ", e);
                throw new StarRocksPlannerException(e.getMessage(), ErrorType.INTERNAL_ERROR);
            }

            try {
                computeMinMaxConjuncts(logicalScanOperator, context);
            } catch (Exception e) {
                LOG.warn("Remote scan min max conjuncts exception : ", e);
                throw new StarRocksPlannerException(e.getMessage(), ErrorType.INTERNAL_ERROR);
            }
        }
        return logicalScanOperator;
    }

    private static List<ScalarOperator> getColumnEQConstantPredicates(ScalarOperator predicate) {
        List<ScalarOperator> predicateList = Utils.extractConjuncts(predicate);
        List<ScalarOperator> equalPredicates = Lists.newArrayList();
        for (ScalarOperator scalarOperator : predicateList) {
            if (scalarOperator instanceof BinaryPredicateOperator) {
                BinaryPredicateOperator binary = (BinaryPredicateOperator) scalarOperator;
                ScalarOperator leftChild = scalarOperator.getChild(0);
                ScalarOperator rightChild = scalarOperator.getChild(1);
                BinaryType binaryType = binary.getBinaryType();
                if (binaryType.isEqual() && leftChild.isColumnRef() && rightChild.isConstantRef()) {
                    equalPredicates.add(scalarOperator);
                }
            }
        }
        return equalPredicates;
    }

    private static Map<ColumnRefOperator, List<String>> getColumnINConstantValues(ScalarOperator predicate) {
        List<ScalarOperator> predicateList = Utils.extractConjuncts(predicate);
        Map<ColumnRefOperator, List<String>> result = Maps.newHashMap();
        for (ScalarOperator op : predicateList) {
            if (op instanceof InPredicateOperator) {
                InPredicateOperator inOp = (InPredicateOperator) op;
                if (!inOp.isNotIn() && inOp.getChild(0).isColumnRef()
                        && inOp.allValuesMatch(ScalarOperator::isConstantRef)) {
                    ColumnRefOperator colRef = (ColumnRefOperator) inOp.getChild(0);
                    List<String> values = new ArrayList<>();
                    for (int i = 1; i < inOp.getChildren().size(); i++) {
                        ConstantOperator constant = inOp.getChild(i).cast();
                        values.add(constant.toString());
                    }
                    result.put(colRef, values);
                }
            }
        }
        return result;
    }

    /**
     * If the following conditions are met, false is returned:
     * 1. Predicates does not contain partition columns
     * 2. The left and right children of the partition predicate cannot be function parameters
     */
    private static boolean checkPartitionPredicates(LogicalScanOperator operator, List<Column> partitionColumns) {
        List<ScalarOperator> predicateList = Utils.extractConjuncts(operator.getPredicate());
        Set<ColumnRefOperator> partitionColRefSet = new HashSet<>();
        partitionColumns.forEach(partitionColumn -> partitionColRefSet.add(operator.getColumnReference(partitionColumn)));

        List<ScalarOperator> partitionPredicateList = new ArrayList<>();
        for (ScalarOperator scalarOperator : predicateList) {
            if (containsPartitionColumn(scalarOperator, partitionColRefSet)) {
                partitionPredicateList.add(scalarOperator);
            }
        }
        if (partitionPredicateList.isEmpty()) {
            return false;
        }

        for (ScalarOperator scalarOperator : partitionPredicateList) {
            if (canPartitionPrune(scalarOperator, partitionColRefSet)) {
                return true;
            }
        }
        return false;
    }

    private static boolean canPartitionPrune(ScalarOperator partitionPredicate, Set<ColumnRefOperator> partitionColRefSet) {
        if (partitionPredicate instanceof BinaryPredicateOperator) {
            ScalarOperator leftChild = partitionPredicate.getChild(0);
            ScalarOperator rightChild = partitionPredicate.getChild(1);
            if (leftChild.isColumnRef() && rightChild.isColumnRef()) {
                return false;
            }
            // Any child is neither a constant nor a ColumnRef, indicating that there is a function expression
            return isConstantOrColumnRef(leftChild) && isConstantOrColumnRef(rightChild);
        } else if (partitionPredicate instanceof InPredicateOperator) {
            List<ScalarOperator> children = partitionPredicate.getChildren();
            ScalarOperator firstChild = children.get(0);
            if (!firstChild.isColumnRef()) {
                return false;
            }
            for (int i = 1; i < partitionPredicate.getChildren().size(); ++i) {
                ScalarOperator child = children.get(i);
                if (!(child instanceof ConstantOperator)) {
                    return false;
                }
            }
        } else if (partitionPredicate instanceof CompoundPredicateOperator) {
            CompoundPredicateOperator cpo = (CompoundPredicateOperator) partitionPredicate;
            if (cpo.isNot()) {
                return false;
            }
            ScalarOperator leftChild = partitionPredicate.getChild(0);
            ScalarOperator rightChild = partitionPredicate.getChild(1);
            return containsPartitionColumn(leftChild, partitionColRefSet)
                    && containsPartitionColumn(rightChild, partitionColRefSet)
                    && canPartitionPrune(leftChild, partitionColRefSet)
                    && canPartitionPrune(rightChild, partitionColRefSet);
        } else if (partitionPredicate instanceof LikePredicateOperator || partitionPredicate instanceof CallOperator) {
            return false;
        }
        return true;
    }

    // Note: The isConstant() method cannot be used here. If the child of CallOperator is constant, isConstant() will return
    // true, but partition pruning cannot be performed.
    private static boolean isConstantOrColumnRef(ScalarOperator scalarOperator) {
        return (scalarOperator instanceof ConstantOperator) || scalarOperator.isColumnRef();
    }

    private static boolean containsPartitionColumn(ScalarOperator scalarOperator, Set<ColumnRefOperator> partitionColRefSet) {
        for (ScalarOperator child : scalarOperator.getChildren()) {
            List<ColumnRefOperator> columnRefs = child.getColumnRefs();
            for (ColumnRefOperator columnRef : columnRefs) {
                if (partitionColRefSet.contains(columnRef)) {
                    return true;
                }
            }
        }
        return false;
    }

    // Shared by Hive/Hudi/ODPS (HMS), Iceberg and Delta Lake: reject the query if it doesn't carry a usable
    // partition predicate, when allow_lake_without_partition_filter is disabled.
    private static void checkPartitionFilterRequired(LogicalScanOperator operator, OptimizerContext context,
                                                      Table table, List<Column> partitionColumns) throws AnalysisException {
        if (context.getSessionVariable().isAllowLakeWithoutPartitionFilter()) {
            return;
        }
        if (!checkPartitionPredicates(operator, partitionColumns)) {
            LOG.warn("Partition pruning is invalid. queryId: {}", DebugUtil.printId(context.getQueryId()));
            throw new AnalysisException("Partition pruning is invalid, please check: "
                    + "1. The partition predicate must be included. "
                    + "2. The left and right children of the partition predicate cannot be function parameters. "
                    + "Table: " + table.getCatalogName() + "." + table.getCatalogDBName()
                    + "." + table.getCatalogTableName() + " " + "Partition columns: "
                    + partitionColumns.stream().map(Column::getName).collect(Collectors.joining(", ")));
        }
    }

    private static List<Optional<List<String>>> getEffectiveInPartitionValues(
            LogicalScanOperator operator, List<Column> partitionColumns, ScalarOperator predicate) {
        Map<ColumnRefOperator, List<String>> inValues = getColumnINConstantValues(predicate);
        Map<ColumnRefOperator, List<String>> dateRangeValues =
                getDateRangeConstantValues(operator, partitionColumns, predicate);
        if (dateRangeValues != null && !dateRangeValues.isEmpty()) {
            for (ScalarOperator equality : getColumnEQConstantPredicates(predicate)) {
                ColumnRefOperator column = (ColumnRefOperator) equality.getChild(0);
                ConstantOperator constant = (ConstantOperator) equality.getChild(1);
                mergePartitionValues(inValues, column, Lists.newArrayList(constant.toString()));
            }
            dateRangeValues.forEach((column, values) -> mergePartitionValues(inValues, column, values));
        }
        return buildEffectivePartitionValues(operator, partitionColumns, inValues);
    }

    private static void mergePartitionValues(Map<ColumnRefOperator, List<String>> valuesByColumn,
                                             ColumnRefOperator column, List<String> values) {
        valuesByColumn.merge(column, values, (left, right) -> {
            List<String> intersection = new ArrayList<>(left);
            intersection.retainAll(right);
            return intersection;
        });
    }

    private static List<Optional<List<String>>> buildEffectivePartitionValues(
            LogicalScanOperator operator, List<Column> partitionColumns,
            Map<ColumnRefOperator, List<String>> valuesByColumn) {
        if (valuesByColumn.isEmpty()) {
            return null;
        }

        List<Optional<List<String>>> result = new ArrayList<>();
        boolean hasAny = false;
        for (Column column : partitionColumns) {
            ColumnRefOperator columnRef = operator.getColumnReference(column);
            if (valuesByColumn.containsKey(columnRef)) {
                result.add(Optional.of(valuesByColumn.get(columnRef)));
                hasAny = true;
            } else {
                result.add(Optional.empty());
            }
        }
        return hasAny ? result : null;
    }

    // Convert a bounded DATE range, including a normalized BETWEEN predicate, into discrete partition values.
    // Returning null means that at least one DATE range cannot be included in the partition-value fast path.
    private static Map<ColumnRefOperator, List<String>> getDateRangeConstantValues(
            LogicalScanOperator operator, List<Column> partitionColumns, ScalarOperator predicate) {
        Map<ColumnRefOperator, DateRangeBounds> dateRanges = Maps.newHashMap();
        Set<ColumnRefOperator> datePartitionColumns = partitionColumns.stream()
                .filter(column -> column.getType().isDate())
                .map(operator::getColumnReference)
                .collect(Collectors.toSet());
        if (datePartitionColumns.isEmpty()) {
            return Maps.newHashMap();
        }

        for (ScalarOperator conjunct : Utils.extractConjuncts(predicate)) {
            if (!(conjunct instanceof BinaryPredicateOperator)) {
                continue;
            }
            BinaryPredicateOperator binaryPredicate = (BinaryPredicateOperator) conjunct;
            if (!(binaryPredicate.getChild(0) instanceof ColumnRefOperator) ||
                    !(binaryPredicate.getChild(1) instanceof ConstantOperator)) {
                continue;
            }
            ColumnRefOperator column = (ColumnRefOperator) binaryPredicate.getChild(0);
            ConstantOperator constant = (ConstantOperator) binaryPredicate.getChild(1);
            if (!datePartitionColumns.contains(column) || !constant.getType().isDate() || constant.isNull() ||
                    !binaryPredicate.getBinaryType().isRange()) {
                continue;
            }
            dateRanges.computeIfAbsent(column, ignored -> new DateRangeBounds())
                    .update(binaryPredicate.getBinaryType(), constant.getDate().toLocalDate());
        }

        Map<ColumnRefOperator, List<String>> result = Maps.newHashMap();
        for (Map.Entry<ColumnRefOperator, DateRangeBounds> entry : dateRanges.entrySet()) {
            List<String> values = entry.getValue().enumerate();
            if (values == null) {
                return null;
            }
            result.put(entry.getKey(), values);
        }
        return result;
    }

    private static final class DateRangeBounds {
        private LocalDate lowerBound;
        private LocalDate upperBound;
        private boolean lowerInclusive;
        private boolean upperInclusive;

        private void update(BinaryType binaryType, LocalDate value) {
            switch (binaryType) {
                case GE:
                    updateLowerBound(value, true);
                    break;
                case GT:
                    updateLowerBound(value, false);
                    break;
                case LE:
                    updateUpperBound(value, true);
                    break;
                case LT:
                    updateUpperBound(value, false);
                    break;
                default:
                    break;
            }
        }

        private void updateLowerBound(LocalDate value, boolean inclusive) {
            if (lowerBound == null || value.isAfter(lowerBound)) {
                lowerBound = value;
                lowerInclusive = inclusive;
            } else if (value.equals(lowerBound)) {
                lowerInclusive &= inclusive;
            }
        }

        private void updateUpperBound(LocalDate value, boolean inclusive) {
            if (upperBound == null || value.isBefore(upperBound)) {
                upperBound = value;
                upperInclusive = inclusive;
            } else if (value.equals(upperBound)) {
                upperInclusive &= inclusive;
            }
        }

        private List<String> enumerate() {
            if (lowerBound == null || upperBound == null) {
                return null;
            }

            try {
                LocalDate first = lowerInclusive ? lowerBound : lowerBound.plusDays(1);
                LocalDate last = upperInclusive ? upperBound : upperBound.minusDays(1);
                long distance = ChronoUnit.DAYS.between(first, last);
                if (distance < 0) {
                    return new ArrayList<>();
                }
                if (distance >= MAX_IN_COMBINATIONS) {
                    return null;
                }

                List<String> values = new ArrayList<>((int) distance + 1);
                for (long offset = 0; offset <= distance; offset++) {
                    values.add(first.plusDays(offset).toString());
                }
                return values;
            } catch (DateTimeException e) {
                return new ArrayList<>();
            }
        }
    }

    private static List<String> listPartitionNamesForInValues(
            Table table, List<Optional<List<String>>> inPartitionValues) {
        long combinations = 1;
        for (Optional<List<String>> values : inPartitionValues) {
            if (values.isPresent()) {
                long size = values.get().size();
                if (size == 0) {
                    return new ArrayList<>();
                }
                combinations = Math.multiplyExact(combinations, size);
                if (combinations > MAX_IN_COMBINATIONS) {
                    return null;
                }
            }
        }

        List<List<Optional<String>>> allCombinations = new ArrayList<>();
        allCombinations.add(new ArrayList<>());
        for (Optional<List<String>> values : inPartitionValues) {
            List<List<Optional<String>>> newCombinations = new ArrayList<>();
            for (List<Optional<String>> existing : allCombinations) {
                if (values.isPresent()) {
                    for (String value : values.get()) {
                        List<Optional<String>> copy = new ArrayList<>(existing);
                        copy.add(Optional.of(value));
                        newCombinations.add(copy);
                    }
                } else {
                    List<Optional<String>> copy = new ArrayList<>(existing);
                    copy.add(Optional.empty());
                    newCombinations.add(copy);
                }
            }
            allCombinations = newCombinations;
        }

        Set<String> partitionNames = Sets.newConcurrentHashSet();
        CompletableFuture<?>[] futures = allCombinations.stream()
                .map(values -> CompletableFuture.runAsync(() -> {
                    List<String> names = GlobalStateMgr.getCurrentState().getMetadataMgr()
                            .listPartitionNamesByValue(table.getCatalogName(), table.getCatalogDBName(),
                                    table.getCatalogTableName(), values);
                    partitionNames.addAll(names);
                }))
                .toArray(CompletableFuture[]::new);
        CompletableFuture.allOf(futures).join();
        return new ArrayList<>(partitionNames);
    }

    // get equivalence predicate which column ref is partition column
    public static List<Optional<ScalarOperator>> getEffectivePartitionPredicate(LogicalScanOperator operator,
                                                                                List<Column> partitionColumns,
                                                                                ScalarOperator predicate) {
        if (partitionColumns.isEmpty()) {
            return Lists.newArrayList();
        }

        List<ScalarOperator> equalPredicates = getColumnEQConstantPredicates(predicate);
        Map<ColumnRefOperator, ScalarOperator> equalPredicateMap = equalPredicates.stream().collect(
                Collectors.toMap(rangePredicate -> rangePredicate.getChild(0).cast(),
                        rangePredicate -> rangePredicate));

        List<Optional<ScalarOperator>> effectivePartitionPredicate = Lists.newArrayList();
        for (Column partitionColumn : partitionColumns) {
            ColumnRefOperator partitionColumnRefOperator = operator.getColumnReference(partitionColumn);
            // only support string type partition column
            if (partitionColumn.getType().isStringType() && equalPredicateMap.containsKey(partitionColumnRefOperator)) {
                effectivePartitionPredicate.add(Optional.of(equalPredicateMap.get(partitionColumnRefOperator)));
            } else {
                effectivePartitionPredicate.add(Optional.empty());
            }
        }
        return effectivePartitionPredicate;
    }

    private static List<Optional<String>> getPartitionValue(List<Optional<ScalarOperator>> predicates) {
        List<Optional<String>> partitionValues = Lists.newArrayList();
        for (Optional<ScalarOperator> predicate : predicates) {
            if (predicate.isPresent()) {
                Preconditions.checkState(predicate.get() instanceof BinaryPredicateOperator);
                ConstantOperator constantOperator = predicate.get().getChild(1).cast();
                partitionValues.add(Optional.of(constantOperator.getVarchar()));
            } else {
                partitionValues.add(Optional.empty());
            }
        }
        return partitionValues;
    }

    private static void initPartitionInfo(LogicalScanOperator operator, OptimizerContext context,
                                          Map<ColumnRefOperator,
                                                  ConcurrentNavigableMap<LiteralExpr, Set<Long>>> columnToPartitionValuesMap,
                                          Map<ColumnRefOperator, Set<Long>> columnToNullPartitions) throws AnalysisException {
        Table table = operator.getTable();
        // RemoteScanPartitionPruneRule may be run multiple times, such like after MaterializedViewRewriter rewrite，
        // the predicates of scan operator may changed, it need to re-compute the ScanOperatorPredicates.
        operator.getScanOperatorPredicates().clear();
        if (table.isHMSTable()) {
            List<Column> partitionColumns = table.getPartitionColumns();
            List<ColumnRefOperator> partitionColumnRefOperators = new ArrayList<>();
            for (Column column : partitionColumns) {
                ColumnRefOperator partitionColumnRefOperator = operator.getColumnReference(column);
                columnToPartitionValuesMap.put(partitionColumnRefOperator, new ConcurrentSkipListMap<>());
                columnToNullPartitions.put(partitionColumnRefOperator, Sets.newConcurrentHashSet());
                partitionColumnRefOperators.add(partitionColumnRefOperator);
            }

            if (context.getDumpInfo() != null) {
                context.getDumpInfo()
                        .getHMSTable(table.getResourceName(), table.getCatalogDBName(), table.getCatalogTableName())
                        .setPartitionNames(new ArrayList<>());
            }

            List<Pair<PartitionKey, Long>> partitionKeys = Lists.newArrayList();
            if (!table.isUnPartitioned()) {
                checkPartitionFilterRequired(operator, context, table, partitionColumns);

                // get partition names
                List<String> partitionNames = null;
                boolean partitionNamesFiltered = false;
                // Prefer partition-value APIs for equality, IN, and small bounded DATE ranges.
                List<Optional<ScalarOperator>> effectivePartitionPredicate =
                        getEffectivePartitionPredicate(operator, partitionColumns, operator.getPredicate());
                boolean hasEffectivePartitionPredicate =
                        effectivePartitionPredicate.stream().anyMatch(Optional::isPresent);
                if (hasEffectivePartitionPredicate) {
                    List<Optional<String>> partitionValues = getPartitionValue(effectivePartitionPredicate);
                    partitionNames = GlobalStateMgr.getCurrentState().getMetadataMgr()
                            .listPartitionNamesByValue(table.getCatalogName(), table.getCatalogDBName(),
                                    table.getCatalogTableName(), partitionValues);
                    partitionNamesFiltered = true;
                } else {
                    List<Optional<List<String>>> inPartitionValues =
                            getEffectiveInPartitionValues(operator, partitionColumns, operator.getPredicate());
                    if (inPartitionValues != null) {
                        partitionNames = listPartitionNamesForInValues(table, inPartitionValues);
                        partitionNamesFiltered = partitionNames != null;
                    }
                }

                // Use the HMS filter API only when the partition-value fast paths cannot handle the predicate.
                if (partitionNames == null) {
                    Optional<HivePartitionFilterConverter.Result> metastoreFilter =
                            HivePartitionFilterConverter.convert(
                                    operator, partitionColumns, operator.getPredicate());
                    if (metastoreFilter.isPresent() && metastoreFilter.get().requiresFilterApi()) {
                        Optional<List<String>> filteredPartitionNames =
                                GlobalStateMgr.getCurrentState().getMetadataMgr()
                                        .listPartitionNamesByFilter(
                                                table.getCatalogName(), table.getCatalogDBName(),
                                                table.getCatalogTableName(), metastoreFilter.get().getFilter());
                        if (filteredPartitionNames.isPresent()) {
                            partitionNames = filteredPartitionNames.get();
                            partitionNamesFiltered = true;
                            LOG.debug("Use HMS partition filter [{}] for table {}.{}.{}",
                                    metastoreFilter.get().getFilter(), table.getCatalogName(),
                                    table.getCatalogDBName(), table.getCatalogTableName());
                        }
                    }
                }

                if (partitionNames == null) {
                    partitionNames = GlobalStateMgr.getCurrentState().getMetadataMgr()
                            .listPartitionNames(table.getCatalogName(), table.getCatalogDBName(),
                                    table.getCatalogTableName(), ConnectorMetadataRequestContext.DEFAULT);
                }

                // For the query dump, capture the FULL (unfiltered) partition name list so replay can
                // reproduce the true denominator in partitions=X/Y. The list used for pruning above may
                // already be value-filtered, which would collapse the denominator to the pruned count.
                if (context.getDumpInfo() != null) {
                    List<String> allPartitionNames = partitionNamesFiltered
                            ? GlobalStateMgr.getCurrentState().getMetadataMgr().listPartitionNames(
                                    table.getCatalogName(), table.getCatalogDBName(),
                                    table.getCatalogTableName(), ConnectorMetadataRequestContext.DEFAULT)
                            : partitionNames;
                    context.getDumpInfo().getHMSTable(table.getResourceName(), table.getCatalogDBName(),
                            table.getCatalogTableName()).setPartitionNames(allPartitionNames);
                }

                List<PartitionKey> keys = new ArrayList<>();
                List<Long> ids = new ArrayList<>();
                for (String partName : partitionNames) {
                    List<String> values = toPartitionValues(partName);
                    PartitionKey partitionKey = createPartitionKey(values, partitionColumns, table);
                    keys.add(partitionKey);
                    ids.add(context.getNextUniquePartitionId());
                }
                for (int i = 0; i < keys.size(); i++) {
                    partitionKeys.add(new Pair<>(keys.get(i), ids.get(i)));
                }
            } else {
                partitionKeys.add(new Pair<>(new PartitionKey(), 0L));
            }

            partitionKeys.stream().parallel().forEach(entry -> {
                PartitionKey key = entry.first;
                long partitionId = entry.second;
                List<LiteralExpr> literals = key.getKeys();
                for (int i = 0; i < literals.size(); i++) {
                    ColumnRefOperator columnRefOperator = partitionColumnRefOperators.get(i);
                    LiteralExpr literal = literals.get(i);
                    if (ExprUtils.IS_NULL_LITERAL.apply(literal)) {
                        columnToNullPartitions.get(columnRefOperator).add(partitionId);
                        continue;
                    }

                    Set<Long> partitions = columnToPartitionValuesMap.get(columnRefOperator)
                            .computeIfAbsent(literal, k -> Sets.newConcurrentHashSet());
                    partitions.add(partitionId);
                }
            });

            for (Pair<PartitionKey, Long> entry : partitionKeys) {
                PartitionKey key = entry.first;
                long partitionId = entry.second;
                operator.getScanOperatorPredicates().getIdToPartitionKey().put(partitionId, key);
            }
        } else if (table instanceof FlussTable) {
            initFlussPartitionInfo(operator, context, columnToPartitionValuesMap, columnToNullPartitions,
                    (FlussTable) table);
        } else if (table instanceof DeltaLakeTable) {
            // Init columnToPartitionValuesMap for delta lake, it will be used in classifyConjuncts function
            // to classify partition conjuncts
            DeltaLakeTable deltaLakeTable = (DeltaLakeTable) table;
            List<Column> partitionColumns = deltaLakeTable.getPartitionColumns();
            for (Column column : partitionColumns) {
                ColumnRefOperator partitionColumnRefOperator = operator.getColumnReference(column);
                columnToPartitionValuesMap.put(partitionColumnRefOperator, new ConcurrentSkipListMap<>());
            }
            if (!deltaLakeTable.isUnPartitioned()) {
                checkPartitionFilterRequired(operator, context, table, partitionColumns);
            }
        } else if (table instanceof IcebergTable) {
            // Iceberg splits are enumerated incrementally during scan-range dispatch (not upfront here), so only
            // the partition-filter-required check (a pure predicate-shape check) can run at optimize time.
            IcebergTable icebergTable = (IcebergTable) table;
            if (icebergTable.isPartitioned()) {
                checkPartitionFilterRequired(operator, context, table, icebergTable.getPartitionColumns());
            }
        }
        LOG.debug("Table: {}, partition values map: {}, null partition map: {}", table.getName(),
                columnToPartitionValuesMap, columnToNullPartitions);
    }

    private static void classifyConjuncts(LogicalScanOperator operator,
                                          Map<ColumnRefOperator,
                                                  ConcurrentNavigableMap<LiteralExpr, Set<Long>>> columnToPartitionValuesMap)
            throws AnalysisException {
        for (ScalarOperator scalarOperator : Utils.extractConjuncts(operator.getPredicate())) {
            List<ColumnRefOperator> columnRefOperatorList = Utils.extractColumnRef(scalarOperator);
            if (!columnRefOperatorList.isEmpty() && !columnRefOperatorList.retainAll(columnToPartitionValuesMap.keySet())) {
                operator.getScanOperatorPredicates().getPartitionConjuncts().add(scalarOperator);
            } else {
                operator.getScanOperatorPredicates().getNonPartitionConjuncts().add(scalarOperator);
            }
        }
    }

    private static void initFlussPartitionInfo(LogicalScanOperator operator, OptimizerContext context,
                                               Map<ColumnRefOperator,
                                                       ConcurrentNavigableMap<LiteralExpr, Set<Long>>> columnToPartitionValuesMap,
                                               Map<ColumnRefOperator, Set<Long>> columnToNullPartitions,
                                               FlussTable flussTable) throws AnalysisException {
        List<Column> partitionColumns = flussTable.getPartitionColumns();
        List<ColumnRefOperator> partitionColumnRefOperators = new ArrayList<>();
        for (Column column : partitionColumns) {
            ColumnRefOperator partitionColumnRefOperator = operator.getColumnReference(column);
            columnToPartitionValuesMap.put(partitionColumnRefOperator, new ConcurrentSkipListMap<>());
            columnToNullPartitions.put(partitionColumnRefOperator, Sets.newConcurrentHashSet());
            partitionColumnRefOperators.add(partitionColumnRefOperator);
        }

        List<Pair<PartitionKey, Long>> partitionKeys = Lists.newArrayList();
        if (!flussTable.isUnPartitioned()) {
            List<String> partitionNames = GlobalStateMgr.getCurrentState().getMetadataMgr()
                    .listPartitionNames(flussTable.getCatalogName(), flussTable.getCatalogDBName(),
                            flussTable.getCatalogTableName(), ConnectorMetadataRequestContext.DEFAULT);
            for (String partitionName : partitionNames) {
                List<String> values = toPartitionValues(partitionName);
                PartitionKey partitionKey = createPartitionKey(values, partitionColumns, flussTable);
                partitionKeys.add(new Pair<>(partitionKey, context.getNextUniquePartitionId()));
            }
        } else {
            partitionKeys.add(new Pair<>(new PartitionKey(), 0L));
        }

        partitionKeys.stream().parallel().forEach(entry -> {
            PartitionKey key = entry.first;
            long partitionId = entry.second;
            List<LiteralExpr> literals = key.getKeys();
            for (int i = 0; i < literals.size(); i++) {
                ColumnRefOperator columnRefOperator = partitionColumnRefOperators.get(i);
                LiteralExpr literal = literals.get(i);
                if (ExprUtils.IS_NULL_LITERAL.apply(literal)) {
                    columnToNullPartitions.get(columnRefOperator).add(partitionId);
                    continue;
                }

                Set<Long> partitions = columnToPartitionValuesMap.get(columnRefOperator)
                        .computeIfAbsent(literal, k -> Sets.newConcurrentHashSet());
                partitions.add(partitionId);
            }
        });

        for (Pair<PartitionKey, Long> entry : partitionKeys) {
            operator.getScanOperatorPredicates().getIdToPartitionKey().put(entry.second, entry.first);
        }
    }

    private static void computePartitionInfo(LogicalScanOperator operator, OptimizerContext context,
                                             Map<ColumnRefOperator,
                                                     ConcurrentNavigableMap<LiteralExpr, Set<Long>>> columnToPartitionValuesMap,
                                             Map<ColumnRefOperator, Set<Long>> columnToNullPartitions) throws AnalysisException {
        Table table = operator.getTable();
        ScanOperatorPredicates scanOperatorPredicates = operator.getScanOperatorPredicates();
        if (table.isHMSTable()) {
            ListPartitionPruner partitionPruner =
                    new ListPartitionPruner(columnToPartitionValuesMap, columnToNullPartitions,
                            scanOperatorPredicates.getPartitionConjuncts(), null);
            partitionPruner.setScanOperator(operator);
            Collection<Long> selectedPartitionIds = partitionPruner.prune();
            if (selectedPartitionIds == null) {
                selectedPartitionIds = scanOperatorPredicates.getIdToPartitionKey().keySet();
            }

            int scanLakePartitionNumLimit = context.getSessionVariable().getScanLakePartitionNumLimit();
            if (scanLakePartitionNumLimit > 0 && !table.isUnPartitioned()
                    && selectedPartitionIds.size() > scanLakePartitionNumLimit) {
                String msg = "Exceeded the limit of " + scanLakePartitionNumLimit + " max scan hive external partitions";
                LOG.warn("{} queryId: {}", msg, DebugUtil.printId(context.getQueryId()));
                throw new AnalysisException(msg);
            }

            scanOperatorPredicates.setSelectedPartitionIds(selectedPartitionIds);
            scanOperatorPredicates.getNoEvalPartitionConjuncts().addAll(partitionPruner.getNoEvalConjuncts());
        } else if (table instanceof PaimonTable) {
            List<String> fieldNames = operator.getColRefToColumnMetaMap().keySet().stream()
                    .map(ColumnRefOperator::getName)
                    .collect(Collectors.toList());
            GetRemoteFilesParams params =
                    GetRemoteFilesParams.newBuilder().setPredicate(operator.getPredicate()).setFieldNames(fieldNames)
                            .setTableVersionRange(operator.getTvrVersionRange()).setLimit(operator.getLimit()).build();
            List<RemoteFileInfo> fileInfos = GlobalStateMgr.getCurrentState().getMetadataMgr().getRemoteFiles(table, params);
            if (fileInfos.isEmpty()) {
                return;
            }

            PaimonRemoteFileDesc remoteFileDesc = (PaimonRemoteFileDesc) fileInfos.get(0).getFiles().get(0);
            if (remoteFileDesc == null) {
                return;
            }
            List<Split> splits = remoteFileDesc.getPaimonSplitsInfo().getPaimonSplits();
            if (splits.isEmpty()) {
                return;
            }
            long rowCount = getRowCount(splits);
            if (rowCount > 0) {
                scanOperatorPredicates.getSelectedPartitionIds().add(1L);
            }

            //check scan partition num
            Set<BinaryRow> selectedPartitions = new HashSet<>();
            for (Split split : splits) {
                if (split instanceof DataSplit) {
                    DataSplit dataSplit = (DataSplit) split;
                    selectedPartitions.add(dataSplit.partition());
                }
            }
            int scanLakePartitionNumLimit = context.getSessionVariable().getScanLakePartitionNumLimit();
            if (scanLakePartitionNumLimit > 0 && selectedPartitions.size() > scanLakePartitionNumLimit) {
                String msg = "Exceeded the limit of number of paimon table partitions to be scanned. " +
                        "Number of partitions allowed: " + scanLakePartitionNumLimit +
                        ", number of partitions to be scanned: " + selectedPartitions.size() +
                        ". Please adjust the SQL or change the limit by set variable scan_lake_partition_num_limit.";
                LOG.warn("{} queryId: {}", msg, DebugUtil.printId(context.getQueryId()));
                throw new AnalysisException(msg);
            }
        } else if (table instanceof FlussTable) {
            ListPartitionPruner partitionPruner =
                    new ListPartitionPruner(columnToPartitionValuesMap, columnToNullPartitions,
                            scanOperatorPredicates.getPartitionConjuncts(), null);
            partitionPruner.setScanOperator(operator);
            Collection<Long> selectedPartitionIds = partitionPruner.prune();
            if (selectedPartitionIds == null) {
                selectedPartitionIds = scanOperatorPredicates.getIdToPartitionKey().keySet();
            }

            int scanLakePartitionNumLimit = context.getSessionVariable().getScanLakePartitionNumLimit();
            if (scanLakePartitionNumLimit > 0 && !table.isUnPartitioned()
                    && selectedPartitionIds.size() > scanLakePartitionNumLimit) {
                String msg = "Exceeded the limit of number of fluss table partitions to be scanned. " +
                        "Number of partitions allowed: " + scanLakePartitionNumLimit +
                        ", number of partitions to be scanned: " + selectedPartitionIds.size() +
                        ". Please adjust the SQL or change the limit by set variable scan_lake_partition_num_limit.";
                LOG.warn("{} queryId: {}", msg, DebugUtil.printId(context.getQueryId()));
                throw new AnalysisException(msg);
            }

            scanOperatorPredicates.setSelectedPartitionIds(selectedPartitionIds);
            scanOperatorPredicates.getNoEvalPartitionConjuncts().addAll(partitionPruner.getNoEvalConjuncts());
        }
    }

    /**
     * if the index name is an alias or index pattern, then the es table is related
     * with one or more indices some indices could be pruned by using partition info
     * in index settings currently only support range partition setting
     *
     * @param partitionInfo
     * @return
     * @throws AnalysisException
     */
    private static Collection<Long> partitionPrune(Table table, PartitionInfo partitionInfo,
                                                   Map<String, PartitionColumnFilter> columnFilters) throws AnalysisException {
        if (partitionInfo == null) {
            return null;
        }
        PartitionPruner partitionPruner = null;
        switch (partitionInfo.getType()) {
            case RANGE:
            case EXPR_RANGE: {
                RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) partitionInfo;
                Map<Long, Range<PartitionKey>> keyRangeById = rangePartitionInfo.getIdToRange(false);
                partitionPruner = new RangePartitionPruner(
                        keyRangeById,
                        rangePartitionInfo.getPartitionColumns(table.getIdToColumn()),
                        columnFilters);
                return partitionPruner.prune();
            }
            default: {
                return null;
            }
        }
    }

    private static void computeMinMaxConjuncts(LogicalScanOperator operator, OptimizerContext context)
            throws AnalysisException {
        ScanOperatorPredicates scanOperatorPredicates = operator.getScanOperatorPredicates();
        for (ScalarOperator scalarOperator : scanOperatorPredicates.getNonPartitionConjuncts()) {
            if (isSupportedMinMaxConjuncts(operator, scalarOperator)) {
                addMinMaxConjuncts(scalarOperator, operator);
            }
        }
    }

    /**
     * Only conjuncts of the form <column> <op> <constant> and <column> in <constant> are supported,
     * and <op> must be one of LT, LE, GE, GT, or EQ.
     */
    private static boolean isSupportedMinMaxConjuncts(LogicalScanOperator scanOperator, ScalarOperator operator) {
        if (operator instanceof BinaryPredicateOperator) {
            ScalarOperator leftChild = operator.getChild(0);
            ScalarOperator rightChild = operator.getChild(1);
            if (!(leftChild.isColumnRef()) || !(rightChild.isConstantRef())) {
                return false;
            }
            if (!scanOperator.getColRefToColumnMetaMap().containsKey((ColumnRefOperator) leftChild)) {
                return false;
            }
            return !((ConstantOperator) rightChild).isNull();
        } else if (operator instanceof InPredicateOperator) {
            if (!(operator.getChild(0).isColumnRef())) {
                return false;
            }
            if (((InPredicateOperator) operator).isNotIn()) {
                return false;
            }
            if (!scanOperator.getColRefToColumnMetaMap().containsKey((ColumnRefOperator) operator.getChild(0))) {
                return false;
            }
            return ((InPredicateOperator) operator).allValuesMatch(ScalarOperator::isConstantRef) &&
                    !((InPredicateOperator) operator).hasAnyNullValues();
        } else {
            return false;
        }
    }

    private static void addMinMaxConjuncts(ScalarOperator scalarOperator, LogicalScanOperator operator)
            throws AnalysisException {
        List<ScalarOperator> minMaxConjuncts = operator.getScanOperatorPredicates().getMinMaxConjuncts();
        if (scalarOperator instanceof BinaryPredicateOperator) {
            BinaryPredicateOperator binaryPredicateOperator = (BinaryPredicateOperator) scalarOperator;
            ScalarOperator leftChild = binaryPredicateOperator.getChild(0);
            ScalarOperator rightChild = binaryPredicateOperator.getChild(1);
            if (binaryPredicateOperator.getBinaryType().isEqual()) {
                minMaxConjuncts.add(buildMinMaxConjunct(BinaryType.LE, leftChild, rightChild, operator));
                minMaxConjuncts.add(buildMinMaxConjunct(BinaryType.GE, leftChild, rightChild, operator));
            } else if (binaryPredicateOperator.getBinaryType().isRange()) {
                minMaxConjuncts.add(
                        buildMinMaxConjunct(binaryPredicateOperator.getBinaryType(), leftChild, rightChild, operator));
            }
        } else if (scalarOperator instanceof InPredicateOperator) {
            InPredicateOperator inPredicateOperator = (InPredicateOperator) scalarOperator;
            ConstantOperator max = null;
            ConstantOperator min = null;
            for (int i = 1; i < inPredicateOperator.getChildren().size(); ++i) {
                ConstantOperator child = (ConstantOperator) inPredicateOperator.getChild(i);
                if (min == null || child.compareTo(min) < 0) {
                    min = child;
                }
                if (max == null || child.compareTo(max) > 0) {
                    max = child;
                }
            }
            Preconditions.checkState(min != null);

            BinaryPredicateOperator minBound =
                    buildMinMaxConjunct(BinaryType.GE, inPredicateOperator.getChild(0), min, operator);
            BinaryPredicateOperator maxBound =
                    buildMinMaxConjunct(BinaryType.LE, inPredicateOperator.getChild(0), max, operator);
            minMaxConjuncts.add(minBound);
            minMaxConjuncts.add(maxBound);
        }
    }

    private static BinaryPredicateOperator buildMinMaxConjunct(BinaryType type, ScalarOperator left,
                                                               ScalarOperator right, LogicalScanOperator operator)
            throws AnalysisException {
        ScanOperatorPredicates scanOperatorPredicates = operator.getScanOperatorPredicates();
        ColumnRefOperator columnRefOperator = (ColumnRefOperator) left;
        scanOperatorPredicates.getMinMaxColumnRefMap().putIfAbsent(columnRefOperator,
                operator.getColRefToColumnMetaMap().get(columnRefOperator));
        return new BinaryPredicateOperator(type, columnRefOperator, right);
    }
}
