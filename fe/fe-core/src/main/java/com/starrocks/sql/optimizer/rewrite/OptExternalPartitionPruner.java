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
import com.starrocks.analysis.BinaryType;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DeltaLakeTable;
<<<<<<< HEAD
import com.starrocks.catalog.HiveMetaStoreTable;
=======
import com.starrocks.catalog.PaimonTable;
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
<<<<<<< HEAD
import com.starrocks.connector.elasticsearch.EsShardPartitions;
import com.starrocks.connector.elasticsearch.EsTablePartitions;
=======
import com.starrocks.common.Pair;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.connector.ConnectorMetadatRequestContext;
import com.starrocks.connector.GetRemoteFilesParams;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.connector.elasticsearch.EsShardPartitions;
import com.starrocks.connector.elasticsearch.EsTablePartitions;
import com.starrocks.connector.paimon.PaimonRemoteFileDesc;
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
import com.starrocks.planner.PartitionColumnFilter;
import com.starrocks.planner.PartitionPruner;
import com.starrocks.planner.RangePartitionPruner;
import com.starrocks.server.GlobalStateMgr;
<<<<<<< HEAD
import com.starrocks.sql.PlannerProfile;
=======
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.ScanOperatorPredicates;
import com.starrocks.sql.optimizer.operator.logical.LogicalEsScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
<<<<<<< HEAD
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
=======
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.LikePredicateOperator;
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.transformation.ListPartitionPruner;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
<<<<<<< HEAD

import java.util.ArrayList;
import java.util.Collection;
=======
import org.apache.paimon.table.source.Split;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;

import static com.starrocks.connector.PartitionUtil.createPartitionKey;
import static com.starrocks.connector.PartitionUtil.toPartitionValues;
<<<<<<< HEAD
=======
import static com.starrocks.connector.paimon.PaimonMetadata.getRowCount;
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))

public class OptExternalPartitionPruner {
    private static final Logger LOG = LogManager.getLogger(OptExternalPartitionPruner.class);

    public static LogicalScanOperator prunePartitions(OptimizerContext context,
<<<<<<< HEAD
            LogicalScanOperator logicalScanOperator) {
        try (PlannerProfile.ScopedTimer ignore = PlannerProfile.getScopedTimer(
                "RuleBaseOptimize.RewriteTreeTask.ExternalTablePartitionPrune")) {
            return prunePartitionsImpl(context, logicalScanOperator);
        }
    }

    public static LogicalScanOperator prunePartitionsImpl(OptimizerContext context,
            LogicalScanOperator logicalScanOperator) {
=======
                                                      LogicalScanOperator logicalScanOperator) {
        return prunePartitionsImpl(context, logicalScanOperator);
    }

    public static LogicalScanOperator prunePartitionsImpl(OptimizerContext context,
                                                          LogicalScanOperator logicalScanOperator) {
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        if (logicalScanOperator instanceof LogicalEsScanOperator) {
            LogicalEsScanOperator operator = (LogicalEsScanOperator) logicalScanOperator;
            EsTablePartitions esTablePartitions = operator.getEsTablePartitions();

            Collection<Long> partitionIds = null;
            try {
<<<<<<< HEAD
                partitionIds = partitionPrune(esTablePartitions.getPartitionInfo(), operator.getColumnFilters());
=======
                partitionIds = partitionPrune(operator.getTable(),
                        esTablePartitions.getPartitionInfo(), operator.getColumnFilters());
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
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
<<<<<<< HEAD
                computePartitionInfo(logicalScanOperator, columnToPartitionValuesMap, columnToNullPartitions);
=======
                computePartitionInfo(logicalScanOperator, context, columnToPartitionValuesMap, columnToNullPartitions);
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
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

<<<<<<< HEAD
    // get equivalence predicate which column ref is partition column
    public static List<Optional<ScalarOperator>> getEffectivePartitionPredicate(LogicalScanOperator operator,
            List<Column> partitionColumns, ScalarOperator predicate) {
=======
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

    // get equivalence predicate which column ref is partition column
    public static List<Optional<ScalarOperator>> getEffectivePartitionPredicate(LogicalScanOperator operator,
                                                                                List<Column> partitionColumns,
                                                                                ScalarOperator predicate) {
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
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
<<<<<<< HEAD
            Map<ColumnRefOperator, ConcurrentNavigableMap<LiteralExpr, Set<Long>>> columnToPartitionValuesMap,
            Map<ColumnRefOperator, Set<Long>> columnToNullPartitions) throws AnalysisException {
=======
                                          Map<ColumnRefOperator,
                                                  ConcurrentNavigableMap<LiteralExpr, Set<Long>>> columnToPartitionValuesMap,
                                          Map<ColumnRefOperator, Set<Long>> columnToNullPartitions) throws AnalysisException {
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        Table table = operator.getTable();
        // RemoteScanPartitionPruneRule may be run multiple times, such like after MaterializedViewRewriter rewrite，
        // the predicates of scan operator may changed, it need to re-compute the ScanOperatorPredicates.
        operator.getScanOperatorPredicates().clear();
<<<<<<< HEAD
        if (table instanceof HiveMetaStoreTable) {
            HiveMetaStoreTable hmsTable = (HiveMetaStoreTable) table;
            List<Column> partitionColumns = hmsTable.getPartitionColumns();
=======
        if (table.isHMSTable()) {
            List<Column> partitionColumns = table.getPartitionColumns();
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
            List<ColumnRefOperator> partitionColumnRefOperators = new ArrayList<>();
            for (Column column : partitionColumns) {
                ColumnRefOperator partitionColumnRefOperator = operator.getColumnReference(column);
                columnToPartitionValuesMap.put(partitionColumnRefOperator, new ConcurrentSkipListMap<>());
                columnToNullPartitions.put(partitionColumnRefOperator, Sets.newConcurrentHashSet());
                partitionColumnRefOperators.add(partitionColumnRefOperator);
            }

            if (context.getDumpInfo() != null) {
                context.getDumpInfo()
<<<<<<< HEAD
                        .getHMSTable(hmsTable.getResourceName(), hmsTable.getDbName(), hmsTable.getTableName())
                        .setPartitionNames(new ArrayList<>());
            }

            Map<PartitionKey, Long> partitionKeys = Maps.newHashMap();
            if (!hmsTable.isUnPartitioned()) {
=======
                        .getHMSTable(table.getResourceName(), table.getCatalogDBName(), table.getCatalogTableName())
                        .setPartitionNames(new ArrayList<>());
            }

            List<Pair<PartitionKey, Long>> partitionKeys = Lists.newArrayList();
            if (!table.isUnPartitioned()) {
                if (!context.getSessionVariable().isAllowHiveWithoutPartitionFilter()
                        && !checkPartitionPredicates(operator, partitionColumns)) {
                    LOG.warn("Partition pruning is invalid. queryId: {}", DebugUtil.printId(context.getQueryId()));
                    throw new AnalysisException("Partition pruning is invalid, please check: "
                            + "1. The partition predicate must be included. "
                            + "2. The left and right children of the partition predicate cannot be function parameters. "
                            + "Table: " + table.getCatalogName() + "." + table.getCatalogDBName()
                            + "." + table.getCatalogTableName() + " " + "Partition columns: "
                            + partitionColumns.stream().map(Column::getName).collect(Collectors.joining(", ")));
                }

>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
                // get partition names
                List<String> partitionNames;
                // check if the partition predicate could be used for filter partition names
                List<Optional<ScalarOperator>> effectivePartitionPredicate =
                        getEffectivePartitionPredicate(operator, partitionColumns, operator.getPredicate());
                if (effectivePartitionPredicate.stream().anyMatch(Optional::isPresent)) {
                    List<Optional<String>> partitionValues = getPartitionValue(effectivePartitionPredicate);
                    partitionNames = GlobalStateMgr.getCurrentState().getMetadataMgr()
<<<<<<< HEAD
                            .listPartitionNamesByValue(hmsTable.getCatalogName(), hmsTable.getDbName(),
                                    hmsTable.getTableName(), partitionValues);
                } else {
                    partitionNames = GlobalStateMgr.getCurrentState().getMetadataMgr()
                            .listPartitionNames(hmsTable.getCatalogName(), hmsTable.getDbName(),
                                    hmsTable.getTableName());
                }

                List<PartitionKey> keys = new ArrayList<>();
                for (String partName : partitionNames) {
                    List<String> values = toPartitionValues(partName);
                    PartitionKey partitionKey = createPartitionKey(values, partitionColumns, table.getType());
                    keys.add(partitionKey);
                }
                List<Long> ids = table.allocatePartitionIdByKey(keys);
                for (int i = 0; i < keys.size(); i++) {
                    partitionKeys.put(keys.get(i), ids.get(i));
                }
            } else {
                partitionKeys.put(new PartitionKey(), 0L);
            }

            partitionKeys.entrySet().stream().parallel().forEach(entry -> {
                PartitionKey key = entry.getKey();
                long partitionId = entry.getValue();
=======
                            .listPartitionNamesByValue(table.getCatalogName(), table.getCatalogDBName(),
                                    table.getCatalogTableName(), partitionValues);
                } else {
                    partitionNames = GlobalStateMgr.getCurrentState().getMetadataMgr()
                            .listPartitionNames(table.getCatalogName(), table.getCatalogDBName(),
                                    table.getCatalogTableName(), ConnectorMetadatRequestContext.DEFAULT);
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
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
                List<LiteralExpr> literals = key.getKeys();
                for (int i = 0; i < literals.size(); i++) {
                    ColumnRefOperator columnRefOperator = partitionColumnRefOperators.get(i);
                    LiteralExpr literal = literals.get(i);
                    if (Expr.IS_NULL_LITERAL.apply(literal)) {
                        columnToNullPartitions.get(columnRefOperator).add(partitionId);
                        continue;
                    }

                    Set<Long> partitions = columnToPartitionValuesMap.get(columnRefOperator)
                            .computeIfAbsent(literal, k -> Sets.newConcurrentHashSet());
                    partitions.add(partitionId);
                }
            });

<<<<<<< HEAD
            for (Map.Entry<PartitionKey, Long> entry : partitionKeys.entrySet()) {
                PartitionKey key = entry.getKey();
                long partitionId = entry.getValue();
=======
            for (Pair<PartitionKey, Long> entry : partitionKeys) {
                PartitionKey key = entry.first;
                long partitionId = entry.second;
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
                operator.getScanOperatorPredicates().getIdToPartitionKey().put(partitionId, key);
            }
        } else if (table instanceof DeltaLakeTable) {
            // Init columnToPartitionValuesMap for delta lake, it will be used in classifyConjuncts function
            // to classify partition conjuncts
            DeltaLakeTable deltaLakeTable = (DeltaLakeTable) table;
            List<Column> partitionColumns = deltaLakeTable.getPartitionColumns();
            for (Column column : partitionColumns) {
                ColumnRefOperator partitionColumnRefOperator = operator.getColumnReference(column);
                columnToPartitionValuesMap.put(partitionColumnRefOperator, new ConcurrentSkipListMap<>());
            }
        }
        LOG.debug("Table: {}, partition values map: {}, null partition map: {}", table.getName(),
                columnToPartitionValuesMap, columnToNullPartitions);
    }

    private static void classifyConjuncts(LogicalScanOperator operator,
<<<<<<< HEAD
            Map<ColumnRefOperator, ConcurrentNavigableMap<LiteralExpr, Set<Long>>> columnToPartitionValuesMap)
=======
                                          Map<ColumnRefOperator,
                                                  ConcurrentNavigableMap<LiteralExpr, Set<Long>>> columnToPartitionValuesMap)
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
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

<<<<<<< HEAD
    private static void computePartitionInfo(LogicalScanOperator operator,
            Map<ColumnRefOperator, ConcurrentNavigableMap<LiteralExpr, Set<Long>>> columnToPartitionValuesMap,
            Map<ColumnRefOperator, Set<Long>> columnToNullPartitions) throws AnalysisException {
        Table table = operator.getTable();
        if (table instanceof HiveMetaStoreTable) {
            ScanOperatorPredicates scanOperatorPredicates = operator.getScanOperatorPredicates();
=======
    private static void computePartitionInfo(LogicalScanOperator operator, OptimizerContext context,
                                             Map<ColumnRefOperator,
                                                     ConcurrentNavigableMap<LiteralExpr, Set<Long>>> columnToPartitionValuesMap,
                                             Map<ColumnRefOperator, Set<Long>> columnToNullPartitions) throws AnalysisException {
        Table table = operator.getTable();
        ScanOperatorPredicates scanOperatorPredicates = operator.getScanOperatorPredicates();
        if (table.isHMSTable()) {
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
            ListPartitionPruner partitionPruner =
                    new ListPartitionPruner(columnToPartitionValuesMap, columnToNullPartitions,
                            scanOperatorPredicates.getPartitionConjuncts(), null);
            Collection<Long> selectedPartitionIds = partitionPruner.prune();
            if (selectedPartitionIds == null) {
                selectedPartitionIds = scanOperatorPredicates.getIdToPartitionKey().keySet();
            }
<<<<<<< HEAD
            scanOperatorPredicates.setSelectedPartitionIds(selectedPartitionIds);
            scanOperatorPredicates.getNoEvalPartitionConjuncts().addAll(partitionPruner.getNoEvalConjuncts());
=======

            int scanHivePartitionNumLimit = context.getSessionVariable().getScanHivePartitionNumLimit();
            if (scanHivePartitionNumLimit > 0 && !table.isUnPartitioned()
                    && selectedPartitionIds.size() > scanHivePartitionNumLimit) {
                String msg = "Exceeded the limit of " + scanHivePartitionNumLimit + " max scan hive external partitions";
                LOG.warn("{} queryId: {}", msg, DebugUtil.printId(context.getQueryId()));
                throw new AnalysisException(msg);
            }

            scanOperatorPredicates.setSelectedPartitionIds(selectedPartitionIds);
            scanOperatorPredicates.getNoEvalPartitionConjuncts().addAll(partitionPruner.getNoEvalConjuncts());
        } else if (table instanceof PaimonTable) {
            PaimonTable paimonTable = (PaimonTable) table;
            List<String> fieldNames = operator.getColRefToColumnMetaMap().keySet().stream()
                    .map(ColumnRefOperator::getName)
                    .collect(Collectors.toList());
            GetRemoteFilesParams params =
                    GetRemoteFilesParams.newBuilder().setPredicate(operator.getPredicate()).setFieldNames(fieldNames).build();
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
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
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
<<<<<<< HEAD
    private static Collection<Long> partitionPrune(PartitionInfo partitionInfo,
            Map<String, PartitionColumnFilter> columnFilters) throws AnalysisException {
=======
    private static Collection<Long> partitionPrune(Table table, PartitionInfo partitionInfo,
                                                   Map<String, PartitionColumnFilter> columnFilters) throws AnalysisException {
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        if (partitionInfo == null) {
            return null;
        }
        PartitionPruner partitionPruner = null;
        switch (partitionInfo.getType()) {
            case RANGE:
            case EXPR_RANGE: {
                RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) partitionInfo;
                Map<Long, Range<PartitionKey>> keyRangeById = rangePartitionInfo.getIdToRange(false);
<<<<<<< HEAD
                partitionPruner =
                        new RangePartitionPruner(keyRangeById, rangePartitionInfo.getPartitionColumns(), columnFilters);
=======
                partitionPruner = new RangePartitionPruner(
                        keyRangeById,
                        rangePartitionInfo.getPartitionColumns(table.getIdToColumn()),
                        columnFilters);
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
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
<<<<<<< HEAD
            if (isSupportedMinMaxConjuncts(scalarOperator)) {
                addMinMaxConjuncts(scalarOperator, operator, context);
=======
            if (isSupportedMinMaxConjuncts(operator, scalarOperator)) {
                addMinMaxConjuncts(scalarOperator, operator);
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
            }
        }
    }

    /**
     * Only conjuncts of the form <column> <op> <constant> and <column> in <constant> are supported,
     * and <op> must be one of LT, LE, GE, GT, or EQ.
     */
<<<<<<< HEAD
    private static boolean isSupportedMinMaxConjuncts(ScalarOperator operator) {
=======
    private static boolean isSupportedMinMaxConjuncts(LogicalScanOperator scanOperator, ScalarOperator operator) {
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        if (operator instanceof BinaryPredicateOperator) {
            ScalarOperator leftChild = operator.getChild(0);
            ScalarOperator rightChild = operator.getChild(1);
            if (!(leftChild.isColumnRef()) || !(rightChild.isConstantRef())) {
                return false;
            }
<<<<<<< HEAD
=======
            if (!scanOperator.getColRefToColumnMetaMap().containsKey((ColumnRefOperator) leftChild)) {
                return false;
            }
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
            return !((ConstantOperator) rightChild).isNull();
        } else if (operator instanceof InPredicateOperator) {
            if (!(operator.getChild(0).isColumnRef())) {
                return false;
            }
            if (((InPredicateOperator) operator).isNotIn()) {
                return false;
            }
<<<<<<< HEAD
=======
            if (!scanOperator.getColRefToColumnMetaMap().containsKey((ColumnRefOperator) operator.getChild(0))) {
                return false;
            }
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
            return ((InPredicateOperator) operator).allValuesMatch(ScalarOperator::isConstantRef) &&
                    !((InPredicateOperator) operator).hasAnyNullValues();
        } else {
            return false;
        }
    }

<<<<<<< HEAD
    private static void addMinMaxConjuncts(ScalarOperator scalarOperator, LogicalScanOperator operator,
            OptimizerContext context) throws AnalysisException {
=======
    private static void addMinMaxConjuncts(ScalarOperator scalarOperator, LogicalScanOperator operator)
            throws AnalysisException {
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        List<ScalarOperator> minMaxConjuncts = operator.getScanOperatorPredicates().getMinMaxConjuncts();
        if (scalarOperator instanceof BinaryPredicateOperator) {
            BinaryPredicateOperator binaryPredicateOperator = (BinaryPredicateOperator) scalarOperator;
            ScalarOperator leftChild = binaryPredicateOperator.getChild(0);
            ScalarOperator rightChild = binaryPredicateOperator.getChild(1);
            if (binaryPredicateOperator.getBinaryType().isEqual()) {
<<<<<<< HEAD
                minMaxConjuncts.add(buildMinMaxConjunct(BinaryType.LE, leftChild, rightChild, operator, context));
                minMaxConjuncts.add(buildMinMaxConjunct(BinaryType.GE, leftChild, rightChild, operator, context));
            } else if (binaryPredicateOperator.getBinaryType().isRange()) {
                minMaxConjuncts.add(
                        buildMinMaxConjunct(binaryPredicateOperator.getBinaryType(), leftChild, rightChild, operator,
                                context));
=======
                minMaxConjuncts.add(buildMinMaxConjunct(BinaryType.LE, leftChild, rightChild, operator));
                minMaxConjuncts.add(buildMinMaxConjunct(BinaryType.GE, leftChild, rightChild, operator));
            } else if (binaryPredicateOperator.getBinaryType().isRange()) {
                minMaxConjuncts.add(
                        buildMinMaxConjunct(binaryPredicateOperator.getBinaryType(), leftChild, rightChild, operator));
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
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
<<<<<<< HEAD
                    buildMinMaxConjunct(BinaryType.GE, inPredicateOperator.getChild(0), min, operator, context);
            BinaryPredicateOperator maxBound =
                    buildMinMaxConjunct(BinaryType.LE, inPredicateOperator.getChild(0), max, operator, context);
=======
                    buildMinMaxConjunct(BinaryType.GE, inPredicateOperator.getChild(0), min, operator);
            BinaryPredicateOperator maxBound =
                    buildMinMaxConjunct(BinaryType.LE, inPredicateOperator.getChild(0), max, operator);
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
            minMaxConjuncts.add(minBound);
            minMaxConjuncts.add(maxBound);
        }
    }

    private static BinaryPredicateOperator buildMinMaxConjunct(BinaryType type, ScalarOperator left,
<<<<<<< HEAD
            ScalarOperator right, LogicalScanOperator operator, OptimizerContext context) throws AnalysisException {
        ScanOperatorPredicates scanOperatorPredicates = operator.getScanOperatorPredicates();
        ColumnRefOperator newColumnRef = context.getColumnRefFactory().create(left, left.getType(), left.isNullable());
        scanOperatorPredicates.getMinMaxColumnRefMap().put(newColumnRef, operator.getColRefToColumnMetaMap().get(left));
        return new BinaryPredicateOperator(type, newColumnRef, right);
=======
                                                               ScalarOperator right, LogicalScanOperator operator)
            throws AnalysisException {
        ScanOperatorPredicates scanOperatorPredicates = operator.getScanOperatorPredicates();
        ColumnRefOperator columnRefOperator = (ColumnRefOperator) left;
        scanOperatorPredicates.getMinMaxColumnRefMap().putIfAbsent(columnRefOperator,
                operator.getColRefToColumnMetaMap().get(columnRefOperator));
        return new BinaryPredicateOperator(type, columnRefOperator, right);
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    }
}
