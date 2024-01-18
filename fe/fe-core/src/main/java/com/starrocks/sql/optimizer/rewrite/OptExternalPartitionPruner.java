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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.starrocks.analysis.BinaryType;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.HiveMetaStoreTable;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.PaimonTable;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.connector.RemoteFileDesc;
import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.connector.elasticsearch.EsShardPartitions;
import com.starrocks.connector.elasticsearch.EsTablePartitions;
import com.starrocks.planner.PartitionColumnFilter;
import com.starrocks.planner.PartitionPruner;
import com.starrocks.planner.RangePartitionPruner;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.ScanOperatorPredicates;
import com.starrocks.sql.optimizer.operator.logical.LogicalEsScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.transformation.ListPartitionPruner;
import com.starrocks.sql.optimizer.rule.transformation.ListPartitionPruner.PartitionType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.paimon.table.source.Split;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;

import static com.starrocks.connector.PartitionUtil.createPartitionKey;
import static com.starrocks.connector.PartitionUtil.toPartitionValues;
import static com.starrocks.connector.paimon.PaimonMetadata.getRowCount;

public class OptExternalPartitionPruner {
    private static final Logger LOG = LogManager.getLogger(OptExternalPartitionPruner.class);

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
                partitionIds = partitionPrune(esTablePartitions.getPartitionInfo(), operator.getColumnFilters());
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
                computePartitionInfo(logicalScanOperator, columnToPartitionValuesMap, columnToNullPartitions, context);
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

    // get equivalence predicate which column ref is partition column
    public static List<Optional<ScalarOperator>> getEffectivePartitionPredicate(LogicalScanOperator operator,
            List<Column> partitionColumns, ScalarOperator predicate) {
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
            Map<ColumnRefOperator, ConcurrentNavigableMap<LiteralExpr, Set<Long>>> columnToPartitionValuesMap,
            Map<ColumnRefOperator, Set<Long>> columnToNullPartitions) throws AnalysisException {
        Table table = operator.getTable();
        // RemoteScanPartitionPruneRule may be run multiple times, such like after MaterializedViewRewriter rewrite，
        // the predicates of scan operator may changed, it need to re-compute the ScanOperatorPredicates.
        operator.getScanOperatorPredicates().clear();
        if (table instanceof HiveMetaStoreTable) {
            HiveMetaStoreTable hmsTable = (HiveMetaStoreTable) table;
            List<Column> partitionColumns = hmsTable.getPartitionColumns();
            List<ColumnRefOperator> partitionColumnRefOperators = new ArrayList<>();
            for (Column column : partitionColumns) {
                ColumnRefOperator partitionColumnRefOperator = operator.getColumnReference(column);
                columnToPartitionValuesMap.put(partitionColumnRefOperator, new ConcurrentSkipListMap<>());
                columnToNullPartitions.put(partitionColumnRefOperator, Sets.newConcurrentHashSet());
                partitionColumnRefOperators.add(partitionColumnRefOperator);
            }

            if (context.getDumpInfo() != null) {
                context.getDumpInfo()
                        .getHMSTable(hmsTable.getResourceName(), hmsTable.getDbName(), hmsTable.getTableName())
                        .setPartitionNames(new ArrayList<>());
            }

            Map<PartitionKey, Long> partitionKeys = Maps.newHashMap();
            if (!hmsTable.isUnPartitioned()) {
                // get partition names
                List<String> partitionNames;
                // check if the partition predicate could be used for filter partition names
                List<Optional<ScalarOperator>> effectivePartitionPredicate =
                        getEffectivePartitionPredicate(operator, partitionColumns, operator.getPredicate());
                if (effectivePartitionPredicate.stream().anyMatch(Optional::isPresent)) {
                    List<Optional<String>> partitionValues = getPartitionValue(effectivePartitionPredicate);
                    partitionNames = GlobalStateMgr.getCurrentState().getMetadataMgr()
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

            for (Map.Entry<PartitionKey, Long> entry : partitionKeys.entrySet()) {
                PartitionKey key = entry.getKey();
                long partitionId = entry.getValue();
                operator.getScanOperatorPredicates().getIdToPartitionKey().put(partitionId, key);
            }
        }
        LOG.debug("Table: {}, partition values map: {}, null partition map: {}", table.getName(),
                columnToPartitionValuesMap, columnToNullPartitions);
    }

    private static void classifyConjuncts(LogicalScanOperator operator,
            Map<ColumnRefOperator, ConcurrentNavigableMap<LiteralExpr, Set<Long>>> columnToPartitionValuesMap)
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

    private static void computePartitionInfo(LogicalScanOperator operator,
            Map<ColumnRefOperator, ConcurrentNavigableMap<LiteralExpr, Set<Long>>> columnToPartitionValuesMap,
            Map<ColumnRefOperator, Set<Long>> columnToNullPartitions, OptimizerContext context) throws AnalysisException {
        Table table = operator.getTable();
        ScanOperatorPredicates scanOperatorPredicates = operator.getScanOperatorPredicates();
        if (table instanceof HiveMetaStoreTable) {
            ListPartitionPruner partitionPruner =
                    new ListPartitionPruner(columnToPartitionValuesMap, columnToNullPartitions,
                            scanOperatorPredicates.getPartitionConjuncts(), null,
                                    Optional.of(PartitionType.HIVE), context.getSessionVariable().isEnableExprPrunePartition());
            Collection<Long> selectedPartitionIds = partitionPruner.prune();
            if (selectedPartitionIds == null) {
                selectedPartitionIds = scanOperatorPredicates.getIdToPartitionKey().keySet();
            }
            scanOperatorPredicates.setSelectedPartitionIds(selectedPartitionIds);
            scanOperatorPredicates.getNoEvalPartitionConjuncts().addAll(partitionPruner.getNoEvalConjuncts());
        } else if (table instanceof IcebergTable) {
            IcebergTable icebergTable = (IcebergTable) table;
            if (!icebergTable.getSnapshot().isPresent()) {
                return;
            }

            ImmutableMap.Builder<Long, PartitionKey> idToPartitionKey = ImmutableMap.builder();
            if (table.isUnPartitioned()) {
                idToPartitionKey.put(0L, new PartitionKey());
            } else {
                String catalogName = icebergTable.getCatalogName();
                List<PartitionKey> partitionKeys = GlobalStateMgr.getCurrentState().getMetadataMgr()
                        .getPrunedPartitions(catalogName, icebergTable, operator.getPredicate(), operator.getLimit());
                List<Long> ids = table.allocatePartitionIdByKey(partitionKeys);
                for (int i = 0; i < partitionKeys.size(); i++) {
                    idToPartitionKey.put(ids.get(i), partitionKeys.get(i));
                }
            }

            Map<Long, PartitionKey> partitionKeyMap = idToPartitionKey.build();
            scanOperatorPredicates.getIdToPartitionKey().putAll(partitionKeyMap);
            scanOperatorPredicates.setSelectedPartitionIds(partitionKeyMap.keySet());
        } else if (table instanceof PaimonTable) {
            PaimonTable paimonTable = (PaimonTable) table;
            List<String> fieldNames = operator.getColRefToColumnMetaMap().keySet().stream()
                    .map(ColumnRefOperator::getName)
                    .collect(Collectors.toList());
            List<RemoteFileInfo> fileInfos = GlobalStateMgr.getCurrentState().getMetadataMgr().getRemoteFileInfos(
                    paimonTable.getCatalogName(), table, null, -1, operator.getPredicate(), fieldNames, -1);
            if (fileInfos.isEmpty()) {
                return;
            }

            RemoteFileDesc remoteFileDesc = fileInfos.get(0).getFiles().get(0);
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
    private static Collection<Long> partitionPrune(PartitionInfo partitionInfo,
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
                partitionPruner =
                        new RangePartitionPruner(keyRangeById, rangePartitionInfo.getPartitionColumns(), columnFilters);
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
            ScalarOperator right, LogicalScanOperator operator) throws AnalysisException {
        ScanOperatorPredicates scanOperatorPredicates = operator.getScanOperatorPredicates();
        ColumnRefOperator columnRefOperator = (ColumnRefOperator) left;
        scanOperatorPredicates.getMinMaxColumnRefMap().putIfAbsent(columnRefOperator,
                operator.getColRefToColumnMetaMap().get(columnRefOperator));
        return new BinaryPredicateOperator(type, columnRefOperator, right);
    }
}
