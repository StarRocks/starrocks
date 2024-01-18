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
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.HiveMetaStoreTable;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.external.elasticsearch.EsShardPartitions;
import com.starrocks.external.elasticsearch.EsTablePartitions;
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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static com.starrocks.connector.PartitionUtil.createPartitionKey;
import static com.starrocks.connector.PartitionUtil.toPartitionValues;

public class OptExternalPartitionPruner {
    private static final Logger LOG = LogManager.getLogger(OptExternalPartitionPruner.class);

    public static LogicalScanOperator prunePartitions(OptimizerContext context, LogicalScanOperator logicalScanOperator) {
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
                LOG.debug("partition prune finished, unpartitioned index [{}], "
                                + "partitioned index [{}]",
                        String.join(",", unPartitionedIndices),
                        String.join(",", partitionedIndices));
            }
        } else {
            // partitionColumnName -> (LiteralExpr -> partition ids)
            // no null partitions in this map, used by ListPartitionPruner
            Map<ColumnRefOperator, TreeMap<LiteralExpr, Set<Long>>> columnToPartitionValuesMap = Maps.newHashMap();
            // Store partitions with null partition values separately, used by ListPartitionPruner
            // partitionColumnName -> null partitionIds
            Map<ColumnRefOperator, Set<Long>> columnToNullPartitions = Maps.newHashMap();

            try {
                initPartitionInfo(logicalScanOperator, context, columnToPartitionValuesMap, columnToNullPartitions);
                classifyConjuncts(logicalScanOperator, columnToPartitionValuesMap);
                computePartitionInfo(logicalScanOperator, columnToPartitionValuesMap, columnToNullPartitions);
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

    private static void initPartitionInfo(LogicalScanOperator operator, OptimizerContext context,
                                          Map<ColumnRefOperator, TreeMap<LiteralExpr, Set<Long>>> columnToPartitionValuesMap,
                                          Map<ColumnRefOperator, Set<Long>> columnToNullPartitions)
            throws AnalysisException {
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
                columnToPartitionValuesMap.put(partitionColumnRefOperator, new TreeMap<>());
                columnToNullPartitions.put(partitionColumnRefOperator, Sets.newHashSet());
                partitionColumnRefOperators.add(partitionColumnRefOperator);
            }

            List<String> partitionNames = GlobalStateMgr.getCurrentState().getMetadataMgr().listPartitionNames(
                    hmsTable.getCatalogName(), hmsTable.getDbName(), hmsTable.getTableName());
<<<<<<< HEAD
            context.getDumpInfo().getHMSTable(hmsTable.getResourceName(), hmsTable.getDbName(),
                    hmsTable.getTableName()).setPartitionNames(new ArrayList<>());
=======
            if (context.getDumpInfo() != null) {
                context.getDumpInfo().getHMSTable(hmsTable.getResourceName(), hmsTable.getDbName(),
                        hmsTable.getTableName()).setPartitionNames(new ArrayList<>());
            }
>>>>>>> 2.5.18

            Map<PartitionKey, Long> partitionKeys = Maps.newHashMap();
            if (!hmsTable.isUnPartitioned()) {
                long partitionId = 0;
                for (String partName : partitionNames) {
                    List<String> values = toPartitionValues(partName);
                    PartitionKey partitionKey = createPartitionKey(values, partitionColumns, table.getType());
                    partitionKeys.put(partitionKey, partitionId++);
                }
            } else {
                partitionKeys.put(new PartitionKey(), 0L);
            }

            for (Map.Entry<PartitionKey, Long> entry : partitionKeys.entrySet()) {
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
                            .computeIfAbsent(literal, k -> Sets.newHashSet());
                    partitions.add(partitionId);
                }
                operator.getScanOperatorPredicates().getIdToPartitionKey().put(partitionId, key);
            }
        }
        LOG.debug("Table: {}, partition values map: {}, null partition map: {}",
                table.getName(), columnToPartitionValuesMap, columnToNullPartitions);
    }

    private static void classifyConjuncts(LogicalScanOperator operator,
                                          Map<ColumnRefOperator, TreeMap<LiteralExpr, Set<Long>>> columnToPartitionValuesMap)
            throws AnalysisException {
        for (ScalarOperator scalarOperator : Utils.extractConjuncts(operator.getPredicate())) {
            List<ColumnRefOperator> columnRefOperatorList = Utils.extractColumnRef(scalarOperator);
            if (!columnRefOperatorList.retainAll(columnToPartitionValuesMap.keySet())) {
                operator.getScanOperatorPredicates().getPartitionConjuncts().add(scalarOperator);
            } else {
                operator.getScanOperatorPredicates().getNonPartitionConjuncts().add(scalarOperator);
            }
        }
    }

    private static void computePartitionInfo(LogicalScanOperator operator,
                                             Map<ColumnRefOperator, TreeMap<LiteralExpr, Set<Long>>> columnToPartitionValuesMap,
                                             Map<ColumnRefOperator, Set<Long>> columnToNullPartitions) throws AnalysisException {
        Table table = operator.getTable();
        if (table instanceof HiveMetaStoreTable) {
            ScanOperatorPredicates scanOperatorPredicates = operator.getScanOperatorPredicates();
            ListPartitionPruner partitionPruner =
                    new ListPartitionPruner(columnToPartitionValuesMap, columnToNullPartitions,
                            scanOperatorPredicates.getPartitionConjuncts());
            Collection<Long> selectedPartitionIds = partitionPruner.prune();
            if (selectedPartitionIds == null) {
                selectedPartitionIds = scanOperatorPredicates.getIdToPartitionKey().keySet();
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
    private static Collection<Long> partitionPrune(PartitionInfo partitionInfo,
                                                   Map<String, PartitionColumnFilter> columnFilters) throws AnalysisException {
        if (partitionInfo == null) {
            return null;
        }
        PartitionPruner partitionPruner = null;
        switch (partitionInfo.getType()) {
            case RANGE: {
                RangePartitionInfo rangePartitionInfo = (RangePartitionInfo) partitionInfo;
                Map<Long, Range<PartitionKey>> keyRangeById = rangePartitionInfo.getIdToRange(false);
                partitionPruner = new RangePartitionPruner(keyRangeById, rangePartitionInfo.getPartitionColumns(),
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
            if (isSupportedMinMaxConjuncts(scalarOperator)) {
                addMinMaxConjuncts(scalarOperator, operator, context);
            }
        }
    }

    /**
     * Only conjuncts of the form <column> <op> <constant> and <column> in <constant> are supported,
     * and <op> must be one of LT, LE, GE, GT, or EQ.
     */
    private static boolean isSupportedMinMaxConjuncts(ScalarOperator operator) {
        if (operator instanceof BinaryPredicateOperator) {
            ScalarOperator leftChild = operator.getChild(0);
            ScalarOperator rightChild = operator.getChild(1);
            if (!(leftChild.isColumnRef()) || !(rightChild.isConstantRef())) {
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
            return ((InPredicateOperator) operator).allValuesMatch(ScalarOperator::isConstantRef)
                    && !((InPredicateOperator) operator).hasAnyNullValues();
        } else {
            return false;
        }
    }

    private static void addMinMaxConjuncts(ScalarOperator scalarOperator, LogicalScanOperator operator,
                                           OptimizerContext context) throws AnalysisException {
        List<ScalarOperator> minMaxConjuncts = operator.getScanOperatorPredicates().getMinMaxConjuncts();
        if (scalarOperator instanceof BinaryPredicateOperator) {
            BinaryPredicateOperator binaryPredicateOperator = (BinaryPredicateOperator) scalarOperator;
            ScalarOperator leftChild = binaryPredicateOperator.getChild(0);
            ScalarOperator rightChild = binaryPredicateOperator.getChild(1);
            if (binaryPredicateOperator.getBinaryType().isEqual()) {
                minMaxConjuncts.add(buildMinMaxConjunct(BinaryPredicateOperator.BinaryType.LE,
                        leftChild, rightChild, operator, context));
                minMaxConjuncts.add(buildMinMaxConjunct(BinaryPredicateOperator.BinaryType.GE,
                        leftChild, rightChild, operator, context));
            } else if (binaryPredicateOperator.getBinaryType().isRange()) {
                minMaxConjuncts.add(buildMinMaxConjunct(binaryPredicateOperator.getBinaryType(),
                        leftChild, rightChild, operator, context));
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

            BinaryPredicateOperator minBound = buildMinMaxConjunct(BinaryPredicateOperator.BinaryType.GE,
                    inPredicateOperator.getChild(0), min, operator, context);
            BinaryPredicateOperator maxBound = buildMinMaxConjunct(BinaryPredicateOperator.BinaryType.LE,
                    inPredicateOperator.getChild(0), max, operator, context);
            minMaxConjuncts.add(minBound);
            minMaxConjuncts.add(maxBound);
        }
    }

    private static BinaryPredicateOperator buildMinMaxConjunct(BinaryPredicateOperator.BinaryType type,
                                                               ScalarOperator left, ScalarOperator right,
                                                               LogicalScanOperator operator,
                                                               OptimizerContext context) throws AnalysisException {
        ScanOperatorPredicates scanOperatorPredicates = operator.getScanOperatorPredicates();
        ColumnRefOperator newColumnRef = context.getColumnRefFactory().create(left, left.getType(), left.isNullable());
        scanOperatorPredicates.getMinMaxColumnRefMap().put(newColumnRef, operator.getColRefToColumnMetaMap().get(left));
        return new BinaryPredicateOperator(type, newColumnRef, right);
    }
}
