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

package com.starrocks.sql.optimizer.rule.transformation.materialization.compensation;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvUpdateInfo;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableProperty;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Pair;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.common.PCellSortedSet;
import com.starrocks.sql.common.PCellUtils;
import com.starrocks.sql.optimizer.MaterializationContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.ScanOperatorPredicates;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalViewScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MVTransparentState;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvPartitionCompensator;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.sql.optimizer.OptimizerTraceUtil.logMVRewrite;

/**
 * MVCompensationBuilder is used to build a mv compensation for materialized view in mv rewrite.
 */
public class MVCompensationBuilder {
    private static final Logger LOG = LogManager.getLogger(MVCompensationBuilder.class);

    private final MaterializationContext mvContext;
    private final MvUpdateInfo mvUpdateInfo;
    private final MaterializedView mv;

    public MVCompensationBuilder(MaterializationContext mvContext,
                                 MvUpdateInfo mvUpdateInfo) {
        this.mvContext = mvContext;
        this.mvUpdateInfo = mvUpdateInfo;
        this.mv = mvContext.getMv();
    }

    /**
     * Build input opt expression query profile for mv compensation. See more details in MaterializationContext.QueryOptProfile.
     */
    public static MaterializationContext.QueryOptProfile buildQueryOptProfile(MaterializedView mv,
                                                                              OptExpression queryPlan) {
        List<LogicalScanOperator> scanOperators = MvUtils.getScanOperator(queryPlan);
        Set<MaterializationContext.TableOptProfile> tableOptProfiles = Sets.newHashSet();
        Map<Table, List<Column>> refBaseTablePartitionColumns = mv.getRefBaseTablePartitionColumns();
        for (LogicalScanOperator scanOperator : scanOperators) {
            Table refBaseTable = scanOperator.getTable();
            if (refBaseTable == null) {
                continue;
            }
            // skip if the table is not ref base table
            if (!refBaseTablePartitionColumns.containsKey(refBaseTable)) {
                continue;
            }
            List<Column> partitionColumns = refBaseTablePartitionColumns.get(refBaseTable);
            // only consider predicates with table's partition columns
            if (refBaseTable.isNativeTableOrMaterializedView()) {
                LogicalOlapScanOperator olapScanOperator = (LogicalOlapScanOperator) scanOperator;
                tableOptProfiles.add(getTableOptProfileOfOlapTable(olapScanOperator, refBaseTable, partitionColumns));
            } else {
                tableOptProfiles.add(getTableOptProfileOfExternalTable(scanOperator, refBaseTable, partitionColumns));
            }
        }
        return new MaterializationContext.QueryOptProfile(tableOptProfiles);
    }

    private static MaterializationContext.TableOptProfile getTableOptProfileOfOlapTable(
            LogicalOlapScanOperator olapScanOperator,
            Table refBaseTable,
            List<Column> partitionColumns) {
        Set<ScalarOperator> predicates = Sets.newHashSet();
        Set<Long> partitionIds = Sets.newHashSet();
        // only consider selected partitions ids
        if (olapScanOperator.getSelectedPartitionId() != null) {
            partitionIds.addAll(olapScanOperator.getSelectedPartitionId());
        }
        // consider pruned partition predicates
        if (olapScanOperator.getPrunedPartitionPredicates() != null) {
            predicates.addAll(olapScanOperator.getPrunedPartitionPredicates());
        }
        // consider partition predicates
        if (olapScanOperator.getPredicate() != null) {
            ColumnRefSet partitionColumnRefSet = getPartitionColumnRefSet(partitionColumns, olapScanOperator);
            List<ScalarOperator> partitionPredicates = Utils.extractConjuncts(olapScanOperator.getPredicate());
            for (ScalarOperator predicate : partitionPredicates) {
                if (predicate.getUsedColumns().isIntersect(partitionColumnRefSet)) {
                    predicates.add(predicate);
                }
            }
        }
        return new MaterializationContext.TableOptProfile(refBaseTable, partitionIds, predicates);
    }

    private static MaterializationContext.TableOptProfile getTableOptProfileOfExternalTable(
            LogicalScanOperator scanOperator,
            Table refBaseTable,
            List<Column> partitionColumns) {
        Set<ScalarOperator> predicates = Sets.newHashSet();
        Set<Long> partitionIds = Sets.newHashSet();
        ScanOperatorPredicates scanOperatorPredicates = null;
        try {
            scanOperatorPredicates = scanOperator.getScanOperatorPredicates();
        } catch (AnalysisException e) {
            logMVRewrite("Failed to get scan operator predicates for table {}: {}",
                    refBaseTable.getName(), e.getMessage());
        }
        // if failed to get scan operator predicates, return empty profile
        if (scanOperatorPredicates == null) {
            return new MaterializationContext.TableOptProfile(refBaseTable, partitionIds, predicates);
        }

        // consider selected partitions ids
        if (scanOperatorPredicates.getSelectedPartitionIds() != null) {
            partitionIds.addAll(scanOperatorPredicates.getSelectedPartitionIds());
        }
        // consider pruned partition predicates and partition predicates
        if (scanOperatorPredicates.getPrunedPartitionConjuncts() != null) {
            predicates.addAll(scanOperatorPredicates.getPrunedPartitionConjuncts());
        }
        if (scanOperatorPredicates.getPartitionConjuncts() != null) {
            predicates.addAll(scanOperatorPredicates.getPartitionConjuncts());
        }
        // for non partition predicates, only consider those related to partition columns
        if (scanOperatorPredicates.getNonPartitionConjuncts() != null) {
            ColumnRefSet partitionColumnRefSet = getPartitionColumnRefSet(partitionColumns, scanOperator);
            List<ScalarOperator> nonPartitionPredicates = scanOperatorPredicates.getNonPartitionConjuncts();
            for (ScalarOperator predicate : nonPartitionPredicates) {
                if (predicate.getUsedColumns().isIntersect(partitionColumnRefSet)) {
                    predicates.add(predicate);
                }
            }
        }
        return new MaterializationContext.TableOptProfile(refBaseTable, partitionIds, predicates);
    }

    private static ColumnRefSet getPartitionColumnRefSet(List<Column> partitionColumns,
                                                         LogicalScanOperator scanOperator) {
        if (partitionColumns.isEmpty()) {
            return new ColumnRefSet();
        }
        Map<Column, ColumnRefOperator> columnColumnRefOperatorMap = scanOperator.getColumnMetaToColRefMap();
        Set<ColumnRefOperator>  partitionColumnRefs = partitionColumns.stream()
                .map(columnColumnRefOperatorMap::get)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());
        ColumnRefSet partitionColumnRefSet = new ColumnRefSet();
        partitionColumnRefs.forEach(colRef -> partitionColumnRefSet.union(colRef.getId()));
        return partitionColumnRefSet;
    }

    /**
     * Get mv compensation info by mv's update info.
     */
    public MVCompensation buildMvCompensation(Optional<OptExpression> queryPlanOpt) {
        SessionVariable sessionVariable = mvContext.getOptimizerContext().getSessionVariable();
        List<Pair<Optional<LogicalScanOperator>, Table>> refBaseTables;
        if (queryPlanOpt.isPresent()) {
            List<LogicalScanOperator> scanOperators = MvUtils.getScanOperator(queryPlanOpt.get());
            // If no partition to refresh, return directly.
            PCellSortedSet mvToRefreshPartitionNames = mvUpdateInfo.getMVToRefreshPCells();
            if (PCellUtils.isEmpty(mvToRefreshPartitionNames)) {
                return MVCompensation.noCompensate(sessionVariable);
            }

            // If no scan operator, no need compensate
            if (scanOperators.isEmpty() || scanOperators.stream().anyMatch(scan -> scan instanceof LogicalViewScanOperator)) {
                return MVCompensation.unknown(sessionVariable);
            }
            refBaseTables = scanOperators.stream()
                    .map(scan -> Pair.create(Optional.of(scan), scan.getTable()))
                    .collect(Collectors.toList());
        } else {
            refBaseTables = mvUpdateInfo.getBaseTableUpdateInfos()
                    .keySet()
                    .stream()
                    .map(table -> Pair.create(Optional.<LogicalScanOperator>empty(), table))
                    .collect(Collectors.toList());
        }

        MaterializedView mv = mvContext.getMv();
        Map<Table, List<Column>> refBaseTablePartitionColumns = mv.getRefBaseTablePartitionColumns();
        if (CollectionUtils.sizeIsEmpty(refBaseTablePartitionColumns)) {
            logMVRewrite("MV's not partitioned, failed to get partition keys: {}", mv.getName());
            return MVCompensation.unknown(sessionVariable);
        }

        Map<Table, TableCompensation> compensations = Maps.newHashMap();
        for (Pair<Optional<LogicalScanOperator>, Table> pair : refBaseTables) {
            Table refBaseTable = pair.second;
            if (!refBaseTablePartitionColumns.containsKey(refBaseTable)) {
                continue;
            }
            // If the ref table contains no partitions to refresh, no need compensate.
            // If the mv is partitioned and non-ref table needs refresh, then all partitions need to be refreshed;
            // it cannot be a candidate.
            TableCompensation tableCompensation = getRefBaseTableCompensation(refBaseTable, pair.first);
            // NPE check
            if (tableCompensation == null) {
                logMVRewrite(mvContext, "MV's ref base table {} to compensation is null, unknown state",
                        refBaseTable.getName());
                return MVCompensation.unknown(sessionVariable);
            }
            // if the table is unnecessary to compensate, skip
            if (tableCompensation.isNoCompensate()) {
                continue;
            }
            // if the table is not rewritable, return unknown state
            if (tableCompensation.isUncompensable()) {
                MVTransparentState state = tableCompensation.getState();
                return MVCompensation.withState(sessionVariable, state);
            }
            compensations.put(refBaseTable, tableCompensation);
        }
        return MVCompensation.compensate(sessionVariable, compensations);
    }

    public TableCompensation getRefBaseTableCompensation(Table refBaseTable,
                                                         Optional<LogicalScanOperator> scanOperatorOpt) {
        // if query consistency is not `force_mv`, use the old compensation logic.
        if (isEnableRefBaseTableCompensationByPartitionKeys(refBaseTable)) {
            return getRefBaseTableCompensationByPartitionKeys(refBaseTable, scanOperatorOpt);
        } else {
            return PartitionRetentionTableCompensation.build(refBaseTable, mvUpdateInfo, scanOperatorOpt);
        }
    }

    private boolean isEnableRefBaseTableCompensationByPartitionKeys(Table refBaseTable) {
        TableProperty.QueryRewriteConsistencyMode consistencyMode =
                mvUpdateInfo.getQueryRewriteConsistencyMode();
        if (consistencyMode != TableProperty.QueryRewriteConsistencyMode.FORCE_MV) {
            return true;
        }
        MaterializedView mv = mvUpdateInfo.getMv();
        String partitionRetention = mv.getTableProperty().getPartitionRetentionCondition();
        return Strings.isNullOrEmpty(partitionRetention);
    }

    /**
     * Get the compensation for the ref base table which has no input query plan but to compensate a consistent mv.
     */
    public TableCompensation getRefBaseTableCompensationByPartitionKeys(Table refBaseTable,
                                                                        Optional<LogicalScanOperator> scanOperatorOpt) {
        PCellSortedSet toRefreshPartitionNames = mvUpdateInfo.getBaseTableToRefreshPartitionNames(refBaseTable);
        if (toRefreshPartitionNames == null) {
            return TableCompensation.unknown();
        }
        if (toRefreshPartitionNames.isEmpty()) {
            return TableCompensation.noCompensation();
        }
        if (refBaseTable.isNativeTableOrMaterializedView()) {
            return OlapTableCompensation.build(refBaseTable, mvUpdateInfo, scanOperatorOpt);
        } else if (MvPartitionCompensator.isSupportPartitionCompensate(refBaseTable)) {
            return ExternalTableCompensation.build(refBaseTable, mvUpdateInfo, scanOperatorOpt);
        } else {
            // TODO: support more ref base table types
            logMVRewrite(mv.getName(), "Unsupported ref base table type: {}", refBaseTable.getName());
            return TableCompensation.unknown();
        }
    }
}