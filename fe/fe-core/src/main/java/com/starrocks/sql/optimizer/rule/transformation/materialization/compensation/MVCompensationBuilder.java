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
import com.starrocks.catalog.Column;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvUpdateInfo;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableProperty;
import com.starrocks.common.Pair;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.optimizer.MaterializationContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalViewScanOperator;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MVTransparentState;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvPartitionCompensator;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
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
     * Get mv compensation info by mv's update info.
     */
    public MVCompensation buildMvCompensation(Optional<OptExpression> queryPlanOpt) {
        SessionVariable sessionVariable = mvContext.getOptimizerContext().getSessionVariable();
        List<Pair<Optional<LogicalScanOperator>, Table>> refBaseTables;
        if (queryPlanOpt.isPresent()) {
            List<LogicalScanOperator> scanOperators = MvUtils.getScanOperator(queryPlanOpt.get());
            // If no partition to refresh, return directly.
            Set<String> mvToRefreshPartitionNames = mvUpdateInfo.getMvToRefreshPartitionNames();
            if (CollectionUtils.isEmpty(mvToRefreshPartitionNames)) {
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
        Set<String> toRefreshPartitionNames = mvUpdateInfo.getBaseTableToRefreshPartitionNames(refBaseTable);
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