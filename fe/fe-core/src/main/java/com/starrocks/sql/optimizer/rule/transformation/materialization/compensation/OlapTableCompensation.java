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

import com.google.common.collect.Lists;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvUpdateInfo;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MVTransparentState;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import org.apache.commons.collections4.CollectionUtils;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.sql.optimizer.OptimizerTraceUtil.logMVRewrite;

public final class OlapTableCompensation extends TableCompensation {
    private final List<Long> compensations;

    public OlapTableCompensation(Table refBaseTable, List<Long> partitionIds) {
        super(refBaseTable, MVTransparentState.COMPENSATE);
        this.compensations = partitionIds;
    }

    public List<Long> getCompensations() {
        return compensations;
    }

    @Override
    public boolean isNoCompensate() {
        return super.isNoCompensate() || (state.isCompensate() && CollectionUtils.isEmpty(compensations));
    }

    @Override
    public LogicalScanOperator compensate(OptimizerContext optimizerContext,
                                          MaterializedView mv,
                                          LogicalScanOperator scanOperator) {
        final LogicalOlapScanOperator.Builder builder = new LogicalOlapScanOperator.Builder();
        // reset original partition predicates to prune partitions/tablets again
        builder.withOperator((LogicalOlapScanOperator) scanOperator)
                .setSelectedPartitionId(compensations)
                .setSelectedTabletId(Lists.newArrayList());
        return builder.build();
    }

    @Override
    public String toString() {
        if (compensations == null) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        sb.append("size=").append(compensations.size()).append(", ");
        sb.append(MvUtils.shrinkToSize(compensations, Config.max_mv_task_run_meta_message_values_length));
        return sb.toString();
    }

    public static TableCompensation build(Table refBaseTable,
                                          MvUpdateInfo mvUpdateInfo,
                                          Optional<LogicalScanOperator> scanOperatorOpt) {
        MaterializedView mv = mvUpdateInfo.getMv();
        Set<String> toRefreshPartitionNames = mvUpdateInfo.getBaseTableToRefreshPartitionNames(refBaseTable);
        if (toRefreshPartitionNames == null) {
            logMVRewrite(mv.getName(), "MV's ref base table {} to refresh partition is null, unknown state",
                    refBaseTable.getName());
            return null;
        }
        if (toRefreshPartitionNames.isEmpty()) {
            logMVRewrite(mv.getName(), "MV's ref base table {} to refresh partition is empty, no need compensate",
                    refBaseTable.getName());
            return TableCompensation.noCompensation();
        }
        // only retain the partition ids which are selected by the query plan.
        if (scanOperatorOpt.isPresent()) {
            LogicalOlapScanOperator logicalOlapScanOperator = (LogicalOlapScanOperator) scanOperatorOpt.get();
            List<Long> selectPartitionIds = logicalOlapScanOperator.getSelectedPartitionId();
            if (CollectionUtils.isEmpty(selectPartitionIds)) {
                return TableCompensation.noCompensation();
            }
            Set<String> selectedPartitionNames = selectPartitionIds
                    .stream()
                    .map(p -> refBaseTable.getPartition(p))
                    .map(p -> p.getName())
                    .collect(Collectors.toSet());
            // if all selected partitions need to refresh, no need to rewrite.
            if (toRefreshPartitionNames.containsAll(selectedPartitionNames)) {
                return TableCompensation.noRewrite();
            }
            // only retain the selected partitions to refresh.
            toRefreshPartitionNames.retainAll(selectedPartitionNames);
        }
        // if no partition need to refresh, no need to compensate.
        if (toRefreshPartitionNames.isEmpty()) {
            return TableCompensation.noCompensation();
        }
        List<Long> refTablePartitionIdsToRefresh = toRefreshPartitionNames.stream()
                .map(name -> refBaseTable.getPartition(name))
                .filter(Objects::nonNull)
                .map(p -> p.getId())
                .collect(Collectors.toList());
        return new OlapTableCompensation(refBaseTable, refTablePartitionIdsToRefresh);
    }
}