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

package com.starrocks.catalog.mv;

import com.google.common.base.Preconditions;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ListPartitionInfo;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvUpdateInfo;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableProperty;
import com.starrocks.common.AnalysisException;
import com.starrocks.sql.common.ListPartitionDiffer;
import com.starrocks.sql.common.PCell;
import com.starrocks.sql.common.PartitionDiff;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.sql.optimizer.OptimizerTraceUtil.logMVPrepare;

public final class MVTimelinessListPartitionArbiter extends MVTimelinessArbiter {
    private static final Logger LOG = LogManager.getLogger(MVTimelinessListPartitionArbiter.class);

    public MVTimelinessListPartitionArbiter(MaterializedView mv, boolean isQueryRewrite) {
        super(mv, isQueryRewrite);
        differ = new ListPartitionDiffer(mv, isQueryRewrite);
    }

    @Override
    public MvUpdateInfo getMVTimelinessUpdateInfoInChecked() throws AnalysisException {
        PartitionInfo partitionInfo = mv.getPartitionInfo();
        Preconditions.checkState(partitionInfo instanceof ListPartitionInfo);
        Map<Table, List<Column>> refBaseTablePartitionColumns = mv.getRefBaseTablePartitionColumns();
        if (refBaseTablePartitionColumns.isEmpty()) {
            mv.setInactiveAndReason("partition configuration changed");
            LOG.warn("mark mv:{} inactive for get partition info failed", mv.getName());
            throw new RuntimeException(String.format("getting partition info failed for mv: %s", mv.getName()));
        }

        // if it needs to refresh based on non-ref base tables, return full refresh directly.
        boolean isRefreshBasedOnNonRefTables = needsRefreshOnNonRefBaseTables(refBaseTablePartitionColumns);
        logMVPrepare(mv, "MV refresh based on non-ref base table:{}", isRefreshBasedOnNonRefTables);
        if (isRefreshBasedOnNonRefTables) {
            return MvUpdateInfo.fullRefresh(mv);
        }

        // update mv's to refresh partitions based on base table's partition changes
        MvUpdateInfo mvTimelinessInfo = MvUpdateInfo.partialRefresh(mv, TableProperty.QueryRewriteConsistencyMode.CHECKED);
        Map<Table, Set<String>> baseChangedPartitionNames = collectBaseTableUpdatePartitionNames(refBaseTablePartitionColumns,
                mvTimelinessInfo);

        // collect base table's partition infos
        Map<Table, Map<String, PCell>> refBaseTablePartitionMap = syncBaseTablePartitions(mv);
        if (refBaseTablePartitionMap == null) {
            logMVPrepare(mv, "Sync base table partition infos failed");
            return MvUpdateInfo.fullRefresh(mv);
        }
        // If base table is materialized view, add partition name to cell mapping into base table partition mapping,
        // otherwise base table(mv) may lose partition names of the real base table changed partitions.
        collectExtraBaseTableChangedPartitions(mvTimelinessInfo.getBaseTableUpdateInfos(), refBaseTablePartitionMap);

        PartitionDiff diff = getChangedPartitionDiff(mv, refBaseTablePartitionMap);
        if (diff == null) {
            logMVPrepare(mv, "Partitioned mv compute list diff failed");
            return MvUpdateInfo.fullRefresh(mv);
        }

        // update into mv's to refresh partitions
        final Set<String> mvToRefreshPartitionNames = mvTimelinessInfo.getMvToRefreshPartitionNames();
        mvToRefreshPartitionNames.addAll(diff.getDeletes().keySet());
        mvToRefreshPartitionNames.addAll(diff.getAdds().keySet());

        // remove ref base table's deleted partitions from `mvPartitionMap`
        // refresh ref base table's new added partitions
        Map<String, PCell> mvPartitionNameToListMap = mv.getPartitionCells(Optional.empty());
        diff.getDeletes().keySet().forEach(mvPartitionNameToListMap::remove);
        mvPartitionNameToListMap.putAll(diff.getAdds());

        Map<String, PCell> mvPartitionNameToCell = mvPartitionNameToListMap.entrySet().stream()
                .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
        mvTimelinessInfo.addMVPartitionNameToCellMap(mvPartitionNameToCell);

        Map<Table, Map<String, Set<String>>> baseToMvNameRef =
                differ.generateBaseRefMap(refBaseTablePartitionMap, mvPartitionNameToListMap);
        Map<String, Map<Table, Set<String>>> mvToBaseNameRef =
                differ.generateMvRefMap(mvPartitionNameToListMap, refBaseTablePartitionMap);
        mvTimelinessInfo.getBasePartToMvPartNames().putAll(baseToMvNameRef);
        mvTimelinessInfo.getMvPartToBasePartNames().putAll(mvToBaseNameRef);

        mvToRefreshPartitionNames.addAll(getMVToRefreshPartitionNames(baseChangedPartitionNames, baseToMvNameRef));

        return mvTimelinessInfo;
    }
}