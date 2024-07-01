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
import com.starrocks.sql.common.ListPartitionDiff;
import com.starrocks.sql.common.ListPartitionDiffResult;
import com.starrocks.sql.common.ListPartitionDiffer;
import com.starrocks.sql.common.PListCell;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.starrocks.sql.optimizer.OptimizerTraceUtil.logMVPrepare;

public final class MVTimelinessListPartitionArbiter extends MVTimelinessArbiter {
    private static final Logger LOG = LogManager.getLogger(MVTimelinessListPartitionArbiter.class);

    public MVTimelinessListPartitionArbiter(MaterializedView mv, boolean isQueryRewrite) {
        super(mv, isQueryRewrite);
    }

    @Override
    public MvUpdateInfo getMVTimelinessUpdateInfoInChecked() throws AnalysisException {
        PartitionInfo partitionInfo = mv.getPartitionInfo();
        Preconditions.checkState(partitionInfo instanceof ListPartitionInfo);
        Map<Table, Column> refBaseTableAndColumns = mv.getRelatedPartitionTableAndColumn();
        if (refBaseTableAndColumns.isEmpty()) {
            mv.setInactiveAndReason("partition configuration changed");
            LOG.warn("mark mv:{} inactive for get partition info failed", mv.getName());
            throw new RuntimeException(String.format("getting partition info failed for mv: %s", mv.getName()));
        }

        // if it needs to refresh based on non-ref base tables, return full refresh directly.
        boolean isRefreshBasedOnNonRefTables = needsRefreshOnNonRefBaseTables(refBaseTableAndColumns);
        logMVPrepare(mv, "Is refresh based on non-ref base table:{}", isRefreshBasedOnNonRefTables);
        if (isRefreshBasedOnNonRefTables) {
            return new MvUpdateInfo(MvUpdateInfo.MvToRefreshType.FULL);
        }

        MvUpdateInfo mvTimelinessInfo = new MvUpdateInfo(MvUpdateInfo.MvToRefreshType.PARTIAL);
        ListPartitionDiffResult result = ListPartitionDiffer.computeListPartitionDiff(mv);
        if (result == null) {
            logMVPrepare(mv, "Partitioned mv compute list diff failed");
            return new MvUpdateInfo(MvUpdateInfo.MvToRefreshType.FULL);
        }

        final Map<Table, Map<String, PListCell>> refBaseTablePartitionMap = result.refBaseTablePartitionMap;
        // update into mv's to refresh partitions
        Set<String> mvToRefreshPartitionNames = mvTimelinessInfo.getMvToRefreshPartitionNames();
        final ListPartitionDiff listPartitionDiff = result.listPartitionDiff;
        mvToRefreshPartitionNames.addAll(listPartitionDiff.getDeletes().keySet());
        // remove ref base table's deleted partitions from `mvPartitionMap`
        Map<String, PListCell> mvPartitionNameToListMap = mv.getListPartitionItems();
        listPartitionDiff.getDeletes().keySet().forEach(mvPartitionNameToListMap::remove);
        // refresh ref base table's new added partitions
        mvToRefreshPartitionNames.addAll(listPartitionDiff.getAdds().keySet());
        mvPartitionNameToListMap.putAll(listPartitionDiff.getAdds());

        final Map<Table, List<Integer>> refBaseTableRefIdxMap = result.refBaseTableRefIdxMap;
        Map<Table, Map<String, Set<String>>> baseToMvNameRef = ListPartitionDiffer
                .generateBaseRefMap(refBaseTablePartitionMap, refBaseTableRefIdxMap, mvPartitionNameToListMap);
        Map<String, Map<Table, Set<String>>> mvToBaseNameRef = ListPartitionDiffer
                .generateMvRefMap(mvPartitionNameToListMap, refBaseTableRefIdxMap, refBaseTablePartitionMap);
        mvTimelinessInfo.getBasePartToMvPartNames().putAll(baseToMvNameRef);
        mvTimelinessInfo.getMvPartToBasePartNames().putAll(mvToBaseNameRef);

        // update mv's to refresh partitions based on base table's partition changes
        Map<Table, Set<String>> baseChangedPartitionNames = collectBaseTableUpdatePartitionNames(refBaseTableAndColumns,
                mvTimelinessInfo);
        mvToRefreshPartitionNames.addAll(getMVToRefreshPartitionNames(baseChangedPartitionNames, baseToMvNameRef));

        return mvTimelinessInfo;
    }

    @Override
    public MvUpdateInfo getMVTimelinessUpdateInfoInLoose() {
        MvUpdateInfo mvUpdateInfo = new MvUpdateInfo(MvUpdateInfo.MvToRefreshType.PARTIAL,
                TableProperty.QueryRewriteConsistencyMode.LOOSE);
        ListPartitionDiff listPartitionDiff = null;
        try {
            ListPartitionDiffResult result = ListPartitionDiffer.computeListPartitionDiff(mv);
            if (result == null) {
                logMVPrepare(mv, "Partitioned mv compute list diff failed");
                return new MvUpdateInfo(MvUpdateInfo.MvToRefreshType.FULL);
            }
            listPartitionDiff = result.listPartitionDiff;
        } catch (Exception e) {
            LOG.warn("Materialized view compute partition difference with base table failed.", e);
            return null;
        }
        if (listPartitionDiff == null) {
            LOG.warn("Materialized view compute partition difference with base table failed, the diff of range partition" +
                    " is null.");
            return null;
        }
        Map<String, PListCell> adds = listPartitionDiff.getAdds();
        for (Map.Entry<String, PListCell> addEntry : adds.entrySet()) {
            String mvPartitionName = addEntry.getKey();
            mvUpdateInfo.getMvToRefreshPartitionNames().add(mvPartitionName);
        }
        return mvUpdateInfo;
    }
}