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
<<<<<<< HEAD
import com.google.common.collect.Maps;
=======
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ListPartitionInfo;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvUpdateInfo;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.Table;
<<<<<<< HEAD
import com.starrocks.catalog.TableProperty;
import com.starrocks.common.AnalysisException;
import com.starrocks.sql.common.ListPartitionDiff;
import com.starrocks.sql.common.ListPartitionDiffResult;
import com.starrocks.sql.common.ListPartitionDiffer;
import com.starrocks.sql.common.PCell;
import com.starrocks.sql.common.PListCell;
=======
import com.starrocks.common.AnalysisException;
import com.starrocks.sql.common.ListPartitionDiffer;
import com.starrocks.sql.common.PCell;
import com.starrocks.sql.common.PartitionDiff;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
<<<<<<< HEAD
=======
import java.util.Optional;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.sql.optimizer.OptimizerTraceUtil.logMVPrepare;

public final class MVTimelinessListPartitionArbiter extends MVTimelinessArbiter {
    private static final Logger LOG = LogManager.getLogger(MVTimelinessListPartitionArbiter.class);

    public MVTimelinessListPartitionArbiter(MaterializedView mv, boolean isQueryRewrite) {
        super(mv, isQueryRewrite);
<<<<<<< HEAD
=======
        differ = new ListPartitionDiffer(mv, isQueryRewrite);
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
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
            return new MvUpdateInfo(MvUpdateInfo.MvToRefreshType.FULL);
        }

        // update mv's to refresh partitions based on base table's partition changes
        MvUpdateInfo mvTimelinessInfo = new MvUpdateInfo(MvUpdateInfo.MvToRefreshType.PARTIAL);
        Map<Table, Set<String>> baseChangedPartitionNames = collectBaseTableUpdatePartitionNames(refBaseTablePartitionColumns,
                mvTimelinessInfo);
<<<<<<< HEAD
        Map<Table, Map<String, PListCell>> refBaseTablePartitionMap = Maps.newHashMap();
        Map<String, PListCell> allBasePartitionItems = Maps.newHashMap();

        // collect base table's partition infos
        if (!ListPartitionDiffer.syncBaseTablePartitionInfos(mv, refBaseTablePartitionMap, allBasePartitionItems)) {
            logMVPrepare(mv, "Sync base table partition infos failed");
            return new MvUpdateInfo(MvUpdateInfo.MvToRefreshType.FULL);
        }

        // If base table is materialized view, add partition name to cell mapping into base table partition mapping,
        // otherwise base table(mv) may lose partition names of the real base table changed partitions.
        collectExtraBaseTableChangedPartitions(mvTimelinessInfo.getBaseTableUpdateInfos(), entry -> {
            Table baseTable = entry.getKey();
            Preconditions.checkState(refBaseTablePartitionMap.containsKey(baseTable));
            Map<String, PListCell> refBaseTablePartitionRangeMap = refBaseTablePartitionMap.get(baseTable);
            Map<String, PCell> basePartitionNameToRanges = entry.getValue();
            basePartitionNameToRanges.entrySet().forEach(e -> refBaseTablePartitionRangeMap.put(e.getKey(),
                    ((PListCell) e.getValue())));
        });

        ListPartitionDiffResult result = ListPartitionDiffer.computeListPartitionDiff(mv, refBaseTablePartitionMap,
                allBasePartitionItems, isQueryRewrite);
        if (result == null) {
=======

        // collect base table's partition infos
        Map<Table, Map<String, PCell>> refBaseTablePartitionMap = syncBaseTablePartitions(mv);
        if (refBaseTablePartitionMap == null) {
            logMVPrepare(mv, "Sync base table partition infos failed");
            return new MvUpdateInfo(MvUpdateInfo.MvToRefreshType.FULL);
        }
        // If base table is materialized view, add partition name to cell mapping into base table partition mapping,
        // otherwise base table(mv) may lose partition names of the real base table changed partitions.
        collectExtraBaseTableChangedPartitions(mvTimelinessInfo.getBaseTableUpdateInfos(), refBaseTablePartitionMap);

        PartitionDiff diff = getChangedPartitionDiff(mv, refBaseTablePartitionMap);
        if (diff == null) {
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
            logMVPrepare(mv, "Partitioned mv compute list diff failed");
            return new MvUpdateInfo(MvUpdateInfo.MvToRefreshType.FULL);
        }

        // update into mv's to refresh partitions
<<<<<<< HEAD
        Set<String> mvToRefreshPartitionNames = mvTimelinessInfo.getMvToRefreshPartitionNames();
        final ListPartitionDiff listPartitionDiff = result.listPartitionDiff;
        mvToRefreshPartitionNames.addAll(listPartitionDiff.getDeletes().keySet());
        // remove ref base table's deleted partitions from `mvPartitionMap`
        Map<String, PListCell> mvPartitionNameToListMap = mv.getListPartitionItems();
        listPartitionDiff.getDeletes().keySet().forEach(mvPartitionNameToListMap::remove);
        // refresh ref base table's new added partitions
        mvToRefreshPartitionNames.addAll(listPartitionDiff.getAdds().keySet());
        mvPartitionNameToListMap.putAll(listPartitionDiff.getAdds());
=======
        final Set<String> mvToRefreshPartitionNames = mvTimelinessInfo.getMvToRefreshPartitionNames();
        mvToRefreshPartitionNames.addAll(diff.getDeletes().keySet());
        mvToRefreshPartitionNames.addAll(diff.getAdds().keySet());

        // remove ref base table's deleted partitions from `mvPartitionMap`
        // refresh ref base table's new added partitions
        Map<String, PCell> mvPartitionNameToListMap = mv.getPartitionCells(Optional.empty());
        diff.getDeletes().keySet().forEach(mvPartitionNameToListMap::remove);
        mvPartitionNameToListMap.putAll(diff.getAdds());

>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        Map<String, PCell> mvPartitionNameToCell = mvPartitionNameToListMap.entrySet().stream()
                .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
        mvTimelinessInfo.addMVPartitionNameToCellMap(mvPartitionNameToCell);

<<<<<<< HEAD
        Map<Table, Map<String, Set<String>>> baseToMvNameRef = ListPartitionDiffer
                .generateBaseRefMap(refBaseTablePartitionMap, mvPartitionNameToListMap);
        Map<String, Map<Table, Set<String>>> mvToBaseNameRef = ListPartitionDiffer
                .generateMvRefMap(mvPartitionNameToListMap, refBaseTablePartitionMap);
        mvTimelinessInfo.getBasePartToMvPartNames().putAll(baseToMvNameRef);
        mvTimelinessInfo.getMvPartToBasePartNames().putAll(mvToBaseNameRef);


=======
        Map<Table, Map<String, Set<String>>> baseToMvNameRef =
                differ.generateBaseRefMap(refBaseTablePartitionMap, mvPartitionNameToListMap);
        Map<String, Map<Table, Set<String>>> mvToBaseNameRef =
                differ.generateMvRefMap(mvPartitionNameToListMap, refBaseTablePartitionMap);
        mvTimelinessInfo.getBasePartToMvPartNames().putAll(baseToMvNameRef);
        mvTimelinessInfo.getMvPartToBasePartNames().putAll(mvToBaseNameRef);

>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        mvToRefreshPartitionNames.addAll(getMVToRefreshPartitionNames(baseChangedPartitionNames, baseToMvNameRef));

        return mvTimelinessInfo;
    }
<<<<<<< HEAD

    @Override
    public MvUpdateInfo getMVTimelinessUpdateInfoInLoose() {
        MvUpdateInfo mvUpdateInfo = new MvUpdateInfo(MvUpdateInfo.MvToRefreshType.PARTIAL,
                TableProperty.QueryRewriteConsistencyMode.LOOSE);
        ListPartitionDiff listPartitionDiff = null;
        try {
            ListPartitionDiffResult result = ListPartitionDiffer.computeListPartitionDiff(mv, isQueryRewrite);
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
        addEmptyPartitionsToRefresh(mvUpdateInfo);
        return mvUpdateInfo;
    }
=======
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
}