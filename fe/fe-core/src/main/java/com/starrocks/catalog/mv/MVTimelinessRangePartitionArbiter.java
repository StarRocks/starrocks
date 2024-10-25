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
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvUpdateInfo;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableProperty;
import com.starrocks.common.AnalysisException;
import com.starrocks.scheduler.TableWithPartitions;
import com.starrocks.sql.common.PCell;
import com.starrocks.sql.common.PRangeCell;
import com.starrocks.sql.common.RangePartitionDiff;
import com.starrocks.sql.common.RangePartitionDiffResult;
import com.starrocks.sql.common.RangePartitionDiffer;
import com.starrocks.sql.common.SyncPartitionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.sql.optimizer.OptimizerTraceUtil.logMVPrepare;

/**
 * A lot of methods in this class have the same syntax as the methods in {@code MVPCTRefreshRangePartitioner}, we may
 * unify them in the future.
 */
public final class MVTimelinessRangePartitionArbiter extends MVTimelinessArbiter {
    private static final Logger LOG = LogManager.getLogger(MVTimelinessRangePartitionArbiter.class);

    public MVTimelinessRangePartitionArbiter(MaterializedView mv, boolean isQueryRewrite) {
        super(mv, isQueryRewrite);
    }

    @Override
    protected MvUpdateInfo getMVTimelinessUpdateInfoInChecked() throws AnalysisException {
        PartitionInfo partitionInfo = mv.getPartitionInfo();
        Preconditions.checkState(partitionInfo.isExprRangePartitioned());
        // If non-partition-by table has changed, should refresh all mv partitions
        Expr partitionExpr = mv.getPartitionExpr();
        Map<Table, Column> refBaseTableAndColumns = mv.getRefBaseTablePartitionColumns();
        if (refBaseTableAndColumns.isEmpty()) {
            mv.setInactiveAndReason("partition configuration changed");
            LOG.warn("mark mv:{} inactive for get partition info failed", mv.getName());
            throw new RuntimeException(String.format("getting partition info failed for mv: %s", mv.getName()));
        }

        // if it needs to refresh based on non-ref base tables, return full refresh directly.
        boolean isRefreshBasedOnNonRefTables = needsRefreshOnNonRefBaseTables(refBaseTableAndColumns);
        logMVPrepare(mv, "MV refresh based on non-ref base table:{}", isRefreshBasedOnNonRefTables);
        if (isRefreshBasedOnNonRefTables) {
            return new MvUpdateInfo(MvUpdateInfo.MvToRefreshType.FULL);
        }

        // record the relation of partitions between materialized view and base partition table
        MvUpdateInfo mvTimelinessInfo = new MvUpdateInfo(MvUpdateInfo.MvToRefreshType.PARTIAL);
        // collect & update mv's to refresh partitions based on base table's partition changes
        Map<Table, Set<String>> baseChangedPartitionNames = collectBaseTableUpdatePartitionNames(refBaseTableAndColumns,
                mvTimelinessInfo);

        // collect all ref base table's partition range map
        Map<Table, Map<String, Range<PartitionKey>>> basePartitionNameToRangeMap =
                RangePartitionDiffer.syncBaseTablePartitionInfos(mv, partitionExpr);

        // If base table is materialized view, add partition name to cell mapping into base table partition mapping,
        // otherwise base table(mv) may lose partition names of the real base table changed partitions.
        collectExtraBaseTableChangedPartitions(mvTimelinessInfo.getBaseTableUpdateInfos(), entry -> {
            Table baseTable = entry.getKey();
            Preconditions.checkState(basePartitionNameToRangeMap.containsKey(baseTable));
            Map<String, Range<PartitionKey>> refBaseTablePartitionRangeMap = basePartitionNameToRangeMap.get(baseTable);
            Map<String, PCell> basePartitionNameToRanges = entry.getValue();
            basePartitionNameToRanges.entrySet().forEach(e -> refBaseTablePartitionRangeMap.put(e.getKey(),
                    ((PRangeCell) e.getValue()).getRange()));
        });

        // There may be a performance issue here, because it will fetch all partitions of base tables and mv partitions.
        RangePartitionDiffResult differ = RangePartitionDiffer.computeRangePartitionDiff(mv, null,
                basePartitionNameToRangeMap, isQueryRewrite);
        if (differ == null) {
            throw new AnalysisException(String.format("Compute partition difference of mv %s with base table failed.",
                    mv.getName()));
        }

        // no needs to refresh the deleted partitions, because the deleted partitions are not in the mv's partition map.
        Set<String> mvToRefreshPartitionNames = Sets.newHashSet();
        Map<String, Range<PartitionKey>> mvPartitionNameToRangeMap = differ.mvRangePartitionMap;
        RangePartitionDiff rangePartitionDiff = differ.rangePartitionDiff;

        // remove ref base table's deleted partitions from `mvPartitionMap`
        mvToRefreshPartitionNames.addAll(rangePartitionDiff.getDeletes().keySet());
        rangePartitionDiff.getDeletes().keySet().stream().forEach(mvPartitionNameToRangeMap::remove);
        // add all ref base table's added partitions to `mvPartitionMap`
        mvToRefreshPartitionNames.addAll(rangePartitionDiff.getAdds().keySet());
        mvPartitionNameToRangeMap.putAll(rangePartitionDiff.getAdds());
        // add mv partition name to range map into timeline info to be used if it's a sub mv of nested mv
        Map<String, PCell> mvPartitionNameToCell = mvPartitionNameToRangeMap.entrySet().stream()
                .collect(Collectors.toMap(e -> e.getKey(), e -> new PRangeCell(e.getValue())));
        mvTimelinessInfo.addMVPartitionNameToCellMap(mvPartitionNameToCell);

        Map<Table, Expr> baseTableToPartitionExprs = mv.getRefBaseTablePartitionExprs();
        Map<Table, Map<String, Set<String>>> baseToMvNameRef = RangePartitionDiffer
                .generateBaseRefMap(basePartitionNameToRangeMap, baseTableToPartitionExprs, mvPartitionNameToRangeMap);
        Map<String, Map<Table, Set<String>>> mvToBaseNameRef = RangePartitionDiffer
                .generateMvRefMap(mvPartitionNameToRangeMap, baseTableToPartitionExprs, basePartitionNameToRangeMap);
        mvTimelinessInfo.getBasePartToMvPartNames().putAll(baseToMvNameRef);
        mvTimelinessInfo.getMvPartToBasePartNames().putAll(mvToBaseNameRef);

        mvToRefreshPartitionNames.addAll(getMVToRefreshPartitionNames(baseChangedPartitionNames, baseToMvNameRef));

        // handle mv's partition expr is function call expr
        if (partitionExpr instanceof FunctionCallExpr) {
            List<TableWithPartitions> baseTableWithPartitions = baseChangedPartitionNames.entrySet().stream()
                    .map(e -> new TableWithPartitions(e.getKey(), e.getValue()))
                    .collect(Collectors.toList());
            if (mv.isCalcPotentialRefreshPartition(baseTableWithPartitions,
                    basePartitionNameToRangeMap, mvToRefreshPartitionNames, mvPartitionNameToRangeMap)) {
                // because the relation of partitions between materialized view and base partition table is n: m,
                // should calculate the candidate partitions recursively.
                SyncPartitionUtils.calcPotentialRefreshPartition(mvToRefreshPartitionNames, baseChangedPartitionNames,
                        baseToMvNameRef, mvToBaseNameRef, Sets.newHashSet());
            }
        }
        // update mv's to refresh partitions
        mvTimelinessInfo.addMvToRefreshPartitionNames(mvToRefreshPartitionNames);
        return mvTimelinessInfo;
    }

    @Override
    protected MvUpdateInfo getMVTimelinessUpdateInfoInLoose() {
        MvUpdateInfo mvUpdateInfo = new MvUpdateInfo(MvUpdateInfo.MvToRefreshType.PARTIAL,
                TableProperty.QueryRewriteConsistencyMode.LOOSE);
        RangePartitionDiff rangePartitionDiff = null;
        try {
            // There may be a performance issue here, because it will fetch all partitions of base tables and mv partitions.
            RangePartitionDiffResult differ = RangePartitionDiffer.computeRangePartitionDiff(mv, null, true);
            if (differ == null) {
                return new MvUpdateInfo(MvUpdateInfo.MvToRefreshType.UNKNOWN);
            }
            rangePartitionDiff = differ.rangePartitionDiff;
        } catch (Exception e) {
            LOG.warn("Materialized view compute partition difference with base table failed.", e);
            return null;
        }
        if (rangePartitionDiff == null) {
            LOG.warn("Materialized view compute partition difference with base table failed, the diff of range partition" +
                    " is null.");
            return null;
        }

        Map<String, Range<PartitionKey>> adds = rangePartitionDiff.getAdds();
        for (Map.Entry<String, Range<PartitionKey>> addEntry : adds.entrySet()) {
            String mvPartitionName = addEntry.getKey();
            mvUpdateInfo.addMvToRefreshPartitionNames(mvPartitionName);
        }
        addEmptyPartitionsToRefresh(mvUpdateInfo);
        return mvUpdateInfo;
    }
}
