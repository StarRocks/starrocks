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

package com.starrocks.catalog;

import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.starrocks.analysis.Expr;
import com.starrocks.catalog.mv.MVTimelinessArbiter;
import com.starrocks.catalog.mv.MVTimelinessNonPartitionArbiter;
import com.starrocks.catalog.mv.MVTimelinessRangePartitionArbiter;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.sql.common.PListCell;
import com.starrocks.sql.common.UnsupportedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.starrocks.connector.PartitionUtil.getMVPartitionNameWithList;
import static com.starrocks.connector.PartitionUtil.getMVPartitionNameWithRange;
import static com.starrocks.sql.optimizer.OptimizerTraceUtil.logMVPrepare;

/**
* The arbiter of materialized view refresh. All implementations of refresh strategies should be here.
*/
public class MvRefreshArbiter {
    private static final Logger LOG = LogManager.getLogger(MvRefreshArbiter.class);

    public static boolean needsToRefreshTable(MaterializedView mv, Table table, boolean isQueryRewrite) {
        Optional<Boolean> needsToRefresh = needsToRefreshTable(mv, table, true, isQueryRewrite);
        if (needsToRefresh.isPresent()) {
            return needsToRefresh.get();
        }
        return true;
    }

    /**
     * Once materialized view's base tables have updated, we need to check correspond materialized views' partitions
     * to be refreshed.
     * @param mv The materialized view to check
     * @param isQueryRewrite Mark whether this caller is query rewrite or not, when it's true we can use staleness to shortcut
     * @return mv timeliness update info which contains all need refreshed partitions of materialized view and partition name
     * to partition values.
     */
    public static MvUpdateInfo getMVTimelinessUpdateInfo(MaterializedView mv, boolean isQueryRewrite) {
        // Skip check for sync materialized view.
        if (mv.getRefreshScheme().isSync()) {
            return new MvUpdateInfo(MvUpdateInfo.MvToRefreshType.NO_REFRESH);
        }

        // check mv's query rewrite consistency mode property only in query rewrite.
        TableProperty tableProperty = mv.getTableProperty();
        TableProperty.QueryRewriteConsistencyMode mvConsistencyRewriteMode = tableProperty.getQueryRewriteConsistencyMode();
        if (isQueryRewrite) {
            switch (mvConsistencyRewriteMode) {
                case DISABLE:
                    return new MvUpdateInfo(MvUpdateInfo.MvToRefreshType.FULL);
                case NOCHECK:
                    return new MvUpdateInfo(MvUpdateInfo.MvToRefreshType.NO_REFRESH);
                case LOOSE:
                case CHECKED:
                default:
                    break;
            }
        }

        logMVPrepare(mv, "MV refresh arbiter start to get partition names to refresh, query rewrite mode: {}",
                mvConsistencyRewriteMode);
        MVTimelinessArbiter timelinessArbiter = buildMVTimelinessArbiter(mv, isQueryRewrite);
        try {
            return timelinessArbiter.getMVTimelinessUpdateInfo(mvConsistencyRewriteMode);
        } catch (AnalysisException e) {
            logMVPrepare(mv, "Failed to get mv timeliness info: {}", DebugUtil.getStackTrace(e));
            return new MvUpdateInfo(MvUpdateInfo.MvToRefreshType.UNKNOWN);
        }
    }

    /**
     * Create the MVTimelinessArbiter instance according to the partition info of the materialized view.
     * @param mv the materialized view to get the timeliness arbiter
     * @param isQueryRewrite whether this caller is query rewrite or mv refresh
     * @return MVTimelinessArbiter instance according to the partition info of the materialized view
     */
    public static MVTimelinessArbiter buildMVTimelinessArbiter(MaterializedView mv,
                                                               boolean isQueryRewrite) {
        PartitionInfo partitionInfo = mv.getPartitionInfo();
        if (partitionInfo.isUnPartitioned()) {
            return new MVTimelinessNonPartitionArbiter(mv, isQueryRewrite);
        } else if (partitionInfo.isRangePartition()) {
            return new MVTimelinessRangePartitionArbiter(mv, isQueryRewrite);
        } else {
            throw UnsupportedException.unsupportedException("unsupported partition info type:" +
                    partitionInfo.getClass().getName());
        }
    }

    /**
     * Check whether mv needs to refresh based on the ref base table. It's a shortcut version of getMvBaseTableUpdateInfo.
     * @return Optional<Boolean> : true if needs to refresh, false if not, empty if there are some unkown results.
     */
    public static Optional<Boolean> needsToRefreshTable(MaterializedView mv,
                                                        Table baseTable,
                                                        boolean withMv,
                                                        boolean isQueryRewrite) {
        if (baseTable.isView()) {
            // do nothing
            return Optional.of(false);
        } else if (baseTable.isNativeTableOrMaterializedView()) {
            OlapTable olapBaseTable = (OlapTable) baseTable;
            Set<String> baseUpdatedPartitionNames = mv.getUpdatedPartitionNamesOfOlapTable(olapBaseTable, isQueryRewrite);
            if (!baseUpdatedPartitionNames.isEmpty()) {
                return Optional.of(true);
            }

            // recursive check its children
            if (withMv && baseTable.isMaterializedView()) {
                MvUpdateInfo mvUpdateInfo = getMVTimelinessUpdateInfo((MaterializedView) baseTable, isQueryRewrite);
                if (mvUpdateInfo == null || !mvUpdateInfo.isValidRewrite()) {
                    return Optional.empty();
                }
                // NOTE: if base table is mv, check to refresh partition names as the base table's update info.
                return Optional.of(!mvUpdateInfo.getMvToRefreshPartitionNames().isEmpty());
            }
            return Optional.of(false);
        } else {
            Set<String> baseUpdatedPartitionNames = mv.getUpdatedPartitionNamesOfExternalTable(baseTable, isQueryRewrite);
            if (baseUpdatedPartitionNames == null) {
                return Optional.empty();
            }
            return Optional.of(!baseUpdatedPartitionNames.isEmpty());
        }
    }

    /**
     * Get to refresh partition info of the specific table.
     * @param baseTable: the table to check
     * @param withMv: whether to check the materialized view if it's a materialized view
     * @param isQueryRewrite: whether this caller is query rewrite or not
     * @return MvBaseTableUpdateInfo: the update info of the base table
     */
    public static MvBaseTableUpdateInfo getMvBaseTableUpdateInfo(MaterializedView mv,
                                                                 Table baseTable,
                                                                 boolean withMv,
                                                                 boolean isQueryRewrite) {
        MvBaseTableUpdateInfo baseTableUpdateInfo = new MvBaseTableUpdateInfo();
        if (baseTable.isView()) {
            // do nothing
            return baseTableUpdateInfo;
        } else if (baseTable.isNativeTableOrMaterializedView()) {
            OlapTable olapBaseTable = (OlapTable) baseTable;
            Set<String> baseUpdatedPartitionNames = mv.getUpdatedPartitionNamesOfOlapTable(olapBaseTable, isQueryRewrite);

            // recursive check its children
            if (withMv && baseTable.isMaterializedView()) {
                MvUpdateInfo mvUpdateInfo = getMVTimelinessUpdateInfo((MaterializedView) baseTable, isQueryRewrite);
                if (mvUpdateInfo == null || !mvUpdateInfo.isValidRewrite()) {
                    return null;
                }
                // NOTE: if base table is mv, check to refresh partition names as the base table's update info.
                baseUpdatedPartitionNames.addAll(mvUpdateInfo.getMvToRefreshPartitionNames());
            }
            // update base table's partition info
            baseTableUpdateInfo.addToRefreshPartitionNames(baseUpdatedPartitionNames);
        } else {
            Set<String> baseUpdatedPartitionNames = mv.getUpdatedPartitionNamesOfExternalTable(baseTable, isQueryRewrite);
            if (baseUpdatedPartitionNames == null) {
                return null;
            }
            Map<Table, Column> partitionTableAndColumns = mv.getRelatedPartitionTableAndColumn();
            if (!partitionTableAndColumns.containsKey(baseTable)) {
                baseTableUpdateInfo.addToRefreshPartitionNames(baseUpdatedPartitionNames);
                return baseTableUpdateInfo;
            }

            try {
                List<String> updatedPartitionNamesList = Lists.newArrayList(baseUpdatedPartitionNames);
                Column partitionColumn = partitionTableAndColumns.get(baseTable);
                PartitionInfo mvPartitionInfo = mv.getPartitionInfo();
                if (mvPartitionInfo.isListPartition()) {
                    Map<String, PListCell> partitionNameWithRange = getMVPartitionNameWithList(baseTable,
                            partitionColumn, updatedPartitionNamesList);
                    for (Map.Entry<String, PListCell> e : partitionNameWithRange.entrySet()) {
                        baseTableUpdateInfo.addListPartitionKeys(e.getKey(), e.getValue());
                    }
                    baseTableUpdateInfo.addToRefreshPartitionNames(partitionNameWithRange.keySet());
                } else if (mvPartitionInfo.isRangePartition()) {
                    Expr partitionExpr = MaterializedView.getPartitionExpr(mv);
                    Map<String, Range<PartitionKey>> partitionNameWithRange = getMVPartitionNameWithRange(baseTable,
                            partitionColumn, updatedPartitionNamesList, partitionExpr);
                    for (Map.Entry<String, Range<PartitionKey>> e : partitionNameWithRange.entrySet()) {
                        baseTableUpdateInfo.addRangePartitionKeys(e.getKey(), e.getValue());
                    }
                    baseTableUpdateInfo.addToRefreshPartitionNames(partitionNameWithRange.keySet());
                } else {
                    return null;
                }
            } catch (AnalysisException e) {
                LOG.warn("Mv {}'s base table {} get partition name fail", mv.getName(), baseTable.getName(), e);
                return null;
            }
        }
        return baseTableUpdateInfo;
    }
}
