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

package com.starrocks.summary;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.TabletInvertedIndex;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.common.ThreadPoolManager;
import com.starrocks.planner.OlapScanNode;
import com.starrocks.planner.ScanNode;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.GlobalVariable;
import com.starrocks.qe.SimpleExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.statistic.StatisticUtils;
import com.starrocks.statistic.StatsConstants;
import com.starrocks.thrift.TResultSinkType;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

public class TabletStatisticsMgr {
    private static final Logger LOG = LogManager.getLogger(TabletStatisticsMgr.class);

    private static final int CONVERT_BATCH_SIZE = 500;

    private static final int MAX_MEMORY_SIZE = 200 * 1024 * 1024; // 200 MB

    private static final ExecutorService EXECUTOR = ThreadPoolManager.newDaemonFixedThreadPool(
            1, Integer.MAX_VALUE,
            "tablet-statistics-executor", true);

    private final List<TabletStatistics> infosList = Lists.newArrayListWithCapacity(CONVERT_BATCH_SIZE + 1);
    private final Map<Long, Double> infosMap = new HashMap<>(CONVERT_BATCH_SIZE + 1); // <scanTabletId, readCount>

    private StringBuffer buffer = new StringBuffer("[");

    private int batchSize = 0;

    private LocalDateTime lastLoadTime = LocalDateTime.now();

    public LocalDateTime getLastLoadTime() {
        return lastLoadTime;
    }

    public synchronized void addTabletStatistics(ConnectContext ctx, ExecPlan plan) {
        if (!GlobalVariable.isEnableTabletStatistics()) {
            return;
        }
        if (ctx.isStatisticsConnection() || ctx.isStatisticsJob() || plan.getScanNodes().isEmpty()
                || StringUtils.containsIgnoreCase(ctx.getExecutor().getOriginStmtInString(),
                StatsConstants.INFORMATION_SCHEMA)) {
            return;
        }

        List<ScanNode> scanNodes = plan.getScanNodes();
        for (ScanNode sn : scanNodes) {
            if (sn instanceof OlapScanNode) {
                OlapScanNode osn = (OlapScanNode) sn;
                for (long scanTabletId : osn.getScanTabletIds()) {
                    // during loading interval, to avoid produce many rows statistics data, here aggragate it.
                    infosMap.put(scanTabletId, infosMap.getOrDefault(scanTabletId, 0.0d) + 1);
                }
            }
        }
        if (infosMap.size() > CONVERT_BATCH_SIZE || lastLoadTime.plusSeconds(
                GlobalVariable.tabletStatisticsLoadIntervalSeconds).isBefore(LocalDateTime.now())) {
            TabletInvertedIndex invertedIndex = GlobalStateMgr.getCurrentState().getTabletInvertedIndex();
            for (long scanTabletId : infosMap.keySet()) {
                TabletMeta tabletMeta = invertedIndex.getTabletMeta(scanTabletId);
                if (tabletMeta != null) {
                    Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().
                            getDbIncludeRecycleBin(tabletMeta.getDbId());
                    OlapTable olapTable = (OlapTable)  GlobalStateMgr.getCurrentState().getLocalMetastore().
                                getTableIncludeRecycleBin(db, tabletMeta.getTableId());
                    PhysicalPartition physicalPartition = GlobalStateMgr.getCurrentState().getLocalMetastore().
                                getPhysicalPartitionIncludeRecycleBin(olapTable, tabletMeta.getPhysicalPartitionId());
                    infosList.add(new TabletStatistics(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME,
                            db.getOriginName(), olapTable.getName(),
                            // getName() return partitionName_partitionId, remove _partitionId
                            physicalPartition.getName().replace("_" + tabletMeta.getPhysicalPartitionId(), ""),
                            scanTabletId, infosMap.get(scanTabletId)));
                }
            }
            loadTabletStatistics(Lists.newArrayList(infosList));
            infosList.clear();
            infosMap.clear();
        }
    }

    private void loadTabletStatistics(List<TabletStatistics> list) {
        if (!StatisticUtils.checkStatisticTables(List.of(StatsConstants.TABLET_STATISTICS_TABLE_NAME))) {
            return;
        }

        ConnectContext ctx = StatisticUtils.buildConnectContext();
        EXECUTOR.submit(() -> {
            ctx.setThreadLocalInfo();
            for (TabletStatistics info : list) {
                try {
                    buffer.append(info.toJSON()).append(",");
                } catch (Exception e) {
                    LOG.warn("Failed to convert tablet statistics info", e);
                }
            }
            try {
                batchSize += list.size();

                if (buffer.isEmpty()) {
                    return;
                }
                if (buffer.length() < MAX_MEMORY_SIZE && lastLoadTime.plusSeconds(
                        GlobalVariable.tabletStatisticsLoadIntervalSeconds).isAfter(LocalDateTime.now())) {
                    return;
                }
                buffer.setLength(buffer.length() - 1);
                buffer.append("]");
                StreamLoader loader = new StreamLoader(StatsConstants.STATISTICS_DB_NAME,
                        StatsConstants.TABLET_STATISTICS_TABLE_NAME,
                        List.of("dt", "catalog_name", "db_name", "table_name", "partition_name", "tablet_id", "read_count"));
                StreamLoader.Response response = loader.loadBatch("tablet_statistics", buffer.toString());

                if (response != null && response.status() == HttpStatus.SC_OK) {
                    LOG.debug("load tablet statistics success, batch size[{}], response[{}]", batchSize, response);
                } else {
                    LOG.warn("load tablet statistics failed, batch size[{}], response[{}]", batchSize, response);
                }

                buffer = new StringBuffer();
                buffer.append("[");
                lastLoadTime = LocalDateTime.now();
                batchSize = 0;
            } catch (Exception e) {
                LOG.warn("Failed to load tablet statistics.", e);
            }
        });
    }

    public void alterTtlTabletStatistics() {
        if (!GlobalVariable.isEnableTabletStatistics()) {
            return;
        }
        if (!StatisticUtils.checkStatisticTables(List.of(StatsConstants.TABLET_STATISTICS_TABLE_NAME))) {
            return;
        }

        SimpleExecutor executor = new SimpleExecutor("TabletStatisticsAlterTTL", TResultSinkType.MYSQL_PROTOCAL);
        executor.executeDDL(
                "alter table " + StatsConstants.STATISTICS_DB_NAME + "." + StatsConstants.TABLET_STATISTICS_TABLE_NAME
                        + " SET ('partition_live_number' = '" + GlobalVariable.tabletStatisticsKeepDays + "')");
    }
}
