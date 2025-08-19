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
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.ThreadPoolManager;
import com.starrocks.planner.OlapScanNode;
import com.starrocks.planner.ScanNode;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.GlobalVariable;
import com.starrocks.qe.SimpleExecutor;
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

public class PartitionsScanMgr {
    private static final Logger LOG = LogManager.getLogger(PartitionsScanMgr.class);

    private static final int CONVERT_BATCH_SIZE = 100;

    private static final int MAX_MEMORY_SIZE = 200 * 1024 * 1024; // 200 MB

    private static final ExecutorService EXECUTOR = ThreadPoolManager.newDaemonFixedThreadPool(
            1, Integer.MAX_VALUE,
            "partitions-scan-executor", true);

    private final List<PartitionsScan> infosList = Lists.newArrayListWithCapacity(CONVERT_BATCH_SIZE + 1);
    private final Map<String, Double> infosMap = new HashMap<>(CONVERT_BATCH_SIZE + 1);

    private StringBuffer buffer = new StringBuffer("[");

    private int batchSize = 0;

    private LocalDateTime lastLoadTime = LocalDateTime.now();

    public LocalDateTime getLastLoadTime() {
        return lastLoadTime;
    }

    public synchronized void addPartitionsScan(ConnectContext ctx, ExecPlan plan) {
        if (!GlobalVariable.isEnablePartitionsScan()) {
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
                OlapTable olapTable = osn.getOlapTable();
                String catalogName = InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME;
                String dbName = olapTable.mayGetDatabaseName().orElse("Unknown");
                String tableName = osn.getTableName();
                for (String selectedPartitionName : osn.getSelectedPartitionNames()) {
                    String key = catalogName + "-" + dbName + "-" + tableName + "-" + selectedPartitionName;
                    infosMap.put(key, infosMap.getOrDefault(key, 0.0d) + 1);
                }
            }
        }

        if (infosMap.size() > CONVERT_BATCH_SIZE || lastLoadTime.plusSeconds(
                GlobalVariable.partitionsScanLoadIntervalSeconds).isBefore(LocalDateTime.now())) {
            for (String key : infosMap.keySet()) {
                infosList.add(new PartitionsScan(key.split("-")[0], key.split("-")[1], key.split("-")[2],
                        key.split("-")[3], infosMap.get(key)));
            }
            loadPartitionsScan(Lists.newArrayList(infosList));
            infosMap.clear();
            infosList.clear();
        }
    }

    private void loadPartitionsScan(List<PartitionsScan> list) {
        if (!StatisticUtils.checkStatisticTables(List.of(StatsConstants.PARTITIONS_SCAN_TABLE_NAME))) {
            return;
        }

        ConnectContext ctx = StatisticUtils.buildConnectContext();
        EXECUTOR.submit(() -> {
            ctx.setThreadLocalInfo();
            for (PartitionsScan info : list) {
                try {
                    buffer.append(info.toJSON()).append(",");
                } catch (Exception e) {
                    LOG.warn("Failed to convert partitions scan info", e);
                }
            }
            try {
                batchSize += list.size();

                if (buffer.isEmpty()) {
                    return;
                }
                if (buffer.length() < MAX_MEMORY_SIZE && lastLoadTime.plusSeconds(
                        GlobalVariable.partitionsScanLoadIntervalSeconds).isAfter(LocalDateTime.now())) {
                    return;
                }
                buffer.setLength(buffer.length() - 1);
                buffer.append("]");
                StreamLoader loader = new StreamLoader(StatsConstants.STATISTICS_DB_NAME,
                        StatsConstants.PARTITIONS_SCAN_TABLE_NAME,
                        List.of("dt", "catalog_name", "db_name", "table_name", "partition_name", "scan_count"));
                StreamLoader.Response response = loader.loadBatch("partitions_scan", buffer.toString());

                if (response != null && response.status() == HttpStatus.SC_OK) {
                    LOG.debug("load partitions scan success, batch size[{}], response[{}]", batchSize, response);
                } else {
                    LOG.warn("load partitions scan failed, batch size[{}], response[{}]", batchSize, response);
                }

                buffer = new StringBuffer();
                buffer.append("[");
                lastLoadTime = LocalDateTime.now();
                batchSize = 0;
            } catch (Exception e) {
                LOG.warn("Failed to load partitions scan.", e);
            }
        });
    }

    public void alterTtlPartitionsScan() {
        if (!GlobalVariable.isEnablePartitionsScan()) {
            return;
        }
        if (!StatisticUtils.checkStatisticTables(List.of(StatsConstants.PARTITIONS_SCAN_TABLE_NAME))) {
            return;
        }

        SimpleExecutor executor = new SimpleExecutor("PartitionsScanAlterTTL", TResultSinkType.MYSQL_PROTOCAL);
        executor.executeDDL(
                "alter table " + StatsConstants.STATISTICS_DB_NAME + "." + StatsConstants.PARTITIONS_SCAN_TABLE_NAME
                        + " SET ('partition_live_number' = '" + GlobalVariable.partitionsScanKeepDays + "')");
    }
}