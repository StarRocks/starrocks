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
import com.starrocks.common.ThreadPoolManager;
import com.starrocks.planner.ScanNode;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.GlobalVariable;
import com.starrocks.qe.SimpleExecutor;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.statistic.StatisticUtils;
import com.starrocks.statistic.StatsConstants;
import com.starrocks.thrift.TResultSinkType;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.ExecutorService;

public class PartitionsScanMgr {
    private static final Logger LOG = LogManager.getLogger(PartitionsScanMgr.class);

    private static final int CONVERT_BATCH_SIZE = 100;

    private static final int MAX_MEMORY_SIZE = 200 * 1024 * 1024; // 200 MB

    private static final ExecutorService EXECUTOR = ThreadPoolManager.newDaemonFixedThreadPool(
            1, Integer.MAX_VALUE,
            "partitions-scan-executor", true);

    private final List<PartitionsScan> infos = Lists.newArrayListWithCapacity(CONVERT_BATCH_SIZE + 1);

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
            Database db = MetaUtils.getDatabaseByTableId(sn.getTableId());
            if (db.getOriginName() != "") {
                String catalogName = InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME;
                String dbName = db.getOriginName();
                String tableName = sn.getTableName();
                List<String> selectedPartitionNames = sn.getSelectedPartitionNames();
                for (String selectedPartitionName : selectedPartitionNames) {
                    infos.add(new PartitionsScan(catalogName, dbName, tableName, selectedPartitionName, 1.0));
                }
            }
        }

        if (infos.size() > CONVERT_BATCH_SIZE || lastLoadTime.plusSeconds(
                GlobalVariable.partitionsScanLoadIntervalSeconds).isBefore(LocalDateTime.now())) {
            loadPartitionsScan(Lists.newArrayList(infos));
            infos.clear();
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