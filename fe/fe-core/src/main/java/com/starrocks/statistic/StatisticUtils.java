// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.statistic;

import com.google.common.collect.ImmutableList;
import com.starrocks.analysis.UserIdentity;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.SystemInfoService;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.List;

public class StatisticUtils {
    private static final List<String> COLLECT_DATABASES_BLACKLIST = ImmutableList.<String>builder()
            .add(StatsConstants.StatisticsDBName)
            .add(SystemInfoService.DEFAULT_CLUSTER + ":starrocks_monitor")
            .add(SystemInfoService.DEFAULT_CLUSTER + ":information_schema").build();

    public static ConnectContext buildConnectContext() {
        ConnectContext context = new ConnectContext();
        // Note: statistics query does not register query id to QeProcessorImpl::coordinatorMap,
        // but QeProcessorImpl::reportExecStatus will check query id,
        // So we must disable report query status from BE to FE
        context.getSessionVariable().setReportSuccess(false);
        int parallel = context.getSessionVariable().getStatisticCollectParallelism();
        if (null != ConnectContext.get()) {
            // from current session, may execute analyze stmt
            parallel = ConnectContext.get().getSessionVariable().getStatisticCollectParallelism();
        }
        context.getSessionVariable().setParallelExecInstanceNum(parallel);
        context.getSessionVariable().setPipelineDop(1);
        // TODO(kks): remove this if pipeline support STATISTIC result sink type
        context.getSessionVariable().setEnablePipelineEngine(false);
        context.setCluster(SystemInfoService.DEFAULT_CLUSTER);
        context.setDatabase(StatsConstants.StatisticsDBName);
        context.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        context.setCurrentUserIdentity(UserIdentity.ROOT);
        context.setQualifiedUser(UserIdentity.ROOT.getQualifiedUser());
        context.setQueryId(UUIDUtil.genUUID());
        context.setExecutionId(UUIDUtil.toTUniqueId(context.getQueryId()));
        context.setThreadLocalInfo();
        context.setStartTime();
        return context;
    }

    // check database in black list
    public static boolean statisticDatabaseBlackListCheck(String databaseName) {
        if (null == databaseName) {
            return true;
        }

        return COLLECT_DATABASES_BLACKLIST.stream().anyMatch(d -> databaseName.toLowerCase().contains(d.toLowerCase()));
    }

    public static boolean statisticTableBlackListCheck(long tableId) {
        for (String dbName : COLLECT_DATABASES_BLACKLIST) {
            Database db = GlobalStateMgr.getCurrentState().getDb(dbName);
            if (null != db && null != db.getTable(tableId)) {
                return true;
            }
        }

        return false;
    }

    public static boolean checkStatisticTableStateNormal() {
        Database db = GlobalStateMgr.getCurrentState().getDb(StatsConstants.StatisticsDBName);

        // check database
        if (db == null) {
            return false;
        }

        // check table
        OlapTable table = (OlapTable) db.getTable(StatsConstants.SampleStatisticsTableName);
        if (table == null) {
            return false;
        }

        // check replicate miss
        for (Partition partition : table.getPartitions()) {
            if (partition.getBaseIndex().getTablets().stream()
                    .anyMatch(t -> ((LocalTablet) t).getNormalReplicaBackendIds().isEmpty())) {
                return false;
            }
        }

        return true;
    }

    public static LocalDateTime getTableLastUpdateTime(Table table) {
        long maxTime = ((OlapTable) table).getPartitions().stream().map(Partition::getVisibleVersionTime)
                .max(Long::compareTo).orElse(0L);
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(maxTime), Clock.systemDefaultZone().getZone());
    }

    public static LocalDateTime getPartitionLastUpdateTime(Partition partition) {
        long time = partition.getVisibleVersionTime();
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(time), Clock.systemDefaultZone().getZone());
    }

    public static boolean isEmptyTable(Table table) {
        return ((OlapTable) table).getPartitions().stream().noneMatch(Partition::hasData);
    }
}
