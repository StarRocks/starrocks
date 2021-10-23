// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.statistic;

import com.google.common.collect.ImmutableList;
import com.starrocks.analysis.UserIdentity;
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.system.SystemInfoService;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.List;

public class StatisticUtils {
    private static final List<String> COLLECT_DATABASES_BLACKLIST = ImmutableList.<String>builder()
            .add(Constants.StatisticsDBName)
            .add(SystemInfoService.DEFAULT_CLUSTER + ":starrocks_monitor")
            .add(SystemInfoService.DEFAULT_CLUSTER + ":information_schema").build();

    public static ConnectContext buildConnectContext() {
        ConnectContext context = new ConnectContext();
        // Note: statistics query does not register query id to QeProcessorImpl::coordinatorMap,
        // but QeProcessorImpl::reportExecStatus will check query id,
        // So we must disable report query status from BE to FE
        context.getSessionVariable().setReportSuccess(false);
        // Always use 1 parallel to avoid affect normal query
        context.getSessionVariable().setParallelExecInstanceNum(1);
        context.setCluster(SystemInfoService.DEFAULT_CLUSTER);
        context.setDatabase(Constants.StatisticsDBName);
        context.setCatalog(Catalog.getCurrentCatalog());
        context.setCurrentUserIdentity(UserIdentity.ROOT);
        context.setQualifiedUser(UserIdentity.ROOT.getQualifiedUser());
        context.setQueryId(UUIDUtil.genUUID());
        context.setExecutionId(UUIDUtil.toTUniqueId(context.getQueryId()));
        context.setThreadLocalInfo();
        context.setStartTime();
        return context;
    }

    public static Table getStatisticsTable() {
        Database db = Catalog.getCurrentCatalog().getDb(Constants.StatisticsDBName);
        if (db != null) {
            return db.getTable(Constants.StatisticsTableName);
        } else {
            return null;
        }
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
            Database db = Catalog.getCurrentCatalog().getDb(dbName);
            if (null != db && null != db.getTable(tableId)) {
                return true;
            }
        }

        return false;
    }

    public static LocalDateTime getTableLastUpdateTime(Table table) {
        long maxTime = ((OlapTable) table).getPartitions().stream().map(Partition::getVisibleVersionTime)
                .max(Long::compareTo).orElse(0L);
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(maxTime), Clock.systemDefaultZone().getZone());
    }

    public static boolean isEmptyTable(Table table) {
        return ((OlapTable) table).getPartitions().stream().noneMatch(Partition::hasData);
    }

}
