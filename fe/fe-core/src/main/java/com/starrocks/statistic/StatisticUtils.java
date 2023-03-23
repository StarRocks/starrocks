// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.statistic;

import com.google.common.collect.ImmutableList;
import com.starrocks.analysis.UserIdentity;
import com.starrocks.catalog.AggregateType;
import com.starrocks.catalog.Column;
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
import java.util.ArrayList;
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
        context.setDatabase(Constants.StatisticsDBName);
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
        Database db = GlobalStateMgr.getCurrentState().getDb(Constants.StatisticsDBName);

        // check database
        if (db == null) {
            return false;
        }

        // check table
        OlapTable table = (OlapTable) db.getTable(Constants.StatisticsTableName);
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

    public static boolean isEmptyTable(Table table) {
<<<<<<< HEAD
        return ((OlapTable) table).getPartitions().stream().noneMatch(Partition::hasData);
=======
        return table.getPartitions().stream().noneMatch(Partition::hasData);
    }

    public static List<ColumnDef> buildStatsColumnDef(String tableName) {
        ScalarType columnNameType = ScalarType.createVarcharType(65530);
        ScalarType tableNameType = ScalarType.createVarcharType(65530);
        ScalarType partitionNameType = ScalarType.createVarcharType(65530);
        ScalarType dbNameType = ScalarType.createVarcharType(65530);
        ScalarType maxType = ScalarType.createMaxVarcharType();
        ScalarType minType = ScalarType.createMaxVarcharType();
        ScalarType bucketsType = ScalarType.createMaxVarcharType();
        ScalarType mostCommonValueType = ScalarType.createMaxVarcharType();

        // varchar type column need call setAssignedStrLenInColDefinition here,
        // otherwise it will be set length to 1 at analyze
        columnNameType.setAssignedStrLenInColDefinition();
        tableNameType.setAssignedStrLenInColDefinition();
        partitionNameType.setAssignedStrLenInColDefinition();
        dbNameType.setAssignedStrLenInColDefinition();
        maxType.setAssignedStrLenInColDefinition();
        minType.setAssignedStrLenInColDefinition();
        bucketsType.setAssignedStrLenInColDefinition();
        mostCommonValueType.setAssignedStrLenInColDefinition();

        if (tableName.equals(StatsConstants.SAMPLE_STATISTICS_TABLE_NAME)) {
            return ImmutableList.of(
                    new ColumnDef("table_id", new TypeDef(ScalarType.createType(PrimitiveType.BIGINT))),
                    new ColumnDef("column_name", new TypeDef(columnNameType)),
                    new ColumnDef("db_id", new TypeDef(ScalarType.createType(PrimitiveType.BIGINT))),
                    new ColumnDef("table_name", new TypeDef(tableNameType)),
                    new ColumnDef("db_name", new TypeDef(dbNameType)),
                    new ColumnDef("row_count", new TypeDef(ScalarType.createType(PrimitiveType.BIGINT))),
                    new ColumnDef("data_size", new TypeDef(ScalarType.createType(PrimitiveType.BIGINT))),
                    new ColumnDef("distinct_count", new TypeDef(ScalarType.createType(PrimitiveType.BIGINT))),
                    new ColumnDef("null_count", new TypeDef(ScalarType.createType(PrimitiveType.BIGINT))),
                    new ColumnDef("max", new TypeDef(maxType)),
                    new ColumnDef("min", new TypeDef(minType)),
                    new ColumnDef("update_time", new TypeDef(ScalarType.createType(PrimitiveType.DATETIME)))
            );
        } else if (tableName.equals(StatsConstants.FULL_STATISTICS_TABLE_NAME)) {
            return ImmutableList.of(
                    new ColumnDef("table_id", new TypeDef(ScalarType.createType(PrimitiveType.BIGINT))),
                    new ColumnDef("partition_id", new TypeDef(ScalarType.createType(PrimitiveType.BIGINT))),
                    new ColumnDef("column_name", new TypeDef(columnNameType)),
                    new ColumnDef("db_id", new TypeDef(ScalarType.createType(PrimitiveType.BIGINT))),
                    new ColumnDef("table_name", new TypeDef(tableNameType)),
                    new ColumnDef("partition_name", new TypeDef(partitionNameType)),
                    new ColumnDef("row_count", new TypeDef(ScalarType.createType(PrimitiveType.BIGINT))),
                    new ColumnDef("data_size", new TypeDef(ScalarType.createType(PrimitiveType.BIGINT))),
                    new ColumnDef("ndv", new TypeDef(ScalarType.createType(PrimitiveType.HLL))),
                    new ColumnDef("null_count", new TypeDef(ScalarType.createType(PrimitiveType.BIGINT))),
                    new ColumnDef("max", new TypeDef(maxType)),
                    new ColumnDef("min", new TypeDef(minType)),
                    new ColumnDef("update_time", new TypeDef(ScalarType.createType(PrimitiveType.DATETIME)))
            );
        } else if (tableName.equals(StatsConstants.HISTOGRAM_STATISTICS_TABLE_NAME)) {
            return ImmutableList.of(
                    new ColumnDef("table_id", new TypeDef(ScalarType.createType(PrimitiveType.BIGINT))),
                    new ColumnDef("column_name", new TypeDef(columnNameType)),
                    new ColumnDef("db_id", new TypeDef(ScalarType.createType(PrimitiveType.BIGINT))),
                    new ColumnDef("table_name", new TypeDef(tableNameType)),
                    new ColumnDef("buckets", new TypeDef(bucketsType), false, null,
                            true, ColumnDef.DefaultValueDef.NOT_SET, ""),
                    new ColumnDef("mcv", new TypeDef(mostCommonValueType), false, null,
                            true, ColumnDef.DefaultValueDef.NOT_SET, ""),
                    new ColumnDef("update_time", new TypeDef(ScalarType.createType(PrimitiveType.DATETIME)))
            );
        } else {
            throw new StarRocksPlannerException("Not support stats table " + tableName, ErrorType.INTERNAL_ERROR);
        }
    }

    public static Optional<Double> convertStatisticsToDouble(Type type, String statistic) {
        if (!type.canStatistic()) {
            throw new StarRocksPlannerException("Error statistic type : " + type.toSql(), ErrorType.INTERNAL_ERROR);
        }
        try {
            switch (type.getPrimitiveType()) {
                case BOOLEAN:
                    if (statistic.equalsIgnoreCase("TRUE")) {
                        return Optional.of(1D);
                    } else {
                        return Optional.of(0D);
                    }
                case DATE:
                    return Optional.of((double) getLongFromDateTime(DateUtils.parseStringWithDefaultHSM(
                            statistic, DateUtils.DATE_FORMATTER_UNIX)));
                case DATETIME:
                    return Optional.of((double) getLongFromDateTime(DateUtils.parseStringWithDefaultHSM(
                            statistic, DateUtils.DATE_TIME_FORMATTER_UNIX)));
                case CHAR:
                case VARCHAR:
                    return Optional.empty();
                default:
                    return Optional.of(Double.parseDouble(statistic));
            }
        } catch (Exception e) {
            LOG.warn(String.format("Statistic convert error, type %s, statistic %s, %s",
                    type.toSql(), statistic, e.getMessage()));
            return Optional.empty();
        }
>>>>>>> 655380a5c ([BugFix] enlarge the varchar length for statistics (#19848))
    }

    // Get all the columns in the table that can be collected.
    // The list will only contain aggregated and non-aggregated columns of the "replace" type.
    // This is because in aggregate type tables, metric columns generally do not participate in predicate.
    // Collecting these columns is not meaningful but time-consuming, so we exclude them.
    public static List<String> getCollectibleColumns(Table table) {
        List<String> columns = new ArrayList<>();
        for (Column column : table.getBaseSchema()) {
            if (!column.isAggregated()) {
                columns.add(column.getName());
            } else if (column.getAggregationType().equals(AggregateType.REPLACE)) {
                columns.add(column.getName());
            }
        }
        return columns;
    }
}
