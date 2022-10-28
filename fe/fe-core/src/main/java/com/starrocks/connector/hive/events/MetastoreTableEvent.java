// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.connector.hive.events;

import com.clearspring.analytics.util.Lists;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.starrocks.connector.hive.CacheUpdateProcessor;
import com.starrocks.connector.hive.HiveColumnStats;
import com.starrocks.connector.hive.HiveCommonStats;
import com.starrocks.connector.hive.HivePartitionName;
import com.starrocks.connector.hive.HivePartitionStats;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.api.Table;

import java.util.List;
import java.util.Map;

import static com.starrocks.connector.hive.HiveMetastoreApiConverter.toHiveCommonStats;

/**
 * Base class for all the table events
 */
public abstract class MetastoreTableEvent extends MetastoreEvent {
    // tblName from the event
    protected final String tblName;

    // tbl object from the Notification event, corresponds to the before tableObj in case of alter events.
    protected Table hmsTbl;

    // HivePartitionKeys of each event to process. for unpartition table, the partition values are empty.
    protected List<HivePartitionName> hivePartitionNames = Lists.newArrayList();

    protected MetastoreTableEvent(NotificationEvent event, CacheUpdateProcessor cacheProcessor, String catalogName) {
        super(event, cacheProcessor, catalogName);
        Preconditions.checkNotNull(dbName, "Database name cannot be null");
        tblName = Preconditions.checkNotNull(event.getTableName());

        HivePartitionName hivePartitionKey = new HivePartitionName(dbName, tblName, Lists.newArrayList());
        hivePartitionNames.add(hivePartitionKey);
    }

    /**
     * Returns a list of parameters that are set by Hive for tables/partitions that can be
     * ignored to determine if the alter table/partition event is a trivial one.
     */
    private static final List<String> PARAMETERS_TO_IGNORE =
            new ImmutableList.Builder<String>()
                    .add("transient_lastDdlTime")
                    .add("numFilesErasureCoded")
                    .add("numFiles")
                    .add("comment")
                    .build();

    /**
     * Util method that sets the parameters that can be ignored equal before and after event.
     */
    protected static void filterParameters(Map<String, String> parametersBefore, Map<String, String> parametersAfter) {
        for (String parameter : PARAMETERS_TO_IGNORE) {
            String val = parametersBefore.get(parameter);
            if (val == null) {
                parametersAfter.remove(parameter);
            } else {
                parametersAfter.put(parameter, val);
            }
        }
    }

    protected List<HivePartitionName> getHivePartitionKeys() {
        return hivePartitionNames;
    }

    /**
     * According to the current processing method, each event only needs to process one {@link HivePartitionName}.
     */
    protected HivePartitionName getHivePartitionKey() {
        return hivePartitionNames.get(0);
    }

    /**
     * Util method to return the fully qualified table name which is of the format dbName.tblName for this event.
     */
    protected String getFullyQualifiedTblName() {
        return dbName + "." + tblName;
    }

    private <K, V> HivePartitionStats getPartitionStats(Map<String, String> params, Map<String, HiveColumnStats> columnStats) {
        HiveCommonStats commonStats = toHiveCommonStats(params);
        long totalRowNums = commonStats.getRowNums();
        if (totalRowNums == -1) {
            return HivePartitionStats.empty();
        }
        return new HivePartitionStats(commonStats, columnStats);
    }
}
