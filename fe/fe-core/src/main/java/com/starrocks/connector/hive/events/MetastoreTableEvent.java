// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.connector.hive.events;

import com.clearspring.analytics.util.Lists;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.starrocks.connector.hive.CacheUpdateProcessor;
import com.starrocks.connector.hive.HivePartitionName;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.api.Table;

import java.util.List;

/**
 * Base class for all the table events
 */
public abstract class MetastoreTableEvent extends MetastoreEvent {
    // tblName from the event
    protected final String tblName;

    // tbl object from the Notification event, corresponds to the before tableObj in case of alter events.
    protected Table hmsTbl;

    // HivePartitionName of each event to process. for unpartition table, the partition values are empty.
    protected List<HivePartitionName> hivePartitionNames = Lists.newArrayList();

    protected MetastoreTableEvent(NotificationEvent event, CacheUpdateProcessor cacheProcessor, String catalogName) {
        super(event, cacheProcessor, catalogName);
        Preconditions.checkNotNull(dbName, "Database name cannot be null");
        tblName = Preconditions.checkNotNull(event.getTableName());

        HivePartitionName hivePartitionName = new HivePartitionName(dbName, tblName, Lists.newArrayList());
        hivePartitionNames.add(hivePartitionName);
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
     * According to the current processing method, each event only needs to process one {@link HivePartitionName}.
     */
    protected HivePartitionName getHivePartitionName() {
        return hivePartitionNames.get(0);
    }
}
