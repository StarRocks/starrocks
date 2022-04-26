// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.external.hive.events;

import com.clearspring.analytics.util.Lists;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.Table.TableType;
import com.starrocks.external.hive.HiveMetaCache;
import com.starrocks.external.hive.HivePartitionKey;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.api.Table;

import java.util.List;
import java.util.Map;

/**
 * Base class for all the table events
 */
public abstract class MetastoreTableEvent extends MetastoreEvent {
    // tblName from the event
    protected final String tblName;

    // tbl object from the Notification event, corresponds to the before tableObj in case of alter events.
    protected Table hmsTbl;

    // HivePartitionKeys of each event to process. for unpartition table, the partition values are empty.
    protected List<HivePartitionKey> hivePartitionKeys = Lists.newArrayList();

    protected MetastoreTableEvent(NotificationEvent event, HiveMetaCache metaCache) {
        super(event, metaCache);
        Preconditions.checkNotNull(dbName, "Database name cannot be null");
        tblName = Preconditions.checkNotNull(event.getTableName());

        HivePartitionKey hivePartitionKey = new HivePartitionKey(dbName, tblName, TableType.HIVE, Lists.newArrayList());
        hivePartitionKeys.add(hivePartitionKey);
    }

    /**
     * Returns a list of parameters that are set by Hive for tables/partitions that can be
     * ignored to determine if the alter table/partition event is a trivial one.
     */
    static final List<String> parametersToIgnore =
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
        for (String parameter : parametersToIgnore) {
            String val = parametersBefore.get(parameter);
            if (val == null) {
                parametersAfter.remove(parameter);
            } else {
                parametersAfter.put(parameter, val);
            }
        }
    }

    protected List<HivePartitionKey> getHivePartitionKeys() {
        return hivePartitionKeys;
    }

    /**
     * According to the current processing method, each event only needs to process one {@link HivePartitionKey}.
     */
    protected HivePartitionKey getHivePartitionKey() {
        return hivePartitionKeys.get(0);
    }

    /**
     * Util method to return the fully qualified table name which is of the format dbName.tblName for this event.
     */
    protected String getFullyQualifiedTblName() {
        return dbName + "." + tblName;
    }
}
