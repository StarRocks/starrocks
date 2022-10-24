// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.external.hive.events;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.connector.hive.CacheUpdateProcessor;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.messaging.InsertMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;

/**
 * Metastore event handler for INSERT events. Handles insert events at both table and partition scopes.
 * If partition is null, treat it as ALTER_TABLE event, otherwise as ALTER_PARTITION event.
 */
public class InsertEvent extends MetastoreTableEvent {
    private static final Logger LOG = LogManager.getLogger(InsertEvent.class);

    // Represents the partition for this insert. Null if the table is unpartitioned.
    private final Partition insertPartition;

    private InsertEvent(NotificationEvent event, CacheUpdateProcessor metaCache) {
        super(event, metaCache);
        Preconditions.checkArgument(MetastoreEventType.INSERT.equals(getEventType()));
        InsertMessage insertMessage =
                MetastoreEventsProcessor.getMessageDeserializer()
                        .getInsertMessage(event.getMessage());
        try {
            hmsTbl = Preconditions.checkNotNull(insertMessage.getTableObj());
            insertPartition = insertMessage.getPtnObj();
            if (insertPartition != null) {
                hivePartitionKeys.clear();
                // TODO(stephen): refactor this function
                // hivePartitionKeys.add(new HivePartitionName(dbName, tblName, Table.TableType.HIVE, insertPartition.getValues()));
            }
        } catch (Exception e) {
            LOG.warn("The InsertEvent of the current hive version cannot be parsed, " +
                            "and there will be a corresponding Alter Event in next, InsertEvent is ignored here. {}",
                    e.getMessage());
            throw new MetastoreNotificationException(debugString("Unable to "
                    + "parse insert message"), e);
        }
    }

    public static List<MetastoreEvent> getEvents(NotificationEvent event, CacheUpdateProcessor metaCache) {
        try {
            return Lists.newArrayList(new InsertEvent(event, metaCache));
        } catch (MetastoreNotificationException e) {
            return Collections.emptyList();
        }
    }

    @Override
    public boolean canBeBatched(MetastoreEvent event) {
        return true;
    }

    @Override
    protected MetastoreEvent addToBatchEvents(MetastoreEvent event) {
        BatchEvent<MetastoreTableEvent> batchEvent = new BatchEvent<>(this);
        Preconditions.checkState(batchEvent.canBeBatched(event));
        batchEvent.addToBatchEvents(event);
        return batchEvent;
    }

    @Override
    protected boolean existInCache() {
        // TODO(stephen): refactor this function
        return false;
    }

    @Override
    protected boolean isSupported() {
        return true;
    }

    public boolean isPartitionTbl() {
        return insertPartition != null;
    }

    @Override
    protected void process() throws MetastoreNotificationException {
        if (!existInCache()) {
            return;
        }

        try {
            // TODO(stephen): refactor this function
        } catch (Exception e) {
            LOG.error("Failed to process {} event, event detail msg: {}",
                    getEventType(), metastoreNotificationEvent, e);
            throw new MetastoreNotificationException(
                    debugString("Failed to process insert event"));
        }
    }
}
