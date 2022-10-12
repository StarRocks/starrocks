// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.external.hive.events;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Table;
import com.starrocks.external.hive.HiveMetaCache;
import com.starrocks.external.hive.HivePartitionName;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.messaging.AlterPartitionMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/**
 * MetastoreEvent for ALTER_PARTITION event type
 */
public class AlterPartitionEvent extends MetastoreTableEvent {
    private static final Logger LOG = LogManager.getLogger(AlterPartitionEvent.class);

    // the Partition object before alter operation, as parsed from the NotificationEvent
    private final Partition partitionBefore;
    // the Partition object after alter operation, as parsed from the NotificationEvent
    private final Partition partitionAfter;

    private AlterPartitionEvent(NotificationEvent event, HiveMetaCache metaCache) {
        super(event, metaCache);
        Preconditions.checkState(getEventType() == MetastoreEventType.ALTER_PARTITION);
        Preconditions.checkNotNull(event.getMessage());
        AlterPartitionMessage alterPartitionMessage =
                MetastoreEventsProcessor.getMessageDeserializer()
                        .getAlterPartitionMessage(event.getMessage());

        try {
            partitionBefore = Preconditions.checkNotNull(alterPartitionMessage.getPtnObjBefore());
            partitionAfter = Preconditions.checkNotNull(alterPartitionMessage.getPtnObjAfter());
            hmsTbl = alterPartitionMessage.getTableObj();
            hivePartitionKeys.clear();
            hivePartitionKeys.add(
                    new HivePartitionName(dbName, tblName, Table.TableType.HIVE, partitionAfter.getValues()));
        } catch (Exception e) {
            throw new MetastoreNotificationException(
                    debugString("Unable to parse the alter partition message"), e);
        }
    }

    public static List<MetastoreEvent> getEvents(NotificationEvent event, HiveMetaCache metaCache) {
        return Lists.newArrayList(new AlterPartitionEvent(event, metaCache));
    }

    @Override
    protected boolean canBeBatched(MetastoreEvent event) {
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
        return cache.partitionExistInCache(getHivePartitionKey());
    }

    @Override
    protected boolean canBeSkipped() {
        return false;
    }

    @Override
    protected boolean isSupported() {
        return true;
    }

    @Override
    protected void process() throws MetastoreNotificationException {
        if (!existInCache()) {
            LOG.warn("Partition [Resource: [{}], Table: [{}.{}]. Partition values: [{}] ] " +
                            "doesn't exist in cache on event id [{}]", cache.getResourceName(),
                    getDbName(), getTblName(), getHivePartitionKey().getPartitionValues(), getEventId());
            return;
        }

        if (canBeSkipped()) {
            infoLog("Not processing this event as it only modifies some partition "
                    + "parameters which can be ignored.");
            return;
        }

        try {
            cache.alterPartitionByEvent(getHivePartitionKey(), partitionAfter.getSd(), partitionAfter.getParameters());
        } catch (Exception e) {
            LOG.error("Failed to process {} event, event detail msg: {}",
                    getEventType(), metastoreNotificationEvent, e);
            throw new MetastoreNotificationException(
                    debugString("Failed to process alter partition event"));
        }
    }
}
