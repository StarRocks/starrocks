// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.connector.hive.events;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.connector.hive.CacheUpdateProcessor;
import com.starrocks.connector.hive.HiveMetastoreApiConverter;
import com.starrocks.connector.hive.HivePartitionName;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.messaging.DropPartitionMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.starrocks.connector.hive.events.MetastoreEventType.DROP_PARTITION;

/**
 * MetastoreEvent for DROP_PARTITION event type
 */
public class DropPartitionEvent extends MetastoreTableEvent {
    private static final Logger LOG = LogManager.getLogger(DropPartitionEvent.class);
    public static final String EVENT_TYPE = "DROP_PARTITION";

    private final Map<String, String> droppedPartition;

    private DropPartitionEvent(NotificationEvent event,
                               CacheUpdateProcessor cacheProcessor,
                               Map<String, String> droppedPartition,
                               String catalogName) {
        super(event, cacheProcessor, catalogName);
        Preconditions.checkState(getEventType().equals(DROP_PARTITION));
        Preconditions.checkNotNull(event.getMessage());
        DropPartitionMessage dropPartitionMessage =
                MetastoreEventsProcessor.getMessageDeserializer()
                        .getDropPartitionMessage(event.getMessage());
        try {
            hmsTbl = Preconditions.checkNotNull(dropPartitionMessage.getTableObj());
            Preconditions.checkNotNull(droppedPartition);
            this.droppedPartition = droppedPartition;
            hivePartitionNames.clear();
            hivePartitionNames.add(new HivePartitionName(dbName, tblName,
                    Lists.newArrayList(FileUtils.makePartName(
                            hmsTbl.getPartitionKeys().stream()
                                    .map(FieldSchema::getName)
                                    .collect(Collectors.toList()), Lists.newArrayList(droppedPartition.values())))));
        } catch (Exception ex) {
            throw new MetastoreNotificationException(
                    debugString("Could not parse drop event message. "), ex);
        }
    }

    protected static List<MetastoreEvent> getEvents(NotificationEvent event,
                                                    CacheUpdateProcessor cacheProcessor,
                                                    String catalogName) {
        DropPartitionMessage dropPartitionMessage =
                MetastoreEventsProcessor.getMessageDeserializer()
                        .getDropPartitionMessage(event.getMessage());
        List<MetastoreEvent> dropPartitionEvents = Lists.newArrayList();
        try {
            List<Map<String, String>> droppedPartitions = dropPartitionMessage.getPartitions();
            droppedPartitions.forEach(part ->
                    dropPartitionEvents.add(new DropPartitionEvent(event, cacheProcessor, part, catalogName)));
        } catch (Exception e) {
            throw new MetastoreNotificationException(e);
        }
        return dropPartitionEvents;
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
        return true;
    }

    @Override
    protected boolean isSupported() {
        return true;
    }

    @Override
    protected void process() throws MetastoreNotificationException {
        try {
            cache.refreshCacheByEvent(DROP_PARTITION, null, getHivePartitionKey(),
                    null, null, HiveMetastoreApiConverter.toHiveTable(hmsTbl, catalogName));
        } catch (Exception e) {
            LOG.error("Failed to process {} event, event detail msg: {}",
                    getEventType(), metastoreNotificationEvent, e);
            throw new MetastoreNotificationException(
                    debugString("Failed to process drop partition event"));
        }
    }
}
