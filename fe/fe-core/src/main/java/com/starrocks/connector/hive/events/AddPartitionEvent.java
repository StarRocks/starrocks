// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.connector.hive.events;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.connector.hive.CacheUpdateProcessor;
import com.starrocks.connector.hive.HivePartitionName;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.messaging.AddPartitionMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.stream.Collectors;

/**
 * MetastoreEvent for ADD_PARTITION event type
 */
public class AddPartitionEvent extends MetastoreTableEvent {
    private static final Logger LOG = LogManager.getLogger(AddPartitionEvent.class);

    private final Partition addedPartition;

    /**
     * Prevent instantiation from outside should use MetastoreEventFactory instead
     */
    private AddPartitionEvent(NotificationEvent event,
                              CacheUpdateProcessor cacheProcessor,
                              Partition addedPartition,
                              String catalogName) {
        super(event, cacheProcessor, catalogName);
        Preconditions.checkState(getEventType().equals(MetastoreEventType.ADD_PARTITION));
        if (event.getMessage() == null) {
            throw new IllegalStateException(debugString("Event message is null"));
        }

        try {
            AddPartitionMessage addPartitionMessage =
                    MetastoreEventsProcessor.getMessageDeserializer()
                            .getAddPartitionMessage(event.getMessage());
            this.addedPartition = addedPartition;
            hmsTbl = addPartitionMessage.getTableObj();
            hivePartitionNames.clear();
            hivePartitionNames.add(new HivePartitionName(dbName, tblName,
                    Lists.newArrayList(FileUtils.makePartName(
                            hmsTbl.getPartitionKeys().stream()
                                    .map(FieldSchema::getName)
                                    .collect(Collectors.toList()), Lists.newArrayList(addedPartition.getValues())))));
        } catch (Exception ex) {
            throw new MetastoreNotificationException(ex);
        }
    }

    protected static List<MetastoreEvent> getEvents(NotificationEvent event,
                                                    CacheUpdateProcessor cacheProcessor,
                                                    String catalogName) {
        List<MetastoreEvent> addPartitionEvents = Lists.newArrayList();
        try {
            AddPartitionMessage addPartitionMessage =
                    MetastoreEventsProcessor.getMessageDeserializer()
                            .getAddPartitionMessage(event.getMessage());
            addPartitionMessage.getPartitionObjs().forEach(partition ->
                    addPartitionEvents.add(new AddPartitionEvent(event, cacheProcessor, partition, catalogName)));
        } catch (Exception ex) {
            throw new MetastoreNotificationException(ex);
        }
        return addPartitionEvents;
    }

    @Override
    protected void process() throws MetastoreNotificationException {
        throw new UnsupportedOperationException("Unsupported event type: " + getEventType());
    }
}
