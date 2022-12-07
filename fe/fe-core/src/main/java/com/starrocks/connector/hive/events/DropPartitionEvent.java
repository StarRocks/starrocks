// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


package com.starrocks.connector.hive.events;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.connector.hive.CacheUpdateProcessor;
import com.starrocks.connector.hive.HivePartitionName;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.messaging.DropPartitionMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.starrocks.connector.PartitionUtil.toHivePartitionName;
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
            List<String> partitionColNames = hmsTbl.getPartitionKeys().stream()
                    .map(FieldSchema::getName).collect(Collectors.toList());
            hivePartitionNames.add(HivePartitionName.of(
                    dbName, tblName, toHivePartitionName(partitionColNames, droppedPartition)));
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
        return cache.isPartitionPresent(getHivePartitionName());
    }

    @Override
    protected boolean isSupported() {
        return true;
    }

    @Override
    protected void process() throws MetastoreNotificationException {
        if (!existInCache()) {
            LOG.warn("Partition [Catalog: [{}], Table: [{}.{}]. Partition name: [{}] ] " +
                            "doesn't exist in cache on event id [{}]",
                    catalogName, dbName, tblName, getHivePartitionName(), getEventId());
            return;
        }

        try {
            LOG.info("Start to process DROP_PARTITION event on {}.{}.{}.{}",
                    catalogName, dbName, tblName, getHivePartitionName());
            cache.invalidatePartition(getHivePartitionName());
        } catch (Exception e) {
            LOG.error("Failed to process {} event, event detail msg: {}",
                    getEventType(), metastoreNotificationEvent, e);
            throw new MetastoreNotificationException(
                    debugString("Failed to process drop partition event"));
        }
    }
}
