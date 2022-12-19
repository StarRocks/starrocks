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
import com.starrocks.connector.hive.HiveCommonStats;
import com.starrocks.connector.hive.HiveMetastoreApiConverter;
import com.starrocks.connector.hive.HivePartitionName;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.messaging.AlterPartitionMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.stream.Collectors;

import static com.starrocks.connector.hive.HiveMetastoreApiConverter.toHiveCommonStats;
import static com.starrocks.connector.hive.events.MetastoreEventType.ALTER_PARTITION;

/**
 * MetastoreEvent for ALTER_PARTITION event type
 */
public class AlterPartitionEvent extends MetastoreTableEvent {
    private static final Logger LOG = LogManager.getLogger(AlterPartitionEvent.class);

    // the Partition object before alter operation, as parsed from the NotificationEvent
    private final Partition partitionBefore;
    // the Partition object after alter operation, as parsed from the NotificationEvent
    private final Partition partitionAfter;

    private AlterPartitionEvent(NotificationEvent event, CacheUpdateProcessor cacheProcessor, String catalogName) {
        super(event, cacheProcessor, catalogName);
        Preconditions.checkState(getEventType() == ALTER_PARTITION);
        Preconditions.checkNotNull(event.getMessage());
        AlterPartitionMessage alterPartitionMessage =
                MetastoreEventsProcessor.getMessageDeserializer()
                        .getAlterPartitionMessage(event.getMessage());

        try {
            partitionBefore = Preconditions.checkNotNull(alterPartitionMessage.getPtnObjBefore());
            partitionAfter = Preconditions.checkNotNull(alterPartitionMessage.getPtnObjAfter());
            hmsTbl = alterPartitionMessage.getTableObj();
            hivePartitionNames.clear();
            List<String> partitionColNames = hmsTbl.getPartitionKeys().stream()
                    .map(FieldSchema::getName).collect(Collectors.toList());
            hivePartitionNames.add(HivePartitionName.of(dbName, tblName,
                    FileUtils.makePartName(partitionColNames, partitionAfter.getValues())));
        } catch (Exception e) {
            throw new MetastoreNotificationException(
                    debugString("Unable to parse the alter partition message"), e);
        }
    }

    public static List<MetastoreEvent> getEvents(NotificationEvent event,
                                                 CacheUpdateProcessor cacheProcessor, String catalogName) {
        return Lists.newArrayList(new AlterPartitionEvent(event, cacheProcessor, catalogName));
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
        return cache.isPartitionPresent(getHivePartitionName());
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
            LOG.warn("Partition [Catalog: [{}], Table: [{}.{}]. Partition name: [{}] ] " +
                            "doesn't exist in cache on event id [{}]",
                    catalogName, dbName, tblName, getHivePartitionName(), getEventId());
            return;
        }

        if (canBeSkipped()) {
            infoLog("Not processing this event as it only modifies some partition "
                    + "parameters which can be ignored.");
            return;
        }

        try {
            com.starrocks.connector.hive.Partition partition = HiveMetastoreApiConverter.toPartition(
                    partitionAfter.getSd(), partitionAfter.getParameters());
            HiveCommonStats hiveCommonStats = toHiveCommonStats(partitionAfter.getParameters());

            LOG.info("Start to process ALTER_PARTITION event on [{}.{}.{}.{}]. Partition:[{}], HiveCommonStats:[{}]",
                    catalogName, dbName, tblName, getHivePartitionName(), partition, hiveCommonStats);

            cache.refreshPartitionByEvent(getHivePartitionName(), hiveCommonStats, partition);
        } catch (Exception e) {
            LOG.error("Failed to process {} event, event detail msg: {}",
                    getEventType(), metastoreNotificationEvent, e);
            throw new MetastoreNotificationException(
                    debugString("Failed to process alter partition event"));
        }
    }
}
