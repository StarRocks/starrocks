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
import com.google.common.collect.Maps;
import com.starrocks.connector.hive.CacheUpdateProcessor;
import com.starrocks.connector.hive.HivePartitionName;
import com.starrocks.connector.hive.HiveTableName;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.starrocks.server.CatalogMgr.ResourceMappingCatalog.isResourceMappingCatalog;

/**
 * Factory class to create various MetastoreEvents.
 */
public class MetastoreEventFactory implements EventFactory {
    private static final Logger LOG = LogManager.getLogger(MetastoreEventFactory.class);
    private final List<String> externalTables;

    public MetastoreEventFactory(List<String> externalTables) {
        this.externalTables = externalTables;
    }

    public boolean needToProcess(String catalogTableName) {
        return externalTables.contains(catalogTableName);
    }

    /**
     * For an {@link AddPartitionEvent} and {@link DropPartitionEvent} drop event,
     * we need to divide it into multiple events according to the number of partitions it processes.
     * It is convenient for creating batch tasks to parallel processing.
     */
    @Override
    public List<MetastoreEvent> get(NotificationEvent event, CacheUpdateProcessor cacheProcessor,
                                    String catalogName) {
        Preconditions.checkNotNull(event.getEventType());
        MetastoreEventType metastoreEventType = MetastoreEventType.from(event.getEventType());
        switch (metastoreEventType) {
            case CREATE_TABLE:
                return CreateTableEvent.getEvents(event, cacheProcessor, catalogName);
            case ALTER_TABLE:
                return AlterTableEvent.getEvents(event, cacheProcessor, catalogName);
            case DROP_TABLE:
                return DropTableEvent.getEvents(event, cacheProcessor, catalogName);
            case ALTER_PARTITION:
                return AlterPartitionEvent.getEvents(event, cacheProcessor, catalogName);
            case DROP_PARTITION:
                return DropPartitionEvent.getEvents(event, cacheProcessor, catalogName);
            case INSERT:
                return InsertEvent.getEvents(event, cacheProcessor, catalogName);
            default:
                // ignore all the unknown events by creating a IgnoredEvent
                return Lists.newArrayList(new IgnoredEvent(event, cacheProcessor, catalogName));
        }
    }

    List<MetastoreEvent> getFilteredEvents(List<NotificationEvent> events,
                                           CacheUpdateProcessor cacheProcessor, String catalogName) {
        List<MetastoreEvent> metastoreEvents = Lists.newArrayList();

        // Currently, the hive external table needs to be manually created in StarRocks to map with the hms table.
        // Therefore, it's necessary to filter the events pulled this time from the hms instance,
        // and the events of the tables that don't register in the fe MetastoreEventsProcessor need to be filtered out.
        for (NotificationEvent event : events) {
            String dbName = event.getDbName();
            String tableName = event.getTableName();
            if (isResourceMappingCatalog(catalogName)) {
                if (!needToProcess(String.join(".", catalogName, dbName, tableName))) {
                    LOG.warn("Table is null on catalog [{}], table [{}.{}]. Skipping notification event {}",
                            catalogName, event.getDbName(), event.getTableName(), event);
                    continue;
                }
            } else {
                if (!cacheProcessor.isTablePresent(HiveTableName.of(dbName, tableName))) {
                    LOG.warn("Table is null on catalog [{}], table [{}.{}]. Skipping notification event {}",
                            catalogName, event.getDbName(), event.getTableName(), event);
                    continue;
                }
            }

            metastoreEvents.addAll(get(event, cacheProcessor, catalogName));
        }

        List<MetastoreEvent> tobeProcessEvents = metastoreEvents.stream()
                .filter(MetastoreEvent::isSupported)
                .collect(Collectors.toList());

        if (tobeProcessEvents.isEmpty()) {
            LOG.warn("The metastore events to process is empty on catalog {}", catalogName);
            return Collections.emptyList();
        }

        return createBatchEvents(tobeProcessEvents);
    }

    /**
     * Create batch event tasks according to HivePartitionName to facilitate subsequent parallel processing.
     * For ADD_PARTITION and DROP_PARTITION, we directly override any events before that partition.
     * For a partition, it is meaningless to process any events before the drop partition.
     */
    List<MetastoreEvent> createBatchEvents(List<MetastoreEvent> events) {
        Map<HivePartitionName, MetastoreEvent> batchEvents = Maps.newHashMap();
        for (MetastoreEvent event : events) {
            MetastoreTableEvent metastoreTableEvent = (MetastoreTableEvent) event;
            HivePartitionName hivePartitionName = metastoreTableEvent.getHivePartitionName();
            switch (event.getEventType()) {
                case ADD_PARTITION:
                case DROP_PARTITION:
                    batchEvents.put(hivePartitionName, metastoreTableEvent);
                    break;
                case ALTER_PARTITION:
                case ALTER_TABLE:
                case INSERT:
                    MetastoreEvent batchEvent = batchEvents.get(hivePartitionName);
                    if (batchEvent != null && batchEvent.canBeBatched(metastoreTableEvent)) {
                        batchEvents.put(hivePartitionName, batchEvent.addToBatchEvents(metastoreTableEvent));
                    } else {
                        batchEvents.put(hivePartitionName, metastoreTableEvent);
                    }
                    if (batchEvent instanceof AlterTableEvent && ((AlterTableEvent) batchEvent).isSchemaChange()) {
                        return Lists.newArrayList(batchEvents.values());
                    }
                    break;
                case DROP_TABLE:
                    String dbName = event.getDbName();
                    String tblName = event.getTblName();
                    batchEvents = batchEvents.entrySet().stream()
                            .filter(entry -> !entry.getKey().approximateMatchTable(dbName, tblName))
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                    batchEvents.put(hivePartitionName, metastoreTableEvent);
                    return Lists.newArrayList(batchEvents.values());
                default:
                    LOG.warn("Failed to create batch event on {}", event);
            }
        }
        return Lists.newArrayList(batchEvents.values());
    }
}
