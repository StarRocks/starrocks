// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.external.hive.events;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.HiveTable;
import com.starrocks.common.DdlException;
import com.starrocks.external.HiveMetaStoreTableUtils;
import com.starrocks.external.hive.HiveMetaCache;
import com.starrocks.external.hive.HivePartitionName;
import com.starrocks.external.hive.HiveTableName;
import com.starrocks.server.GlobalStateMgr;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Factory class to create various MetastoreEvents.
 */
public class MetastoreEventFactory implements EventFactory {
    private static final Logger LOG = LogManager.getLogger(MetastoreEventFactory.class);

    /**
     * For an {@link AddPartitionEvent} and {@link DropPartitionEvent} drop event,
     * we need to divide it into multiple events according to the number of partitions it processes.
     * It is convenient for creating batch tasks to parallel processing.
     */
    @Override
    public List<MetastoreEvent> get(NotificationEvent event, HiveMetaCache metaCache, HiveTable table) {
        Preconditions.checkNotNull(event.getEventType());
        MetastoreEventType metastoreEventType = MetastoreEventType.from(event.getEventType());
        switch (metastoreEventType) {
            case CREATE_TABLE:
                return CreateTableEvent.getEvents(event, metaCache);
            case ALTER_TABLE:
                return AlterTableEvent.getEvents(event, metaCache);
            case DROP_TABLE:
                return DropTableEvent.getEvents(event, metaCache);
            case ADD_PARTITION:
                return AddPartitionEvent.getEvents(event, metaCache, table.getPartitionColumns());
            case ALTER_PARTITION:
                return AlterPartitionEvent.getEvents(event, metaCache);
            case DROP_PARTITION:
                return DropPartitionEvent.getEvents(event, metaCache, table.getPartitionColumns());
            case INSERT:
                return InsertEvent.getEvents(event, metaCache);
            default:
                // ignore all the unknown events by creating a IgnoredEvent
                return Lists.newArrayList(new IgnoredEvent(event, metaCache));
        }
    }

    List<MetastoreEvent> getFilteredEvents(List<NotificationEvent> events, String resourceName) {
        List<MetastoreEvent> metastoreEvents = Lists.newArrayList();
        HiveMetaCache metaCache = null;
        try {
            metaCache = GlobalStateMgr.getCurrentState().getHiveRepository().getMetaCache(resourceName);
        } catch (DdlException e) {
            LOG.error("Filed to get meta cache on resource [{}]", resourceName, e);
        }
        if (metaCache == null) {
            LOG.error("Meta cache is null on resource [{}]", resourceName);
            return metastoreEvents;
        }

        // Currently, the hive external table needs to be manually created in StarRocks to map with the hms table.
        // Therefore, it's necessary to filter the events pulled this time from the hms instance,
        // and the events of the tables that don't register in the fe MetastoreEventsProcessor need to be filtered out.
        for (NotificationEvent event : events) {
            HiveTable table;
            if (HiveMetaStoreTableUtils.isInternalCatalog(resourceName)) {
                table = GlobalStateMgr.getCurrentState().getMetastoreEventsProcessor()
                        .getHiveTable(resourceName, event.getDbName(), event.getTableName());
            } else {
                table = (HiveTable) metaCache.getTableFromCache(
                        HiveTableName.of(event.getDbName(), event.getTableName()));
            }

            if (table == null) {
                LOG.warn("Table is null on resource [{}], table [{}.{}]. Skipping notification event {}",
                        resourceName, event.getDbName(), event.getTableName(), event);
                continue;
            }
            metastoreEvents.addAll(get(event, metaCache, table));
        }

        List<MetastoreEvent> tobeProcessEvents = metastoreEvents.stream()
                .filter(MetastoreEvent::isSupported)
                .collect(Collectors.toList());

        if (tobeProcessEvents.isEmpty()) {
            LOG.warn("The metastore events to process is empty on resource {}", resourceName);
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
            HivePartitionName hivePartitionKey = metastoreTableEvent.getHivePartitionKey();
            switch (event.getEventType()) {
                case ADD_PARTITION:
                case DROP_PARTITION:
                    batchEvents.put(hivePartitionKey, metastoreTableEvent);
                    break;
                case ALTER_PARTITION:
                case ALTER_TABLE:
                case INSERT:
                    MetastoreEvent batchEvent = batchEvents.get(hivePartitionKey);
                    if (batchEvent != null && batchEvent.canBeBatched(metastoreTableEvent)) {
                        batchEvents.put(hivePartitionKey, batchEvent.addToBatchEvents(metastoreTableEvent));
                    } else {
                        batchEvents.put(hivePartitionKey, metastoreTableEvent);
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
                    batchEvents.put(hivePartitionKey, metastoreTableEvent);
                    return Lists.newArrayList(batchEvents.values());
                default:
                    LOG.warn("Failed to create batch event on {}", event);
            }
        }
        return Lists.newArrayList(batchEvents.values());
    }
}
