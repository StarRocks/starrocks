// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.connector.hive.events;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.connector.hive.CacheUpdateProcessor;
import com.starrocks.connector.hive.HiveMetastoreApiConverter;
import com.starrocks.connector.hive.HivePartitionName;
import com.starrocks.connector.hive.HiveTableName;
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

<<<<<<< HEAD:fe/fe-core/src/main/java/com/starrocks/connector/hive/events/AlterPartitionEvent.java
    private AlterPartitionEvent(NotificationEvent event, CacheUpdateProcessor cacheProcessor, String catalogName) {
        super(event, cacheProcessor, catalogName);
=======
    private AlterPartitionEvent(NotificationEvent event, CacheUpdateProcessor metaCache, String catalogName) {
        super(event, metaCache, catalogName);
>>>>>>> 4ae77f3d0 (refactor hive meta incremental sync by events):fe/fe-core/src/main/java/com/starrocks/external/hive/events/AlterPartitionEvent.java
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
            hivePartitionNames.add(new HivePartitionName(dbName, tblName,
                    Lists.newArrayList(FileUtils.makePartName(
                            hmsTbl.getPartitionKeys().stream()
                                    .map(FieldSchema::getName)
                                    .collect(Collectors.toList()), Lists.newArrayList(partitionAfter.getValues())))));
        } catch (Exception e) {
            throw new MetastoreNotificationException(
                    debugString("Unable to parse the alter partition message"), e);
        }
    }

<<<<<<< HEAD:fe/fe-core/src/main/java/com/starrocks/connector/hive/events/AlterPartitionEvent.java
    public static List<MetastoreEvent> getEvents(NotificationEvent event,
                                                 CacheUpdateProcessor cacheProcessor, String catalogName) {
        return Lists.newArrayList(new AlterPartitionEvent(event, cacheProcessor, catalogName));
=======
    public static List<MetastoreEvent> getEvents(NotificationEvent event, CacheUpdateProcessor metaCache, String catalogName) {
        return Lists.newArrayList(new AlterPartitionEvent(event, metaCache, catalogName));
>>>>>>> 4ae77f3d0 (refactor hive meta incremental sync by events):fe/fe-core/src/main/java/com/starrocks/external/hive/events/AlterPartitionEvent.java
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
        return cache.existIncache(ALTER_PARTITION, getHivePartitionKey());
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
            LOG.warn("Partition [Catalog: [{}], Table: [{}.{}]. Partition values: [{}] ] " +
                            "doesn't exist in cache on event id [{}]", catalogName,
                    getDbName(), getTblName(), getHivePartitionKey().getPartitionValues(), getEventId());
            return;
        }

        if (canBeSkipped()) {
            infoLog("Not processing this event as it only modifies some partition "
                    + "parameters which can be ignored.");
            return;
        }

        try {
<<<<<<< HEAD:fe/fe-core/src/main/java/com/starrocks/connector/hive/events/AlterPartitionEvent.java
            cache.refreshCacheByEvent(ALTER_PARTITION, HiveTableName.of(getDbName(), getTblName()),
                    getHivePartitionKey(), toHiveCommonStats(hmsTbl.getParameters()),
                    HiveMetastoreApiConverter.toPartition(hmsTbl.getSd(), hmsTbl.getParameters()),
                    HiveMetastoreApiConverter.toHiveTable(hmsTbl, catalogName));
=======
            HiveTable updatedTable = HiveMetastoreApiConverter.toHiveTable(hmsTbl, catalogName);
            cache.alterCacheByEvent(ALTER_PARTITION, HiveTableName.of(getDbName(), getTblName()),
                    getHivePartitionKey(), partitionAfter.getParameters(), partitionAfter.getSd(), updatedTable);
>>>>>>> 4ae77f3d0 (refactor hive meta incremental sync by events):fe/fe-core/src/main/java/com/starrocks/external/hive/events/AlterPartitionEvent.java
        } catch (Exception e) {
            LOG.error("Failed to process {} event, event detail msg: {}",
                    getEventType(), metastoreNotificationEvent, e);
            throw new MetastoreNotificationException(
                    debugString("Failed to process alter partition event"));
        }
    }
}
