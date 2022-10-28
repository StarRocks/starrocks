// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.connector.hive.events;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.connector.hive.CacheUpdateProcessor;
<<<<<<< HEAD:fe/fe-core/src/main/java/com/starrocks/connector/hive/events/InsertEvent.java
import com.starrocks.connector.hive.HiveMetastoreApiConverter;
import com.starrocks.connector.hive.HivePartitionName;
import com.starrocks.connector.hive.HiveTableName;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
=======
import com.starrocks.external.hive.HiveMetastoreApiConverter;
import com.starrocks.external.hive.HivePartitionName;
import com.starrocks.external.hive.HiveTableName;
>>>>>>> 4ae77f3d0 (refactor hive meta incremental sync by events):fe/fe-core/src/main/java/com/starrocks/external/hive/events/InsertEvent.java
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.messaging.InsertMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static com.starrocks.connector.hive.HiveMetastoreApiConverter.toHiveCommonStats;
import static com.starrocks.connector.hive.events.MetastoreEventType.INSERT;

import static com.starrocks.external.hive.events.MetastoreEventType.INSERT;

/**
 * Metastore event handler for INSERT events. Handles insert events at both table and partition scopes.
 * If partition is null, treat it as ALTER_TABLE event, otherwise as ALTER_PARTITION event.
 */
public class InsertEvent extends MetastoreTableEvent {
    private static final Logger LOG = LogManager.getLogger(InsertEvent.class);

    // Represents the partition for this insert. Null if the table is unpartitioned.
    private final Partition insertPartition;

<<<<<<< HEAD:fe/fe-core/src/main/java/com/starrocks/connector/hive/events/InsertEvent.java
    private InsertEvent(NotificationEvent event, CacheUpdateProcessor cacheProcessor, String catalogName) {
        super(event, cacheProcessor, catalogName);
=======
    private InsertEvent(NotificationEvent event, CacheUpdateProcessor metaCache, String catalogName) {
        super(event, metaCache, catalogName);
>>>>>>> 4ae77f3d0 (refactor hive meta incremental sync by events):fe/fe-core/src/main/java/com/starrocks/external/hive/events/InsertEvent.java
        Preconditions.checkArgument(INSERT.equals(getEventType()));
        InsertMessage insertMessage =
                MetastoreEventsProcessor.getMessageDeserializer()
                        .getInsertMessage(event.getMessage());
        try {
            hmsTbl = Preconditions.checkNotNull(insertMessage.getTableObj());
            insertPartition = insertMessage.getPtnObj();
            if (insertPartition != null) {
<<<<<<< HEAD:fe/fe-core/src/main/java/com/starrocks/connector/hive/events/InsertEvent.java
                hivePartitionNames.clear();
                hivePartitionNames.add(new HivePartitionName(dbName, tblName,
                        Lists.newArrayList(FileUtils.makePartName(
                                hmsTbl.getPartitionKeys().stream()
                                        .map(FieldSchema::getName)
                                        .collect(Collectors.toList()), Lists.newArrayList(insertPartition.getValues())))));
=======
                hivePartitionKeys.clear();
                hivePartitionKeys.add(new HivePartitionName(dbName, tblName, insertPartition.getValues()));
>>>>>>> 4ae77f3d0 (refactor hive meta incremental sync by events):fe/fe-core/src/main/java/com/starrocks/external/hive/events/InsertEvent.java
            }
        } catch (Exception e) {
            LOG.warn("The InsertEvent of the current hive version cannot be parsed, " +
                            "and there will be a corresponding Alter Event in next, InsertEvent is ignored here. {}",
                    e.getMessage());
            throw new MetastoreNotificationException(debugString("Unable to "
                    + "parse insert message"), e);
        }
    }

<<<<<<< HEAD:fe/fe-core/src/main/java/com/starrocks/connector/hive/events/InsertEvent.java
    public static List<MetastoreEvent> getEvents(NotificationEvent event,
                                                 CacheUpdateProcessor cacheProcessor, String catalogName) {
        try {
            return Lists.newArrayList(new InsertEvent(event, cacheProcessor, catalogName));
=======
    public static List<MetastoreEvent> getEvents(NotificationEvent event, CacheUpdateProcessor metaCache, String catalogName) {
        try {
            return Lists.newArrayList(new InsertEvent(event, metaCache, catalogName));
>>>>>>> 4ae77f3d0 (refactor hive meta incremental sync by events):fe/fe-core/src/main/java/com/starrocks/external/hive/events/InsertEvent.java
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
        if (isPartitionTbl()) {
            List<String> partVals = insertPartition.getValues();
            HivePartitionName hivePartitionName = new HivePartitionName(dbName, tblName, partVals);
            return cache.existIncache(INSERT, hivePartitionName);
        } else {
            HiveTableName hiveTableName = HiveTableName.of(dbName, tblName);
            return cache.existIncache(INSERT, hiveTableName);
        }
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
            HivePartitionName hivePartitionName = new HivePartitionName(dbName, tblName, insertPartition.getValues());
            HiveTableName hiveTableName = HiveTableName.of(dbName, tblName);
<<<<<<< HEAD:fe/fe-core/src/main/java/com/starrocks/connector/hive/events/InsertEvent.java
            cache.refreshCacheByEvent(INSERT, hiveTableName, hivePartitionName,
                    toHiveCommonStats(hmsTbl.getParameters()),
                    HiveMetastoreApiConverter.toPartition(hmsTbl.getSd(), hmsTbl.getParameters()),
=======
            cache.alterCacheByEvent(INSERT, hiveTableName, hivePartitionName,
                    insertPartition.getParameters(), insertPartition.getSd(),
>>>>>>> 4ae77f3d0 (refactor hive meta incremental sync by events):fe/fe-core/src/main/java/com/starrocks/external/hive/events/InsertEvent.java
                    HiveMetastoreApiConverter.toHiveTable(hmsTbl, catalogName));
        } catch (Exception e) {
            LOG.error("Failed to process {} event, event detail msg: {}",
                    getEventType(), metastoreNotificationEvent, e);
            throw new MetastoreNotificationException(
                    debugString("Failed to process insert event"));
        }
    }
}
