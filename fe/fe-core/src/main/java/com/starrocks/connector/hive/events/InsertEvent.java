// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.connector.hive.events;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.catalog.HiveTable;
import com.starrocks.connector.hive.CacheUpdateProcessor;
import com.starrocks.connector.hive.HiveCommonStats;
import com.starrocks.connector.hive.HiveMetastoreApiConverter;
import com.starrocks.connector.hive.HivePartitionName;
import com.starrocks.connector.hive.HiveTableName;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
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

/**
 * Metastore event handler for INSERT events. Handles insert events at both table and partition scopes.
 * If partition is null, treat it as ALTER_TABLE event, otherwise as ALTER_PARTITION event.
 */
public class InsertEvent extends MetastoreTableEvent {
    private static final Logger LOG = LogManager.getLogger(InsertEvent.class);

    // Represents the partition for this insert. Null if the table is unpartitioned.
    private final Partition insertPartition;

    private InsertEvent(NotificationEvent event, CacheUpdateProcessor cacheProcessor, String catalogName) {
        super(event, cacheProcessor, catalogName);
        Preconditions.checkArgument(INSERT.equals(getEventType()));
        InsertMessage insertMessage =
                MetastoreEventsProcessor.getMessageDeserializer()
                        .getInsertMessage(event.getMessage());
        try {
            hmsTbl = Preconditions.checkNotNull(insertMessage.getTableObj());
            insertPartition = insertMessage.getPtnObj();
            if (insertPartition != null) {
                List<String> partitionColNames = hmsTbl.getPartitionKeys().stream()
                        .map(FieldSchema::getName).collect(Collectors.toList());
                hivePartitionNames.add(HivePartitionName.of(dbName, tblName,
                        FileUtils.makePartName(partitionColNames, insertPartition.getValues())));
            }
        } catch (Exception e) {
            LOG.warn("The InsertEvent of the current hive version cannot be parsed, " +
                            "and there will be a corresponding Alter Event in next, InsertEvent is ignored here. {}",
                    e.getMessage());
            throw new MetastoreNotificationException(debugString("Unable to "
                    + "parse insert message"), e);
        }
    }

    public static List<MetastoreEvent> getEvents(NotificationEvent event,
                                                 CacheUpdateProcessor cacheProcessor, String catalogName) {
        try {
            return Lists.newArrayList(new InsertEvent(event, cacheProcessor, catalogName));
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
            return cache.isPartitionPresent(getHivePartitionKey());
        } else {
            HiveTableName hiveTableName = HiveTableName.of(dbName, tblName);
            return cache.isTablePresent(hiveTableName);
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
            HiveTable hiveTable = HiveMetastoreApiConverter.toHiveTable(hmsTbl, catalogName);
            com.starrocks.connector.hive.Partition partition;
            HiveCommonStats hiveCommonStats;
            if (hiveTable.isUnPartitioned()) {
                partition = HiveMetastoreApiConverter.toPartition(hmsTbl.getSd(), hmsTbl.getParameters());
                hiveCommonStats = toHiveCommonStats(hmsTbl.getParameters());
                LOG.info("Start to process INSERT_EVENT on {}.{}.{}. Partition:[{}], HveCommonStats:[{}]",
                        catalogName, dbName, tblName, partition, hiveCommonStats);
                cache.refreshTableByEvent(hiveTable, hiveCommonStats, partition);
            } else {
                partition = HiveMetastoreApiConverter.toPartition(insertPartition.getSd(), insertPartition.getParameters());
                hiveCommonStats = toHiveCommonStats(insertPartition.getParameters());
                LOG.info("Start to process INSERT_EVENT event on {}.{}.{}.{}. Partition:[{}], HveCommonStats:[{}]",
                        catalogName, dbName, tblName, getHivePartitionKey(), partition, hiveCommonStats);
                cache.refreshPartitionByEvent(getHivePartitionKey(), hiveCommonStats, partition);
            }
        } catch (Exception e) {
            LOG.error("Failed to process {} event, event detail msg: {}",
                    getEventType(), metastoreNotificationEvent, e);
            throw new MetastoreNotificationException(
                    debugString("Failed to process insert event"));
        }
    }
}
