// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.external.hive.events;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.Table;
import com.starrocks.external.hive.HiveMetaCache;
import com.starrocks.external.hive.HivePartitionKeysKey;
import com.starrocks.external.hive.HivePartitionName;
import com.starrocks.external.hive.HiveTableKey;
import com.starrocks.external.hive.Utils;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.messaging.AddPartitionMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/**
 * MetastoreEvent for ADD_PARTITION event type
 */
public class AddPartitionEvent extends MetastoreTableEvent {
    private static final Logger LOG = LogManager.getLogger(AddPartitionEvent.class);

    private final Partition addedPartition;
    // partCols use to generate HivePartitionKeysKey
    private final List<Column> partCols;

    /**
     * Prevent instantiation from outside should use MetastoreEventFactory instead
     */
    private AddPartitionEvent(NotificationEvent event,
                              HiveMetaCache metaCache,
                              Partition addedPartition,
                              List<Column> partCols) {
        super(event, metaCache);
        Preconditions.checkState(getEventType().equals(MetastoreEventType.ADD_PARTITION));
        if (event.getMessage() == null) {
            throw new IllegalStateException(debugString("Event message is null"));
        }

        try {
            AddPartitionMessage addPartitionMessage =
                    MetastoreEventsProcessor.getMessageDeserializer()
                            .getAddPartitionMessage(event.getMessage());
            this.addedPartition = addedPartition;
            this.partCols = partCols;
            hmsTbl = addPartitionMessage.getTableObj();
            hivePartitionKeys.clear();
            hivePartitionKeys.add(
                    new HivePartitionName(dbName, tblName, Table.TableType.HIVE, addedPartition.getValues()));
        } catch (Exception ex) {
            throw new MetastoreNotificationException(ex);
        }
    }

    protected static List<MetastoreEvent> getEvents(NotificationEvent event,
                                                    HiveMetaCache metaCache,
                                                    List<Column> partCols) {
        List<MetastoreEvent> addPartitionEvents = Lists.newArrayList();
        try {
            AddPartitionMessage addPartitionMessage =
                    MetastoreEventsProcessor.getMessageDeserializer()
                            .getAddPartitionMessage(event.getMessage());
            addPartitionMessage.getPartitionObjs().forEach(partition ->
                    addPartitionEvents.add(new AddPartitionEvent(event, metaCache, partition, partCols)));
        } catch (Exception ex) {
            throw new MetastoreNotificationException(ex);
        }
        return addPartitionEvents;
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

    /**
     * If the table name of {@link AddPartitionEvent} exists in the hive cache,
     * then the {@link PartitionKey} of the table needs to be updated.
     */
    @Override
    protected boolean existInCache() {
        HiveTableKey tableKey = HiveTableKey.gen(dbName, tblName);
        return cache.tableExistInCache(tableKey);
    }

    @Override
    protected boolean isSupported() {
        return true;
    }

    @Override
    protected void process() throws MetastoreNotificationException {
        if (!existInCache()) {
            LOG.warn("Table [{}.{}.{}] doesn't exist in cache on event id: [{}]",
                    cache.getResourceName(), getDbName(), getTblName(), getEventId());
            return;
        }
        try {
            HivePartitionKeysKey partitionKeysKey =
                    new HivePartitionKeysKey(dbName, tblName, Table.TableType.HIVE, partCols);
            PartitionKey partitionKey = Utils.createPartitionKey(addedPartition.getValues(), partCols);
            cache.addPartitionKeyByEvent(partitionKeysKey, partitionKey, getHivePartitionKey());
        } catch (Exception e) {
            LOG.error("Failed to process {} event, event detail msg: {}",
                    getEventType(), metastoreNotificationEvent, e);
            throw new MetastoreNotificationException(
                    debugString("Failed to process add partition event"));
        }
    }
}
