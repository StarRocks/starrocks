// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.external.hive.events;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.catalog.HiveTable;
import com.starrocks.external.HiveMetaStoreTableUtils;
import com.starrocks.external.hive.HiveMetaCache;
import com.starrocks.external.hive.HiveTableName;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.messaging.json.JSONDropTableMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/**
 * MetastoreEvent for DROP_TABLE event type
 */
public class DropTableEvent extends MetastoreTableEvent {
    private static final Logger LOG = LogManager.getLogger(DropTableEvent.class);
    private final String dbName;
    private final String tableName;

    private DropTableEvent(NotificationEvent event, HiveMetaCache metaCache) {
        super(event, metaCache);
        Preconditions.checkArgument(MetastoreEventType.DROP_TABLE.equals(getEventType()));
        JSONDropTableMessage dropTableMessage =
                (JSONDropTableMessage) MetastoreEventsProcessor.getMessageDeserializer()
                        .getDropTableMessage(event.getMessage());
        try {
            dbName = dropTableMessage.getDB();
            tableName = dropTableMessage.getTable();
        } catch (Exception e) {
            throw new MetastoreNotificationException(debugString(
                    "Could not parse event message. "
                            + "Check if %s is set to true in metastore configuration",
                    MetastoreEventsProcessor.HMS_ADD_THRIFT_OBJECTS_IN_EVENTS_CONFIG_KEY), e);
        }
    }

    public static List<MetastoreEvent> getEvents(NotificationEvent event, HiveMetaCache metaCache) {
        return Lists.newArrayList(new DropTableEvent(event, metaCache));
    }

    @Override
    protected boolean existInCache() {
        return cache.getTableFromCache(HiveTableName.of(dbName, tableName)) != null;
    }

    @Override
    protected boolean canBeSkipped() {
        return false;
    }

    protected boolean isSupported() {
        return !HiveMetaStoreTableUtils.isInternalCatalog(cache.getResourceName());
    }

    @Override
    protected void process() throws MetastoreNotificationException {
        if (!existInCache()) {
            return;
        }

        try {
            HiveTable table = (HiveTable) cache.getTableFromCache(HiveTableName.of(dbName, tableName));
            if (table == null) {
                return;
            }
            cache.clearCache(table.getHmsTableInfo());
        } catch (Exception e) {
            LOG.error("Failed to process {} event, event detail msg: {}",
                    getEventType(), metastoreNotificationEvent, e);
            throw new MetastoreNotificationException(
                    debugString("Failed to process alter table event"));
        }
    }

}
