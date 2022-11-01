// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.connector.hive.events;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.connector.hive.CacheUpdateProcessor;
import com.starrocks.connector.hive.HiveTableName;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.messaging.json.JSONDropTableMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

import static com.starrocks.connector.hive.events.MetastoreEventType.DROP_TABLE;
import static com.starrocks.server.CatalogMgr.ResourceMappingCatalog.isResourceMappingCatalog;

/**
 * MetastoreEvent for DROP_TABLE event type
 */
public class DropTableEvent extends MetastoreTableEvent {
    private static final Logger LOG = LogManager.getLogger(DropTableEvent.class);
    private final String dbName;
    private final String tableName;

    private DropTableEvent(NotificationEvent event,
                           CacheUpdateProcessor cacheProcessor, String catalogName) {
        super(event, cacheProcessor, catalogName);
        Preconditions.checkArgument(DROP_TABLE.equals(getEventType()));
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

    public static List<MetastoreEvent> getEvents(NotificationEvent event,
                                                 CacheUpdateProcessor cacheProcessor, String catalogName) {
        return Lists.newArrayList(new DropTableEvent(event, cacheProcessor, catalogName));
    }

    @Override
    protected boolean existInCache() {
        return cache.existIncache(DROP_TABLE, HiveTableName.of(dbName, tableName));
    }

    @Override
    protected boolean canBeSkipped() {
        return false;
    }

    protected boolean isSupported() {
        return !isResourceMappingCatalog(catalogName);
    }

    @Override
    protected void process() throws MetastoreNotificationException {
        if (!existInCache()) {
            return;
        }

        try {
            cache.refreshCacheByEvent(DROP_TABLE, HiveTableName.of(dbName, tblName), null, null, null, null);
        } catch (Exception e) {
            LOG.error("Failed to process {} event, event detail msg: {}",
                    getEventType(), metastoreNotificationEvent, e);
            throw new MetastoreNotificationException(
                    debugString("Failed to process alter table event"));
        }
    }
}
