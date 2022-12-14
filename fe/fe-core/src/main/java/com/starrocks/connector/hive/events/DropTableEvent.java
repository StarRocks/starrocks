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
        return cache.isTablePresent(HiveTableName.of(dbName, tableName));
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
            LOG.info("Start to process DROP_TABLE event on {}.{}.{}", catalogName, dbName, tblName);
            cache.invalidateTable(dbName, tableName, null);
        } catch (Exception e) {
            LOG.error("Failed to process {} event, event detail msg: {}",
                    getEventType(), metastoreNotificationEvent, e);
            throw new MetastoreNotificationException(
                    debugString("Failed to process alter table event"));
        }
    }
}
