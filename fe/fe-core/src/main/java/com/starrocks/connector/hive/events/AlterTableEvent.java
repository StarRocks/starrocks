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
import com.starrocks.catalog.HiveTable;
import com.starrocks.connector.hive.CacheUpdateProcessor;
import com.starrocks.connector.hive.HiveCommonStats;
import com.starrocks.connector.hive.HiveMetastoreApiConverter;
import com.starrocks.connector.hive.HiveTableName;
import com.starrocks.connector.hive.Partition;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.messaging.json.JSONAlterTableMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

import static com.starrocks.connector.hive.HiveMetastoreApiConverter.toHiveCommonStats;
import static com.starrocks.server.CatalogMgr.ResourceMappingCatalog.isResourceMappingCatalog;

/**
 * MetastoreEvent for ALTER_TABLE event type
 */
public class AlterTableEvent extends MetastoreTableEvent {
    private static final Logger LOG = LogManager.getLogger(AlterTableEvent.class);

    // the table object before alter operation, as parsed from the NotificationEvent
    protected Table tableBefore;
    // the table object after alter operation, as parsed from the NotificationEvent
    protected Table tableAfter;
    // true if this alter event was due to a rename operation
    protected final boolean isRename;
    // true if this alter event was due to a schema change operation
    protected boolean isSchemaChange = false;

    private AlterTableEvent(NotificationEvent event, CacheUpdateProcessor cacheProcessor, String catalogName) {
        super(event, cacheProcessor, catalogName);
        Preconditions.checkArgument(MetastoreEventType.ALTER_TABLE.equals(getEventType()));
        JSONAlterTableMessage alterTableMessage =
                (JSONAlterTableMessage) MetastoreEventsProcessor.getMessageDeserializer()
                        .getAlterTableMessage(event.getMessage());
        try {
            hmsTbl = Preconditions.checkNotNull(alterTableMessage.getTableObjBefore());
            tableAfter = Preconditions.checkNotNull(alterTableMessage.getTableObjAfter());
            tableBefore = Preconditions.checkNotNull(alterTableMessage.getTableObjBefore());
            // ignore schema change on internal catalog's hive table
            if (!isResourceMappingCatalog(catalogName)) {
                isSchemaChange = isSchemaChange(tableBefore.getSd().getCols(), tableAfter.getSd().getCols());
            }
        } catch (Exception e) {
            throw new MetastoreNotificationException(
                    debugString("Unable to parse the alter table message"), e);
        }
        // this is a rename event if either dbName or tblName of before and after object changed
        isRename = !hmsTbl.getDbName().equalsIgnoreCase(tableAfter.getDbName())
                || !hmsTbl.getTableName().equalsIgnoreCase(tableAfter.getTableName());
    }

    public static List<MetastoreEvent> getEvents(NotificationEvent event,
                                                 CacheUpdateProcessor cacheProcessor, String catalogName) {
        return Lists.newArrayList(new AlterTableEvent(event, cacheProcessor, catalogName));
    }

    private boolean isSchemaChange(List<FieldSchema> before, List<FieldSchema> after) {
        if (before.size() != after.size()) {
            return true;
        }

        if (!before.equals(after)) {
            return true;
        }

        return false;
    }

    public boolean isSchemaChange() {
        return isSchemaChange;
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
        if (isResourceMappingCatalog(catalogName)) {
            return true;
        } else {
            return cache.isTablePresent(HiveTableName.of(dbName, tblName));
        }
    }

    @Override
    protected boolean canBeSkipped() {
        return false;
    }

    @Override
    protected boolean isSupported() {
        return true;
    }

    public boolean isRename() {
        return isRename;
    }

    @Override
    protected void process() throws MetastoreNotificationException {
        if (!existInCache()) {
            LOG.warn("Table [{}.{}.{}] doesn't exist in cache on event id: [{}]",
                    catalogName, getDbName(), getTblName(), getEventId());
            return;
        }

        if (canBeSkipped()) {
            infoLog("Not processing this event as it only modifies some table parameters "
                    + "which can be ignored.");
            return;
        }

        try {
            HiveTable updatedTable = HiveMetastoreApiConverter.toHiveTable(tableAfter, catalogName);
            if (updatedTable.isUnPartitioned()) {
                HiveCommonStats hiveCommonStats = toHiveCommonStats(tableAfter.getParameters());
                Partition partition = HiveMetastoreApiConverter.toPartition(tableAfter.getSd(), tableAfter.getParameters());
                LOG.info("Start to process ALTER_TABLE event on {}.{}.{}. HveCommonStats:[{}], Partition:[{}]",
                        catalogName, dbName, tblName, hiveCommonStats, partition);
                cache.refreshTableByEvent(updatedTable, hiveCommonStats, partition);
            } else {
                LOG.info("Start to process ALTER_TABLE event on {}.{}.{}", catalogName, dbName, tblName);
                cache.refreshTableByEvent(updatedTable, null, null);
            }
        } catch (Exception e) {
            LOG.error("Failed to process {} event, event detail msg: {}",
                    getEventType(), metastoreNotificationEvent, e);
            throw new MetastoreNotificationException(
                    debugString("Failed to process alter table event"));
        }
    }
}
