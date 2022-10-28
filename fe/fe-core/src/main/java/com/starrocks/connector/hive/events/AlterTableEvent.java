// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.connector.hive.events;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.catalog.HiveTable;
import com.starrocks.connector.hive.CacheUpdateProcessor;
<<<<<<< HEAD:fe/fe-core/src/main/java/com/starrocks/connector/hive/events/AlterTableEvent.java
import com.starrocks.connector.hive.HiveMetastoreApiConverter;
import com.starrocks.connector.hive.HivePartitionName;
import com.starrocks.connector.hive.HiveTableName;
=======
import com.starrocks.external.hive.HiveMetastoreApiConverter;
import com.starrocks.external.hive.HivePartitionName;
import com.starrocks.external.hive.HiveTableName;
>>>>>>> 4ae77f3d0 (refactor hive meta incremental sync by events):fe/fe-core/src/main/java/com/starrocks/external/hive/events/AlterTableEvent.java
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

<<<<<<< HEAD:fe/fe-core/src/main/java/com/starrocks/connector/hive/events/AlterTableEvent.java
    private AlterTableEvent(NotificationEvent event, CacheUpdateProcessor cacheProcessor, String catalogName) {
        super(event, cacheProcessor, catalogName);
=======
    private AlterTableEvent(NotificationEvent event, CacheUpdateProcessor metaCache, String catalogName) {
        super(event, metaCache, catalogName);
>>>>>>> 4ae77f3d0 (refactor hive meta incremental sync by events):fe/fe-core/src/main/java/com/starrocks/external/hive/events/AlterTableEvent.java
        Preconditions.checkArgument(MetastoreEventType.ALTER_TABLE.equals(getEventType()));
        JSONAlterTableMessage alterTableMessage =
                (JSONAlterTableMessage) MetastoreEventsProcessor.getMessageDeserializer()
                        .getAlterTableMessage(event.getMessage());
        try {
            hmsTbl = Preconditions.checkNotNull(alterTableMessage.getTableObjBefore());
            tableAfter = Preconditions.checkNotNull(alterTableMessage.getTableObjAfter());
            tableBefore = Preconditions.checkNotNull(alterTableMessage.getTableObjBefore());
            // ignore schema change on internal catalog's hive table
            if (isResourceMappingCatalog(catalogName)) {
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

<<<<<<< HEAD:fe/fe-core/src/main/java/com/starrocks/connector/hive/events/AlterTableEvent.java
    public static List<MetastoreEvent> getEvents(NotificationEvent event,
                                                 CacheUpdateProcessor cacheProcessor, String catalogName) {
        return Lists.newArrayList(new AlterTableEvent(event, cacheProcessor, catalogName));
=======
    public static List<MetastoreEvent> getEvents(NotificationEvent event, CacheUpdateProcessor metaCache, String catalogName) {
        return Lists.newArrayList(new AlterTableEvent(event, metaCache, catalogName));
>>>>>>> 4ae77f3d0 (refactor hive meta incremental sync by events):fe/fe-core/src/main/java/com/starrocks/external/hive/events/AlterTableEvent.java
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
        return cache.existIncache(getEventType(), HiveTableName.of(dbName, tblName));
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
<<<<<<< HEAD:fe/fe-core/src/main/java/com/starrocks/connector/hive/events/AlterTableEvent.java
<<<<<<< HEAD:fe/fe-core/src/main/java/com/starrocks/connector/hive/events/AlterTableEvent.java
                    catalogName, getDbName(), getTblName(), getEventId());
=======
                    cache.getCatalogName(), getDbName(), getTblName(), getEventId());
>>>>>>> bc43ed9ba (refactor hive meta incremental sync by events):fe/fe-core/src/main/java/com/starrocks/external/hive/events/AlterTableEvent.java
=======
                    catalogName, getDbName(), getTblName(), getEventId());
>>>>>>> 4ae77f3d0 (refactor hive meta incremental sync by events):fe/fe-core/src/main/java/com/starrocks/external/hive/events/AlterTableEvent.java
            return;
        }

        if (canBeSkipped()) {
            infoLog("Not processing this event as it only modifies some table parameters "
                    + "which can be ignored.");
            return;
        }

        try {
            HiveTable updatedTable = HiveMetastoreApiConverter.toHiveTable(tableAfter, catalogName);
<<<<<<< HEAD:fe/fe-core/src/main/java/com/starrocks/connector/hive/events/AlterTableEvent.java
            cache.refreshCacheByEvent(getEventType(), HiveTableName.of(dbName, tblName),
                    HivePartitionName.of(dbName, tblName, Lists.newArrayList()),
                    toHiveCommonStats(hmsTbl.getParameters()),
                    HiveMetastoreApiConverter.toPartition(hmsTbl.getSd(), hmsTbl.getParameters()), updatedTable);
=======
            cache.alterCacheByEvent(getEventType(), HiveTableName.of(dbName, tblName), updatedTable.isUnPartitioned() ?
                            HivePartitionName.of(dbName, tblName, Lists.newArrayList()) : getHivePartitionKey(),
                    tableAfter.getParameters(), tableAfter.getSd(), updatedTable);
>>>>>>> 4ae77f3d0 (refactor hive meta incremental sync by events):fe/fe-core/src/main/java/com/starrocks/external/hive/events/AlterTableEvent.java
        } catch (Exception e) {
            LOG.error("Failed to process {} event, event detail msg: {}",
                    getEventType(), metastoreNotificationEvent, e);
            throw new MetastoreNotificationException(
                    debugString("Failed to process alter table event"));
        }
    }
}
