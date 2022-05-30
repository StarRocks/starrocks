// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.external.hive.events;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.catalog.HiveTable;
import com.starrocks.external.HiveMetaStoreTableUtils;
import com.starrocks.external.hive.HiveMetaCache;
import com.starrocks.external.hive.HiveTableKey;
import com.starrocks.external.hive.HiveTableName;
import com.starrocks.server.GlobalStateMgr;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.messaging.json.JSONAlterTableMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

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
    protected final boolean isSchemaChange;

    private AlterTableEvent(NotificationEvent event, HiveMetaCache metaCache) {
        super(event, metaCache);
        Preconditions.checkArgument(MetastoreEventType.ALTER_TABLE.equals(getEventType()));
        JSONAlterTableMessage alterTableMessage =
                (JSONAlterTableMessage) MetastoreEventsProcessor.getMessageDeserializer()
                        .getAlterTableMessage(event.getMessage());
        try {
            hmsTbl = Preconditions.checkNotNull(alterTableMessage.getTableObjBefore());
            tableAfter = Preconditions.checkNotNull(alterTableMessage.getTableObjAfter());
            tableBefore = Preconditions.checkNotNull(alterTableMessage.getTableObjBefore());
            isSchemaChange = isSchemaChange(tableBefore.getSd().getCols(), tableAfter.getSd().getCols());
        } catch (Exception e) {
            throw new MetastoreNotificationException(
                    debugString("Unable to parse the alter table message"), e);
        }
        // this is a rename event if either dbName or tblName of before and after object changed
        isRename = !hmsTbl.getDbName().equalsIgnoreCase(tableAfter.getDbName())
                || !hmsTbl.getTableName().equalsIgnoreCase(tableAfter.getTableName());
    }

    public static List<MetastoreEvent> getEvents(NotificationEvent event, HiveMetaCache metaCache) {
        return Lists.newArrayList(new AlterTableEvent(event, metaCache));
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
        HiveTableKey tableKey = HiveTableKey.gen(dbName, tblName);
        return cache.tableExistInCache(tableKey);
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
            return;
        }

        if (canBeSkipped()) {
            infoLog("Not processing this event as it only modifies some table parameters "
                    + "which can be ignored.");
            return;
        }

        try {
            if (isSchemaChange) {
                if (HiveMetaStoreTableUtils.isInternalCatalog(cache.getResourceName())) {
                    HiveTable srTable = GlobalStateMgr.getCurrentState().getMetastoreEventsProcessor()
                            .getHiveTable(cache.getResourceName(), dbName, tblName);
                    if (srTable == null) {
                        return;
                    }
                    srTable.refreshExternalTableSchema(tableAfter);
                } else {
                    cache.refreshConnectorTableSchema(HiveTableName.of(dbName, tblName));
                }
            }
            cache.alterTableByEvent(HiveTableKey.gen(dbName, tblName), getHivePartitionKey(),
                    tableAfter.getSd(), tableAfter.getParameters());
        } catch (Exception e) {
            LOG.error("Failed to process {} event, event detail msg: {}",
                    getEventType(), metastoreNotificationEvent, e);
            throw new MetastoreNotificationException(
                    debugString("Failed to process alter table event"));
        }
    }
}
