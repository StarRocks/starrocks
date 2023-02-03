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

package com.starrocks.connector.persist;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Text;
import com.starrocks.persist.ConnectorTableInfoLog;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.spark_project.guava.collect.Maps;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * This class is used to add/remove/get the persistent metadata information of the external table
 * or the metadata information which need to load when generate a new external table.
 */
public class ConnectorTblPersistInfoMgr {
    private static final Logger LOG = LogManager.getLogger(ConnectorTblPersistInfoMgr.class);

    // catalogName -> dbName -> tableIdentifier -> ConnectorTableInfo
    @SerializedName(value = "PersistTableInfos")
    private Table<String, String, Map<String, ConnectorTableInfo>> persistTableInfos;

    private ReentrantReadWriteLock lock;

    public ConnectorTblPersistInfoMgr() {
        persistTableInfos = HashBasedTable.create();
        lock = new ReentrantReadWriteLock();
    }

    public ConnectorTableInfo getConnectorTableInfo(String catalog, String db, String tableIdentifier) {
        readLock();
        try {
            Map<String, ConnectorTableInfo> tableInfoMap = persistTableInfos.get(catalog, db);
            return tableInfoMap == null ? null : tableInfoMap.get(tableIdentifier);
        } finally {
            readUnlock();
        }
    }

    public void addPersistentConnectorTableInfo(String catalog, String db, String tableIdentifier,
                                                ConnectorTableInfo persistTableInfo) {
        addConnectorTableInfo(catalog, db, tableIdentifier, persistTableInfo);
        ConnectorTableInfoLog addConnectorTableInfoLog = new ConnectorTableInfoLog(ConnectorTableInfoLog.Operation.ADD,
                catalog, db, tableIdentifier, persistTableInfo);
        GlobalStateMgr.getCurrentState().getEditLog().logConnectorTablePersistInfo(addConnectorTableInfoLog);
    }

    public void removePersistentConnectorTableInfo(String catalog, String db, String tableIdentifier,
                                                   ConnectorTableInfo persistTableInfo) {
        if (removeConnectorTableInfo(catalog, db, tableIdentifier, persistTableInfo)) {
            ConnectorTableInfoLog removeConnectorTableInfoLog =
                    new ConnectorTableInfoLog(ConnectorTableInfoLog.Operation.DELETE,
                            catalog, db, tableIdentifier, persistTableInfo);
            GlobalStateMgr.getCurrentState().getEditLog().logConnectorTablePersistInfo(removeConnectorTableInfoLog);
        }
    }

    public void addConnectorTableInfo(String catalog, String db, String tableIdentifier,
                                       ConnectorTableInfo persistentTableInfo) {
        writeLock();
        try {
            Map<String, ConnectorTableInfo> tableInfoMap = persistTableInfos.get(catalog, db);
            if (tableInfoMap == null) {
                tableInfoMap = Maps.newHashMap();
            }

            ConnectorTableInfo tableInfo = tableInfoMap.get(tableIdentifier);
            if (tableInfo == null) {
                tableInfo = ConnectorTableInfo.builder().build();
            }
            tableInfo.addPersistentInfo(persistentTableInfo);

            tableInfoMap.put(tableIdentifier, tableInfo);
            persistTableInfos.put(catalog, db, tableInfoMap);
            LOG.info("{}.{}.{} add persistent connector table info : {}", catalog, db, tableIdentifier,
                    persistentTableInfo);
        } finally {
            writeUnlock();
        }
    }

    public boolean removeConnectorTableInfo(String catalog, String db, String tableIdentifier,
                                             ConnectorTableInfo persistentTableInfo) {
        writeLock();
        try {
            Map<String, ConnectorTableInfo> tableInfoMap = persistTableInfos.get(catalog, db);
            if (tableInfoMap == null) {
                return false;
            }

            ConnectorTableInfo tableInfo = tableInfoMap.get(tableIdentifier);
            if (tableInfo == null) {
                return false;
            }
            tableInfo.removePersistentInfo(persistentTableInfo);
            LOG.info("{}.{}.{} remove persistent connector table info : {}", catalog, db, tableIdentifier,
                    persistentTableInfo);
            return true;
        } finally {
            writeUnlock();
        }
    }

    public long saveConnectorTblPersistInfoMgr(DataOutput out, long checksum) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
        checksum ^= getChecksum();
        return checksum;
    }

    public static ConnectorTblPersistInfoMgr loadConnectorTblPersistInfoMgr(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, ConnectorTblPersistInfoMgr.class);
    }

    public long getChecksum() {
        return persistTableInfos.size();
    }

    public void replayConnectorTableInfo(ConnectorTableInfoLog tableInfoLog) {
        if (tableInfoLog.isAddOperation()) {
            addConnectorTableInfo(tableInfoLog.getCatalog(), tableInfoLog.getDb(), tableInfoLog.getTableIdentifier(),
                    tableInfoLog.getConnectorTableInfo());
        } else if (tableInfoLog.isDeleteOperation()) {
            removeConnectorTableInfo(tableInfoLog.getCatalog(), tableInfoLog.getDb(), tableInfoLog.getTableIdentifier(),
                    tableInfoLog.getConnectorTableInfo());
        }
        LOG.info("replay connector table info {}", tableInfoLog);
    }

    public void setTableInfoForConnectorTable(String catalog, String db,
                                              com.starrocks.catalog.Table table) {
        Preconditions.checkState(table != null);
        String tableIdentifier = table.getTableIdentifier();
        ConnectorTableInfo tableInfo = getConnectorTableInfo(catalog, db, tableIdentifier);
        if (tableInfo != null) {
            tableInfo.seTableInfoForConnectorTable(table);
        }
    }

    private void writeLock() {
        lock.writeLock().lock();
    }

    private void writeUnlock() {
        lock.writeLock().unlock();
    }

    private void readLock() {
        lock.readLock().lock();
    }

    private void readUnlock() {
        lock.readLock().unlock();
    }
}
