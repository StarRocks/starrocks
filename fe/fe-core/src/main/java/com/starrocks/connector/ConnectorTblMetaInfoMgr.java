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

package com.starrocks.connector;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ConnectorTblMetaInfoMgr {
    private static final Logger LOG = LogManager.getLogger(ConnectorTblMetaInfoMgr.class);

    // catalogName -> dbName -> tableIdentifier -> ConnectorTableInfo
    private Table<String, String, Map<String, ConnectorTableInfo>> connectorTableMetaInfos;

    private ReentrantReadWriteLock lock;

    public ConnectorTblMetaInfoMgr() {
        connectorTableMetaInfos = HashBasedTable.create();
        lock = new ReentrantReadWriteLock();
    }

    public ConnectorTableInfo getConnectorTableInfo(String catalog, String db, String tableIdentifier) {
        readLock();
        try {
            Map<String, ConnectorTableInfo> tableInfoMap = connectorTableMetaInfos.get(catalog, db);
            return tableInfoMap == null ? null : tableInfoMap.get(tableIdentifier);
        } finally {
            readUnlock();
        }
    }

    public void addConnectorTableInfo(String catalog, String db, String tableIdentifier,
                                      ConnectorTableInfo connectorTableInfo) {
        writeLock();
        try {
            Map<String, ConnectorTableInfo> tableInfoMap = connectorTableMetaInfos.get(catalog, db);
            if (tableInfoMap == null) {
                tableInfoMap = Maps.newHashMap();
            }

            ConnectorTableInfo tableInfo = tableInfoMap.get(tableIdentifier);
            if (tableInfo == null) {
                tableInfo = ConnectorTableInfo.builder().build();
            }
            tableInfo.updateMetaInfo(connectorTableInfo);

            tableInfoMap.put(tableIdentifier, tableInfo);
            connectorTableMetaInfos.put(catalog, db, tableInfoMap);
            LOG.info("{}.{}.{} add persistent connector table info : {}", catalog, db, tableIdentifier,
                    connectorTableInfo);
        } finally {
            writeUnlock();
        }
    }

    public boolean removeConnectorTableInfo(String catalog, String db, String tableIdentifier,
                                            ConnectorTableInfo connectorTableInfo) {
        writeLock();
        try {
            Map<String, ConnectorTableInfo> tableInfoMap = connectorTableMetaInfos.get(catalog, db);
            if (tableInfoMap == null) {
                return false;
            }

            ConnectorTableInfo tableInfo = tableInfoMap.get(tableIdentifier);
            if (tableInfo == null) {
                return false;
            }
            tableInfo.removeMetaInfo(connectorTableInfo);
            LOG.info("{}.{}.{} remove persistent connector table info : {}", catalog, db, tableIdentifier,
                    connectorTableInfo);
            return true;
        } finally {
            writeUnlock();
        }
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