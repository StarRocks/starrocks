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

package com.starrocks.server;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;
import com.starrocks.common.CloseableLock;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

// TemporaryTableMgr is used to manage all temporary tables in the cluster,
// all interfaces are thread-safe.
public class TemporaryTableMgr {
    private static final Logger LOG = LogManager.getLogger(TemporaryTableMgr.class);

    // TemporaryTableTable is used to manage all temporary tables created by a session,
    // all interfaces are thread-safe
    private static class TemporaryTableTable {
        // database id, table name, table id
        private Table<Long, String, Long> temporaryTables = HashBasedTable.create();

        private ReadWriteLock rwLock = new ReentrantReadWriteLock();
        private long createTime;

        public TemporaryTableTable(long createTime) {
            this.createTime = createTime;
        }

        public long getCreateTime() {
            return createTime;
        }

        public Long getTableId(Long databaseId, String tableName) {
            try (CloseableLock ignored = CloseableLock.lock(this.rwLock.readLock())) {
                return temporaryTables.get(databaseId, tableName);
            }
        }

        public void addTable(Long databaseId, String tableName, Long tableId) {
            try (CloseableLock ignored = CloseableLock.lock(this.rwLock.writeLock())) {
                Preconditions.checkArgument(!temporaryTables.contains(databaseId, tableName), "table already exists");
                temporaryTables.put(databaseId, tableName, tableId);
            }
        }

        public void removeTable(Long databaseId, String tableName) {
            try (CloseableLock ignored = CloseableLock.lock(this.rwLock.writeLock())) {
                temporaryTables.remove(databaseId, tableName);
            }
        }

        public void removeTables(Long databaseId) {
            try (CloseableLock ignored = CloseableLock.lock(this.rwLock.writeLock())) {
                temporaryTables.row(databaseId).clear();
            }
        }

        public Table<Long, String, Long> getAllTables() {
            try (CloseableLock ignored = CloseableLock.lock(this.rwLock.readLock())) {
                return HashBasedTable.create(temporaryTables);
            }
        }

        public List<String> listTables(long databaseId) {
            try (CloseableLock ignored = CloseableLock.lock(this.rwLock.readLock())) {
                Map<String, Long> row = temporaryTables.row(databaseId);
                if (row == null) {
                    return Lists.newArrayList();
                }
                return new ArrayList<>(row.keySet());
            }
        }

    }

    // session id -> TemporaryTableTable
    private Map<UUID, TemporaryTableTable> tablesMap = Maps.newConcurrentMap();
    private ReadWriteLock rwLock = new ReentrantReadWriteLock();

    public void addTemporaryTable(UUID sessionId, long databaseId, String tableName, long tableId) {
        try (CloseableLock ignored = CloseableLock.lock(rwLock.writeLock())) {
            tablesMap.computeIfAbsent(sessionId, k -> new TemporaryTableTable(System.nanoTime()));
        }
        TemporaryTableTable tables = tablesMap.get(sessionId);
        tables.addTable(databaseId, tableName, tableId);
        LOG.info("add temporary table, session[{}], db id[{}], table name[{}], table id[{}]",
                sessionId.toString(), databaseId, tableName, tableId);
    }

    public Long getTable(UUID sessionId, long databaseId, String tableName) {
        if (!tablesMap.containsKey(sessionId)) {
            return null;
        }
        return tablesMap.get(sessionId).getTableId(databaseId, tableName);
    }

    public boolean tableExists(UUID sessionId, long databaseId, String tblName) {
        if (!tablesMap.containsKey(sessionId)) {
            return false;
        }
        return tablesMap.get(sessionId).getTableId(databaseId, tblName) != null;
    }

    public void dropTemporaryTable(UUID sessionId, long databaseId, String tableName) {
        TemporaryTableTable tables = tablesMap.get(sessionId);
        if (tables == null) {
            return;
        }
        tables.removeTable(databaseId, tableName);
        LOG.info("drop temporary table, session[{}], db id[{}], table name[{}]",
                sessionId.toString(), databaseId, tableName);
    }

    public void dropTemporaryTables(UUID sessionId, long databaseId) {
        TemporaryTableTable tables = tablesMap.get(sessionId);
        if (tables == null) {
            return;
        }
        tables.removeTables(databaseId);
        LOG.info("drop all temporary tables on database[{}], session[{}]", databaseId, sessionId.toString());
    }

    public Table<Long, String, Long> getTemporaryTables(UUID sessionId) {
        TemporaryTableTable tables = tablesMap.get(sessionId);
        if (tables == null) {
            return HashBasedTable.create();
        }
        return tables.getAllTables();
    }

    public void removeTemporaryTables(UUID sessionId) {
        try (CloseableLock ignored = CloseableLock.lock(rwLock.writeLock())) {
            tablesMap.remove(sessionId);
        }
        LOG.info("remove all temporary tables in session[{}]", sessionId.toString());
    }

    public List<String> listTemporaryTables(UUID sessionId, long databaseId) {
        TemporaryTableTable tables = tablesMap.get(sessionId);
        if (tables == null) {
            return Lists.newArrayList();
        }
        return tables.listTables(databaseId);
    }

    // get all temporary tables under specific databases, return a Table<databaseId, tableId, sessionId>
    public Table<Long, Long, UUID> getAllTemporaryTables(Set<Long> requiredDatabaseIds) {
        Table<Long, Long, UUID> result = HashBasedTable.create();
        try (CloseableLock ignored = CloseableLock.lock(rwLock.readLock())) {
            tablesMap.forEach((sessionId, tables) -> {
                // db id -> table name -> table id
                Table<Long, String, Long> allTables = tables.getAllTables();
                for (Table.Cell<Long, String, Long> cell : allTables.cellSet()) {
                    if (requiredDatabaseIds.contains(cell.getRowKey())) {
                        result.put(cell.getRowKey(), cell.getValue(), sessionId);
                    }
                }
            });
        }
        return result;
    }

    public boolean sessionExists(UUID sessionId) {
        return tablesMap.containsKey(sessionId);
    }

    public Map<UUID, Long> listSessions() {
        Map<UUID, Long> result = Maps.newHashMap();
        try (CloseableLock ignored = CloseableLock.lock(rwLock.readLock())) {
            tablesMap.forEach((sessionId, tables) -> {
                result.put(sessionId, tables.getCreateTime());
            });
        }
        return result;
    }


    @VisibleForTesting
    public void clear() {
        if (tablesMap != null) {
            tablesMap.clear();
        }
    }
}
