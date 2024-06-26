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

package com.starrocks.datacache.copilot;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class DataCacheCopilotStorage {
    private static final DataCacheCopilotStorage STORAGE = new DataCacheCopilotStorage();

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final CatalogMapping catalogMapping = new CatalogMapping();
    private long estimateMemorySize = 0L;

    public static DataCacheCopilotStorage getInstance() {
        return STORAGE;
    }

    public void addAccessLogs(List<AccessLog> accessLogs) {
        writeLock();

        try {
            for (AccessLog accessLog : accessLogs) {
                catalogMapping.update(accessLog);
            }
        } finally {
            writeUnLock();
        }
    }

    public long getEstimateMemorySize() {
        readLock();

        try {
            return estimateMemorySize;
        } finally {
            readUnlock();
        }
    }

    public List<AccessLog> exportAccessLogs() {
        writeLock();
        try {
            List<AccessLog> accessLogs = new LinkedList<>();
            if (estimateMemorySize == 0) {
                return accessLogs;
            }

            // catalog level
            for (Map.Entry<String, DbMapping> catalogEntry : catalogMapping.mapping.entrySet()) {
                final String catalogName = catalogEntry.getKey();
                final DbMapping dbMapping = catalogEntry.getValue();
                // db level
                for (Map.Entry<String, TblMapping> dbEntry : dbMapping.mapping.entrySet()) {
                    final String dbName = dbEntry.getKey();
                    final TblMapping tblMapping = dbEntry.getValue();
                    // tbl level
                    for (Map.Entry<String, PartitionMapping> tblEntry : tblMapping.mapping.entrySet()) {
                        final String tblName = tblEntry.getKey();
                        final PartitionMapping partitionMapping = tblEntry.getValue();
                        // partition level
                        for (Map.Entry<String, ColumnMapping> partitionEntry : partitionMapping.mapping.entrySet()) {
                            final String partitionName = partitionEntry.getKey();
                            final ColumnMapping columnMapping = partitionEntry.getValue();
                            // column level
                            for (Map.Entry<String, AccessTimeMapping> columnEntry : columnMapping.mapping.entrySet()) {
                                String columnName = columnEntry.getKey();
                                final AccessTimeMapping accessTimeMapping = columnEntry.getValue();
                                // access time level
                                for (Map.Entry<Long, Long> accessTimeEntry : accessTimeMapping.mapping.entrySet()) {
                                    long accessTime = accessTimeEntry.getKey();
                                    long count = accessTimeEntry.getValue();

                                    accessLogs.add(
                                            new AccessLog(catalogName, dbName, tblName, partitionName, columnName,
                                                    accessTime, count));
                                }
                            }
                        }
                    }
                }
            }

            return accessLogs;
        } finally {
            catalogMapping.clear();
            estimateMemorySize = 0L;
            writeUnLock();
        }
    }

    private void readLock() {
        this.lock.readLock().lock();
    }

    private void readUnlock() {
        this.lock.readLock().unlock();
    }

    private void writeLock() {
        this.lock.writeLock().lock();
    }

    private void writeUnLock() {
        this.lock.writeLock().unlock();
    }

    private class CatalogMapping {
        private final Map<String, DbMapping> mapping = new HashMap<>();

        protected void update(AccessLog accessLog) {
            String catalogName = accessLog.getCatalogName();
            DbMapping dbMapping = mapping.get(catalogName);
            if (dbMapping == null) {
                dbMapping = new DbMapping();
                mapping.put(catalogName, dbMapping);
                estimateMemorySize += catalogName.length();
            }
            dbMapping.update(accessLog);
        }

        protected void clear() {
            mapping.clear();
        }
    }

    private class DbMapping {
        private final Map<String, TblMapping> mapping = new HashMap<>();

        protected void update(AccessLog accessLog) {
            String dbName = accessLog.getDbName();
            TblMapping tblMapping = mapping.get(dbName);
            if (tblMapping == null) {
                tblMapping = new TblMapping();
                mapping.put(dbName, tblMapping);
                estimateMemorySize += dbName.length();
            }
            tblMapping.update(accessLog);
        }
    }

    private class TblMapping {
        private final Map<String, PartitionMapping> mapping = new HashMap<>();

        protected void update(AccessLog accessLog) {
            String tblName = accessLog.getTableName();
            PartitionMapping partitionMapping = mapping.get(tblName);
            if (partitionMapping == null) {
                partitionMapping = new PartitionMapping();
                mapping.put(tblName, partitionMapping);
                estimateMemorySize += tblName.length();
            }
            partitionMapping.update(accessLog);
        }
    }

    private class PartitionMapping {
        private final Map<String, ColumnMapping> mapping = new HashMap<>();

        protected void update(AccessLog accessLog) {
            String partitionName = accessLog.getPartitionName();
            ColumnMapping columnMapping = mapping.get(partitionName);
            if (columnMapping == null) {
                columnMapping = new ColumnMapping();
                mapping.put(partitionName, columnMapping);
                estimateMemorySize += partitionName.length();
            }
            columnMapping.update(accessLog);
        }
    }

    private class ColumnMapping {
        private final Map<String, AccessTimeMapping> mapping = new HashMap<>();

        protected void update(AccessLog accessLog) {
            String columnName = accessLog.getColumnName();
            AccessTimeMapping accessTimeMapping = mapping.get(columnName);
            if (accessTimeMapping == null) {
                accessTimeMapping = new AccessTimeMapping();
                mapping.put(columnName, accessTimeMapping);
                estimateMemorySize += columnName.length();
            }
            accessTimeMapping.update(accessLog);
        }
    }

    private class AccessTimeMapping {
        private final Map<Long, Long> mapping = new HashMap<>();

        protected void update(AccessLog accessLog) {
            long accessTime = accessLog.getAccessTimeSec();
            Long count = mapping.get(accessTime);
            if (count == null) {
                count = accessLog.getCount();
                estimateMemorySize += 16;
            } else {
                count += accessLog.getCount();
            }
            mapping.put(accessTime, count);
        }
    }
}