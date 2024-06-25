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

import com.starrocks.catalog.InternalCatalog;
import com.starrocks.statistic.StatsConstants;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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

    public void addAccessItems(List<AccessLog> accessItems) {
        writeLock();

        try {
            for (AccessLog accessItem : accessItems) {
                catalogMapping.update(accessItem);
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

    public Optional<String> exportInsertSQL() {
        writeLock();
        try {
            if (estimateMemorySize == 0) {
                return Optional.empty();
            }

            SQLBuilder sqlBuilder =
                    new SQLBuilder(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, StatsConstants.STATISTICS_DB_NAME,
                            DataCacheCopilotConstants.DATACACHE_COPILOT_STATISTICS_TABLE_NAME);

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

                                    sqlBuilder.addAccessLog(catalogName, dbName, tblName, partitionName, columnName,
                                            accessTime, count);
                                }
                            }
                        }
                    }
                }
            }

            return Optional.of(sqlBuilder.build());
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

        protected void update(AccessLog accessItem) {
            String catalogName = accessItem.getCatalogName();
            DbMapping dbMapping = mapping.get(catalogName);
            if (dbMapping == null) {
                dbMapping = new DbMapping();
                mapping.put(catalogName, dbMapping);
                estimateMemorySize += catalogName.length();
            }
            dbMapping.update(accessItem);
        }

        protected void clear() {
            mapping.clear();
        }
    }

    private class DbMapping {
        private final Map<String, TblMapping> mapping = new HashMap<>();

        protected void update(AccessLog accessItem) {
            String dbName = accessItem.getDbName();
            TblMapping tblMapping = mapping.get(dbName);
            if (tblMapping == null) {
                tblMapping = new TblMapping();
                mapping.put(dbName, tblMapping);
                estimateMemorySize += dbName.length();
            }
            tblMapping.update(accessItem);
        }
    }

    private class TblMapping {
        private final Map<String, PartitionMapping> mapping = new HashMap<>();

        protected void update(AccessLog accessItem) {
            String tblName = accessItem.getTableName();
            PartitionMapping partitionMapping = mapping.get(tblName);
            if (partitionMapping == null) {
                partitionMapping = new PartitionMapping();
                mapping.put(tblName, partitionMapping);
                estimateMemorySize += tblName.length();
            }
            partitionMapping.update(accessItem);
        }
    }

    private class PartitionMapping {
        private final Map<String, ColumnMapping> mapping = new HashMap<>();

        protected void update(AccessLog accessItem) {
            String partitionName = accessItem.getPartitionName();
            ColumnMapping columnMapping = mapping.get(partitionName);
            if (columnMapping == null) {
                columnMapping = new ColumnMapping();
                mapping.put(partitionName, columnMapping);
                estimateMemorySize += partitionName.length();
            }
            columnMapping.update(accessItem);
        }
    }

    private class ColumnMapping {
        private final Map<String, AccessTimeMapping> mapping = new HashMap<>();

        protected void update(AccessLog accessItem) {
            String columnName = accessItem.getColumnName();
            AccessTimeMapping accessTimeMapping = mapping.get(columnName);
            if (accessTimeMapping == null) {
                accessTimeMapping = new AccessTimeMapping();
                mapping.put(columnName, accessTimeMapping);
                estimateMemorySize += columnName.length();
            }
            accessTimeMapping.update(accessItem);
        }
    }

    private class AccessTimeMapping {
        private final Map<Long, Long> mapping = new HashMap<>();

        protected void update(AccessLog accessItem) {
            long accessTime = accessItem.getAccessTimeSec();
            Long count = mapping.get(accessTime);
            if (count == null) {
                count = 0L;
                estimateMemorySize += 16;
            }
            mapping.put(accessTime, count + 1);
        }
    }

    private static class SQLBuilder {
        private final String targetCatalogName;
        private final String targetDbName;
        private final String targetTblName;
        private final List<String> values = new LinkedList<>();

        private SQLBuilder(String targetCatalogName, String targetDbName, String targetTblName) {
            this.targetCatalogName = targetCatalogName;
            this.targetDbName = targetDbName;
            this.targetTblName = targetTblName;
        }

        private void addAccessLog(String catalogName, String dbName, String tblName, String partitionName,
                                  String columnName, long accessTime, long count) {
            String s = String.format("('%s', '%s', '%s', '%s', '%s', from_unixtime(%d, 'yyyy-MM-dd HH:mm:ss'), %d)",
                    catalogName, dbName, tblName, partitionName, columnName, accessTime, count);
            values.add(s);
        }

        private String build() {
            StringBuilder sb = new StringBuilder();
            sb.append(String.format("INSERT INTO `%s`.`%s`.`%s` VALUES ", targetCatalogName, targetDbName,
                    targetTblName));
            sb.append(String.join(", ", values));
            return sb.toString();
        }
    }
}