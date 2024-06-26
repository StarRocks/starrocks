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

public class AccessLog {
    private final String catalogName;
    private final String dbName;
    private final String tableName;
    private final String partitionName;
    private final String columnName;
    private final long accessTimeSec;
    private final long count;

    public AccessLog(String catalogName, String dbName, String tableName, String partitionName,
                     String columnName, long accessTimeSec) {
        this(catalogName, dbName, tableName, partitionName, columnName, accessTimeSec, 1);
    }

    public AccessLog(String catalogName, String dbName, String tableName, String partitionName,
                     String columnName, long accessTimeSec, long count) {
        this.catalogName = catalogName;
        this.dbName = dbName;
        this.tableName = tableName;
        this.partitionName = partitionName;
        this.columnName = columnName;
        this.accessTimeSec = accessTimeSec;
        this.count = count;
    }

    public String getCatalogName() {
        return catalogName;
    }

    public String getDbName() {
        return dbName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getColumnName() {
        return columnName;
    }

    public String getPartitionName() {
        return partitionName;
    }

    public long getAccessTimeSec() {
        return accessTimeSec;
    }

    public long getCount() {
        return count;
    }
}
