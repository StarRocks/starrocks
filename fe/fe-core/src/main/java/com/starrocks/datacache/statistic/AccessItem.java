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

package com.starrocks.datacache.statistic;

import java.util.Objects;
import java.util.Optional;

public class AccessItem {
    private final Optional<String> partitionName;
    private final String catalogName;
    private final String dbName;
    private final String tableName;
    private final String columnName;
    // todo ignore it now
    private final long accessTime;
    private long accessFrequency = 0;

    public AccessItem(Optional<String> partitionName, String catalogName, String dbName, String tableName,
                      String columnName, long accessTime) {
        this.partitionName = partitionName;
        this.catalogName = catalogName;
        this.dbName = dbName;
        this.tableName = tableName;
        this.columnName = columnName;
        this.accessTime = accessTime;
    }

    public void setAccessFrequency(long accessFrequency) {
        this.accessFrequency = accessFrequency;
    }

    public long getAccessFrequency() {
        return accessFrequency;
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

    public Optional<String> getPartitionName() {
        return partitionName;
    }

    public long getAccessTime() {
        return accessTime;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof AccessItem)) {
            return false;
        }
        AccessItem that = (AccessItem) o;
        return Objects.equals(partitionName, that.partitionName) &&
                Objects.equals(catalogName, that.catalogName) && Objects.equals(dbName, that.dbName) &&
                Objects.equals(tableName, that.tableName) &&
                Objects.equals(columnName, that.columnName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(partitionName, catalogName, dbName, tableName, columnName);
    }
}
