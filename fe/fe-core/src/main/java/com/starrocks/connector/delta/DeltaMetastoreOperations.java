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

package com.starrocks.connector.delta;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.connector.MetastoreType;

import java.util.List;

public class DeltaMetastoreOperations {
    private final CachingDeltaLakeMetastore metastore;
    private final boolean enableCatalogLevelCache;
    private final MetastoreType metastoreType;

    public DeltaMetastoreOperations(CachingDeltaLakeMetastore metastore, boolean enableCatalogLevelCache,
                                    MetastoreType metastoreType) {
        this.metastore = metastore;
        this.enableCatalogLevelCache = enableCatalogLevelCache;
        this.metastoreType = metastoreType;
    }

    public List<String> getAllDatabaseNames() {
        return metastore.getAllDatabaseNames();
    }

    public List<String> getAllTableNames(String dbName) {
        return metastore.getAllTableNames(dbName);
    }

    public Database getDb(String dbName) {
        return metastore.getDb(dbName);
    }

    public Table getTable(String dbName, String tableName) {
        return metastore.getTable(dbName, tableName);
    }

    public List<String> getPartitionKeys(String dbName, String tableName) {
        return metastore.getPartitionKeys(dbName, tableName);
    }

    public boolean tableExists(String dbName, String tableName) {
        return metastore.tableExists(dbName, tableName);
    }

    public MetastoreType getMetastoreType() {
        return metastoreType;
    }

    public void invalidateAll() {
        this.metastore.invalidateAll();
    }
}
