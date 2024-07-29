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

import com.starrocks.catalog.DeltaLakeTable;
import com.starrocks.catalog.Table;
import com.starrocks.connector.CacheUpdateProcessor;
import com.starrocks.connector.DatabaseTableName;

import java.util.Set;
import java.util.concurrent.ExecutorService;

public class DeltaLakeCacheUpdateProcessor implements CacheUpdateProcessor {
    private final CachingDeltaLakeMetastore cachingMetastore;
    public DeltaLakeCacheUpdateProcessor(CachingDeltaLakeMetastore cachingMetastore) {
        this.cachingMetastore = cachingMetastore;
    }

    public void refreshTable(String dbName, Table table, boolean onlyCachedPartitions) {
        cachingMetastore.refreshTable(dbName, table.getName(), onlyCachedPartitions);
    }

    @Override
    public Set<DatabaseTableName> getCachedTableNames() {
        return cachingMetastore.getCachedTableNames();
    }

    @Override
    public void refreshTableBackground(Table table, boolean onlyCachedPartitions, ExecutorService executor) {
        DeltaLakeTable deltaLakeTable = (DeltaLakeTable) table;
        cachingMetastore.refreshTableBackground(deltaLakeTable.getDbName(), deltaLakeTable.getTableName());
    }

    public void invalidateAll() {
        cachingMetastore.invalidateAll();
    }

}
