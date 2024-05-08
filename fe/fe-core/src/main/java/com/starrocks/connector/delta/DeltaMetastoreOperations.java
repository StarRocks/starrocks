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
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.Table;
import com.starrocks.connector.MetastoreType;
import com.starrocks.connector.metastore.IMetastore;
import org.apache.hadoop.conf.Configuration;

import java.util.List;

public class DeltaMetastoreOperations {
    private final String catalogName;
    private final IMetastore metastore;
    private final boolean enableCatalogLevelCache;
    private final Configuration hadoopConf;
    private final MetastoreType metastoreType;

    public DeltaMetastoreOperations(String catalogName, IMetastore metastore, boolean enableCatalogLevelCache,
                                    Configuration hadoopConf, MetastoreType metastoreType) {
        this.catalogName = catalogName;
        this.metastore = metastore;
        this.enableCatalogLevelCache = enableCatalogLevelCache;
        this.hadoopConf = hadoopConf;
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
        Table table = metastore.getTable(dbName, tableName);
        if (table == null) {
            return null;
        }
        if (table.isDeltalakeTable()) {
            return table;
        }
        HiveTable hiveTable = (HiveTable) table;
        String path = hiveTable.getTableLocation();
        long createTime = table.getCreateTime();
        return DeltaUtils.convertDeltaToSRTable(catalogName, dbName, table.getName(), path, hadoopConf, createTime);
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
}
