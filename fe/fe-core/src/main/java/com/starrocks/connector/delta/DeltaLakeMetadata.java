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
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.hive.HiveMetastoreOperations;
import com.starrocks.credential.CloudConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class DeltaLakeMetadata implements ConnectorMetadata {
    private static final Logger LOG = LogManager.getLogger(DeltaLakeMetadata.class);
    private final String catalogName;
    private final HiveMetastoreOperations hmsOps;
    private final HdfsEnvironment hdfsEnvironment;

    public DeltaLakeMetadata(HdfsEnvironment hdfsEnvironment, String catalogName, HiveMetastoreOperations hmsOps) {
        this.hdfsEnvironment = hdfsEnvironment;
        this.catalogName = catalogName;
        this.hmsOps = hmsOps;
    }

    @Override
    public List<String> listDbNames() {
        return hmsOps.getAllDatabaseNames();
    }

    @Override
    public List<String> listTableNames(String dbName) {
        return hmsOps.getAllTableNames(dbName);
    }

    @Override
    public Database getDb(String dbName) {
        return hmsOps.getDb(dbName);
    }

    @Override
    public List<String> listPartitionNames(String databaseName, String tableName) {
        return hmsOps.getPartitionKeys(databaseName, tableName);
    }

    @Override
    public Table getTable(String dbName, String tblName) {
        try {
            Table table = hmsOps.getTable(dbName, tblName);
            if (table == null) {
                return null;
            }
            HiveTable hiveTable = (HiveTable) table;
            String path = hiveTable.getTableLocation();
            long createTime = table.getCreateTime();
            return DeltaUtils.convertDeltaToSRTable(catalogName, dbName, tblName, path, hdfsEnvironment.getConfiguration(),
                    createTime);
        } catch (Exception e) {
            LOG.warn(e.getMessage());
            return null;
        }
    }

    @Override
    public boolean tableExists(String dbName, String tblName) {
        return hmsOps.tableExists(dbName, tblName);
    }

    @Override
    public CloudConfiguration getCloudConfiguration() {
        return hdfsEnvironment.getCloudConfiguration();
    }
}
