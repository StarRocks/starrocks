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
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.MetastoreType;
import com.starrocks.credential.CloudConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class DeltaLakeMetadata implements ConnectorMetadata {
    private static final Logger LOG = LogManager.getLogger(DeltaLakeMetadata.class);
    private final String catalogName;
    private final DeltaMetastoreOperations deltaOps;
    private final HdfsEnvironment hdfsEnvironment;

    public DeltaLakeMetadata(HdfsEnvironment hdfsEnvironment, String catalogName, DeltaMetastoreOperations deltaOps) {
        this.hdfsEnvironment = hdfsEnvironment;
        this.catalogName = catalogName;
        this.deltaOps = deltaOps;
    }

    public String getCatalogName() {
        return catalogName;
    }

    @Override
    public Table.TableType getTableType() {
        return Table.TableType.DELTALAKE;
    }

    @Override
    public List<String> listDbNames() {
        return deltaOps.getAllDatabaseNames();
    }

    @Override
    public List<String> listTableNames(String dbName) {
        return deltaOps.getAllTableNames(dbName);
    }

    @Override
    public Database getDb(String dbName) {
        return deltaOps.getDb(dbName);
    }

    @Override
    public List<String> listPartitionNames(String databaseName, String tableName, long snapshotId) {
        return deltaOps.getPartitionKeys(databaseName, tableName);
    }

    @Override
    public Table getTable(String dbName, String tblName) {
        try {
            return deltaOps.getTable(dbName, tblName);
        } catch (Exception e) {
            LOG.error("Failed to get table {}.{}", dbName, tblName, e);
            return null;
        }
    }

    @Override
    public boolean tableExists(String dbName, String tblName) {
        return deltaOps.tableExists(dbName, tblName);
    }

    @Override
    public CloudConfiguration getCloudConfiguration() {
        return hdfsEnvironment.getCloudConfiguration();
    }

    public MetastoreType getMetastoreType() {
        return deltaOps.getMetastoreType();
    }
}
