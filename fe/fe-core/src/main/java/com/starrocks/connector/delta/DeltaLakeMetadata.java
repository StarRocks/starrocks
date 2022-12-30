// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.connector.delta;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.Table;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.hive.HiveMetastoreOperations;
import org.apache.hadoop.conf.Configuration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class DeltaLakeMetadata implements ConnectorMetadata {
    private static final Logger LOG = LogManager.getLogger(DeltaLakeMetadata.class);
    private final Configuration configuration;
    private final String catalogName;
    private final HiveMetastoreOperations hmsOps;

    public DeltaLakeMetadata(Configuration configuration, String catalogName, HiveMetastoreOperations hmsOps) {
        this.configuration = configuration;
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
            HiveTable hiveTable = (HiveTable) table;
            String path = hiveTable.getTableLocation();
            return DeltaUtils.convertDeltaToSRTable(catalogName, dbName, tblName, path, configuration);
        } catch (Exception e) {
            LOG.warn(e.getMessage());
            return null;
        }
    }
}
