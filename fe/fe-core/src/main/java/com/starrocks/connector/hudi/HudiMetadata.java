// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.connector.hudi;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.common.DdlException;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.external.hive.HiveMetaCache;
import com.starrocks.external.hive.HiveTableName;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class HudiMetadata implements ConnectorMetadata {
    private static final Logger LOG = LogManager.getLogger(HudiMetadata.class);

    private HiveMetaCache metaCache = null;
    private final String resourceName;

    public HudiMetadata(String resourceName) {
        this.resourceName = resourceName;
    }

    // TODO(Stephen): normalize exception
    public HiveMetaCache getCache() throws DdlException {
        if (metaCache == null) {
            metaCache = GlobalStateMgr.getCurrentState().getHiveRepository().getMetaCache(resourceName);
        }
        return metaCache;
    }

    @Override
    public List<String> listDbNames() throws DdlException {
        return getCache().getAllDatabaseNames();
    }

    @Override
    public Database getDb(String dbName) {
        Database database;
        try {
            database = getCache().getDb(dbName);
        } catch (DdlException e) {
            LOG.error("Failed to get hive meta cache on {}.{}", resourceName, dbName);
            return null;
        }
        return database;
    }

    @Override
    public List<String> listTableNames(String dbName) throws DdlException {
        return getCache().getAllTableNames(dbName);
    }

    @Override
    public Table getTable(String dbName, String tblName) {
        Table table;
        try {
            table = getCache().getTable(HiveTableName.of(dbName, tblName));
        } catch (DdlException e) {
            LOG.error("Failed to get hive meta cache on {}.{}.{}", resourceName, dbName, tblName);
            return null;
        }

        return table;
    }
}
