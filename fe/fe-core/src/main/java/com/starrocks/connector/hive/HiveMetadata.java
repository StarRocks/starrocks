// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.connector.hive;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.common.DdlException;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.external.hive.HiveMetaCache;
import com.starrocks.external.hive.HiveTableName;
import com.starrocks.server.GlobalStateMgr;

import java.util.List;

public class HiveMetadata implements ConnectorMetadata {
    private final HiveMetaCache metaCache;

    public HiveMetadata(String resourceName) throws DdlException {
        metaCache = GlobalStateMgr.getCurrentState().getHiveRepository().getMetaCache(resourceName);
    }

    @Override
    public List<String> listDbNames() throws DdlException {
        return metaCache.getAllDatabaseNames();
    }

    @Override
    public Database getDb(String dbName) {
        return metaCache.getDb(dbName);
    }

    @Override
    public List<String> listTableNames(String dbName) throws DdlException {
        return metaCache.getAllTableNames(dbName);
    }

    @Override
    public Table getTable(String dbName, String tblName) {
        return metaCache.getTable(HiveTableName.of(dbName, tblName));
    }
}
