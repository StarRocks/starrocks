package com.starrocks.spi;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Table;
import com.starrocks.common.DdlException;
import com.starrocks.external.hive.HiveMetaCache;
import com.starrocks.server.GlobalStateMgr;

import java.util.List;

public class HiveMetadata implements Metadata {
    private HiveMetaCache metaCache;


    public HiveMetadata(String resourceName) throws DdlException {
        metaCache = GlobalStateMgr.getCurrentState().getHiveRepository().getMetaCache(resourceName, false);
    }

    @Override
    public List<String> listDatabaseNames() throws DdlException {
        return metaCache.getAllDatabaseNames();
    }

    @Override
    public Table getTable(String dbName, String tblName) {
        try {
            return metaCache.getTable(dbName, tblName);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        return null;
    }

    @Override
    public List<String> listTableNames(String dbName) {
        try {
            return metaCache.getTableNames(dbName);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return Lists.newArrayList();
    }
}
