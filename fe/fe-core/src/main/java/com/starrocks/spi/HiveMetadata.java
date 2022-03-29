package com.starrocks.spi;

import com.starrocks.common.DdlException;
import com.starrocks.external.hive.HiveMetaCache;
import com.starrocks.external.hive.HiveRepository;
import com.starrocks.server.GlobalStateMgr;

import java.util.List;

public class HiveMetadata implements Metadata {
    private HiveMetaCache metaCache;


    public HiveMetadata(String resourceName) throws DdlException {
        metaCache = GlobalStateMgr.getCurrentState().getHiveRepository().getMetaCache(resourceName);
    }

    @Override
    public List<String> listDatabaseNames() throws DdlException {
        return metaCache.getAllDatabaseNames();
    }
}
