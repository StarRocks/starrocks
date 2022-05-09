// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.connector.hive;

import com.starrocks.common.DdlException;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.external.hive.HiveMetaCache;
import com.starrocks.server.GlobalStateMgr;

public class HiveMetadata implements ConnectorMetadata {
    private HiveMetaCache metaCache;

    public HiveMetadata(String resourceName) throws DdlException {
        metaCache = GlobalStateMgr.getCurrentState().getHiveRepository().getMetaCache(resourceName);
    }
}
