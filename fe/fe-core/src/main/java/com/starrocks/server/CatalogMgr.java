// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.server;

import com.starrocks.analysis.CreateCatalogStmt;
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.CatalogName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.ConcurrentHashMap;

public class CatalogMgr {
    private static final Logger LOG = LogManager.getLogger(CatalogMgr.class);

    private final ConcurrentHashMap<CatalogName, Catalog> catalogs = new ConcurrentHashMap<>();

    public void createCatalog(CreateCatalogStmt stmt) {
        // create catalog
        // create connector
        // editLog
    }

    public void dropCatalog(String catalogName) {

    }

}
