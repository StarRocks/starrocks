// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.server;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.ExternalCatalog;
import com.starrocks.common.DdlException;
import com.starrocks.connector.ConnectorContext;
import com.starrocks.connector.ConnectorMgr;
import com.starrocks.persist.CreateCatalogLog;
import com.starrocks.persist.DropCatalogLog;
import com.starrocks.sql.ast.CreateCatalogStmt;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CatalogMgr {
    private static final Logger LOG = LogManager.getLogger(CatalogMgr.class);

    private final ConcurrentHashMap<String, Catalog> catalogs = new ConcurrentHashMap<>();
    private final ConnectorMgr connectorMgr;

    public CatalogMgr(ConnectorMgr connectorMgr) {
        this.connectorMgr = connectorMgr;
    }

    public synchronized void createCatalog(CreateCatalogStmt stmt) throws DdlException {
        String type = stmt.getCatalogType();
        String catalogName = stmt.getCatalogName();
        Map<String, String> properties = stmt.getProperties();
        if (Strings.isNullOrEmpty(type)) {
            throw new DdlException("Missing properties 'type'");
        }

        Preconditions.checkState(!catalogs.containsKey(catalogName), "Catalog '%s' already exists", catalogName);
        connectorMgr.createConnector(new ConnectorContext(catalogName, type, properties));
        Catalog catalog = new ExternalCatalog(catalogName, properties);
        catalogs.put(catalogName, catalog);
        // TODO edit log
    }

    public synchronized void dropCatalog(String catalogName) {
        Preconditions.checkState(catalogs.containsKey(catalogName), "Catalog '%s' doesn't exist", catalogName);
        connectorMgr.removeConnector(catalogName);
        catalogs.remove(catalogName);
        // TODO edit log
    }

    public boolean catalogExists(String catalogName) {
        return catalogs.containsKey(catalogName);
    }

    public void replayCreateCatalog(CreateCatalogLog log) throws DdlException {
        String type = log.getCatalogType();
        String catalogName = log.getCatalogName();
        Map<String, String> properties = log.getProperties();
        if (Strings.isNullOrEmpty(type)) {
            throw new DdlException("Missing properties 'type'");
        }

        Preconditions.checkState(!catalogs.containsKey(catalogName), "Catalog '%s' already exists", catalogName);
        connectorMgr.createConnector(new ConnectorContext(catalogName, type, properties));
        Catalog catalog = new ExternalCatalog(catalogName, properties);
        catalogs.put(catalogName, catalog);
    }

    public void replayDropCatalog(DropCatalogLog log) {
        String catalogName = log.getCatalogName();
        dropCatalog(catalogName);
    }
}
