// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.server;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.ExternalCatalog;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.common.DdlException;
import com.starrocks.common.proc.BaseProcResult;
import com.starrocks.common.proc.ProcNodeInterface;
import com.starrocks.common.proc.ProcResult;
import com.starrocks.connector.ConnectorContext;
import com.starrocks.connector.ConnectorMgr;
import com.starrocks.persist.CreateCatalogLog;
import com.starrocks.persist.DropCatalogLog;
import com.starrocks.sql.ast.CreateCatalogStmt;
import com.starrocks.sql.ast.DropCatalogStmt;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class CatalogMgr {

    private final ConcurrentHashMap<String, Catalog> catalogs = new ConcurrentHashMap<>();
    private final ConnectorMgr connectorMgr;

    public static final ImmutableList<String> CATALOG_PROC_NODE_TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("Catalog").add("Type").add("Comment")
            .build();

    private final CatalogProcNode procNode = new CatalogProcNode();

    public CatalogMgr(ConnectorMgr connectorMgr) {
        this.connectorMgr = connectorMgr;
    }

    public synchronized void createCatalog(CreateCatalogStmt stmt) throws DdlException {
        String type = stmt.getCatalogType();
        String catalogName = stmt.getCatalogName();
        String comment = stmt.getComment();
        Map<String, String> properties = stmt.getProperties();
        if (Strings.isNullOrEmpty(type)) {
            throw new DdlException("Missing properties 'type'");
        }

        Preconditions.checkState(!catalogs.containsKey(catalogName), "Catalog '%s' already exists", catalogName);
        connectorMgr.createConnector(new ConnectorContext(catalogName, type, properties));
        Catalog catalog = new ExternalCatalog(catalogName, comment, properties);
        catalogs.put(catalogName, catalog);
        // TODO edit log
    }

    public synchronized void dropCatalog(DropCatalogStmt stmt) {
        String catalogName = stmt.getName();
        Preconditions.checkState(catalogs.containsKey(catalogName), "Catalog '%s' doesn't exist", catalogName);
        connectorMgr.removeConnector(catalogName);
        catalogs.remove(catalogName);
        // TODO edit log
    }

    // TODO @caneGuy we should put internal catalog into catalogmgr
    public boolean catalogExists(String catalogName) {
        return catalogName.equalsIgnoreCase(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME) ||
                catalogs.containsKey(catalogName);
    }

    public static boolean isInternalCatalog(String name) {
        return name.equalsIgnoreCase(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME);
    }

    public void replayCreateCatalog(CreateCatalogLog log) throws DdlException {
        String type = log.getCatalogType();
        String catalogName = log.getCatalogName();
        String comment = log.getComment();
        Map<String, String> properties = log.getProperties();
        if (Strings.isNullOrEmpty(type)) {
            throw new DdlException("Missing properties 'type'");
        }

        Preconditions.checkState(!catalogs.containsKey(catalogName), "Catalog '%s' already exists", catalogName);
        connectorMgr.createConnector(new ConnectorContext(catalogName, type, properties));
        Catalog catalog = new ExternalCatalog(catalogName, comment, properties);
        catalogs.put(catalogName, catalog);
    }

    public void replayDropCatalog(DropCatalogLog log) {
        String catalogName = log.getCatalogName();
        dropCatalog(new DropCatalogStmt(catalogName));
    }

    public List<List<String>> getCatalogsInfo() {
        return procNode.fetchResult().getRows();
    }

    public CatalogProcNode getProcNode() {
        return procNode;
    }

    public class CatalogProcNode implements ProcNodeInterface {
        @Override
        public ProcResult fetchResult() {
            BaseProcResult result = new BaseProcResult();
            result.setNames(CATALOG_PROC_NODE_TITLE_NAMES);
            for (Map.Entry<String, Catalog> entry : catalogs.entrySet()) {
                Catalog catalog = entry.getValue();
                if (catalog == null) {
                    continue;
                }
                ExternalCatalog externalCatalog = (ExternalCatalog) catalog;
                externalCatalog.getProcNodeData(result);
            }
            return result;
        }
    }
}
