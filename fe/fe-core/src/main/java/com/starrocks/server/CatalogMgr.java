// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.server;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.ExternalCatalog;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.common.DdlException;
import com.starrocks.common.io.Text;
import com.starrocks.common.proc.BaseProcResult;
import com.starrocks.common.proc.ProcNodeInterface;
import com.starrocks.common.proc.ProcResult;
import com.starrocks.connector.ConnectorContext;
import com.starrocks.connector.ConnectorMgr;
import com.starrocks.persist.DropCatalogLog;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.sql.ast.CreateCatalogStmt;
import com.starrocks.sql.ast.DropCatalogStmt;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.starrocks.connector.hive.HiveConnector.HIVE_METASTORE_URIS;

public class CatalogMgr {
    private static final Logger LOG = LogManager.getLogger(CatalogMgr.class);
    private final ConcurrentHashMap<String, Catalog> catalogs = new ConcurrentHashMap<>();
    private final ConnectorMgr connectorMgr;
    private final ReadWriteLock catalogLock = new ReentrantReadWriteLock();

    public static final ImmutableList<String> CATALOG_PROC_NODE_TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("Catalog").add("Type").add("Comment")
            .build();

    private final CatalogProcNode procNode = new CatalogProcNode();

    public CatalogMgr(ConnectorMgr connectorMgr) {
        Preconditions.checkNotNull(connectorMgr, "ConnectorMgr is null");
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

        readLock();
        try {
            Preconditions.checkState(!catalogs.containsKey(catalogName), "Catalog '%s' already exists", catalogName);
        } finally {
            readUnlock();
        }

        connectorMgr.createConnector(new ConnectorContext(catalogName, type, properties));
        long id = GlobalStateMgr.getCurrentState().getNextId();
        Catalog catalog = new ExternalCatalog(id, catalogName, comment, properties);

        writeLock();
        try {
            catalogs.put(catalogName, catalog);
        } finally {
            writeUnLock();
        }

        GlobalStateMgr.getCurrentState().getEditLog().logCreateCatalog(catalog);
    }

    public synchronized void dropCatalog(DropCatalogStmt stmt) {
        String catalogName = stmt.getName();
        readLock();
        try {
            Preconditions.checkState(catalogs.containsKey(catalogName), "Catalog '%s' doesn't exist", catalogName);
        } finally {
            readUnlock();
        }
        connectorMgr.removeConnector(catalogName);

        writeLock();
        try {
            catalogs.remove(catalogName);
        } finally {
            writeUnLock();
        }

        DropCatalogLog dropCatalogLog = new DropCatalogLog(catalogName);
        GlobalStateMgr.getCurrentState().getEditLog().logDropCatalog(dropCatalogLog);
    }


    // TODO @caneGuy we should put internal catalog into catalogmgr
    public boolean catalogExists(String catalogName) {
        if (catalogName.equalsIgnoreCase(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)) {
            return true;
        }

        readLock();
        try {
            return catalogs.containsKey(catalogName);
        } finally {
            readUnlock();
        }
    }

    public static boolean isInternalCatalog(String name) {
        return name.equalsIgnoreCase(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME);
    }

    public void replayCreateCatalog(Catalog catalog) throws DdlException {
        String type = catalog.getType();
        String catalogName = catalog.getName();
        Map<String, String> config = catalog.getConfig();
        if (Strings.isNullOrEmpty(type)) {
            throw new DdlException("Missing properties 'type'");
        }

        readLock();
        try {
            Preconditions.checkState(!catalogs.containsKey(catalogName), "Catalog '%s' already exists", catalogName);
        } finally {
            readUnlock();
        }

        if (connectorMgr.supportConnectorType.contains(type)) {
            connectorMgr.createConnector(new ConnectorContext(catalogName, type, config));
        }
        writeLock();
        try {
            catalogs.put(catalogName, catalog);
        } finally {
            writeUnLock();
        }
    }

    public void replayDropCatalog(DropCatalogLog log) {
        String catalogName = log.getCatalogName();
        readLock();
        try {
            Preconditions.checkState(catalogs.containsKey(catalogName), "Catalog '%s' doesn't exist", catalogName);
        } finally {
            readUnlock();
        }
        if (connectorMgr.supportConnectorType.contains(catalogs.get(catalogName).getType())) {
            connectorMgr.removeConnector(catalogName);
        }

        writeLock();
        try {
            catalogs.remove(catalogName);
        } finally {
            writeUnLock();
        }
    }

    public boolean existSameUrlCatalog(String url) {
        long hasSameUriCatalogNum =  catalogs.entrySet().stream()
                .filter(entry -> entry.getValue().getConfig().getOrDefault(HIVE_METASTORE_URIS, "").equals(url))
                .count();
        return hasSameUriCatalogNum > 1;
    }

    public long loadCatalogs(DataInputStream dis, long checksum) throws IOException, DdlException {
        int catalogCount = 0;
        try {
            String s = Text.readString(dis);
            SerializeData data = GsonUtils.GSON.fromJson(s, SerializeData.class);
            if (data != null) {
                if (data.catalogs != null) {
                    for (Catalog catalog : data.catalogs.values()) {
                        replayCreateCatalog(catalog);
                    }
                    catalogCount = data.catalogs.size();
                }
            }
            checksum ^= catalogCount;
            LOG.info("finished replaying CatalogMgr from image");
        } catch (EOFException e) {
            LOG.info("no CatalogMgr to replay.");
        }
        return checksum;
    }

    public long saveCatalogs(DataOutputStream dos, long checksum) throws IOException {
        SerializeData data = new SerializeData();
        data.catalogs = new HashMap<>(catalogs);
        checksum ^= data.catalogs.size();
        String s = GsonUtils.GSON.toJson(data);
        Text.writeString(dos, s);
        return checksum;
    }

    private static class SerializeData {
        @SerializedName("catalogs")
        public Map<String, Catalog> catalogs;

    }

    public List<List<String>> getCatalogsInfo() {
        return procNode.fetchResult().getRows();
    }

    public CatalogProcNode getProcNode() {
        return procNode;
    }

    private void readLock() {
        this.catalogLock.readLock().lock();
    }

    private void readUnlock() {
        this.catalogLock.readLock().unlock();
    }

    private void writeLock() {
        this.catalogLock.writeLock().lock();
    }

    private void writeUnLock() {
        this.catalogLock.writeLock().unlock();
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
                catalog.getProcNodeData(result);
            }
            return result;
        }
    }
}
