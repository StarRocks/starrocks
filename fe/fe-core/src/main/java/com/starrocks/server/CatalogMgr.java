// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


package com.starrocks.server;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.ExternalCatalog;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.Resource;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.common.FeConstants;
import com.starrocks.common.io.Text;
import com.starrocks.common.proc.BaseProcResult;
import com.starrocks.common.proc.DbsProcDir;
import com.starrocks.common.proc.ExternalDbsProcDir;
import com.starrocks.common.proc.ProcDirInterface;
import com.starrocks.common.proc.ProcNodeInterface;
import com.starrocks.common.proc.ProcResult;
import com.starrocks.connector.Connector;
import com.starrocks.connector.ConnectorContext;
import com.starrocks.connector.ConnectorMgr;
import com.starrocks.connector.ConnectorTableId;
import com.starrocks.connector.ConnectorType;
import com.starrocks.persist.DropCatalogLog;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockID;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockWriter;
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
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import static com.starrocks.catalog.ResourceMgr.NEED_MAPPING_CATALOG_RESOURCES;
import static com.starrocks.connector.hive.HiveConnector.HIVE_METASTORE_URIS;
import static com.starrocks.server.CatalogMgr.ResourceMappingCatalog.getResourceMappingCatalogName;
import static com.starrocks.server.CatalogMgr.ResourceMappingCatalog.isResourceMappingCatalog;

public class CatalogMgr {
    private static final Logger LOG = LogManager.getLogger(CatalogMgr.class);
    private final Map<String, Catalog> catalogs = new HashMap<>();
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

    public void createCatalog(CreateCatalogStmt stmt) throws DdlException {
        createCatalog(stmt.getCatalogType(), stmt.getCatalogName(), stmt.getComment(), stmt.getProperties());
    }

    public void createCatalog(String type, String catalogName, String comment, Map<String, String> properties)
            throws DdlException {
        if (Strings.isNullOrEmpty(type)) {
            throw new DdlException("Missing properties 'type'");
        }

        readLock();
        try {
            Preconditions.checkState(!catalogs.containsKey(catalogName), "Catalog '%s' already exists", catalogName);
        } finally {
            readUnlock();
        }

        writeLock();
        try {
            Preconditions.checkState(!catalogs.containsKey(catalogName), "Catalog '%s' already exists", catalogName);
            Connector connector = connectorMgr.createConnector(new ConnectorContext(catalogName, type, properties));
            if (null == connector) {
                LOG.error("connector create failed. catalog [{}] encounter unknown catalog type [{}]", catalogName, type);
                throw new DdlException("connector create failed");
            }
            long id = isResourceMappingCatalog(catalogName) ?
                    ConnectorTableId.CONNECTOR_ID_GENERATOR.getNextId().asInt() :
                    GlobalStateMgr.getCurrentState().getNextId();
            Catalog catalog = new ExternalCatalog(id, catalogName, comment, properties);
            catalogs.put(catalogName, catalog);

            if (!isResourceMappingCatalog(catalogName)) {
                GlobalStateMgr.getCurrentState().getEditLog().logCreateCatalog(catalog);
            }
        } finally {
            writeUnLock();
        }
    }

    public void dropCatalog(DropCatalogStmt stmt) {
        String catalogName = stmt.getName();
        readLock();
        try {
            Preconditions.checkState(catalogs.containsKey(catalogName), "Catalog '%s' doesn't exist", catalogName);
        } finally {
            readUnlock();
        }

        writeLock();
        try {
            connectorMgr.removeConnector(catalogName);
            catalogs.remove(catalogName);
            if (!isResourceMappingCatalog(catalogName)) {
                DropCatalogLog dropCatalogLog = new DropCatalogLog(catalogName);
                GlobalStateMgr.getCurrentState().getEditLog().logDropCatalog(dropCatalogLog);
            }
        } finally {
            writeUnLock();
        }
    }

    // TODO @caneGuy we should put internal catalog into catalogmgr
    public boolean catalogExists(String catalogName) {
        if (catalogName.equalsIgnoreCase(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)) {
            return true;
        }

        readLock();
        try {
            if (catalogs.containsKey(catalogName)) {
                return true;
            }

            // TODO: Used for replay query dump which only supports `hive` catalog for now.
            if (FeConstants.isReplayFromQueryDump &&
                    catalogs.containsKey(getResourceMappingCatalogName(catalogName, "hive"))) {
                return true;
            }

            return false;
        } finally {
            readUnlock();
        }
    }

    public static boolean isInternalCatalog(String name) {
        return name.equalsIgnoreCase(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME);
    }

    public static boolean isInternalCatalog(long catalogId) {
        return catalogId == InternalCatalog.DEFAULT_INTERNAL_CATALOG_ID;
    }

    public static boolean isExternalCatalog(String name) {
        return !Strings.isNullOrEmpty(name) && !isInternalCatalog(name) && !isResourceMappingCatalog(name);
    }

    public void replayCreateCatalog(Catalog catalog) throws DdlException {
        String type = catalog.getType();
        String catalogName = catalog.getName();
        Map<String, String> config = catalog.getConfig();
        if (Strings.isNullOrEmpty(type)) {
            throw new DdlException("Missing properties 'type'");
        }

        // skip unsupport connector type
        if (!ConnectorType.isSupport(type)) {
            LOG.error("Replay catalog [{}] encounter unknown catalog type [{}], ignore it", catalogName, type);
            return;
        }

        readLock();
        try {
            Preconditions.checkState(!catalogs.containsKey(catalogName), "Catalog '%s' already exists", catalogName);
        } finally {
            readUnlock();
        }

        Connector connector = connectorMgr.createConnector(new ConnectorContext(catalogName, type, config));
        if (null == connector) {
            LOG.error("connector create failed. catalog [{}] encounter unknown catalog type [{}]", catalogName, type);
            throw new DdlException("connector create failed");
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
            if (!catalogs.containsKey(catalogName)) {
                LOG.error("Catalog [{}] doesn't exist, unsupport this catalog type, ignore it", catalogName);
                return;
            }
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
    }

    public long loadCatalogs(DataInputStream dis, long checksum) throws IOException, DdlException {
        int catalogCount = 0;
        try {
            String s = Text.readString(dis);
            SerializeData data = GsonUtils.GSON.fromJson(s, SerializeData.class);
            if (data != null) {
                if (data.catalogs != null) {
                    for (Catalog catalog : data.catalogs.values()) {
                        if (!isResourceMappingCatalog(catalog.getName())) {
                            replayCreateCatalog(catalog);
                        }
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

    public void loadResourceMappingCatalog() {
        LOG.info("start to replay resource mapping catalog");

        List<Resource> resources = GlobalStateMgr.getCurrentState().getResourceMgr().getNeedMappingCatalogResources();
        for (Resource resource : resources) {
            Map<String, String> properties = Maps.newHashMap(resource.getProperties());
            String type = resource.getType().name().toLowerCase(Locale.ROOT);
            if (!NEED_MAPPING_CATALOG_RESOURCES.contains(type)) {
                return;
            }

            String catalogName = getResourceMappingCatalogName(resource.getName(), type);
            properties.put("type", type);
            properties.put(HIVE_METASTORE_URIS, resource.getHiveMetastoreURIs());
            try {
                createCatalog(type, catalogName, "mapping " + type + " catalog", properties);
            } catch (Exception e) {
                LOG.error("Failed to load resource mapping inside catalog {}", catalogName, e);
            }
        }
        LOG.info("finished replaying resource mapping catalogs from resources");
    }

    public long saveCatalogs(DataOutputStream dos, long checksum) throws IOException {
        SerializeData data = new SerializeData();
        data.catalogs = catalogs.entrySet().stream()
                .filter(entry -> !isResourceMappingCatalog(entry.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

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

    public String getCatalogType(String catalogName) {
        if (isInternalCatalog(catalogName)) {
            return "internal";
        } else {
            return catalogs.get(catalogName).getType();
        }
    }

    public Catalog getCatalogByName(String name) {
        return catalogs.get(name);
    }

    public Optional<Catalog> getCatalogById(long id) {
        return catalogs.values().stream().filter(catalog -> catalog.getId() == id).findFirst();
    }

    public Map<String, Catalog> getCatalogs() {
        return new HashMap<>(catalogs);
    }

    public boolean checkCatalogExistsById(long id) {
        return catalogs.entrySet().stream().anyMatch(entry -> entry.getValue().getId() == id);
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

    public long getCatalogCount() {
        readLock();
        try {
            return catalogs.size();
        } finally {
            readUnlock();
        }
    }

    public class CatalogProcNode implements ProcDirInterface {
        private static final String DEFAULT_CATALOG_COMMENT =
                "An internal catalog contains this cluster's self-managed tables.";

        @Override
        public boolean register(String name, ProcNodeInterface node) {
            return false;
        }

        @Override
        public ProcNodeInterface lookup(String catalogName) throws AnalysisException {
            if (CatalogMgr.isInternalCatalog(catalogName)) {
                return new DbsProcDir(GlobalStateMgr.getCurrentState());
            }
            return new ExternalDbsProcDir(catalogName);
        }

        @Override
        public ProcResult fetchResult() {
            BaseProcResult result = new BaseProcResult();
            result.setNames(CATALOG_PROC_NODE_TITLE_NAMES);
            readLock();
            try {
                for (Map.Entry<String, Catalog> entry : catalogs.entrySet()) {
                    Catalog catalog = entry.getValue();
                    if (catalog == null || isResourceMappingCatalog(catalog.getName())) {
                        continue;
                    }
                    catalog.getProcNodeData(result);
                }
            } finally {
                readUnlock();
            }
            result.addRow(Lists.newArrayList(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, "Internal", DEFAULT_CATALOG_COMMENT));
            return result;
        }
    }

    public static class ResourceMappingCatalog {
        public static final String RESOURCE_MAPPING_CATALOG_PREFIX = "resource_mapping_inside_catalog_";

        public static boolean isResourceMappingCatalog(String catalogName) {
            return catalogName.startsWith(RESOURCE_MAPPING_CATALOG_PREFIX);
        }

        public static String getResourceMappingCatalogName(String resourceName, String type) {
            return (RESOURCE_MAPPING_CATALOG_PREFIX + type + "_" + resourceName).toLowerCase(Locale.ROOT);
        }

        public static String toResourceName(String catalogName, String type) {
            return isResourceMappingCatalog(catalogName) ?
                    catalogName.substring(RESOURCE_MAPPING_CATALOG_PREFIX.length() + type.length() + 1) : catalogName;
        }
    }

    public void save(DataOutputStream dos) throws IOException, SRMetaBlockException {
        Map<String, Catalog> serializedCatalogs = catalogs.entrySet().stream()
                .filter(entry -> !isResourceMappingCatalog(entry.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        int numJson = 1 + serializedCatalogs.size();
        SRMetaBlockWriter writer = new SRMetaBlockWriter(dos, SRMetaBlockID.CATALOG_MGR, numJson);

        writer.writeJson(serializedCatalogs.size());
        for (Catalog catalog : serializedCatalogs.values()) {
            writer.writeJson(catalog);
        }

        writer.close();
    }

    public void load(SRMetaBlockReader reader) throws IOException, SRMetaBlockException, SRMetaBlockEOFException {
        try {
            int serializedCatalogsSize = reader.readInt();
            for (int i = 0; i < serializedCatalogsSize; ++i) {
                Catalog catalog = reader.readJson(Catalog.class);
                replayCreateCatalog(catalog);
            }
            loadResourceMappingCatalog();
        } catch (DdlException e) {
            throw new IOException(e);
        }
    }
}
