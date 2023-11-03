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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/catalog/ResourceMgr.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.catalog;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.DdlException;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.common.proc.BaseProcResult;
import com.starrocks.common.proc.ProcNodeInterface;
import com.starrocks.common.proc.ProcResult;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.persist.DropResourceOperationLog;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.privilege.PrivilegeActions;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AlterResourceStmt;
import com.starrocks.sql.ast.CreateResourceStmt;
import com.starrocks.sql.ast.DropCatalogStmt;
import com.starrocks.sql.ast.DropResourceStmt;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import static com.starrocks.connector.hive.HiveConnector.HIVE_METASTORE_URIS;
import static com.starrocks.server.CatalogMgr.ResourceMappingCatalog.getResourceMappingCatalogName;

/**
 * Resource manager is responsible for managing external resources used by StarRocks.
 * For example, Spark/MapReduce used for ETL, Spark/GPU used for queries, HDFS/S3 used for external storage.
 * Now only support Spark.
 */
public class ResourceMgr implements Writable {
    private static final Logger LOG = LogManager.getLogger(ResourceMgr.class);

    public static final ImmutableList<String> RESOURCE_PROC_NODE_TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("Name").add("ResourceType").add("Key").add("Value")
            .build();

    public static final ImmutableList<String> NEED_MAPPING_CATALOG_RESOURCES = new ImmutableList.Builder<String>()
            .add("hive").add("hudi").add("iceberg")
            .build();

    @SerializedName(value = "nameToResource")
    private HashMap<String, Resource> nameToResource = new HashMap<>();
    private final ResourceProcNode procNode = new ResourceProcNode();
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();

    public ResourceMgr() {
    }

    private void readLock() {
        this.rwLock.readLock().lock();
    }

    private void readUnlock() {
        this.rwLock.readLock().unlock();
    }

    private void writeLock() {
        this.rwLock.writeLock().lock();
    }

    private void writeUnLock() {
        this.rwLock.writeLock().unlock();
    }

    public void createResource(CreateResourceStmt stmt) throws DdlException {
        Resource resource = Resource.fromStmt(stmt);

        this.writeLock();
        try {
            String resourceName = stmt.getResourceName();
            String typeName = resource.getType().name().toLowerCase(Locale.ROOT);
            if (resource.needMappingCatalog()) {
                try {
                    String catalogName = getResourceMappingCatalogName(resourceName, typeName);
                    GlobalStateMgr.getCurrentState().getCatalogMgr().createCatalog(
                            typeName, catalogName, "mapping " + typeName + " catalog", stmt.getProperties());
                } catch (Exception e) {
                    LOG.error("Failed to create mapping {} catalog {} failed", typeName, resource, e);
                    throw new DdlException("Failed to create mapping catalog " + resourceName + " failed, msg: " +
                            e.getMessage());
                }
            }

            if (nameToResource.putIfAbsent(resourceName, resource) != null) {
                throw new DdlException("Resource(" + resourceName + ") already exist");
            }

            // log add
            GlobalStateMgr.getCurrentState().getEditLog().logCreateResource(resource);
            LOG.info("create resource success. resource: {}", resource);
        } finally {
            this.writeUnLock();
        }
    }

    /**
     * Replay create or alter resource log
     * When we replay alter resource log
     * <p>1. Overwrite the resource </p>
     * <p>2. Clear cache in memory </p>
     */
    public void replayCreateResource(Resource resource) throws DdlException {
        if (resource.needMappingCatalog()) {
            String type = resource.getType().name().toLowerCase(Locale.ROOT);
            String catalogName = getResourceMappingCatalogName(resource.getName(), type);

            if (nameToResource.containsKey(resource.name)) {
                DropCatalogStmt dropCatalogStmt = new DropCatalogStmt(catalogName);
                GlobalStateMgr.getCurrentState().getCatalogMgr().dropCatalog(dropCatalogStmt);
            }

            Map<String, String> properties = Maps.newHashMap(resource.getProperties());
            properties.put("type", type);
            properties.put(HIVE_METASTORE_URIS, resource.getHiveMetastoreURIs());
            GlobalStateMgr.getCurrentState().getCatalogMgr().createCatalog(type, catalogName, "mapping catalog", properties);
        }

        this.writeLock();
        try {
            nameToResource.put(resource.getName(), resource);
        } finally {
            this.writeUnLock();
        }

        LOG.info("replay create/alter resource log success. resource name: {}", resource.getName());
    }

    public void dropResource(DropResourceStmt stmt) throws DdlException {
        this.writeLock();
        try {
            String name = stmt.getResourceName();
            Resource resource = nameToResource.remove(name);
            if (resource == null) {
                throw new DdlException("Resource(" + name + ") does not exist");
            }

            if (resource.needMappingCatalog()) {
                String catalogName = getResourceMappingCatalogName(resource.name, resource.type.name().toLowerCase(Locale.ROOT));
                DropCatalogStmt dropCatalogStmt = new DropCatalogStmt(catalogName);
                GlobalStateMgr.getCurrentState().getCatalogMgr().dropCatalog(dropCatalogStmt);
            }
            // log drop
            GlobalStateMgr.getCurrentState().getEditLog().logDropResource(new DropResourceOperationLog(name));
            LOG.info("drop resource success. resource name: {}", name);
        } finally {
            this.writeUnLock();
        }
    }

    public void replayDropResource(DropResourceOperationLog operationLog) {
        Resource resource = nameToResource.remove(operationLog.getName());
        if (resource.needMappingCatalog()) {
            String catalogName = getResourceMappingCatalogName(resource.name, resource.type.name().toLowerCase(Locale.ROOT));
            DropCatalogStmt dropCatalogStmt = new DropCatalogStmt(catalogName);
            GlobalStateMgr.getCurrentState().getCatalogMgr().dropCatalog(dropCatalogStmt);
        }
    }

    public boolean containsResource(String name) {
        this.readLock();
        try {
            return nameToResource.containsKey(name);
        } finally {
            this.readUnlock();
        }
    }

    public Set<String> getAllResourceName() {
        this.readLock();
        try {
            return nameToResource.keySet();
        } finally {
            this.readUnlock();
        }
    }

    public Resource getResource(String name) {
        this.readLock();
        try {
            return nameToResource.get(name);
        } finally {
            this.readUnlock();
        }
    }

    public List<Resource> getNeedMappingCatalogResources() {
        return nameToResource.values().stream()
                .filter(Resource::needMappingCatalog)
                .collect(Collectors.toList());
    }

    /**
     * alter resource statement only support external hive/hudi now .
     */
    public void alterResource(AlterResourceStmt stmt) throws DdlException {
        this.writeLock();
        try {
            // check if the target resource exists .
            String name = stmt.getResourceName();
            Resource resource = this.getResource(name);
            if (resource == null) {
                throw new DdlException("Resource(" + name + ") does not exist");
            }

            // 1. alter the resource properties
            // 2. clear the cache
            // 3. update the edit log
            if (resource instanceof HiveResource) {
                ((HiveResource) resource).alterProperties(stmt.getProperties());
            } else if (resource instanceof HudiResource) {
                ((HudiResource) resource).alterProperties(stmt.getProperties());
            } else if (resource instanceof IcebergResource) {
                ((IcebergResource) resource).alterProperties(stmt.getProperties());
            } else {
                throw new DdlException("Alter resource statement only support external hive/hudi/iceberg now");
            }

            if (resource.needMappingCatalog()) {
                String type = resource.getType().name().toLowerCase(Locale.ROOT);
                String catalogName = getResourceMappingCatalogName(resource.getName(), type);
                DropCatalogStmt dropCatalogStmt = new DropCatalogStmt(catalogName);
                GlobalStateMgr.getCurrentState().getCatalogMgr().dropCatalog(dropCatalogStmt);

                Map<String, String> properties = Maps.newHashMap(stmt.getProperties());
                properties.put("type", type);
                String uriInProperties = stmt.getProperties().get(HIVE_METASTORE_URIS);
                String uris = uriInProperties == null ? resource.getHiveMetastoreURIs() : uriInProperties;
                properties.put(HIVE_METASTORE_URIS, uris);
                GlobalStateMgr.getCurrentState().getCatalogMgr().createCatalog(type, catalogName, "mapping catalog", properties);
            }

            GlobalStateMgr.getCurrentState().getEditLog().logCreateResource(resource);
        } finally {
            this.writeUnLock();
        }
    }

    public int getResourceNum() {
        this.readLock();
        try {
            return nameToResource.size();
        } finally {
            this.readUnlock();
        }
    }

    public List<List<String>> getResourcesInfo() {
        this.readLock();
        try {
            return procNode.fetchResult().getRows();
        } finally {
            this.readUnlock();
        }
    }

    public ResourceProcNode getProcNode() {
        return procNode;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public static ResourceMgr read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, ResourceMgr.class);
    }

    public class ResourceProcNode implements ProcNodeInterface {
        @Override
        public ProcResult fetchResult() {
            BaseProcResult result = new BaseProcResult();
            result.setNames(RESOURCE_PROC_NODE_TITLE_NAMES);

            for (Map.Entry<String, Resource> entry : nameToResource.entrySet()) {
                Resource resource = entry.getValue();
                // Since `nameToResource.entrySet` may change after it is called, resource
                // may be dropped during `show resources`.So that we should do a null pointer
                // check here. If resource is not null then we should check resource privs.
                if (resource == null) {
                    continue;
                }
                if (GlobalStateMgr.getCurrentState().isUsingNewPrivilege()) {
                    if (!PrivilegeActions.checkAnyActionOnResource(ConnectContext.get(), resource.getName())) {
                        continue;
                    }
                } else {
                    if (!GlobalStateMgr.getCurrentState().getAuth().checkResourcePriv(
                            ConnectContext.get(), resource.getName(), PrivPredicate.SHOW)) {
                        continue;
                    }
                }
                resource.getProcNodeData(result);
            }
            return result;
        }
    }

    public long saveResources(DataOutputStream out, long checksum) throws IOException {
        write(out);
        return checksum;
    }

    public void loadResourcesV2(SRMetaBlockReader reader)
            throws IOException, SRMetaBlockException, SRMetaBlockEOFException {
        ResourceMgr data = reader.readJson(ResourceMgr.class);
        this.nameToResource = data.nameToResource;
    }
}
