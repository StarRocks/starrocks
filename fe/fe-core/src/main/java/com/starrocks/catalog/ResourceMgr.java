// This file is made available under Elastic License 2.0.
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
import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.AlterResourceStmt;
import com.starrocks.analysis.CreateResourceStmt;
import com.starrocks.analysis.DropResourceStmt;
import com.starrocks.common.DdlException;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.common.proc.BaseProcResult;
import com.starrocks.common.proc.ProcNodeInterface;
import com.starrocks.common.proc.ProcResult;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.persist.DropResourceOperationLog;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.ConnectContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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

    @SerializedName(value = "nameToResource")
    private final HashMap<String, Resource> nameToResource = new HashMap<>();
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
        this.writeLock();
        try {
            Resource resource = Resource.fromStmt(stmt);
            String resourceName = stmt.getResourceName();
            if (nameToResource.putIfAbsent(resourceName, resource) != null) {
                throw new DdlException("Resource(" + resourceName + ") already exist");
            }
            // log add
            Catalog.getCurrentCatalog().getEditLog().logCreateResource(resource);
            LOG.info("create resource success. resource: {}", resource);
        } finally {
            this.writeUnLock();
        }
    }

    public void replayCreateResource(Resource resource) {
        nameToResource.put(resource.getName(), resource);
    }

    public void dropResource(DropResourceStmt stmt) throws DdlException {
        this.writeLock();
        try {
            String name = stmt.getResourceName();
            Resource resource = nameToResource.remove(name);
            if (resource == null) {
                throw new DdlException("Resource(" + name + ") does not exist");
            }

            onDropResource(resource);

            // log drop
            Catalog.getCurrentCatalog().getEditLog().logDropResource(new DropResourceOperationLog(name));
            LOG.info("drop resource success. resource name: {}", name);
        } finally {
            this.writeUnLock();
        }
    }

    public void replayDropResource(DropResourceOperationLog operationLog) {
        Resource resource = nameToResource.remove(operationLog.getName());
        onDropResource(resource);
    }

    private void onDropResource(Resource resource) {
        if (resource instanceof HiveResource) {
            Catalog.getCurrentCatalog().getHiveRepository().clearCache(resource.getName());
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

    public Resource getResource(String name) {
        this.readLock();
        try {
            return nameToResource.get(name);
        } finally {
            this.readUnlock();
        }
    }

    /**
     * alter resource statement only support external hive now .
     *
     * @param stmt
     * @throws DdlException
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

            if (resource instanceof HiveResource) {
                // 1. alter the resource properties
                // 2. clear the cache
                // 3. update the edit log
                ((HiveResource) resource).alterProperties(stmt.getProperties());
                Catalog.getCurrentCatalog().getHiveRepository().clearCache(resource.getName());
                Catalog.getCurrentCatalog().getEditLog().logCreateResource(resource);
            } else {
                throw new DdlException("Alter resource statement only support external hive now");
            }
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
                if (resource == null || !Catalog.getCurrentCatalog().getAuth().checkResourcePriv(
                        ConnectContext.get(), resource.getName(), PrivPredicate.SHOW)) {
                    continue;
                }
                resource.getProcNodeData(result);
            }
            return result;
        }
    }
}
