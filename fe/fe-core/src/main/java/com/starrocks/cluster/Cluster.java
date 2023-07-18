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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/cluster/Cluster.java

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

package com.starrocks.cluster;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.starrocks.catalog.system.SystemId;
import com.starrocks.catalog.system.information.InfoSchemaDb;
import com.starrocks.catalog.system.sys.SysDb;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.LinkDbInfo;
import com.starrocks.system.SystemInfoService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

// Now Cluster don't have read interface, in order to be back compatible.
// We will remove the persistent format later.
public class Cluster implements Writable {
    private static final Logger LOG = LogManager.getLogger(Cluster.class);

    private Long id;
    private String name;
    // backend which cluster own
    private Set<Long> backendIdSet = ConcurrentHashMap.newKeySet();
    // compute node which cluster own
    private Set<Long> computeNodeIdSet = ConcurrentHashMap.newKeySet();

    private ConcurrentHashMap<String, LinkDbInfo> linkDbNames = new ConcurrentHashMap<>();
    private ConcurrentHashMap<Long, LinkDbInfo> linkDbIds = new ConcurrentHashMap<>();

    private Set<Long> dbIds = ConcurrentHashMap.newKeySet();
    private Set<String> dbNames = ConcurrentHashMap.newKeySet();
    private ConcurrentHashMap<String, Long> dbNameToIDs = new ConcurrentHashMap<>();

    // lock to perform atomic operations
    private ReentrantLock lock = new ReentrantLock(true);

    private Cluster() {
        // for persist
    }

    public Cluster(String name, long id) {
        this.name = name;
        this.id = id;
    }

    private void lock() {
        this.lock.lock();
    }

    private void unlock() {
        this.lock.unlock();
    }

    public Long getId() {
        return id;
    }

    public void addDb(String name, long id) {
        if (Strings.isNullOrEmpty(name)) {
            return;
        }
        lock();
        try {
            dbNames.add(name);
            dbIds.add(id);
            dbNameToIDs.put(name, id);
        } finally {
            unlock();
        }
    }

    public void removeDb(String name, long id) {
        lock();
        try {
            dbNames.remove(name);
            dbIds.remove(id);
        } finally {
            unlock();
        }
    }

    // Just for check
    public boolean isEmpty() {
        return backendIdSet == null || backendIdSet.isEmpty();
    }

    public boolean isDefaultCluster() {
        return SystemInfoService.DEFAULT_CLUSTER.equalsIgnoreCase(name);
    }

    public List<Long> getComputeNodeIdList() {
        return Lists.newArrayList(computeNodeIdSet);
    }

    public void setBackendIdList(List<Long> backendIdList) {
        if (backendIdList == null) {
            return;
        }
        backendIdSet = ConcurrentHashMap.newKeySet();
        backendIdSet.addAll(backendIdList);
    }

    public void addComputeNode(long computeNodeId) {
        computeNodeIdSet.add(computeNodeId);
    }

    public void addBackend(long backendId) {
        backendIdSet.add(backendId);
    }

    public void removeComputeNode(long removedComputeNodeId) {
        computeNodeIdSet.remove((Long) removedComputeNodeId);
    }

    public void removeBackend(long removedBackendId) {
        backendIdSet.remove((Long) removedBackendId);
    }

    public static Cluster read(DataInput in) throws IOException {
        Cluster cluster = new Cluster();
        cluster.readFields(in);
        return cluster;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(id);
        Text.writeString(out, name);

        out.writeLong(backendIdSet.size());
        for (Long id : backendIdSet) {
            out.writeLong(id);
        }

        int dbCount = dbIds.size();
        if (dbNames.contains(InfoSchemaDb.DATABASE_NAME)) {
            dbCount--;
        }

        if (dbNames.contains(SysDb.DATABASE_NAME) &&
                dbNameToIDs.get(SysDb.DATABASE_NAME).equals(SystemId.SYS_DB_ID)) {
            dbCount--;
        }

        out.writeInt(dbCount);
        // don't persist InfoSchemaDb meta
        for (String name : dbNames) {
            if (name.equals(InfoSchemaDb.DATABASE_NAME)) {
                dbIds.remove(dbNameToIDs.get(name));
            } else if (name.equals(SysDb.DATABASE_NAME) &&
                    dbNameToIDs.get(SysDb.DATABASE_NAME).equals(SystemId.SYS_DB_ID)) {
                dbIds.remove(dbNameToIDs.get(name));
            } else {
                Text.writeString(out, ClusterNamespace.getFullName(name));
            }
        }

        String errMsg = String.format("%d vs %d, fatal error, Write cluster meta failed!", dbCount, dbIds.size());

        // ensure we have removed InfoSchemaDb id
        Preconditions.checkState(dbCount == dbIds.size(), errMsg);

        out.writeInt(dbCount);
        for (long id : dbIds) {
            out.writeLong(id);
        }

        out.writeInt(linkDbNames.size());
        for (Map.Entry<String, LinkDbInfo> infoMap : linkDbNames.entrySet()) {
            Text.writeString(out, infoMap.getKey());
            infoMap.getValue().write(out);
        }

        out.writeInt(linkDbIds.size());
        for (Map.Entry<Long, LinkDbInfo> infoMap : linkDbIds.entrySet()) {
            out.writeLong(infoMap.getKey());
            infoMap.getValue().write(out);
        }
    }

    public void readFields(DataInput in) throws IOException {
        id = in.readLong();
        name = Text.readString(in);
        Long len = in.readLong();
        while (len-- > 0) {
            Long id = in.readLong();
            backendIdSet.add(id);
        }
        int count = in.readInt();
        while (count-- > 0) {
            dbNames.add(ClusterNamespace.getNameFromFullName(Text.readString(in)));
        }

        count = in.readInt();
        while (count-- > 0) {
            dbIds.add(in.readLong());
        }

        count = in.readInt();
        while (count-- > 0) {
            final String key = Text.readString(in);
            final LinkDbInfo value = new LinkDbInfo();
            value.readFields(in);
            linkDbNames.put(key, value);
        }

        count = in.readInt();
        while (count-- > 0) {
            final long key = in.readLong();
            final LinkDbInfo value = new LinkDbInfo();
            value.readFields(in);
            linkDbIds.put(key, value);
        }
    }
}
