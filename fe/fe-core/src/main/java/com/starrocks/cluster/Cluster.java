// This file is made available under Elastic License 2.0.
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
import com.starrocks.catalog.InfoSchemaDb;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * cluster only save db and user's id and name
 */
public class Cluster implements Writable {
    private static final Logger LOG = LogManager.getLogger(Cluster.class);

    private Long id;
    private String name;
    // backend which cluster own
    private Set<Long> backendIdSet = ConcurrentHashMap.newKeySet();

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

    public String getName() {
        return name;
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

    public List<Long> getBackendIdList() {
        return Lists.newArrayList(backendIdSet);
    }

    public void setBackendIdList(List<Long> backendIdList) {
        if (backendIdList == null) {
            return;
        }
        backendIdSet = ConcurrentHashMap.newKeySet();
        backendIdSet.addAll(backendIdList);
    }

    public void addBackend(long backendId) {
        backendIdSet.add(backendId);
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
        if (dbNames.contains(ClusterNamespace.getFullName(this.name, InfoSchemaDb.DATABASE_NAME))) {
            dbCount--;
        }

        out.writeInt(dbCount);
        // don't persist InfoSchemaDb meta
        for (String name : dbNames) {
            if (!name.equals(ClusterNamespace.getFullName(this.name, InfoSchemaDb.DATABASE_NAME))) {
                Text.writeString(out, name);
            } else {
                dbIds.remove(dbNameToIDs.get(name));
            }
        }

        String errMsg = String.format("%d vs %d, fatal error, Write cluster meta failed!",
                dbNames.size(), dbIds.size() + 1);
        // ensure we have removed InfoSchemaDb id
        Preconditions.checkState(dbNames.size() == dbIds.size() + 1, errMsg);

        out.writeInt(dbCount);
        for (long id : dbIds) {
            out.writeLong(id);
        }

        // For back compatible, write two zeros for linkDbNames and linkDbIds
        out.writeInt(0);
        out.writeInt(0);
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
            dbNames.add(Text.readString(in));
        }

        count = in.readInt();
        while (count-- > 0) {
            dbIds.add(in.readLong());
        }

        // For back compatible, write two zeros for linkDbNames and linkDbIds
        count = in.readInt();
        if (count > 0) {
            throw new IOException("linkDbNames in Cluster should be equal with 0, now is " + count);
        }
        count = in.readInt();
        if (count > 0) {
            throw new IOException("linkDbIds in Cluster should be equal with 0, now is " + count);
        }
    }
}
