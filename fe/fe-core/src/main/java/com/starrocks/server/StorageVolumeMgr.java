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
import com.staros.util.LockCloseable;
import com.starrocks.common.AlreadyExistsException;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.sql.ast.AlterStorageVolumeStmt;
import com.starrocks.sql.ast.CreateStorageVolumeStmt;
import com.starrocks.sql.ast.DropStorageVolumeStmt;
import com.starrocks.sql.ast.SetDefaultStorageVolumeStmt;
import com.starrocks.storagevolume.StorageVolume;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public abstract class StorageVolumeMgr {
    private static final String ENABLED = "enabled";

    protected String defaultSV = "";

    protected final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    // volume id to db
    protected Map<Long, Set<Long>> storageVolumeToDB = new HashMap<>();

    // volume id to table
    protected Map<Long, Set<Long>> storageVolumeToTable = new HashMap<>();

    public Long createStorageVolume(CreateStorageVolumeStmt stmt)
            throws AlreadyExistsException, AnalysisException, DdlException {
        Map<String, String> params = new HashMap<>();
        Optional<Boolean> enabled = parseProperties(stmt.getProperties(), params);
        return createStorageVolume(stmt.getName(), stmt.getStorageVolumeType(), stmt.getStorageLocations(), params,
                enabled, stmt.getComment());
    }

    public abstract Long createStorageVolume(String name, String svType, List<String> locations, Map<String, String> params,
                                    Optional<Boolean> enabled, String comment)
            throws AlreadyExistsException, AnalysisException, DdlException;

    public void removeStorageVolume(DropStorageVolumeStmt stmt) throws DdlException, AnalysisException {
        removeStorageVolume(stmt.getName());
    }

    public abstract void removeStorageVolume(String name) throws AnalysisException, DdlException;

    public void updateStorageVolume(AlterStorageVolumeStmt stmt) throws AnalysisException, DdlException {
        Map<String, String> params = new HashMap<>();
        Optional<Boolean> enabled = parseProperties(stmt.getProperties(), params);
        updateStorageVolume(stmt.getName(), params, enabled, stmt.getComment());
    }

    public abstract void updateStorageVolume(String name, Map<String, String> params, Optional<Boolean> enabled, String comment)
            throws AnalysisException, DdlException;

    public void setDefaultStorageVolume(SetDefaultStorageVolumeStmt stmt) throws AnalysisException, DdlException {
        setDefaultStorageVolume(stmt.getName());
    }

    public abstract void setDefaultStorageVolume(String svKey) throws AnalysisException, DdlException;

    public String getDefaultSV() {
        return defaultSV;
    }

    public abstract boolean exists(String svKey) throws DdlException;

    public abstract StorageVolume getStorageVolume(String svKey) throws AnalysisException;

    public abstract List<String> listStorageVolumeNames() throws DdlException;

    private Optional<Boolean> parseProperties(Map<String, String> properties, Map<String, String> params) {
        params.putAll(properties);
        Optional<Boolean> enabled = Optional.empty();
        if (params.containsKey(ENABLED)) {
            enabled = Optional.of(Boolean.parseBoolean(params.get(ENABLED)));
            params.remove(ENABLED);
        }
        return enabled;
    }

    public void bindDBToStorageVolume(Long svId, Long dbId) {
        try (LockCloseable lock = new LockCloseable(rwLock.writeLock())) {
            Set<Long> dbs = storageVolumeToDB.getOrDefault(svId, new HashSet<>());
            dbs.add(dbId);
            storageVolumeToDB.put(svId, dbs);
        }
    }

    public void unbindDBToStorageVolume(Long svId, Long dbId) {
        try (LockCloseable lock = new LockCloseable(rwLock.writeLock())) {
            Preconditions.checkState(storageVolumeToDB.containsKey(svId), "Storage volume does not exist");
            Set<Long> dbs = storageVolumeToDB.get(svId);
            dbs.remove(dbId);
            if (dbs.isEmpty()) {
                storageVolumeToDB.remove(svId);
            }
        }
    }

    public void bindTableToStorageVolume(Long svId, Long dbId) {
        try (LockCloseable lock = new LockCloseable(rwLock.writeLock())) {
            Set<Long> tables = storageVolumeToTable.getOrDefault(svId, new HashSet<>());
            tables.add(dbId);
            storageVolumeToTable.put(svId, tables);
        }
    }

    public void unbindTableToStorageVolume(Long svId, Long dbId) {
        try (LockCloseable lock = new LockCloseable(rwLock.writeLock())) {
            Preconditions.checkState(storageVolumeToTable.containsKey(svId), "Storage volume does not exist");
            Set<Long> tables = storageVolumeToTable.get(svId);
            tables.remove(dbId);
            if (tables.isEmpty()) {
                storageVolumeToTable.remove(svId);
            }
        }
    }
}
