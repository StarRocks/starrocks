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
import com.google.gson.annotations.SerializedName;
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

    @SerializedName("defaultStorageVolumeId")
    protected String defaultStorageVolumeId = "";

    protected final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    // volume id to dbs
    @SerializedName("storageVolumeToDbs")
    protected Map<String, Set<Long>> storageVolumeToDbs = new HashMap<>();

    // volume id to tables
    @SerializedName("storageVolumeToTables")
    protected Map<String, Set<Long>> storageVolumeToTables = new HashMap<>();

    public String createStorageVolume(CreateStorageVolumeStmt stmt)
            throws AlreadyExistsException, AnalysisException, DdlException {
        Map<String, String> params = new HashMap<>();
        Optional<Boolean> enabled = parseProperties(stmt.getProperties(), params);
        return createStorageVolume(stmt.getName(), stmt.getStorageVolumeType(), stmt.getStorageLocations(), params,
                enabled, stmt.getComment());
    }

    public String createStorageVolume(String name, String svType, List<String> locations, Map<String, String> params,
                                    Optional<Boolean> enabled, String comment)
            throws DdlException, AlreadyExistsException, AnalysisException {
        try (LockCloseable lock = new LockCloseable(rwLock.writeLock())) {
            if (exists(name)) {
                throw new AlreadyExistsException(String.format("Storage volume '%s' already exists", name));
            }
            return createInternalNoLock(name, svType, locations, params, enabled, comment);
        }
    }

    public void removeStorageVolume(DropStorageVolumeStmt stmt) throws DdlException, AnalysisException {
        removeStorageVolume(stmt.getName());
    }

    public void removeStorageVolume(String name) throws AnalysisException, DdlException {
        try (LockCloseable lock = new LockCloseable(rwLock.writeLock())) {
            StorageVolume sv = getStorageVolumeByName(name);
            Preconditions.checkState(sv != null,
                    "Storage volume '%s' does not exist", name);
            Preconditions.checkState(!defaultStorageVolumeId.equals(sv.getId()),
                    "default storage volume can not be removed");
            Set<Long> dbs = storageVolumeToDbs.get(sv.getId());
            Set<Long> tables = storageVolumeToTables.get(sv.getId());
            Preconditions.checkState(dbs == null && tables == null,
                    "Storage volume '%s' is referenced by dbs or tables, dbs: %s, tables: %s",
                    name, dbs != null ? dbs.toString() : "[]", tables != null ? tables.toString() : "[]");
            removeInternalNoLock(sv);
        }
    }

    public void updateStorageVolume(AlterStorageVolumeStmt stmt) throws AnalysisException, DdlException {
        Map<String, String> params = new HashMap<>();
        Optional<Boolean> enabled = parseProperties(stmt.getProperties(), params);
        updateStorageVolume(stmt.getName(), params, enabled, stmt.getComment());
    }

    public void updateStorageVolume(String name, Map<String, String> params, Optional<Boolean> enabled, String comment)
            throws AnalysisException, DdlException {
        try (LockCloseable lock = new LockCloseable(rwLock.writeLock())) {
            StorageVolume sv = getStorageVolumeByName(name);
            Preconditions.checkState(sv != null, "Storage volume '%s' does not exist", name);
            StorageVolume copied = new StorageVolume(sv);

            if (enabled.isPresent()) {
                boolean enabledValue = enabled.get();
                if (!enabledValue) {
                    Preconditions.checkState(!copied.getId().equals(defaultStorageVolumeId),
                            "Default volume can not be disabled");
                }
                copied.setEnabled(enabledValue);
            }

            if (!comment.isEmpty()) {
                copied.setComment(comment);
            }

            if (!params.isEmpty()) {
                copied.setCloudConfiguration(params);
            }

            updateInternalNoLock(copied);
        }
    }

    public void setDefaultStorageVolume(SetDefaultStorageVolumeStmt stmt) throws AnalysisException {
        setDefaultStorageVolume(stmt.getName());
    }

    public void setDefaultStorageVolume(String svKey) throws AnalysisException {
        try (LockCloseable lock = new LockCloseable(rwLock.writeLock())) {
            StorageVolume sv = getStorageVolumeByName(svKey);
            Preconditions.checkState(sv != null, "Storage volume '%s' does not exist", svKey);
            Preconditions.checkState(sv.getEnabled(), "Storage volume '%s' is disabled", svKey);
            this.defaultStorageVolumeId = sv.getId();
        }
    }

    public String getDefaultStorageVolumeId() {
        return defaultStorageVolumeId;
    }

    public boolean exists(String svKey) throws DdlException {
        try (LockCloseable lock = new LockCloseable(rwLock.readLock())) {
            try {
                StorageVolume sv = getStorageVolumeByName(svKey);
                return sv != null;
            } catch (AnalysisException e) {
                throw new DdlException(e.getMessage());
            }
        }
    }

    private Optional<Boolean> parseProperties(Map<String, String> properties, Map<String, String> params) {
        params.putAll(properties);
        Optional<Boolean> enabled = Optional.empty();
        if (params.containsKey(ENABLED)) {
            enabled = Optional.of(Boolean.parseBoolean(params.get(ENABLED)));
            params.remove(ENABLED);
        }
        return enabled;
    }

    public void bindDbToStorageVolume(String svId, long dbId) {
        try (LockCloseable lock = new LockCloseable(rwLock.writeLock())) {
            Set<Long> dbs = storageVolumeToDbs.getOrDefault(svId, new HashSet<>());
            dbs.add(dbId);
            storageVolumeToDbs.put(svId, dbs);
        }
    }

    public void unbindDbToStorageVolume(String svId, long dbId) {
        try (LockCloseable lock = new LockCloseable(rwLock.writeLock())) {
            Preconditions.checkState(storageVolumeToDbs.containsKey(svId), "Storage volume does not exist");
            Set<Long> dbs = storageVolumeToDbs.get(svId);
            dbs.remove(dbId);
            if (dbs.isEmpty()) {
                storageVolumeToDbs.remove(svId);
            }
        }
    }

    public void bindTableToStorageVolume(String svId, long tableId) {
        try (LockCloseable lock = new LockCloseable(rwLock.writeLock())) {
            Set<Long> tables = storageVolumeToTables.getOrDefault(svId, new HashSet<>());
            tables.add(tableId);
            storageVolumeToTables.put(svId, tables);
        }
    }

    public void unbindTableToStorageVolume(String svId, long tableId) {
        try (LockCloseable lock = new LockCloseable(rwLock.writeLock())) {
            Preconditions.checkState(storageVolumeToTables.containsKey(svId), "Storage volume does not exist");
            Set<Long> tables = storageVolumeToTables.get(svId);
            tables.remove(tableId);
            if (tables.isEmpty()) {
                storageVolumeToTables.remove(svId);
            }
        }
    }

    public abstract StorageVolume getStorageVolumeByName(String svKey) throws AnalysisException;

    public abstract StorageVolume getStorageVolume(String storageVolumeId) throws AnalysisException;

    public abstract List<String> listStorageVolumeNames() throws DdlException;

    protected abstract String createInternalNoLock(String name, String svType, List<String> locations,
                                                   Map<String, String> params, Optional<Boolean> enabled, String comment)
            throws AnalysisException, DdlException;

    protected abstract void updateInternalNoLock(StorageVolume sv) throws DdlException;

    protected abstract void removeInternalNoLock(StorageVolume sv) throws DdlException;
}
