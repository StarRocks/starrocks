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
import com.starrocks.common.InvalidConfException;
import com.starrocks.credential.CloudConfigurationConstants;
import com.starrocks.persist.DropStorageVolumeLog;
import com.starrocks.persist.SetDefaultStorageVolumeLog;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockID;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockWriter;
import com.starrocks.sql.ast.AlterStorageVolumeStmt;
import com.starrocks.sql.ast.CreateStorageVolumeStmt;
import com.starrocks.sql.ast.DropStorageVolumeStmt;
import com.starrocks.sql.ast.SetDefaultStorageVolumeStmt;
import com.starrocks.storagevolume.StorageVolume;

import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
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

    public static final String DEFAULT = "default";

    public static final String LOCAL = "local";

    public static final String BUILTIN_STORAGE_VOLUME = "builtin_storage_volume";

    @SerializedName("defaultSVId")
    protected String defaultStorageVolumeId = "";

    protected final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    // volume id to dbs
    @SerializedName("svToDbs")
    protected Map<String, Set<Long>> storageVolumeToDbs = new HashMap<>();

    // volume id to tables
    @SerializedName("svToTables")
    protected Map<String, Set<Long>> storageVolumeToTables = new HashMap<>();

    protected Map<Long, String> dbToStorageVolume = new HashMap<>();

    protected Map<Long, String> tableToStorageVolume = new HashMap<>();

    protected static final Set<String> PARAM_NAMES = new HashSet<>();

    static {
        Field[] fields = CloudConfigurationConstants.class.getFields();
        for (int i = 0; i < fields.length; ++i) {
            try {
                Object obj = CloudConfigurationConstants.class.newInstance();
                Object value = fields[i].get(obj);
                PARAM_NAMES.add((String) value);
            } catch (InstantiationException | IllegalAccessException e) {
                // do nothing
            }
        }
    }

    public String createStorageVolume(CreateStorageVolumeStmt stmt)
            throws AlreadyExistsException, DdlException {
        Map<String, String> params = new HashMap<>();
        Optional<Boolean> enabled = parseProperties(stmt.getProperties(), params);
        return createStorageVolume(stmt.getName(), stmt.getStorageVolumeType(), stmt.getStorageLocations(), params,
                enabled, stmt.getComment());
    }

    public String createStorageVolume(String name, String svType, List<String> locations, Map<String, String> params,
                                      Optional<Boolean> enabled, String comment)
            throws DdlException, AlreadyExistsException {
        try (LockCloseable lock = new LockCloseable(rwLock.writeLock())) {
            validateParams(params);
            if (exists(name)) {
                throw new AlreadyExistsException(String.format("Storage volume '%s' already exists", name));
            }
            return createInternalNoLock(name, svType, locations, params, enabled, comment);
        }
    }

    public void removeStorageVolume(DropStorageVolumeStmt stmt) throws DdlException, AnalysisException {
        removeStorageVolume(stmt.getName());
    }

    public void removeStorageVolume(String name) throws DdlException {
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

    public void updateStorageVolume(AlterStorageVolumeStmt stmt) throws DdlException {
        Map<String, String> params = new HashMap<>();
        Optional<Boolean> enabled = parseProperties(stmt.getProperties(), params);
        updateStorageVolume(stmt.getName(), params, enabled, stmt.getComment());
    }

    public void updateStorageVolume(String name, Map<String, String> params, Optional<Boolean> enabled, String comment)
            throws DdlException {
        try (LockCloseable lock = new LockCloseable(rwLock.writeLock())) {
            validateParams(params);
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

    public void setDefaultStorageVolume(String svName) {
        try (LockCloseable lock = new LockCloseable(rwLock.writeLock())) {
            StorageVolume sv = getStorageVolumeByName(svName);
            Preconditions.checkState(sv != null, "Storage volume '%s' does not exist", svName);
            Preconditions.checkState(sv.getEnabled(), "Storage volume '%s' is disabled", svName);
            SetDefaultStorageVolumeLog log = new SetDefaultStorageVolumeLog(sv.getId());
            GlobalStateMgr.getCurrentState().getEditLog().logSetDefaultStorageVolume(log);
            this.defaultStorageVolumeId = sv.getId();
        }
    }

    public String getDefaultStorageVolumeId() {
        return defaultStorageVolumeId;
    }

    public boolean exists(String svName) throws DdlException {
        try (LockCloseable lock = new LockCloseable(rwLock.readLock())) {
            StorageVolume sv = getStorageVolumeByName(svName);
            return sv != null;
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

    public String getStorageVolumeIdOfTable(long tableId) {
        try (LockCloseable lock = new LockCloseable(rwLock.readLock())) {
            return tableToStorageVolume.get(tableId);
        }
    }

    public String getStorageVolumeIdOfDb(long dbId) {
        try (LockCloseable lock = new LockCloseable(rwLock.readLock())) {
            return dbToStorageVolume.get(dbId);
        }
    }

    public StorageVolume getDefaultStorageVolume() {
        try (LockCloseable lock = new LockCloseable(rwLock.readLock())) {
            return getStorageVolume(getDefaultStorageVolumeId());
        }
    }

    public String getStorageVolumeName(String svId) {
        try (LockCloseable lock = new LockCloseable(rwLock.readLock())) {
            StorageVolume sv = getStorageVolume(svId);
            if (sv == null) {
                return "";
            }
            return getStorageVolume(svId).getName();
        }
    }

    public void replaySetDefaultStorageVolume(SetDefaultStorageVolumeLog log) {
        try (LockCloseable lock = new LockCloseable(rwLock.writeLock())) {
            defaultStorageVolumeId = log.getId();
        }
    }

    public void replayCreateStorageVolume(StorageVolume sv) {
    }

    public void replayUpdateStorageVolume(StorageVolume sv) {
    }

    public void replayDropStorageVolume(DropStorageVolumeLog log) {
    }

    protected void validateParams(Map<String, String> params) throws DdlException {
        for (String key : params.keySet()) {
            if (!PARAM_NAMES.contains(key)) {
                throw new DdlException("Invalid properties " + key);
            }
        }
    }

    public void save(DataOutputStream dos) throws IOException, SRMetaBlockException {
        SRMetaBlockWriter writer = new SRMetaBlockWriter(dos, SRMetaBlockID.STORAGE_VOLUME_MGR, 1);
        writer.writeJson(this);
        writer.close();
    }

    public void load(SRMetaBlockReader reader)
            throws SRMetaBlockEOFException, IOException, SRMetaBlockException {
        StorageVolumeMgr data = reader.readJson(StorageVolumeMgr.class);
        this.storageVolumeToDbs = data.storageVolumeToDbs;
        this.storageVolumeToTables = data.storageVolumeToTables;
        this.defaultStorageVolumeId = data.defaultStorageVolumeId;

        for (Map.Entry<String, Set<Long>> entry : storageVolumeToDbs.entrySet()) {
            for (Long dbId : entry.getValue()) {
                dbToStorageVolume.put(dbId, entry.getKey());
            }
        }
        for (Map.Entry<String, Set<Long>> entry : storageVolumeToTables.entrySet()) {
            for (Long tableId : entry.getValue()) {
                tableToStorageVolume.put(tableId, entry.getKey());
            }
        }
    }

    public abstract StorageVolume getStorageVolumeByName(String svName);

    public abstract StorageVolume getStorageVolume(String svId);

    public abstract List<String> listStorageVolumeNames() throws DdlException;

    protected abstract String createInternalNoLock(String name, String svType, List<String> locations,
                                                   Map<String, String> params, Optional<Boolean> enabled, String comment)
            throws DdlException;

    protected abstract void updateInternalNoLock(StorageVolume sv) throws DdlException;

    protected abstract void removeInternalNoLock(StorageVolume sv) throws DdlException;

    public abstract boolean bindDbToStorageVolume(String svName, long dbId) throws DdlException;

    public abstract void replayBindDbToStorageVolume(String svId, long dbId);

    public abstract void unbindDbToStorageVolume(long dbId);

    public abstract boolean bindTableToStorageVolume(String svName, long dbId, long tableId) throws DdlException;

    public abstract void replayBindTableToStorageVolume(String svId, long tableId);

    public abstract void unbindTableToStorageVolume(long tableId);

    public abstract String createBuiltinStorageVolume() throws DdlException, AlreadyExistsException;

    public abstract void validateStorageVolumeConfig() throws InvalidConfException;
}
