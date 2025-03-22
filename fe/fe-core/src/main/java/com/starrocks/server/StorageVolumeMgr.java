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
import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import com.staros.util.LockCloseable;
import com.starrocks.common.AlreadyExistsException;
import com.starrocks.common.DdlException;
import com.starrocks.common.InvalidConfException;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.connector.share.credential.CloudConfigurationConstants;
import com.starrocks.persist.DropStorageVolumeLog;
import com.starrocks.persist.ImageWriter;
import com.starrocks.persist.SetDefaultStorageVolumeLog;
import com.starrocks.persist.TableStorageInfos;
import com.starrocks.persist.gson.GsonPostProcessable;
import com.starrocks.persist.gson.GsonUtils;
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

import java.io.DataInput;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Pattern;

public abstract class StorageVolumeMgr implements Writable, GsonPostProcessable {
    private static final String ENABLED = "enabled";

    public static final String DEFAULT = "default";

    public static final String LOCAL = "local";

    public static final String BUILTIN_STORAGE_VOLUME = "builtin_storage_volume";

    private static final String S3 = "s3";

    private static final String AZBLOB = "azblob";

    private static final String ADLS2 = "adls2";

    private static final String HDFS = "hdfs";

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
            validateParams(svType, params);
            validateLocations(svType, locations);
            if (exists(name)) {
                throw new AlreadyExistsException(String.format("Storage volume '%s' already exists", name));
            }
            return createInternalNoLock(name, svType, locations, params, enabled, comment);
        }
    }

    public void removeStorageVolume(DropStorageVolumeStmt stmt) throws DdlException, MetaNotFoundException {
        removeStorageVolume(stmt.getName());
    }

    public void removeStorageVolume(String name) throws DdlException, MetaNotFoundException {
        try (LockCloseable lock = new LockCloseable(rwLock.writeLock())) {
            StorageVolume sv = getStorageVolumeByName(name);
            if (sv == null) {
                throw new MetaNotFoundException(String.format("Storage volume '%s' does not exist", name));
            }
            Preconditions.checkState(!defaultStorageVolumeId.equals(sv.getId()),
                    "default storage volume can not be removed");
            Set<Long> dbs = storageVolumeToDbs.getOrDefault(sv.getId(), new HashSet<>());
            Set<Long> tables = storageVolumeToTables.getOrDefault(sv.getId(), new HashSet<>());
            if (name.equals(BUILTIN_STORAGE_VOLUME)) {
                List<List<Long>> bindings = getBindingsOfBuiltinStorageVolume();
                dbs.addAll(bindings.get(0));
                tables.addAll(bindings.get(1));
            }
            Preconditions.checkState(dbs.isEmpty() && tables.isEmpty(),
                    "Storage volume '%s' is referenced by dbs or tables, dbs: %s, tables: %s",
                    name, dbs.toString(), tables.toString());
            removeInternalNoLock(sv);
        }
    }

    public void updateStorageVolume(AlterStorageVolumeStmt stmt) throws DdlException {
        updateStorageVolume(stmt.getName(), null, null, stmt.getProperties(), stmt.getComment());
    }

    public void updateStorageVolume(String name, String svType, List<String> locations,
            Map<String, String> properties, String comment) throws DdlException {
        Map<String, String> params = new HashMap<>();
        Optional<Boolean> enabled = parseProperties(properties, params);
        updateStorageVolume(name, svType, locations, params, enabled, comment);
    }

    public void updateStorageVolume(String name, String svType, List<String> locations,
            Map<String, String> params, Optional<Boolean> enabled, String comment) throws DdlException {
        List<String> immutableProperties = Lists.newArrayList(CloudConfigurationConstants.AWS_S3_NUM_PARTITIONED_PREFIX,
                CloudConfigurationConstants.AWS_S3_ENABLE_PARTITIONED_PREFIX);
        for (String param : immutableProperties) {
            if (params.containsKey(param)) {
                throw new DdlException(String.format("Storage volume property '%s' is immutable!", param));
            }
        }
        try (LockCloseable lock = new LockCloseable(rwLock.writeLock())) {
            StorageVolume sv = getStorageVolumeByName(name);
            Preconditions.checkState(sv != null, "Storage volume '%s' does not exist", name);
            StorageVolume copied = new StorageVolume(sv);
            validateParams(copied.getType(), params);

            if (enabled.isPresent()) {
                boolean enabledValue = enabled.get();
                if (!enabledValue) {
                    Preconditions.checkState(!copied.getId().equals(defaultStorageVolumeId),
                            "Default volume can not be disabled");
                }
                copied.setEnabled(enabledValue);
            }

            if (!Strings.isNullOrEmpty(svType)) {
                copied.setType(svType);
            }

            if (locations != null) {
                copied.setLocations(locations);
            }

            if (!Strings.isNullOrEmpty(comment)) {
                copied.setComment(comment);
            }

            if (!params.isEmpty()) {
                copied.setCloudConfiguration(params);
            }

            updateInternalNoLock(copied);
        }
    }

    public void replaceStorageVolume(String name, String svType, List<String> locations,
            Map<String, String> properties, String comment) throws DdlException {
        Map<String, String> params = new HashMap<>();
        Optional<Boolean> enabled = parseProperties(properties, params);
        validateParams(svType, params);

        String locationChangedStorageVolumeId = null;
        try (LockCloseable lock = new LockCloseable(rwLock.writeLock())) {
            StorageVolume oldStorageVolume = getStorageVolumeByName(name);
            Preconditions.checkState(oldStorageVolume != null, "Storage volume '%s' does not exist", name);
            if (enabled.isPresent()) {
                if (!enabled.get()) {
                    Preconditions.checkState(!oldStorageVolume.getId().equals(defaultStorageVolumeId),
                            "Default volume can not be disabled");
                }
            }

            StorageVolume newStorageVolume = null;
            if (oldStorageVolume.getType().equalsIgnoreCase(svType)) {
                newStorageVolume = new StorageVolume(oldStorageVolume);
                if (enabled.isPresent()) {
                    newStorageVolume.setEnabled(enabled.get());
                }

                if (!Strings.isNullOrEmpty(svType)) {
                    newStorageVolume.setType(svType);
                }

                if (locations != null) {
                    if (!oldStorageVolume.getLocations().equals(locations)) {
                        validateLocations(svType, locations);
                        newStorageVolume.setLocations(locations);
                        locationChangedStorageVolumeId = newStorageVolume.getId();
                    }
                }

                if (!Strings.isNullOrEmpty(comment)) {
                    newStorageVolume.setComment(comment);
                }

                if (!params.isEmpty()) {
                    newStorageVolume.setCloudConfiguration(params);
                }
            } else {
                Preconditions.checkState(locations != null, "Location is null");
                validateLocations(svType, locations);
                newStorageVolume = new StorageVolume(oldStorageVolume.getId(), name, svType, locations, params,
                        enabled.orElse(oldStorageVolume.getEnabled()), comment);
                locationChangedStorageVolumeId = newStorageVolume.getId();
            }

            replaceInternalNoLock(newStorageVolume);
        }

        if (locationChangedStorageVolumeId != null) {
            updateTableStorageInfo(locationChangedStorageVolumeId);
        }
    }

    public void setDefaultStorageVolume(SetDefaultStorageVolumeStmt stmt) {
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

    public String getStorageVolumeNameOfTable(long tableId) {
        try (LockCloseable lock = new LockCloseable(rwLock.readLock())) {
            String svId = getStorageVolumeIdOfTable(tableId);
            // If sv id is null, the table is upgraded from old version.
            // Builtin storage volume will be returned.
            return svId != null ? getStorageVolume(svId).getName() : BUILTIN_STORAGE_VOLUME;
        }
    }

    public String getStorageVolumeNameOfDb(long dbId) {
        try (LockCloseable lock = new LockCloseable(rwLock.readLock())) {
            String svId = getStorageVolumeIdOfDb(dbId);
            // If sv id is null, the db is upgraded from old version.
            // Builtin storage volume will be returned.
            return svId != null ? getStorageVolume(svId).getName() : BUILTIN_STORAGE_VOLUME;
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

    public void replayUpdateTableStorageInfos(TableStorageInfos tableStorageInfos) {
        throw new RuntimeException("Not implemented");
    }

    protected void validateParams(String svType, Map<String, String> params) throws DdlException {
        if (svType.equalsIgnoreCase(HDFS)) {
            return;
        }
        for (String key : params.keySet()) {
            if (!PARAM_NAMES.contains(key)) {
                throw new DdlException("Invalid properties " + key);
            }
        }

        // storage volume type specific checks
        if (!svType.equalsIgnoreCase(S3)) {
            // The following two properties can be only set when storage volume type is 'S3'
            List<String> s3Params = Lists.newArrayList(
                    CloudConfigurationConstants.AWS_S3_NUM_PARTITIONED_PREFIX,
                    CloudConfigurationConstants.AWS_S3_ENABLE_PARTITIONED_PREFIX);
            for (String param : s3Params) {
                if (params.containsKey(param)) {
                    throw new DdlException(
                            String.format("Invalid property '%s' for storage volume type '%s'", param, svType));
                }
            }
        }
        if (params.containsKey(CloudConfigurationConstants.AWS_S3_NUM_PARTITIONED_PREFIX)) {
            try {
                int value = Integer.parseInt(params.get(CloudConfigurationConstants.AWS_S3_NUM_PARTITIONED_PREFIX));
                if (value < 0) {
                    throw new DdlException(String.format(
                            "Invalid property value '%s' for property '%s', expecting a positive integer string.",
                            params.get(CloudConfigurationConstants.AWS_S3_NUM_PARTITIONED_PREFIX),
                            CloudConfigurationConstants.AWS_S3_NUM_PARTITIONED_PREFIX));
                }
            } catch (NumberFormatException e) {
                throw new DdlException(String.format(
                        "Invalid property value '%s' for property '%s', expecting a valid integer string.",
                        params.get(CloudConfigurationConstants.AWS_S3_NUM_PARTITIONED_PREFIX),
                        CloudConfigurationConstants.AWS_S3_NUM_PARTITIONED_PREFIX));
            }
        }
    }

    private void validateLocations(String svType, List<String> locations) throws DdlException {
        for (String location : locations) {
            try {
                URI uri = new URI(location);
                String scheme = uri.getScheme().toLowerCase();
                switch (svType.toLowerCase()) {
                    case S3:
                    case AZBLOB:
                    case ADLS2:
                        if (!scheme.equalsIgnoreCase(svType)) {
                            throw new DdlException("Invalid location " + location);
                        }
                        break;
                    case HDFS:
                        String pattern = "[a-z][a-z0-9]*";
                        if (!Pattern.matches(pattern, scheme)) {
                            throw new DdlException("Invalid location " + location);
                        }
                        break;
                    default:
                        throw new DdlException("Unknown storage volume type: " + svType);
                }
            } catch (URISyntaxException e) {
                throw new DdlException(String.format("Invalid location %s, error: %s", location, e.getMessage()));
            }
        }
    }

    public void save(ImageWriter imageWriter) throws IOException, SRMetaBlockException {
        SRMetaBlockWriter writer = imageWriter.getBlockWriter(SRMetaBlockID.STORAGE_VOLUME_MGR, 1);
        writer.writeJson(this);
        writer.close();
    }

    public void load(SRMetaBlockReader reader)
            throws SRMetaBlockEOFException, IOException, SRMetaBlockException {
        StorageVolumeMgr data = reader.readJson(StorageVolumeMgr.class);
        this.storageVolumeToDbs = data.storageVolumeToDbs;
        this.storageVolumeToTables = data.storageVolumeToTables;
        this.defaultStorageVolumeId = data.defaultStorageVolumeId;
        this.dbToStorageVolume = data.dbToStorageVolume;
        this.tableToStorageVolume = data.tableToStorageVolume;
    }

    @Override
    public void gsonPostProcess() throws IOException {
        // If user upgrades from 3.0 and the createTableInfo and createDbInfo is replayed,
        // the image will look like: "svToDbs":{"null":[12288,81921,49154,65541,20485]}, "svToTables":{"null":[12288]}
        // The mapping null to dbs and tables should be removed. These code can be removed when 3.0 is not supported.
        storageVolumeToDbs.remove("null");
        for (Map.Entry<String, Set<Long>> entry : storageVolumeToDbs.entrySet()) {
            for (Long dbId : entry.getValue()) {
                dbToStorageVolume.put(dbId, entry.getKey());
            }
        }
        storageVolumeToTables.remove("null");
        for (Map.Entry<String, Set<Long>> entry : storageVolumeToTables.entrySet()) {
            for (Long tableId : entry.getValue()) {
                tableToStorageVolume.put(tableId, entry.getKey());
            }
        }
    }

    public void load(DataInput in) throws IOException {
        String json = Text.readString(in);
        StorageVolumeMgr data = GsonUtils.GSON.fromJson(json, StorageVolumeMgr.class);
        this.storageVolumeToDbs = data.storageVolumeToDbs;
        this.storageVolumeToTables = data.storageVolumeToTables;
        this.defaultStorageVolumeId = data.defaultStorageVolumeId;
        this.dbToStorageVolume = data.dbToStorageVolume;
        this.tableToStorageVolume = data.tableToStorageVolume;
    }

    public abstract StorageVolume getStorageVolumeByName(String svName);

    public abstract StorageVolume getStorageVolume(String svId);

    public abstract List<String> listStorageVolumeNames() throws DdlException;

    protected abstract String createInternalNoLock(String name, String svType, List<String> locations,
            Map<String, String> params, Optional<Boolean> enabled, String comment)
            throws DdlException;

    protected abstract void updateInternalNoLock(StorageVolume sv) throws DdlException;

    protected abstract void replaceInternalNoLock(StorageVolume sv) throws DdlException;

    protected abstract void removeInternalNoLock(StorageVolume sv) throws DdlException;

    public abstract boolean bindDbToStorageVolume(String svName, long dbId) throws DdlException;

    public abstract void replayBindDbToStorageVolume(String svId, long dbId);

    public abstract void unbindDbToStorageVolume(long dbId);

    public abstract boolean bindTableToStorageVolume(String svName, long dbId, long tableId) throws DdlException;

    public abstract void replayBindTableToStorageVolume(String svId, long tableId);

    public abstract void unbindTableToStorageVolume(long tableId);

    public abstract String createBuiltinStorageVolume() throws DdlException, AlreadyExistsException;

    public abstract void validateStorageVolumeConfig() throws InvalidConfException;

    protected abstract List<List<Long>> getBindingsOfBuiltinStorageVolume();

    protected abstract void updateTableStorageInfo(String storageVolumeId) throws DdlException;
}
